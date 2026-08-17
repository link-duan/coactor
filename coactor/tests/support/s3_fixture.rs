#![allow(dead_code)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Method, Request, Response, StatusCode},
    routing::any,
};

#[derive(Clone)]
struct StoredObject {
    body: Vec<u8>,
    etag: String,
}

#[derive(Clone)]
pub(super) struct ProgrammableS3 {
    bucket_path: Arc<str>,
    objects: Arc<Mutex<HashMap<String, StoredObject>>>,
    next_etag: Arc<Mutex<u64>>,
    dropped_put_responses: Arc<Mutex<HashMap<String, usize>>>,
    put_counts: Arc<Mutex<HashMap<String, usize>>>,
    list_count: Arc<Mutex<usize>>,
}

impl ProgrammableS3 {
    pub(super) async fn start(bucket: &str) -> (String, Self, tokio::task::JoinHandle<()>) {
        let state = Self {
            bucket_path: format!("/{bucket}/").into(),
            objects: Arc::default(),
            next_etag: Arc::default(),
            dropped_put_responses: Arc::default(),
            put_counts: Arc::default(),
            list_count: Arc::default(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn({
            let state = state.clone();
            async move {
                axum::serve(
                    listener,
                    Router::new().fallback(any(handle)).with_state(state),
                )
                .await
                .unwrap()
            }
        });
        (format!("http://{address}"), state, task)
    }

    pub(super) fn drop_next_put_response(&self, key: &str) {
        *self
            .dropped_put_responses
            .lock()
            .unwrap()
            .entry(key.to_owned())
            .or_default() += 1;
    }

    pub(super) fn put_count(&self, key: &str) -> usize {
        self.put_counts
            .lock()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    pub(super) fn list_count(&self) -> usize {
        *self.list_count.lock().unwrap()
    }

    pub(super) fn object_body(&self, key: &str) -> Option<Vec<u8>> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .map(|object| object.body.clone())
    }

    pub(super) fn find_object(
        &self,
        mut predicate: impl FnMut(&str, &[u8]) -> bool,
    ) -> Option<(String, Vec<u8>)> {
        self.objects
            .lock()
            .unwrap()
            .iter()
            .find_map(|(key, object)| {
                predicate(key, &object.body).then(|| (key.clone(), object.body.clone()))
            })
    }

    pub(super) fn replace_object_body(&self, key: &str, body: Vec<u8>) {
        self.objects.lock().unwrap().get_mut(key).unwrap().body = body;
    }

    fn next_etag(&self) -> String {
        let mut next = self.next_etag.lock().unwrap();
        *next += 1;
        format!("\"stateful-{next}\"")
    }

    fn take_dropped_put_response(&self, key: &str) -> bool {
        let mut responses = self.dropped_put_responses.lock().unwrap();
        let Some(remaining) = responses.get_mut(key) else {
            return false;
        };
        *remaining -= 1;
        if *remaining == 0 {
            responses.remove(key);
        }
        true
    }

    fn record_put(&self, key: &str) {
        *self
            .put_counts
            .lock()
            .unwrap()
            .entry(key.to_owned())
            .or_default() += 1;
    }
}

async fn handle(State(state): State<ProgrammableS3>, request: Request<Body>) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path();
    let key = path
        .strip_prefix(state.bucket_path.as_ref())
        .unwrap_or_default()
        .to_owned();

    if parts.method == Method::GET && path == state.bucket_path.as_ref() {
        *state.list_count.lock().unwrap() += 1;
        let prefix = parts
            .uri
            .query()
            .and_then(|query| {
                query.split('&').find_map(|part| {
                    part.strip_prefix("prefix=")
                        .map(|value| value.replace("%2F", "/"))
                })
            })
            .unwrap_or_default();
        let objects = state.objects.lock().unwrap();
        let contents = objects
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .map(|key| format!("<Contents><Key>{key}</Key></Contents>"))
            .collect::<String>();
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(format!(
                "<ListBucketResult><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
            )))
            .unwrap();
    }

    match parts.method {
        Method::GET => match state.objects.lock().unwrap().get(&key).cloned() {
            Some(object) => Response::builder()
                .status(StatusCode::OK)
                .header("etag", object.etag)
                .body(Body::from(object.body))
                .unwrap(),
            None => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("<Error><Code>NoSuchKey</Code></Error>"))
                .unwrap(),
        },
        Method::PUT => {
            state.record_put(&key);
            let body = to_bytes(body, usize::MAX).await.unwrap().to_vec();
            let mut objects = state.objects.lock().unwrap();
            let conditional_rejected = parts
                .headers
                .get("if-none-match")
                .is_some_and(|value| value == "*" && objects.contains_key(&key))
                || parts.headers.get("if-match").is_some_and(|expected| {
                    objects
                        .get(&key)
                        .is_none_or(|current| current.etag.as_bytes() != expected.as_bytes())
                });
            if conditional_rejected {
                return Response::builder()
                    .status(StatusCode::PRECONDITION_FAILED)
                    .body(Body::from("<Error><Code>PreconditionFailed</Code></Error>"))
                    .unwrap();
            }
            let etag = state.next_etag();
            objects.insert(
                key.clone(),
                StoredObject {
                    body,
                    etag: etag.clone(),
                },
            );
            drop(objects);
            if state.take_dropped_put_response(&key) {
                panic!("programmable S3 endpoint dropped an applied PUT response");
            }
            Response::builder()
                .status(StatusCode::OK)
                .header("etag", etag)
                .body(Body::empty())
                .unwrap()
        }
        Method::DELETE => {
            let mut objects = state.objects.lock().unwrap();
            let conditional_rejected = parts.headers.get("if-match").is_some_and(|expected| {
                objects
                    .get(&key)
                    .is_none_or(|current| current.etag.as_bytes() != expected.as_bytes())
            });
            if conditional_rejected {
                return Response::builder()
                    .status(StatusCode::PRECONDITION_FAILED)
                    .body(Body::from("<Error><Code>PreconditionFailed</Code></Error>"))
                    .unwrap();
            }
            objects.remove(&key);
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        _ => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .unwrap(),
    }
}

pub(super) fn unused_loopback_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}
