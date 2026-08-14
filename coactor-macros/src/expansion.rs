use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{ImplItem, ImplItemFn, ItemImpl, Type, parse_macro_input};

use crate::syntax::*;

pub(crate) fn expand_actor(attribute: TokenStream, item: TokenStream) -> TokenStream {
    if attribute.is_empty() {
        return compile_error(
            "#[actor] requires an explicit stable name: #[actor(name = \"...\")]",
        );
    }
    let actor_name = parse_macro_input!(attribute as ActorAttribute).name;
    let mut actor_impl = parse_macro_input!(item as ItemImpl);

    let actor_type = actor_impl.self_ty.as_ref().clone();
    let actor_ident = match &actor_type {
        Type::Path(path) => path.path.segments.last().unwrap().ident.clone(),
        _ => return compile_error("actor can only be applied to an inherent impl"),
    };
    let ref_ident = format_ident!("{}Ref", actor_ident);
    let constructor = actor_impl.items.iter().find_map(|item| match item {
        ImplItem::Fn(method) if method.sig.ident == "new" => Some(method.clone()),
        _ => None,
    });

    let commands: Vec<(ImplItemFn, bool)> = actor_impl
        .items
        .iter_mut()
        .filter_map(|item| match item {
            ImplItem::Fn(method) => {
                take_command_attribute(&mut method.attrs).map(|remote| (method.clone(), remote))
            }
            _ => None,
        })
        .collect();
    let activation = actor_impl.items.iter().find_map(|item| match item {
        ImplItem::Fn(method) if method.sig.ident == "on_activate" => Some(method.clone()),
        _ => None,
    });
    let deactivation = actor_impl.items.iter().find_map(|item| match item {
        ImplItem::Fn(method) if method.sig.ident == "on_deactivate" => Some(method.clone()),
        _ => None,
    });

    if commands.is_empty() {
        return compile_error("an actor must declare at least one #[command]");
    }
    let Some(constructor) = constructor else {
        return compile_error(
            "an actor must declare pub fn new(actor_id: ActorId, app_state: Arc<AppState>) -> Self",
        );
    };
    if let Err(message) = validate_constructor(&constructor) {
        return compile_error(message);
    }
    let state_type = match constructor_state_type(&constructor) {
        Ok(state) => state,
        Err(message) => return compile_error(message),
    };
    if let Some(activation) = &activation {
        if let Err(message) = validate_activation(activation) {
            return compile_error(message);
        }
    }
    if let Some(deactivation) = &deactivation {
        if let Err(message) = validate_deactivation(deactivation) {
            return compile_error(message);
        }
    }

    let mut generated_messages = Vec::new();
    let mut generated_ref_methods = Vec::new();
    let mut generated_remote_factories = Vec::new();

    for (command, remote) in &commands {
        if let Err(message) = validate_command(command, *remote) {
            return compile_error(message);
        }

        let method_ident = &command.sig.ident;
        let message_ident = format_ident!("__{}{}Command", actor_ident, pascal_case(method_ident));
        let arguments = command_arguments(command);
        let argument_names: Vec<_> = arguments.iter().map(|(name, _)| name).collect();
        let argument_types: Vec<_> = arguments.iter().map(|(_, ty)| ty).collect();
        let (reply_type, error_type, handler_returns_result) = return_types(&command.sig.output);

        let send_success = if *remote {
            quote! {
                if let ::core::option::Option::Some(reply) = reply {
                    let _ = reply.send(::core::result::Result::Ok(result));
                } else if let ::core::option::Option::Some(remote_reply) = remote_reply {
                    let encoded = ::coactor::__macro::prost::Message::encode_to_vec(&result);
                    let _ = remote_reply.send(::core::result::Result::Ok(encoded));
                }
            }
        } else {
            quote! {
                if let ::core::option::Option::Some(reply) = reply {
                    let _ = reply.send(::core::result::Result::Ok(result));
                }
            }
        };
        let send_handler_result = if *remote {
            quote! {
                if let ::core::option::Option::Some(reply) = reply {
                    let result = result.map_err(::coactor::SendError::HandlerError);
                    let _ = reply.send(result);
                } else if let ::core::option::Option::Some(remote_reply) = remote_reply {
                    match result {
                        ::core::result::Result::Ok(result) => {
                            let encoded = ::coactor::__macro::prost::Message::encode_to_vec(&result);
                            let _ = remote_reply.send(::core::result::Result::Ok(encoded));
                        }
                        ::core::result::Result::Err(error) => {
                            let encoded = ::coactor::__macro::prost::Message::encode_to_vec(&error);
                            let _ = remote_reply.send(::core::result::Result::Err(
                                ::coactor::__macro::RemoteReplyError::Handler(encoded),
                            ));
                        }
                    }
                }
            }
        } else {
            quote! {
                let result = result.map_err(::coactor::SendError::HandlerError);
                if let ::core::option::Option::Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }
        };
        let panic_reply = if *remote {
            quote! {
                if let ::core::option::Option::Some(reply) = reply {
                    let _ = reply.send(::core::result::Result::Err(
                        ::coactor::SendError::ActorStopped,
                    ));
                } else if let ::core::option::Option::Some(remote_reply) = remote_reply {
                    let _ = remote_reply.send(::core::result::Result::Err(
                        ::coactor::__macro::RemoteReplyError::Runtime(
                            ::coactor::__macro::RuntimeError::ActorStopped,
                        ),
                    ));
                }
            }
        } else {
            quote! {
                if let ::core::option::Option::Some(reply) = reply {
                    let _ = reply.send(::core::result::Result::Err(
                        ::coactor::SendError::ActorStopped,
                    ));
                }
            }
        };
        let invoke = if handler_returns_result {
            quote! {
                let #message_ident { #(#argument_names,)* reply, remote_reply } = *self;
                let outcome = ::coactor::__macro::FutureExt::catch_unwind(
                    ::std::panic::AssertUnwindSafe(
                        actor.#method_ident(&context, #(#argument_names),*)
                    )
                ).await;
                match outcome {
                    ::core::result::Result::Ok(result) => {
                        #send_handler_result
                        ::coactor::__macro::CommandOutcome::Completed
                    }
                    ::core::result::Result::Err(_) => {
                        ::coactor::__macro::CommandOutcome::Panicked(
                            ::std::boxed::Box::new(move || {
                                #panic_reply
                            })
                        )
                    }
                }
            }
        } else {
            quote! {
                let #message_ident { #(#argument_names,)* reply, remote_reply } = *self;
                let outcome = ::coactor::__macro::FutureExt::catch_unwind(
                    ::std::panic::AssertUnwindSafe(
                        actor.#method_ident(&context, #(#argument_names),*)
                    )
                ).await;
                match outcome {
                    ::core::result::Result::Ok(result) => {
                        #send_success
                        ::coactor::__macro::CommandOutcome::Completed
                    }
                    ::core::result::Result::Err(_) => {
                        ::coactor::__macro::CommandOutcome::Panicked(
                            ::std::boxed::Box::new(move || {
                                #panic_reply
                            })
                        )
                    }
                }
            }
        };

        let reply_fields = quote! {
            reply: ::core::option::Option<::coactor::__macro::tokio::sync::oneshot::Sender<
                ::core::result::Result<#reply_type, ::coactor::SendError<#error_type>>
            >>,
            remote_reply: ::core::option::Option<::coactor::__macro::tokio::sync::oneshot::Sender<
                ::core::result::Result<
                    ::std::vec::Vec<u8>,
                    ::coactor::__macro::RemoteReplyError,
                >
            >>,
        };

        generated_messages.push(quote! {
            struct #message_ident {
                #(#argument_names: #argument_types,)*
                #reply_fields
            }

            impl ::coactor::__macro::ErasedCommand<#state_type> for #message_ident {
                fn execute<'a>(
                    self: ::std::boxed::Box<Self>,
                    actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                    context: ::coactor::CommandContext,
                ) -> ::coactor::__macro::BoxFuture<'a, ::coactor::__macro::CommandOutcome> {
                    ::std::boxed::Box::pin(async move {
                        let actor = actor
                            .downcast_mut::<#actor_type>()
                            .expect("CoActor registered an incompatible Actor Type");
                        #invoke
                    })
                }

                fn fail(
                    self: ::std::boxed::Box<Self>,
                    error: ::coactor::__macro::RuntimeError,
                ) {
                    if let ::core::option::Option::Some(reply) = self.reply {
                        let _ = reply.send(::core::result::Result::Err(error.into()));
                    }
                    if let ::core::option::Option::Some(reply) = self.remote_reply {
                        let _ = reply.send(::core::result::Result::Err(
                            ::coactor::__macro::RemoteReplyError::Runtime(error),
                        ));
                    }
                }
            }
        });

        let remote_handler_error = if handler_returns_result {
            quote! {
                let error = ::coactor::__macro::prost::Message::decode(bytes.as_slice())
                    .map_err(|_| ::coactor::SendError::RemoteProtocol(
                        ::coactor::RemoteProtocolError::MalformedHandlerError,
                    ))?;
                ::core::result::Result::Err(::coactor::SendError::HandlerError(error))
            }
        } else {
            quote! {
                let _ = bytes;
                ::core::result::Result::Err(::coactor::SendError::RemoteProtocol(
                    ::coactor::RemoteProtocolError::UnexpectedHandlerError,
                ))
            }
        };
        let ref_method = if *remote {
            let request_type = argument_types.first().expect("remote command request");
            let request_name = argument_names.first().expect("remote command request");
            quote! {
                pub async fn #method_ident(
                    &self,
                    #request_name: #request_type,
                ) -> ::core::result::Result<#reply_type, ::coactor::SendError<#error_type>> {
                    let payload = ::coactor::__macro::prost::Message::encode_to_vec(&#request_name);
                    let (reply, receive) = ::coactor::__macro::tokio::sync::oneshot::channel();
                    match self.inner.dispatch(
                        ::core::option::Option::Some(::coactor::__macro::RemoteCall {
                            command: stringify!(#method_ident),
                            payload,
                        }),
                        move || ::std::boxed::Box::new(#message_ident {
                            #request_name,
                            reply: ::core::option::Option::Some(reply),
                            remote_reply: ::core::option::Option::None,
                        }),
                    ).await {
                        ::core::result::Result::Ok(::coactor::__macro::DispatchOutcome::Remote(remote)) => return match remote {
                            ::coactor::__macro::RemotePayload::Success(bytes) => {
                                ::coactor::__macro::prost::Message::decode(bytes.as_slice())
                                    .map_err(|_| ::coactor::SendError::RemoteProtocol(
                                        ::coactor::RemoteProtocolError::MalformedSuccess,
                                    ))
                            }
                            ::coactor::__macro::RemotePayload::HandlerError(bytes) => {
                                #remote_handler_error
                            }
                        },
                        ::core::result::Result::Ok(::coactor::__macro::DispatchOutcome::Local) => {}
                        ::core::result::Result::Err(error) => return ::core::result::Result::Err(error.into()),
                    }
                    let result = receive.await.unwrap_or_else(|_| ::core::result::Result::Err(
                        self.inner.reply_channel_closed_error(),
                    ));
                    self.inner.ensure_reply_authority()?;
                    result
                }
            }
        } else {
            quote! {
            pub async fn #method_ident(
                &self,
                #(#argument_names: #argument_types),*
            ) -> ::core::result::Result<#reply_type, ::coactor::SendError<#error_type>> {
                fn __coactor_assert_send_static<T: ::core::marker::Send + 'static>() {}
                fn __coactor_assert_error<T: ::core::fmt::Debug + ::core::marker::Send + 'static>() {}
                __coactor_assert_send_static::<(#(#argument_types,)*)>();
                __coactor_assert_send_static::<#reply_type>();
                __coactor_assert_error::<#error_type>();
                let (reply, receive) = ::coactor::__macro::tokio::sync::oneshot::channel();
                self.inner
                    .dispatch(
                        ::core::option::Option::None,
                        move || ::std::boxed::Box::new(#message_ident {
                            #(#argument_names,)*
                            reply: ::core::option::Option::Some(reply),
                            remote_reply: ::core::option::Option::None,
                        }),
                    )
                    .await
                    .map_err(::coactor::SendError::from)?;
                let result = receive.await.unwrap_or_else(|_| ::core::result::Result::Err(
                    self.inner.reply_channel_closed_error(),
                ));
                self.inner.ensure_reply_authority()?;
                result
            }
            }
        };
        generated_ref_methods.push(ref_method);

        if *remote {
            let request_type = argument_types.first().expect("remote command request");
            let request_name = argument_names.first().expect("remote command request");
            generated_remote_factories.push(quote! {
                commands.insert(
                    stringify!(#method_ident),
                    (|payload: ::std::vec::Vec<u8>| {
                    let #request_name: #request_type = ::coactor::__macro::prost::Message::decode(payload.as_slice())
                        .map_err(|_| ::coactor::__macro::RuntimeError::MalformedPayload)?;
                    let (remote_reply, receive) = ::coactor::__macro::tokio::sync::oneshot::channel();
                    Ok(::coactor::__macro::RemoteInvocation {
                        command: ::std::boxed::Box::new(#message_ident {
                            #request_name,
                            reply: ::core::option::Option::None,
                            remote_reply: ::core::option::Option::Some(remote_reply),
                        }),
                        reply: ::std::boxed::Box::pin(async move {
                            receive.await.unwrap_or(::core::result::Result::Err(
                                ::coactor::__macro::RemoteReplyError::Runtime(
                                    ::coactor::__macro::RuntimeError::ActorStopped,
                                ),
                            ))
                        }),
                    })
                    }) as ::coactor::__macro::RemoteCommandFactory<#state_type>,
                );
            });
        }
    }

    let activate = if activation.is_some() {
        quote! {
            fn activate<'a>(
                actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
            ) -> ::coactor::__macro::BoxFuture<'a, ::core::result::Result<(), ::std::string::String>> {
                ::std::boxed::Box::pin(async move {
                    let actor = actor
                        .downcast_mut::<#actor_type>()
                        .expect("CoActor registered an incompatible Actor Type");
                    actor.on_activate().await.map_err(|error| ::std::format!("{error:?}"))
                })
            }
        }
    } else {
        quote! {
            fn activate<'a>(
                _actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
            ) -> ::coactor::__macro::BoxFuture<'a, ::core::result::Result<(), ::std::string::String>> {
                ::std::boxed::Box::pin(async { ::core::result::Result::Ok(()) })
            }
        }
    };
    let deactivate = if deactivation.is_some() {
        quote! {
            fn deactivate<'a>(
                actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                reason: ::coactor::DeactivationReason,
            ) -> ::coactor::__macro::BoxFuture<'a, ()> {
                ::std::boxed::Box::pin(async move {
                    let actor = actor
                        .downcast_mut::<#actor_type>()
                        .expect("CoActor registered an incompatible Actor Type");
                    actor.on_deactivate(reason).await
                })
            }
        }
    } else {
        quote! {
            fn deactivate<'a>(
                _actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                _reason: ::coactor::DeactivationReason,
            ) -> ::coactor::__macro::BoxFuture<'a, ()> {
                ::std::boxed::Box::pin(async {})
            }
        }
    };

    quote! {
        #actor_impl

        #(#generated_messages)*

        #[derive(Clone)]
        pub struct #ref_ident {
            inner: ::coactor::__macro::ActorRef<#state_type>,
        }

        impl #ref_ident {
            #(#generated_ref_methods)*
        }

        impl ::coactor::__macro::ActorType<#state_type> for #actor_type {
            const NAME: &'static str = #actor_name;
            type Ref = #ref_ident;

            fn create(
                actor_id: ::coactor::ActorId,
                state: ::std::sync::Arc<#state_type>,
            ) -> Self {
                Self::new(actor_id, state)
            }

            #activate
            #deactivate

            fn make_ref(inner: ::coactor::__macro::ActorRef<#state_type>) -> Self::Ref {
                #ref_ident { inner }
            }

            fn remote_commands() -> ::std::collections::HashMap<
                &'static str,
                ::coactor::__macro::RemoteCommandFactory<#state_type>,
            > {
                let mut commands = ::std::collections::HashMap::new();
                #(#generated_remote_factories)*
                commands
            }
        }
    }
    .into()
}
