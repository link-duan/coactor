fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/transport.proto"], &["proto"])
        .expect("transport protocol should compile");
}
