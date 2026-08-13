fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/peer.proto"], &["proto"])
        .expect("peer protocol should compile");
}
