fn main() {
    capnpc::CompilerCommand::new()
        .file("log.capnp")
        .run()
        .expect("compiling schema");
}
