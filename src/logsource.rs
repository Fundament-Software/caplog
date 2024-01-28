use super::caplog::CapLog;
use super::log_capnp::log_source;

impl log_source::Server for CapLog {
    fn get(
        &mut self,
        _: log_source::GetParams,
        _: log_source::GetResults,
    ) -> ::capnp::capability::Promise<(), ::capnp::Error> {
        ::capnp::capability::Promise::err(::capnp::Error::unimplemented(
            "method log_source::Server::log not implemented".to_string(),
        ))
    }
}
