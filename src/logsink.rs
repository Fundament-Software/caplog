use super::caplog::CapLog;
use super::log_capnp::log_sink;
use capnp::IntoResult;
use capnp_macros::capnp_let;
use capnp_rpc::pry;

impl log_sink::Server for CapLog {
    fn log(
        &mut self,
        params: log_sink::LogParams,
        _: log_sink::LogResults,
    ) -> ::capnp::capability::Promise<(), ::capnp::Error> {
        let rparams = pry!(params.get());
        capnp_let!(
          {snowflake_id : id, machine_id : machineId, schema, data} = rparams
        );
        self.append(id, machineId, schema, data);
        ::capnp::capability::Promise::err(::capnp::Error::unimplemented(
            "method log_sink::Server::log not implemented".to_string(),
        ))
    }
}
