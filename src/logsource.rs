use super::caplog::CapLog;
use super::log_capnp::log_source;
use capnp::capability::Promise;
use capnp_macros::capnproto_rpc;

#[capnproto_rpc(log_source)]
impl<const BUFFER_SIZE: usize> log_source::Server for CapLog<BUFFER_SIZE> {
    fn get(&mut self, snowflake_id: u64, machine_id: u64, schema: u64, verify: bool) {
        let _ = schema;

        match self.get_log(snowflake_id, machine_id, verify, &mut results.get().init_payload()) {
            Ok(_) => capnp::ok(),
            Err(e) => Err(capnp::Error::failed(e.to_string())),
        }
    }
}
