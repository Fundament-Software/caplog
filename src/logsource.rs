use std::cell::RefCell;

use super::caplog::CapLog;
use super::log_capnp::log_source;
use capnp_macros::capnproto_rpc;

#[capnproto_rpc(log_source)]
impl<const BUFFER_SIZE: usize> log_source::Server for RefCell<CapLog<BUFFER_SIZE>> {
    async fn get(&self, snowflake_id: u64, machine_id: u64, schema: u64, verify: bool) {
        let _ = schema;

        match self
            .borrow_mut()
            .get_log(snowflake_id, machine_id, verify, &mut results.get().init_payload())
        {
            Ok(_) => Ok(()),
            Err(e) => Err(capnp::Error::failed(e.to_string())),
        }
    }
}
