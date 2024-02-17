use super::caplog::CapLog;
use super::log_capnp::{log_entry, log_sink};
use capnp::capability::Promise;
use capnp_macros::{capnp_build, capnproto_rpc};
use core::future::Future;
use std::pin::Pin;
use std::sync::mpsc::Receiver;
use std::task::{Context, Poll};

pub struct LogFuture {
    receiver: Receiver<bool>,
}

impl Future for LogFuture {
    type Output = core::result::Result<(), ::capnp::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        return Poll::Ready(Ok(()));
        match self.receiver.try_recv() {
            Ok(_) => Poll::Ready(Ok(())),
            Err(_) => Poll::Pending,
        }
    }
}

#[capnproto_rpc(log_sink)]
impl log_sink::Server for CapLog {
    fn log(&mut self, snowflake_id: u64, machine_id: u64, schema: u64, payload: ::capnp::data::Reader) {
        const EXTRA_WORDS: usize = 4;
        let size = ::capnp_rpc::pry!(rparams.total_size());
        let words = size.word_count as usize + size.cap_count as usize + EXTRA_WORDS;

        match self.append(snowflake_id, machine_id, schema, payload, words) {
            Ok(receiver) => Promise::from_future(LogFuture { receiver }),
            Err(e) => Promise::err(capnp::Error::failed(e.to_string())),
        }
    }
}

#[cfg(test)]
use super::hashed_array_trie::HashedArrayStorage;
#[cfg(test)]
use eyre::Result;
#[cfg(test)]
use std::fs::OpenOptions;
#[cfg(test)]
use std::io::Read;
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_basic_log() -> Result<()> {
    //let data_prefix = NamedTempFile::new()?;
    let data_prefix = std::path::Path::new("D:/TEST_FILE");
    let trie_file = NamedTempFile::new()?;
    {
        let trie_storage = HashedArrayStorage::new(trie_file.path(), 2_u64.pow(16))?;
        let mut set = capnp_rpc::CapabilityServerSet::new();
        let client: log_sink::Client =
            set.new_client(CapLog::new_storage(65535, trie_storage, data_prefix, 10, false)?);

        // log request
        let mut request = client.log_request();
        {
            request.get().set_machine_id(1);
            request.get().set_snowflake_id(2);
            request.get().set_schema(0);
            let payload = request.get().init_payload();
            let mut builder = payload.init_as::<log_entry::Builder>();
            builder.set_machine_id(4);
            builder.set_snowflake_id(5);
            builder.set_schema(6);
        }
        let result = request.send().promise.await?;
        let hook = set.get_local_server(&client).await.unwrap();
        let mut logger = hook.borrow_mut();
        logger.flush();
        logger.process_pending();
    }

    let mut output = Vec::new();
    OpenOptions::new()
        .read(true)
        .open(data_prefix.join("_0"))?
        .read_to_end(&mut output)?;
    println!("OUTPUT: {:?}", output);

    Ok(())
}
