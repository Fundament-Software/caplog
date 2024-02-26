use super::caplog::{CapLog, MAX_BUFFER_SIZE};
use super::log_capnp::log_sink;
#[cfg(not(miri))]
use crate::log_capnp::log_source::Server;
use capnp::any_pointer;
use capnp::capability::Promise;
#[cfg(not(miri))]
use capnp::message::ReaderSegments;
use capnp_macros::capnproto_rpc;
use core::future::Future;
use std::pin::Pin;
use std::sync::mpsc::Receiver;
use std::task::{Context, Poll};

pub struct LogFuture {
    receiver: Receiver<bool>,
}

impl Future for LogFuture {
    type Output = core::result::Result<(), ::capnp::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.receiver.try_recv() {
            Ok(_) => Poll::Ready(Ok(())),
            Err(_) => Poll::Pending,
        }
    }
}

#[capnproto_rpc(log_sink)]
impl<const BUFFER_SIZE: usize> log_sink::Server for CapLog<BUFFER_SIZE> {
    fn log(
        &mut self,
        snowflake_id: u64,
        machine_id: u64,
        instance_id: u64,
        schema: u64,
        payload: ::capnp::data::Reader,
    ) {
        let _ = schema;
        const EXTRA_WORDS: usize = 4;
        let size = ::capnp_rpc::pry!(rparams.total_size());
        let words = size.word_count as usize + size.cap_count as usize + EXTRA_WORDS;

        match self.append(snowflake_id, machine_id, instance_id, schema, payload, words) {
            Ok(receiver) => Promise::from_future(LogFuture { receiver }),
            Err(e) => Promise::err(capnp::Error::failed(e.to_string())),
        }
    }
}

#[cfg(test)]
use super::hashed_array_trie::HashedArrayStorage;
#[cfg(test)]
use crate::log_capnp::log_entry;
#[cfg(test)]
use eyre::Result;
#[cfg(test)]
use std::io::Write;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tempfile::NamedTempFile;

#[cfg(test)]
fn gen_anypointer_message<'a>(index: u64, anypointer: ::capnp::any_pointer::Builder<'a>) -> log_entry::Builder<'a> {
    let mut builder = anypointer.init_as::<log_entry::Builder>();
    builder.set_snowflake_id(index * 10 + 5);
    builder.set_machine_id(index * 10 + 6);
    builder.set_instance_id(index * 10 + 7);
    builder.set_schema(index * 10 + 8);
    builder
}

#[cfg(test)]
fn gen_request<'a>(index: u64, mut request: log_sink::log_params::Builder<'a>) {
    request.set_snowflake_id(index * 10 + 2);
    request.set_machine_id(index * 10 + 3);
    request.set_instance_id(index * 10 + 4);
    request.set_schema(index * 10 + 1);
    gen_anypointer_message(index, request.init_payload());
}

#[cfg(test)]
fn check_payload(index: u64, entry: log_entry::Reader) {
    assert_eq!(entry.get_snowflake_id(), index * 10 + 5);
    assert_eq!(entry.get_machine_id(), index * 10 + 6);
    assert_eq!(entry.get_instance_id(), index * 10 + 7);
    assert_eq!(entry.get_schema(), index * 10 + 8);
}

//#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn test_basic_log() -> Result<()> {
    //let data_prefix = NamedTempFile::new()?;
    let data_prefix = NamedTempFile::new()?;
    let trie_file = NamedTempFile::new()?;
    {
        let trie_storage = HashedArrayStorage::new(trie_file.path(), 2_u64.pow(16))?;
        let mut set = capnp_rpc::CapabilityServerSet::new();
        let client: log_sink::Client = set.new_client(CapLog::<MAX_BUFFER_SIZE>::new_storage(
            65535,
            trie_storage,
            data_prefix.path(),
            10,
            false,
        )?);

        // log request
        let mut request = client.log_request();
        gen_request(0, request.get());
        let mut result = request.send().promise;
        if let Poll::Ready(_) = futures::poll!(&mut result) {
            assert!(false, "Promise shouldn't be ready yet!");
        }
        let hook = set.get_local_server(&client).await.unwrap();
        let mut logger = hook.borrow_mut();
        logger.flush()?;
        logger.process_pending();
        result.await?;

        let mut message = capnp::message::Builder::new_default();
        let mut root = message.init_root();

        logger.get_log(2, 3, true, &mut root)?;

        check_payload(0, root.into_reader().get_as::<log_entry::Reader>()?);
    }

    Ok(())
}

//#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn test_basic_threading() -> Result<()> {
    let data_prefix = NamedTempFile::new()?;
    let trie_file = NamedTempFile::new()?;
    {
        let trie_storage = HashedArrayStorage::new(trie_file.path(), 2_u64.pow(16))?;
        let logger = CapLog::<MAX_BUFFER_SIZE>::new_storage(65535, trie_storage, data_prefix.path(), 10, false)?;
        let flusher = Arc::downgrade(&logger.data_file.clone());
        let mut set = capnp_rpc::CapabilityServerSet::new();
        let client: log_sink::Client = set.new_client(logger);

        tokio::spawn(async move {
            while let Some(f) = flusher.upgrade() {
                let handle = &mut f.as_ref();
                let _ = handle.flush();
                tokio::task::yield_now().await;
            }
        });

        // log request
        let mut request = client.log_request();
        gen_request(0, request.get());
        let mut result = request.send().promise;
        if let Poll::Ready(_) = futures::poll!(&mut result) {
            assert!(false, "Promise shouldn't be ready yet!");
        }

        let hook = set.get_local_server(&client).await.unwrap();
        let mut logger = hook.borrow_mut();
        while logger.process_pending() == 0 {
            tokio::task::yield_now().await;
        }
        result.await?;

        let mut message = capnp::message::Builder::new_default();
        let mut root = message.init_root();

        logger.get_log(2, 3, false, &mut root)?;

        check_payload(0, root.into_reader().get_as::<log_entry::Reader>()?);
    }

    Ok(())
}

#[test]
fn test_basic_miri() -> eyre::Result<()> {
    #[cfg(not(miri))]
    let data_prefix = NamedTempFile::new()?;
    #[cfg(not(miri))]
    let data_prefix = data_prefix.path();
    #[cfg(miri)]
    let data_prefix = std::path::Path::new("/");

    #[cfg(not(miri))]
    let trie_file = NamedTempFile::new()?;

    {
        #[cfg(not(miri))]
        let trie_storage = HashedArrayStorage::new(trie_file.path(), 2_u64.pow(8))?;
        #[cfg(miri)]
        let trie_storage = HashedArrayStorage::new(std::path::Path::new("/"), 2_u64.pow(8))?;

        let mut logger = CapLog::<MAX_BUFFER_SIZE>::new_storage(65535, trie_storage, data_prefix, 10, false)?;

        // log request
        let mut payload = capnp::message::Builder::new_default();
        let anypointer = payload.init_root::<any_pointer::Builder>();
        gen_anypointer_message(0, anypointer);

        let result = logger.append(2, 3, 4, 0, payload.get_root_as_reader()?, 10)?;
        println!("append finish");

        if let Ok(_) = result.try_recv() {
            assert!(false, "Promise shouldn't be ready yet!");
        }
        logger.flush()?;
        logger.process_pending();
        let _ = result.recv()?;

        let mut message = capnp::message::Builder::new_default();
        let mut root = message.init_root();

        logger.get_log(2, 3, false, &mut root)?;

        check_payload(0, root.into_reader().get_as::<log_entry::Reader>()?);
    }

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn test_buffer_loop() -> Result<()> {
    //let data_prefix = NamedTempFile::new()?;
    let data_prefix = std::path::Path::new("D:/TEST_FILE");
    let trie_file = NamedTempFile::new()?;
    {
        let trie_storage = HashedArrayStorage::new(trie_file.path(), 2_u64.pow(16))?;
        let mut set = capnp_rpc::CapabilityServerSet::new();
        let client: log_sink::Client =
            set.new_client(CapLog::<128>::new_storage(512, trie_storage, data_prefix, 10, false)?);

        for i in 0..0xFF {
            // log request
            let mut request = client.log_request();
            gen_request(i, request.get());
            let mut result = request.send().promise;
            if let Poll::Ready(_) = futures::poll!(&mut result) {
                assert!(false, "Promise shouldn't be ready yet!");
            }
            let hook = set.get_local_server(&client).await.unwrap();
            let mut logger = hook.borrow_mut();
            logger.flush()?;
            logger.process_pending();
            result.await?;
        }

        for i in 0..0xFF {
            let hook = set.get_local_server(&client).await.unwrap();
            let mut logger = hook.borrow_mut();
            let mut message = capnp::message::Builder::new_default();
            let mut root = message.init_root();
            logger.get_log(i * 10 + 2, i * 10 + 3, true, &mut root)?;
            check_payload(i, root.into_reader().get_as::<log_entry::Reader>()?);
        }
    }

    Ok(())
}

#[test]
fn test_buffer_bypass() -> eyre::Result<()> {
    #[cfg(not(miri))]
    let data_prefix = NamedTempFile::new()?;
    #[cfg(not(miri))]
    let data_prefix = data_prefix.path();
    #[cfg(miri)]
    let data_prefix = std::path::Path::new("/");

    #[cfg(not(miri))]
    let trie_file = NamedTempFile::new()?;

    {
        #[cfg(not(miri))]
        let trie_storage = HashedArrayStorage::new(trie_file.path(), 2_u64.pow(8))?;
        #[cfg(miri)]
        let trie_storage = HashedArrayStorage::new(std::path::Path::new("/"), 2_u64.pow(8))?;

        let mut logger = CapLog::<8>::new_storage(65535, trie_storage, data_prefix, 10, false)?;

        // log request
        let mut payload = capnp::message::Builder::new_default();
        let anypointer = payload.init_root::<any_pointer::Builder>();
        gen_anypointer_message(0, anypointer);

        let result = logger.append(2, 3, 4, 0, payload.get_root_as_reader()?, 10)?;
        println!("append finish");

        if let Ok(_) = result.try_recv() {
            assert!(false, "Promise shouldn't be ready yet!");
        }
        logger.flush()?;
        logger.process_pending();
        let _ = result.recv()?;

        let mut message = capnp::message::Builder::new_default();
        let mut root = message.init_root();

        logger.get_log(2, 3, false, &mut root)?;

        check_payload(0, root.into_reader().get_as::<log_entry::Reader>()?);
    }

    Ok(())
}

#[test]
fn test_file_bypass() -> eyre::Result<()> {
    #[cfg(not(miri))]
    let data_prefix = NamedTempFile::new()?;
    #[cfg(not(miri))]
    let data_prefix = data_prefix.path();
    #[cfg(miri)]
    let data_prefix = std::path::Path::new("/");

    #[cfg(not(miri))]
    let trie_file = NamedTempFile::new()?;

    {
        #[cfg(not(miri))]
        let trie_storage = HashedArrayStorage::new(trie_file.path(), 2_u64.pow(8))?;
        #[cfg(miri)]
        let trie_storage = HashedArrayStorage::new(std::path::Path::new("/"), 2_u64.pow(8))?;

        let mut logger = CapLog::<8>::new_storage(8, trie_storage, data_prefix, 10, false)?;

        // log request
        let mut payload = capnp::message::Builder::new_default();
        let anypointer = payload.init_root::<any_pointer::Builder>();
        gen_anypointer_message(0, anypointer);

        let result = logger.append(2, 3, 4, 0, payload.get_root_as_reader()?, 10)?;
        println!("append finish");

        if let Ok(_) = result.try_recv() {
            assert!(false, "Promise shouldn't be ready yet!");
        }
        logger.flush()?;
        logger.process_pending();
        let _ = result.recv()?;

        let mut message = capnp::message::Builder::new_default();
        let mut root = message.init_root();

        logger.get_log(2, 3, false, &mut root)?;

        check_payload(0, root.into_reader().get_as::<log_entry::Reader>()?);
    }

    Ok(())
}
