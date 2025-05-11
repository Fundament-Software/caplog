use caplog::MAX_BUFFER_SIZE;
use caplog::hashed_array_trie::{self, HashedArrayTrie, Storage};
use caplog::{CapLog, log_capnp::log_entry};
use capnp::any_pointer;
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;
use std::{path::Path, sync::Arc, time::SystemTime};

#[allow(dead_code)]
#[inline]
fn get_next_u128<T>(rng: &mut T) -> u128
where
    T: rand::RngCore,
{
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    u128::from_le_bytes(bytes)
}

fn gen_anypointer_message(index: u64, anypointer: ::capnp::any_pointer::Builder) -> log_entry::Builder {
    let mut builder = anypointer.init_as::<log_entry::Builder>();
    builder.set_snowflake_id(index * 10 + 5);
    builder.set_machine_id(index * 10 + 6);
    builder.set_instance_id(index * 10 + 7);
    builder.set_schema(index * 10 + 8);
    builder
}

// This benchmark bypasses capnproto's message processing so we can benchmark just the log itself.
fn raw_log_benchmark(start: SystemTime) -> eyre::Result<()> {
    const MAX_COUNT: u64 = 0xFFFFFF;
    let data_prefix = std::path::Path::new("./TEST_DATA");

    {
        let storage = Storage::new(Path::new("trie.dat"), 32).unwrap();
        let mut logger =
            CapLog::<MAX_BUFFER_SIZE>::new_storage(caplog::MAX_FILE_SIZE, storage, data_prefix, 10, false)?;
        let flusher = Arc::downgrade(&logger.data_file.clone());

        let _ = std::thread::spawn(move || {
            while let Some(f) = flusher.upgrade() {
                let handle = &mut f.as_ref();
                let _ = handle.flush();
            }
        });

        // log request
        let mut payload = capnp::message::Builder::new_default();

        for i in 0..MAX_COUNT {
            let anypointer = payload.init_root::<any_pointer::Builder>();
            let _ = gen_anypointer_message(i, anypointer);

            let _ = logger.append(
                i * 10 + 1,
                i * 10 + 2,
                i * 10 + 3,
                i * 10,
                payload.get_root_as_reader()?,
                10,
            )?;

            if (i % 1000000) == 0 {
                //storage.borrow_mut().flush_async().unwrap();
                println!(
                    "Time taken for {} insertions: {:?}",
                    i,
                    SystemTime::now().duration_since(start)
                );
            }
        }

        logger.process_pending();

        /*logger.get_log(1, 2, false, &mut root)?;

        let entry = root.into_reader().get_as::<log_entry::Reader>()?;
        assert_eq!(entry.get_snowflake_id(), 4);
        assert_eq!(entry.get_machine_id(), 5);
        assert_eq!(entry.get_instance_id(), 6);
        assert_eq!(entry.get_schema(), 7);*/
    }

    Ok(())
}

#[allow(dead_code)]
fn raw_trie_benchmark(start: SystemTime) {
    const MAX_COUNT: u64 = 0xFFFFFF;

    let storage = Rc::new(RefCell::new(Storage::new(Path::new("output.txt"), 32).unwrap()));
    let mut trie: HashedArrayTrie<u128> = HashedArrayTrie::new(&storage, 1);

    let mut rng = rand::rng();
    let mut track: Vec<u128> = Vec::new();

    // Fill trie with random noise
    for i in 0..MAX_COUNT {
        let key = get_next_u128(&mut rng);
        track.push(key);
        while let Err(e) = trie.insert(key, key as u64) {
            match e.downcast::<hashed_array_trie::Error>().unwrap() {
                hashed_array_trie::Error::OutOfMemory(_) => storage.borrow_mut().resize(),
                err => Err(err.into()),
            }
            .unwrap();
        }

        if (i % 1000000) == 0 {
            //storage.borrow_mut().flush_async().unwrap();
            println!(
                "Time taken for {} insertions: {:?}",
                i,
                SystemTime::now().duration_since(start)
            );
        }
    }

    // Verify all values are correct
    for i in &track {
        assert_eq!(trie.get(*i).expect("Failed to get key"), *i as u64);
    }

    storage.borrow_mut().flush().unwrap();
}

fn main() {
    let start = SystemTime::now();
    {
        //raw_trie_benchmark(start);
        raw_log_benchmark(start).unwrap();
    }

    println!("Time taken: {:?}", SystemTime::now().duration_since(start));
}
