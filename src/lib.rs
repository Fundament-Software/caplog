mod caplog;
mod fallthrough;
mod logsink;
mod logsource;
pub mod murmur3;
mod ring_buf_writer;
pub mod sorted_map;
use bitfield_struct::bitfield;
mod hashed_array_trie;
capnp_import::capnp_import!("log.capnp");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}

/// Snowflake id without a machine id (sent as a separate 64 bit integer)
#[bitfield(u64)]
struct SnowflakeId {
    #[bits(20)]
    sequence: u32,
    #[bits(44)]
    timestamp: u64,
}

// Full 128-bit ID
#[bitfield(u128)]
struct FullLogID {
    #[bits(20)]
    sequence: u32,
    #[bits(44)]
    timestamp: u64,
    #[bits(64)]
    machine: u64,
}

pub fn as_snowflake(sequence: u32, timestamp: u64) -> u64 {
    SnowflakeId::new()
        .with_sequence(sequence)
        .with_timestamp(timestamp)
        .into()
}
