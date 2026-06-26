pub mod checksum;
pub mod codec;
pub mod flags;
pub mod header;
pub mod kind;

pub use checksum::crc32c_record;
pub use codec::{decode_record, encode_record, RecordBody, RecordDecodeError, RecordEncodeError};
pub use flags::RecordFlags;
pub use header::{RecordHeader, RECORD_HEADER_LEN};
pub use kind::RecordKind;
