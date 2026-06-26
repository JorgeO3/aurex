use bytes::{Buf, BytesMut};

use crate::wire::{read_bit, write_bits, WireError};

pub(crate) fn read_packed_bits(buf: &mut &[u8], count: usize) -> Result<Vec<bool>, WireError> {
    let mut out = Vec::with_capacity(count);
    let mut bit_pos = 0u8;
    for _ in 0..count {
        out.push(read_bit(buf, &mut bit_pos)?);
    }
    if bit_pos != 0 {
        buf.advance(1);
    }
    Ok(out)
}

pub(crate) fn write_packed_bits(dst: &mut BytesMut, bits: &[bool]) {
    let mut byte = 0u8;
    for (i, b) in bits.iter().enumerate() {
        if *b {
            byte |= 1 << (7 - i);
        }
    }
    write_bits(dst, byte);
}
