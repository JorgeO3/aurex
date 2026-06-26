use aurum_intrusive::{Link, Linked};
use aurum_types::Seq;

use super::constants::WORDS_PER_BLOCK;

pub const RETRY_LINK: usize = 0;
pub const SPARSE_LINK: usize = 1;
pub const DIRTY_LINK: usize = 2;
pub const RETRY_TAG: u32 = 0;
pub const SPARSE_TAG: u32 = 1;
pub const DIRTY_TAG: u32 = 2;

#[derive(Debug)]
pub struct MsgBlock {
    pub base_seq: Seq,
    pub inflight: [u64; WORDS_PER_BLOCK],
    pub acked: [u64; WORDS_PER_BLOCK],
    pub retry: [u64; WORDS_PER_BLOCK],
    pub sparse_ready: [u64; WORDS_PER_BLOCK],
    pub redelivered: [u64; WORDS_PER_BLOCK],
    pub retry_word_mask: u8,
    pub sparse_word_mask: u8,
    retry_link: Link,
    sparse_link: Link,
    dirty_link: Link,
}

impl MsgBlock {
    #[must_use]
    pub const fn new(base_seq: Seq) -> Self {
        Self {
            base_seq,
            inflight: [0; WORDS_PER_BLOCK],
            acked: [0; WORDS_PER_BLOCK],
            retry: [0; WORDS_PER_BLOCK],
            sparse_ready: [0; WORDS_PER_BLOCK],
            redelivered: [0; WORDS_PER_BLOCK],
            retry_word_mask: 0,
            sparse_word_mask: 0,
            retry_link: Link::new(),
            sparse_link: Link::new(),
            dirty_link: Link::new(),
        }
    }

    #[inline]
    #[must_use]
    pub fn is_retry_listed(&self) -> bool {
        self.retry_link.is_linked()
    }

    #[inline]
    #[must_use]
    pub fn is_sparse_listed(&self) -> bool {
        self.sparse_link.is_linked()
    }

    #[inline]
    #[must_use]
    pub fn is_retry_empty(&self) -> bool {
        self.retry_word_mask == 0
    }

    #[inline]
    #[must_use]
    pub fn is_sparse_ready_empty(&self) -> bool {
        self.sparse_word_mask == 0
    }

    #[inline]
    pub fn mark_sparse_word(&mut self, word: usize) {
        self.sparse_word_mask |= 1u8 << word;
    }

    #[inline]
    pub fn unmark_sparse_word_if_empty(&mut self, word: usize) {
        if self.sparse_ready[word] == 0 {
            self.sparse_word_mask &= !(1u8 << word);
        }
    }

    #[inline]
    pub fn mark_retry_word(&mut self, word: usize) {
        self.retry_word_mask |= 1u8 << word;
    }

    #[inline]
    pub fn unmark_retry_word_if_empty(&mut self, word: usize) {
        if self.retry[word] == 0 {
            self.retry_word_mask &= !(1u8 << word);
        }
    }
}

impl Linked<RETRY_LINK> for MsgBlock {
    #[inline]
    fn link(&self) -> &Link {
        &self.retry_link
    }

    #[inline]
    fn link_mut(&mut self) -> &mut Link {
        &mut self.retry_link
    }
}

impl Linked<SPARSE_LINK> for MsgBlock {
    #[inline]
    fn link(&self) -> &Link {
        &self.sparse_link
    }

    #[inline]
    fn link_mut(&mut self) -> &mut Link {
        &mut self.sparse_link
    }
}

impl Linked<DIRTY_LINK> for MsgBlock {
    #[inline]
    fn link(&self) -> &Link {
        &self.dirty_link
    }

    #[inline]
    fn link_mut(&mut self) -> &mut Link {
        &mut self.dirty_link
    }
}
