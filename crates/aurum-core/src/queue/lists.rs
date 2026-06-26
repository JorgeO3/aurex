use aurum_intrusive::{Link, NONE};

use super::block::MsgBlock;

#[derive(Debug, Clone, Copy)]
pub(super) enum ListKind {
    Retry,
    Sparse,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BlockList {
    pub head: u32,
    pub tail: u32,
    pub len: u32,
}

impl BlockList {
    pub const fn new() -> Self {
        Self { head: NONE, tail: NONE, len: 0 }
    }

    pub fn push_front(&mut self, blocks: &mut [MsgBlock], idx: u32, kind: ListKind) {
        debug_assert!(!link(blocks, idx, kind).in_list);
        let old_head = self.head;
        {
            let l = link_mut(blocks, idx, kind);
            l.prev = NONE;
            l.next = old_head;
            l.in_list = true;
        }
        if old_head == NONE {
            self.tail = idx;
        } else {
            link_mut(blocks, old_head, kind).prev = idx;
        }
        self.head = idx;
        self.len += 1;
    }

    pub fn push_back(&mut self, blocks: &mut [MsgBlock], idx: u32, kind: ListKind) {
        debug_assert!(!link(blocks, idx, kind).in_list);
        let old_tail = self.tail;
        {
            let l = link_mut(blocks, idx, kind);
            l.prev = old_tail;
            l.next = NONE;
            l.in_list = true;
        }
        if old_tail == NONE {
            self.head = idx;
        } else {
            link_mut(blocks, old_tail, kind).next = idx;
        }
        self.tail = idx;
        self.len += 1;
    }

    pub fn pop_front(&mut self, blocks: &mut [MsgBlock], kind: ListKind) -> Option<u32> {
        if self.head == NONE {
            return None;
        }
        let idx = self.head;
        self.remove(blocks, idx, kind);
        Some(idx)
    }

    pub fn remove(&mut self, blocks: &mut [MsgBlock], idx: u32, kind: ListKind) {
        if !link(blocks, idx, kind).in_list {
            return;
        }
        let prev = link(blocks, idx, kind).prev;
        let next = link(blocks, idx, kind).next;

        if prev == NONE {
            self.head = next;
        } else {
            link_mut(blocks, prev, kind).next = next;
        }
        if next == NONE {
            self.tail = prev;
        } else {
            link_mut(blocks, next, kind).prev = prev;
        }

        *link_mut(blocks, idx, kind) = Link::default();
        self.len -= 1;
    }
}

#[inline(always)]
fn link(blocks: &[MsgBlock], idx: u32, kind: ListKind) -> &Link {
    match kind {
        ListKind::Retry => &blocks[idx as usize].retry_link,
        ListKind::Sparse => &blocks[idx as usize].sparse_link,
    }
}

#[inline(always)]
fn link_mut(blocks: &mut [MsgBlock], idx: u32, kind: ListKind) -> &mut Link {
    match kind {
        ListKind::Retry => &mut blocks[idx as usize].retry_link,
        ListKind::Sparse => &mut blocks[idx as usize].sparse_link,
    }
}
