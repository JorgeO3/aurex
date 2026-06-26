#![forbid(unsafe_code)]
#![cfg_attr(not(test), no_std)]
//! Aurum intrusive index collections.
//!
//! This file is intentionally self-contained. In the real crate, the same code can be split into
//! `index`, `link`, `list`, and `validate` modules without changing the public API.
//!
//! Design goals:
//!
//! - no `unsafe`;
//! - compact indices: `Option<Index>` is 4 bytes;
//! - intrusive links stored inside user nodes;
//! - const-generic link slots for static dispatch and multi-link nodes;
//! - const-generic list tags for cheap ownership checks;
//! - checked APIs for boundaries and tests;
//! - small unchecked/fast APIs for hot paths, guarded by debug assertions.

use core::{fmt, iter::FusedIterator, num::NonZeroU32};

/// Maximum raw index representable by [`Index`].
///
/// `u32::MAX` is reserved as the niche value that lets `Option<Index>` stay 4 bytes.
pub const MAX_INDEX_RAW: u32 = u32::MAX - 1;

/// Maximum list tag value.
///
/// The high bit of [`LinkState`] is reserved as the linked bit. The remaining 31 bits are available
/// to identify the logical list family that owns a link.
pub const MAX_LIST_TAG: u32 = (1u32 << 31) - 1;

/// Validates a list tag at compile time, panicking if it uses the reserved high bit.
#[inline(always)]
const fn checked_list_tag<const TAG: u32>() -> u32 {
    assert!(TAG <= MAX_LIST_TAG, "list tag uses reserved high bit");
    TAG
}

// ===========================================================================
// Index
// ===========================================================================

/// A compact index into an external node slice/arena.
///
/// Internally this stores `raw + 1` as a [`NonZeroU32`], so `Option<Index>` is represented in a
/// single `u32`.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Index(NonZeroU32);

impl Index {
    /// Creates an [`Index`] from a raw `u32`.
    ///
    /// Returns `None` for `u32::MAX`, which is reserved.
    #[inline(always)]
    #[must_use]
    pub const fn new(raw: u32) -> Option<Self> {
        // `raw + 1` only overflows when `raw == u32::MAX`, which `NonZeroU32::new`
        // would map to `None` anyway via the wrapped `0`. Guard explicitly so the
        // addition cannot wrap in debug builds.
        match raw.checked_add(1) {
            Some(shifted) => match NonZeroU32::new(shifted) {
                Some(value) => Some(Self(value)),
                None => None,
            },
            None => None,
        }
    }

    /// Creates an [`Index`] from a `usize`.
    #[inline]
    #[must_use]
    pub const fn from_usize(value: usize) -> Option<Self> {
        if value > MAX_INDEX_RAW as usize { None } else { Self::new(value as u32) }
    }

    /// Returns the raw `u32` index.
    #[inline(always)]
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get() - 1
    }

    /// Returns the index as `usize`, suitable for slice indexing.
    #[inline(always)]
    #[must_use]
    pub const fn as_usize(self) -> usize {
        self.get() as usize
    }
}

impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

impl From<Index> for u32 {
    #[inline(always)]
    fn from(value: Index) -> Self {
        value.get()
    }
}

impl From<Index> for usize {
    #[inline(always)]
    fn from(value: Index) -> Self {
        value.as_usize()
    }
}

impl TryFrom<u32> for Index {
    type Error = IndexError;

    #[inline]
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(IndexError { raw: value })
    }
}

impl TryFrom<usize> for Index {
    type Error = IndexError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Self::from_usize(value).ok_or(IndexError { raw: u32::MAX })
    }
}

/// Error returned when a raw integer cannot be represented as an [`Index`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexError {
    pub raw: u32,
}

// ===========================================================================
// LinkState
// ===========================================================================

/// Internal link state.
///
/// Encoding:
///
/// - bit 31: linked flag;
/// - bits 0..30: list tag.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct LinkState(u32);

impl LinkState {
    const LINKED: u32 = 1 << 31;
    const TAG_MASK: u32 = !Self::LINKED;

    #[inline(always)]
    #[must_use]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline(always)]
    #[must_use]
    pub const fn linked<const TAG: u32>() -> Self {
        Self(Self::LINKED | checked_list_tag::<TAG>())
    }

    #[inline(always)]
    #[must_use]
    pub const fn is_linked(self) -> bool {
        self.0 & Self::LINKED != 0
    }

    #[inline(always)]
    #[must_use]
    pub const fn tag(self) -> Option<u32> {
        if self.is_linked() { Some(self.0 & Self::TAG_MASK) } else { None }
    }

    #[inline(always)]
    #[must_use]
    pub const fn belongs_to<const TAG: u32>(self) -> bool {
        self.0 == (Self::LINKED | checked_list_tag::<TAG>())
    }
}

impl Default for LinkState {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Debug for LinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkState")
            .field("linked", &self.is_linked())
            .field("tag", &self.tag())
            .finish()
    }
}

// ===========================================================================
// Link
// ===========================================================================

/// Intrusive doubly-linked-list link.
///
/// Put one or more of these inside your node type. For multiple independent intrusive lists, store
/// multiple links and implement [`Linked`] once per link slot.
///
/// A link is intentionally move-only. It must not be `Copy`: copying a linked node would duplicate
/// intrusive membership.
///
/// ```compile_fail
/// use aurum_intrusive::Link;
///
/// let link = Link::new();
/// let _moved = link;
/// let _use_after_move = link;
/// ```
#[repr(C)]
#[derive(PartialEq, Eq)]
pub struct Link {
    prev: Option<Index>,
    next: Option<Index>,
    state: LinkState,
}

impl Link {
    /// Creates an unlinked link.
    #[inline(always)]
    #[must_use]
    pub const fn new() -> Self {
        Self { prev: None, next: None, state: LinkState::empty() }
    }

    #[inline(always)]
    #[must_use]
    pub const fn prev(&self) -> Option<Index> {
        self.prev
    }

    #[inline(always)]
    #[must_use]
    pub const fn next(&self) -> Option<Index> {
        self.next
    }

    #[inline(always)]
    #[must_use]
    pub const fn is_linked(&self) -> bool {
        self.state.is_linked()
    }

    #[inline(always)]
    #[must_use]
    pub const fn tag(&self) -> Option<u32> {
        self.state.tag()
    }

    #[inline(always)]
    #[must_use]
    pub const fn belongs_to<const TAG: u32>(&self) -> bool {
        self.state.belongs_to::<TAG>()
    }

    /// Links this node between `prev` and `next`, marking it owned by `TAG`.
    #[inline(always)]
    fn link_between<const TAG: u32>(&mut self, prev: Option<Index>, next: Option<Index>) {
        self.prev = prev;
        self.next = next;
        self.state = LinkState::linked::<TAG>();
    }

    /// Returns this node to the unlinked state.
    #[inline(always)]
    fn reset(&mut self) {
        *self = Self::new();
    }
}

impl Default for Link {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Link")
            .field("prev", &self.prev)
            .field("next", &self.next)
            .field("state", &self.state)
            .finish()
    }
}

/// Trait implemented by node types that contain an intrusive [`Link`].
///
/// The `LINK` const parameter gives static dispatch for nodes that participate in more than one
/// intrusive list.
pub trait Linked<const LINK: usize = 0> {
    fn link(&self) -> &Link;
    fn link_mut(&mut self) -> &mut Link;
}

// ===========================================================================
// Errors
// ===========================================================================

/// Error returned by checked list operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListError {
    IndexOutOfBounds { index: Index, len: usize },
    AlreadyLinked { index: Index, tag: Option<u32> },
    NotLinked { index: Index },
    WrongList { index: Index, expected_tag: u32, actual_tag: Option<u32> },
    LengthOverflow,
}

/// Error returned by [`IndexList::validate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidateError {
    EmptyListHasHead { head: Option<Index> },
    EmptyListHasTail { tail: Option<Index> },
    NonEmptyListMissingHead,
    NonEmptyListMissingTail,
    LenExceedsNodes { len: u32, nodes: usize },
    IndexOutOfBounds { index: Index, nodes: usize },
    UnlinkedNodeInList { index: Index },
    WrongTag { index: Index, expected_tag: u32, actual_tag: Option<u32> },
    BrokenPrev { index: Index, expected: Option<Index>, actual: Option<Index> },
    BrokenNext { index: Index, expected: Option<Index>, actual: Option<Index> },
    TailMismatch { expected: Option<Index>, actual: Option<Index> },
    HeadMismatch { expected: Option<Index>, actual: Option<Index> },
    LengthMismatch { expected: u32, actual: u32 },
    CycleDetected,
}

// ===========================================================================
// IndexList
// ===========================================================================

/// An intrusive doubly-linked list over indices into an external node slice/arena.
///
/// `LINK` selects which [`Link`] slot is used on each node.
///
/// `TAG` identifies the list family that owns this link. The tag is stored in each linked node and
/// makes wrong-list bugs cheap to detect. Use different tags for different logical membership
/// domains that share the same link slot.
///
/// Hot-path methods such as [`IndexList::push_back`] and [`IndexList::remove`] assume their
/// preconditions are upheld and use `debug_assert!` for expensive checks. Checked variants such as
/// [`IndexList::try_push_back`] and [`IndexList::try_remove`] return structured errors.
///
/// A list header is intentionally move-only. It must not be `Copy`: copying the header would create
/// two list values pointing at the same linked nodes.
///
/// ```compile_fail
/// use aurum_intrusive::IndexList;
///
/// let list = IndexList::<0, 0>::new();
/// let _moved = list;
/// let _use_after_move = list;
/// ```
#[repr(C)]
#[derive(PartialEq, Eq)]
pub struct IndexList<const LINK: usize = 0, const TAG: u32 = 0> {
    head: Option<Index>,
    tail: Option<Index>,
    len: u32,
}

// ---------------------------------------------------------------------------
// Construction and accessors
// ---------------------------------------------------------------------------

impl<const LINK: usize, const TAG: u32> IndexList<LINK, TAG> {
    /// Creates an empty list.
    #[inline(always)]
    #[must_use]
    pub const fn new() -> Self {
        let _ = checked_list_tag::<TAG>();
        Self { head: None, tail: None, len: 0 }
    }

    /// Returns the link slot used by this list.
    #[inline(always)]
    #[must_use]
    pub const fn link_slot(&self) -> usize {
        LINK
    }

    /// Returns the tag used by this list.
    #[inline(always)]
    #[must_use]
    pub const fn tag(&self) -> u32 {
        checked_list_tag::<TAG>()
    }

    #[inline(always)]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    #[must_use]
    pub const fn len(&self) -> u32 {
        self.len
    }

    #[inline(always)]
    #[must_use]
    pub const fn head(&self) -> Option<Index> {
        self.head
    }

    #[inline(always)]
    #[must_use]
    pub const fn tail(&self) -> Option<Index> {
        self.tail
    }
}

// ---------------------------------------------------------------------------
// Insertion
// ---------------------------------------------------------------------------

impl<const LINK: usize, const TAG: u32> IndexList<LINK, TAG> {
    #[inline]
    pub fn try_push_back<T>(&mut self, nodes: &mut [T], index: Index) -> Result<(), ListError>
    where
        T: Linked<LINK>,
    {
        self.check_insert_preconditions(nodes, index)?;
        self.push_back(nodes, index);
        Ok(())
    }

    /// Pushes `index` at the back of the list.
    ///
    /// Preconditions in release builds:
    ///
    /// - `index < nodes.len()`;
    /// - the selected link on `nodes[index]` is not currently linked;
    /// - `self.len < u32::MAX`.
    #[inline]
    pub fn push_back<T>(&mut self, nodes: &mut [T], index: Index)
    where
        T: Linked<LINK>,
    {
        self.debug_assert_insertable(nodes, index);

        let old_tail = self.tail;
        link_mut(nodes, index).link_between::<TAG>(old_tail, None);

        match old_tail {
            Some(tail) => link_mut(nodes, tail).next = Some(index),
            None => self.head = Some(index),
        }

        self.tail = Some(index);
        self.len += 1;
    }

    #[inline]
    pub fn try_push_front<T>(&mut self, nodes: &mut [T], index: Index) -> Result<(), ListError>
    where
        T: Linked<LINK>,
    {
        self.check_insert_preconditions(nodes, index)?;
        self.push_front(nodes, index);
        Ok(())
    }

    /// Pushes `index` at the front of the list.
    ///
    /// Same release-build preconditions as [`IndexList::push_back`].
    #[inline]
    pub fn push_front<T>(&mut self, nodes: &mut [T], index: Index)
    where
        T: Linked<LINK>,
    {
        self.debug_assert_insertable(nodes, index);

        let old_head = self.head;
        link_mut(nodes, index).link_between::<TAG>(None, old_head);

        match old_head {
            Some(head) => link_mut(nodes, head).prev = Some(index),
            None => self.tail = Some(index),
        }

        self.head = Some(index);
        self.len += 1;
    }

    #[inline]
    pub fn try_insert_after<T>(
        &mut self,
        nodes: &mut [T],
        at: Index,
        index: Index,
    ) -> Result<(), ListError>
    where
        T: Linked<LINK>,
    {
        self.check_remove_preconditions(nodes, at)?;
        self.check_insert_preconditions(nodes, index)?;
        self.insert_after(nodes, at, index);
        Ok(())
    }

    /// Inserts `index` after `at`.
    ///
    /// Release-build preconditions:
    ///
    /// - `at` is a member of this list;
    /// - `index` is in bounds and unlinked;
    /// - `self.len < u32::MAX`.
    #[inline]
    pub fn insert_after<T>(&mut self, nodes: &mut [T], at: Index, index: Index)
    where
        T: Linked<LINK>,
    {
        debug_assert!(self.contains(nodes, at));
        self.debug_assert_insertable(nodes, index);

        if self.tail == Some(at) {
            self.push_back(nodes, index);
            return;
        }

        let next = link(nodes, at).next.expect("non-tail member must have next");

        link_mut(nodes, index).link_between::<TAG>(Some(at), Some(next));
        link_mut(nodes, at).next = Some(index);
        link_mut(nodes, next).prev = Some(index);
        self.len += 1;
    }

    #[inline]
    pub fn try_insert_before<T>(
        &mut self,
        nodes: &mut [T],
        at: Index,
        index: Index,
    ) -> Result<(), ListError>
    where
        T: Linked<LINK>,
    {
        self.check_remove_preconditions(nodes, at)?;
        self.check_insert_preconditions(nodes, index)?;
        self.insert_before(nodes, at, index);
        Ok(())
    }

    /// Inserts `index` before `at`.
    #[inline]
    pub fn insert_before<T>(&mut self, nodes: &mut [T], at: Index, index: Index)
    where
        T: Linked<LINK>,
    {
        debug_assert!(self.contains(nodes, at));
        self.debug_assert_insertable(nodes, index);

        if self.head == Some(at) {
            self.push_front(nodes, index);
            return;
        }

        let prev = link(nodes, at).prev.expect("non-head member must have prev");

        link_mut(nodes, index).link_between::<TAG>(Some(prev), Some(at));
        link_mut(nodes, prev).next = Some(index);
        link_mut(nodes, at).prev = Some(index);
        self.len += 1;
    }
}

// ---------------------------------------------------------------------------
// Removal and movement
// ---------------------------------------------------------------------------

impl<const LINK: usize, const TAG: u32> IndexList<LINK, TAG> {
    #[inline]
    pub fn pop_front<T>(&mut self, nodes: &mut [T]) -> Option<Index>
    where
        T: Linked<LINK>,
    {
        let index = self.head?;
        self.unlink_known_member(nodes, index);
        Some(index)
    }

    #[inline]
    pub fn pop_back<T>(&mut self, nodes: &mut [T]) -> Option<Index>
    where
        T: Linked<LINK>,
    {
        let index = self.tail?;
        self.unlink_known_member(nodes, index);
        Some(index)
    }

    #[inline]
    pub fn try_remove<T>(&mut self, nodes: &mut [T], index: Index) -> Result<(), ListError>
    where
        T: Linked<LINK>,
    {
        self.check_remove_preconditions(nodes, index)?;
        self.unlink_known_member(nodes, index);
        Ok(())
    }

    /// Removes `index` from the list, returning `false` if the node is not linked at all.
    ///
    /// In release builds, this is O(1) and assumes that a linked node belongs to this exact list.
    /// Use [`IndexList::try_remove`] at API boundaries or in code that cannot prove membership.
    #[inline]
    pub fn remove<T>(&mut self, nodes: &mut [T], index: Index) -> bool
    where
        T: Linked<LINK>,
    {
        debug_assert!(index.as_usize() < nodes.len());

        if !link(nodes, index).is_linked() {
            return false;
        }

        debug_assert!(
            link(nodes, index).belongs_to::<TAG>(),
            "attempted to remove a node linked to a different list tag"
        );
        debug_assert!(
            self.contains(nodes, index),
            "attempted to remove a node from the wrong list"
        );

        self.unlink_known_member(nodes, index);
        true
    }

    /// Moves a known member to the back.
    #[inline]
    pub fn move_to_back<T>(&mut self, nodes: &mut [T], index: Index)
    where
        T: Linked<LINK>,
    {
        if self.tail == Some(index) {
            return;
        }

        debug_assert!(self.contains(nodes, index));
        debug_assert!(link(nodes, index).belongs_to::<TAG>());

        let old_tail = self.tail.expect("non-empty list must have tail");
        self.splice_out(nodes, index);

        link_mut(nodes, index).link_between::<TAG>(Some(old_tail), None);
        link_mut(nodes, old_tail).next = Some(index);
        self.tail = Some(index);
    }

    /// Moves a known member to the front.
    #[inline]
    pub fn move_to_front<T>(&mut self, nodes: &mut [T], index: Index)
    where
        T: Linked<LINK>,
    {
        if self.head == Some(index) {
            return;
        }

        debug_assert!(self.contains(nodes, index));
        debug_assert!(link(nodes, index).belongs_to::<TAG>());

        let old_head = self.head.expect("non-empty list must have head");
        self.splice_out(nodes, index);

        link_mut(nodes, index).link_between::<TAG>(None, Some(old_head));
        link_mut(nodes, old_head).prev = Some(index);
        self.head = Some(index);
    }

    /// Appends `other` to the back of `self` in O(1).
    ///
    /// Both lists must use the same link slot and tag, which is enforced by the type.
    #[inline]
    pub fn append<T>(&mut self, nodes: &mut [T], other: &mut Self)
    where
        T: Linked<LINK>,
    {
        if other.is_empty() {
            return;
        }

        if self.is_empty() {
            self.head = other.head;
            self.tail = other.tail;
            self.len = other.len;
            other.clear_header();
            return;
        }

        let self_tail = self.tail.expect("non-empty list must have tail");
        let other_head = other.head.expect("non-empty list must have head");

        link_mut(nodes, self_tail).next = Some(other_head);
        link_mut(nodes, other_head).prev = Some(self_tail);

        self.tail = other.tail;
        self.len = self.len.checked_add(other.len).expect("IndexList length overflow");
        other.clear_header();
    }

    /// Clears the list and resets all links.
    #[inline]
    pub fn clear<T>(&mut self, nodes: &mut [T])
    where
        T: Linked<LINK>,
    {
        let mut current = self.head;
        let mut remaining = self.len;

        while remaining != 0 {
            let index = current.expect("list ended before len reached");
            debug_assert!(index.as_usize() < nodes.len());
            let next = link(nodes, index).next;
            link_mut(nodes, index).reset();
            current = next;
            remaining -= 1;
        }

        self.clear_header();
    }

    /// Keeps only the nodes for which `keep` returns true.
    ///
    /// The closure may mutate node payload, but must not mutate the selected intrusive link.
    pub fn retain<T, F>(&mut self, nodes: &mut [T], mut keep: F)
    where
        T: Linked<LINK>,
        F: FnMut(Index, &mut T) -> bool,
    {
        let mut current = self.head;

        while let Some(index) = current {
            let next = link(nodes, index).next;
            if !keep(index, &mut nodes[index.as_usize()]) {
                self.unlink_known_member(nodes, index);
            }
            current = next;
        }
    }

    /// Calls `f` for every node in list order.
    ///
    /// The closure may mutate node payload, but must not mutate the selected intrusive link.
    pub fn for_each_mut<T, F>(&self, nodes: &mut [T], mut f: F)
    where
        T: Linked<LINK>,
        F: FnMut(Index, &mut T),
    {
        let mut current = self.head;
        let mut remaining = self.len;

        while remaining != 0 {
            let index = current.expect("list ended before len reached");
            let next = link(nodes, index).next;
            f(index, &mut nodes[index.as_usize()]);
            current = next;
            remaining -= 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Inspection and iteration
// ---------------------------------------------------------------------------

impl<const LINK: usize, const TAG: u32> IndexList<LINK, TAG> {
    /// Returns true if `index` is found by walking this list.
    ///
    /// This is O(n). It is intended for checked APIs, validation, and debug assertions.
    #[inline]
    #[must_use]
    pub fn contains<T>(&self, nodes: &[T], index: Index) -> bool
    where
        T: Linked<LINK>,
    {
        self.iter(nodes).any(|current| current == index)
    }

    #[inline]
    #[must_use]
    pub fn iter<'a, T>(&self, nodes: &'a [T]) -> Iter<'a, T, LINK>
    where
        T: Linked<LINK>,
    {
        Iter { nodes, next: self.head, remaining: self.len }
    }

    #[inline]
    #[must_use]
    pub fn iter_rev<'a, T>(&self, nodes: &'a [T]) -> IterRev<'a, T, LINK>
    where
        T: Linked<LINK>,
    {
        IterRev { nodes, next: self.tail, remaining: self.len }
    }

    #[inline]
    #[must_use]
    pub fn iter_nodes<'a, T>(&self, nodes: &'a [T]) -> IterNodes<'a, T, LINK>
    where
        T: Linked<LINK>,
    {
        IterNodes { iter: self.iter(nodes), nodes }
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

impl<const LINK: usize, const TAG: u32> IndexList<LINK, TAG> {
    /// Performs a full structural validation of this list.
    ///
    /// This is O(n) and intended for tests, fuzzing, debug assertions at subsystem boundaries, and
    /// startup/recovery checks.
    pub fn validate<T>(&self, nodes: &[T]) -> Result<(), ValidateError>
    where
        T: Linked<LINK>,
    {
        if self.len == 0 {
            return self.validate_empty();
        }

        if self.head.is_none() {
            return Err(ValidateError::NonEmptyListMissingHead);
        }
        if self.tail.is_none() {
            return Err(ValidateError::NonEmptyListMissingTail);
        }
        if self.len as usize > nodes.len() {
            return Err(ValidateError::LenExceedsNodes { len: self.len, nodes: nodes.len() });
        }

        self.validate_forward(nodes)?;
        self.validate_backward(nodes)?;
        Ok(())
    }

    /// Checks that an empty list carries no dangling head or tail.
    #[inline]
    fn validate_empty(&self) -> Result<(), ValidateError> {
        if self.head.is_some() {
            return Err(ValidateError::EmptyListHasHead { head: self.head });
        }
        if self.tail.is_some() {
            return Err(ValidateError::EmptyListHasTail { tail: self.tail });
        }
        Ok(())
    }

    /// Walks head to tail, checking node validity, `prev` back-links, the tail anchor, and length.
    fn validate_forward<T>(&self, nodes: &[T]) -> Result<(), ValidateError>
    where
        T: Linked<LINK>,
    {
        let mut count = 0u32;
        let mut prev = None;
        let mut current = self.head;
        let mut last = None;

        while let Some(index) = current {
            if count as usize > nodes.len() {
                return Err(ValidateError::CycleDetected);
            }
            self.validate_index(nodes, index)?;
            let link = nodes[index.as_usize()].link();
            self.validate_link(index, link)?;

            if link.prev != prev {
                return Err(ValidateError::BrokenPrev { index, expected: prev, actual: link.prev });
            }

            last = Some(index);
            prev = Some(index);
            current = link.next;
            count += 1;
        }

        if last != self.tail {
            return Err(ValidateError::TailMismatch { expected: last, actual: self.tail });
        }
        if count != self.len {
            return Err(ValidateError::LengthMismatch { expected: self.len, actual: count });
        }
        Ok(())
    }

    /// Walks tail to head, checking node validity, `next` forward-links, the head anchor, and length.
    fn validate_backward<T>(&self, nodes: &[T]) -> Result<(), ValidateError>
    where
        T: Linked<LINK>,
    {
        let mut count = 0u32;
        let mut next = None;
        let mut current = self.tail;
        let mut first = None;

        while let Some(index) = current {
            if count as usize > nodes.len() {
                return Err(ValidateError::CycleDetected);
            }
            self.validate_index(nodes, index)?;
            let link = nodes[index.as_usize()].link();
            self.validate_link(index, link)?;

            if link.next != next {
                return Err(ValidateError::BrokenNext { index, expected: next, actual: link.next });
            }

            first = Some(index);
            next = Some(index);
            current = link.prev;
            count += 1;
        }

        if first != self.head {
            return Err(ValidateError::HeadMismatch { expected: first, actual: self.head });
        }
        if count != self.len {
            return Err(ValidateError::LengthMismatch { expected: self.len, actual: count });
        }
        Ok(())
    }

    #[inline]
    fn validate_index<T>(&self, nodes: &[T], index: Index) -> Result<(), ValidateError> {
        if index.as_usize() >= nodes.len() {
            return Err(ValidateError::IndexOutOfBounds { index, nodes: nodes.len() });
        }
        Ok(())
    }

    #[inline]
    fn validate_link(&self, index: Index, link: &Link) -> Result<(), ValidateError> {
        if !link.is_linked() {
            return Err(ValidateError::UnlinkedNodeInList { index });
        }
        if !link.belongs_to::<TAG>() {
            return Err(ValidateError::WrongTag {
                index,
                expected_tag: checked_list_tag::<TAG>(),
                actual_tag: link.tag(),
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

impl<const LINK: usize, const TAG: u32> IndexList<LINK, TAG> {
    #[inline]
    fn check_insert_preconditions<T>(&self, nodes: &[T], index: Index) -> Result<(), ListError>
    where
        T: Linked<LINK>,
    {
        self.check_index(nodes, index)?;

        let link = nodes[index.as_usize()].link();
        if link.is_linked() {
            return Err(ListError::AlreadyLinked { index, tag: link.tag() });
        }

        if self.len == u32::MAX {
            return Err(ListError::LengthOverflow);
        }

        Ok(())
    }

    #[inline]
    fn check_remove_preconditions<T>(&self, nodes: &[T], index: Index) -> Result<(), ListError>
    where
        T: Linked<LINK>,
    {
        self.check_index(nodes, index)?;

        let link = nodes[index.as_usize()].link();
        if !link.is_linked() {
            return Err(ListError::NotLinked { index });
        }

        // The node must be linked, carry our tag, and actually appear in this list. The tag
        // check is O(1) and the membership walk is O(n); both map to the same `WrongList` error.
        if !link.belongs_to::<TAG>() || !self.contains(nodes, index) {
            return Err(ListError::WrongList {
                index,
                expected_tag: checked_list_tag::<TAG>(),
                actual_tag: link.tag(),
            });
        }

        Ok(())
    }

    /// Unlinks a node known to belong to this list, repairing neighbor and header links.
    #[inline(always)]
    fn unlink_known_member<T>(&mut self, nodes: &mut [T], index: Index)
    where
        T: Linked<LINK>,
    {
        debug_assert!(self.len > 0);
        debug_assert!(index.as_usize() < nodes.len());
        debug_assert!(link(nodes, index).belongs_to::<TAG>());

        self.splice_out(nodes, index);
        link_mut(nodes, index).reset();
        self.len -= 1;
    }

    /// Detaches `index` from its neighbors, rerouting their links and updating the head/tail
    /// anchors as needed. Leaves `index`'s own link untouched and does not adjust `len`; callers
    /// decide whether to reset and recount (removal) or relink elsewhere (move).
    #[inline(always)]
    fn splice_out<T>(&mut self, nodes: &mut [T], index: Index)
    where
        T: Linked<LINK>,
    {
        let prev = link(nodes, index).prev;
        let next = link(nodes, index).next;

        match prev {
            Some(prev) => link_mut(nodes, prev).next = next,
            None => self.head = next,
        }

        match next {
            Some(next) => link_mut(nodes, next).prev = prev,
            None => self.tail = prev,
        }
    }

    #[inline(always)]
    fn check_index<T>(&self, nodes: &[T], index: Index) -> Result<(), ListError> {
        if index.as_usize() >= nodes.len() {
            return Err(ListError::IndexOutOfBounds { index, len: nodes.len() });
        }
        Ok(())
    }

    /// Shared debug-build precondition check for the unchecked insertion methods.
    #[inline(always)]
    fn debug_assert_insertable<T>(&self, nodes: &[T], index: Index)
    where
        T: Linked<LINK>,
    {
        debug_assert!(index.as_usize() < nodes.len());
        debug_assert!(!nodes[index.as_usize()].link().is_linked());
        debug_assert!(self.len < u32::MAX);
    }

    #[inline(always)]
    fn clear_header(&mut self) {
        self.head = None;
        self.tail = None;
        self.len = 0;
    }
}

impl<const LINK: usize, const TAG: u32> Default for IndexList<LINK, TAG> {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl<const LINK: usize, const TAG: u32> fmt::Debug for IndexList<LINK, TAG> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexList")
            .field("link", &LINK)
            .field("tag", &checked_list_tag::<TAG>())
            .field("head", &self.head)
            .field("tail", &self.tail)
            .field("len", &self.len)
            .finish()
    }
}

// ===========================================================================
// Link access helpers
// ===========================================================================

/// Shared read access to the `LINK` slot of `nodes[index]`.
#[inline(always)]
fn link<T, const LINK: usize>(nodes: &[T], index: Index) -> &Link
where
    T: Linked<LINK>,
{
    nodes[index.as_usize()].link()
}

/// Mutable access to the `LINK` slot of `nodes[index]`.
#[inline(always)]
fn link_mut<T, const LINK: usize>(nodes: &mut [T], index: Index) -> &mut Link
where
    T: Linked<LINK>,
{
    nodes[index.as_usize()].link_mut()
}

// ===========================================================================
// Iterators
// ===========================================================================

/// Forward iterator over indices.
pub struct Iter<'a, T, const LINK: usize = 0> {
    nodes: &'a [T],
    next: Option<Index>,
    remaining: u32,
}

impl<'a, T, const LINK: usize> fmt::Debug for Iter<'a, T, LINK> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Iter")
            .field("next", &self.next)
            .field("remaining", &self.remaining)
            .finish_non_exhaustive()
    }
}

impl<'a, T, const LINK: usize> Iterator for Iter<'a, T, LINK>
where
    T: Linked<LINK>,
{
    type Item = Index;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            self.next = None;
            return None;
        }

        let index = self.next?;
        self.next = self.nodes[index.as_usize()].link().next;
        self.remaining -= 1;
        Some(index)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

impl<'a, T, const LINK: usize> ExactSizeIterator for Iter<'a, T, LINK>
where
    T: Linked<LINK>,
{
    #[inline]
    fn len(&self) -> usize {
        self.remaining as usize
    }
}

impl<'a, T, const LINK: usize> FusedIterator for Iter<'a, T, LINK> where T: Linked<LINK> {}

/// Reverse iterator over indices.
pub struct IterRev<'a, T, const LINK: usize = 0> {
    nodes: &'a [T],
    next: Option<Index>,
    remaining: u32,
}

impl<'a, T, const LINK: usize> fmt::Debug for IterRev<'a, T, LINK> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IterRev")
            .field("next", &self.next)
            .field("remaining", &self.remaining)
            .finish_non_exhaustive()
    }
}

impl<'a, T, const LINK: usize> Iterator for IterRev<'a, T, LINK>
where
    T: Linked<LINK>,
{
    type Item = Index;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            self.next = None;
            return None;
        }

        let index = self.next?;
        self.next = self.nodes[index.as_usize()].link().prev;
        self.remaining -= 1;
        Some(index)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

impl<'a, T, const LINK: usize> ExactSizeIterator for IterRev<'a, T, LINK>
where
    T: Linked<LINK>,
{
    #[inline]
    fn len(&self) -> usize {
        self.remaining as usize
    }
}

impl<'a, T, const LINK: usize> FusedIterator for IterRev<'a, T, LINK> where T: Linked<LINK> {}

/// Iterator over `(Index, &Node)`.
pub struct IterNodes<'a, T, const LINK: usize = 0>
where
    T: Linked<LINK>,
{
    iter: Iter<'a, T, LINK>,
    nodes: &'a [T],
}

impl<'a, T, const LINK: usize> fmt::Debug for IterNodes<'a, T, LINK>
where
    T: Linked<LINK>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IterNodes").field("iter", &self.iter).finish_non_exhaustive()
    }
}

impl<'a, T, const LINK: usize> Iterator for IterNodes<'a, T, LINK>
where
    T: Linked<LINK>,
{
    type Item = (Index, &'a T);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.iter.next()?;
        Some((index, &self.nodes[index.as_usize()]))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, T, const LINK: usize> ExactSizeIterator for IterNodes<'a, T, LINK>
where
    T: Linked<LINK>,
{
    #[inline]
    fn len(&self) -> usize {
        self.iter.len()
    }
}

impl<'a, T, const LINK: usize> FusedIterator for IterNodes<'a, T, LINK> where T: Linked<LINK> {}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, panic, vec, vec::Vec};

    const MAIN: usize = 0;
    const AUX: usize = 1;
    const ACTIVE: u32 = 7;
    const FREE: u32 = 11;
    const BAD_TAG: u32 = MAX_LIST_TAG + 1;

    #[derive(Debug, Default)]
    struct Node {
        links: [Link; 2],
        value: u32,
    }

    impl Node {
        fn with_value(value: u32) -> Self {
            Self { value, ..Self::default() }
        }
    }

    impl Linked<MAIN> for Node {
        #[inline]
        fn link(&self) -> &Link {
            &self.links[MAIN]
        }

        #[inline]
        fn link_mut(&mut self) -> &mut Link {
            &mut self.links[MAIN]
        }
    }

    impl Linked<AUX> for Node {
        #[inline]
        fn link(&self) -> &Link {
            &self.links[AUX]
        }

        #[inline]
        fn link_mut(&mut self) -> &mut Link {
            &mut self.links[AUX]
        }
    }

    fn idx(value: usize) -> Index {
        Index::from_usize(value).unwrap()
    }

    fn make_nodes(count: usize) -> Vec<Node> {
        (0..count).map(|i| Node::with_value(i as u32)).collect()
    }

    fn order<const LINK: usize, const TAG: u32>(
        list: &IndexList<LINK, TAG>,
        nodes: &[Node],
    ) -> Vec<usize>
    where
        Node: Linked<LINK>,
    {
        list.iter(nodes).map(Index::as_usize).collect()
    }

    fn reverse_order<const LINK: usize, const TAG: u32>(
        list: &IndexList<LINK, TAG>,
        nodes: &[Node],
    ) -> Vec<usize>
    where
        Node: Linked<LINK>,
    {
        list.iter_rev(nodes).map(Index::as_usize).collect()
    }

    fn assert_model<const LINK: usize, const TAG: u32>(
        list: &IndexList<LINK, TAG>,
        nodes: &[Node],
        model: &VecDeque<usize>,
    ) where
        Node: Linked<LINK>,
    {
        list.validate(nodes).unwrap();

        let expected: Vec<usize> = model.iter().copied().collect();
        let expected_rev: Vec<usize> = model.iter().rev().copied().collect();

        assert_eq!(order(list, nodes), expected);
        assert_eq!(reverse_order(list, nodes), expected_rev);
        assert_eq!(list.len() as usize, model.len());
        assert_eq!(list.head().map(Index::as_usize), model.front().copied());
        assert_eq!(list.tail().map(Index::as_usize), model.back().copied());
        assert_eq!(list.is_empty(), model.is_empty());
    }

    fn model_remove(model: &mut VecDeque<usize>, value: usize) -> bool {
        match model.iter().position(|&item| item == value) {
            Some(pos) => {
                model.remove(pos);
                true
            }
            None => false,
        }
    }

    fn build_main_list(order: &[usize], node_count: usize) -> (Vec<Node>, IndexList<MAIN, ACTIVE>) {
        let mut nodes = make_nodes(node_count);
        let mut list = IndexList::<MAIN, ACTIVE>::new();
        for &item in order {
            list.try_push_back(&mut nodes, idx(item)).unwrap();
        }
        list.validate(&nodes).unwrap();
        (nodes, list)
    }

    fn all_unique_orders(universe: usize) -> Vec<Vec<usize>> {
        fn rec(
            universe: usize,
            used: &mut [bool],
            current: &mut Vec<usize>,
            out: &mut Vec<Vec<usize>>,
        ) {
            out.push(current.clone());
            for candidate in 0..universe {
                if used[candidate] {
                    continue;
                }
                used[candidate] = true;
                current.push(candidate);
                rec(universe, used, current, out);
                current.pop();
                used[candidate] = false;
            }
        }

        let mut out = Vec::new();
        let mut used = vec![false; universe];
        let mut current = Vec::new();
        rec(universe, &mut used, &mut current, &mut out);
        out
    }

    fn assert_all_links_reset(nodes: &[Node], indices: impl IntoIterator<Item = usize>) {
        for index in indices {
            assert!(!nodes[index].links[MAIN].is_linked(), "link {index} still linked");
            assert_eq!(nodes[index].links[MAIN].prev(), None);
            assert_eq!(nodes[index].links[MAIN].next(), None);
        }
    }

    #[test]
    fn compact_layout() {
        assert_eq!(core::mem::size_of::<Index>(), 4);
        assert_eq!(core::mem::size_of::<Option<Index>>(), 4);
        assert_eq!(core::mem::size_of::<Link>(), 12);
        assert_eq!(core::mem::size_of::<IndexList>(), 12);
    }

    #[test]
    fn default_is_valid_empty_list() {
        let list = IndexList::<MAIN, ACTIVE>::default();
        let nodes: Vec<Node> = Vec::new();

        assert!(list.is_empty());
        assert_eq!(list.len(), 0);
        assert_eq!(list.head(), None);
        assert_eq!(list.tail(), None);
        assert_eq!(list.link_slot(), MAIN);
        assert_eq!(list.tag(), ACTIVE);
        assert_eq!(list.validate(&nodes), Ok(()));
    }

    #[test]
    fn index_rejects_reserved_value() {
        assert!(Index::new(u32::MAX).is_none());
        assert!(Index::from_usize(u32::MAX as usize).is_none());
        assert_eq!(Index::new(0).unwrap().get(), 0);
        assert_eq!(Index::new(MAX_INDEX_RAW).unwrap().get(), MAX_INDEX_RAW);
        assert_eq!(u32::from(idx(3)), 3);
        assert_eq!(usize::from(idx(3)), 3);
        assert_eq!(format!("{}", idx(9)), "9");
        assert_eq!(format!("{:?}", idx(9)), "9");
    }

    #[test]
    fn invalid_tags_panic_instead_of_being_masked() {
        assert!(panic::catch_unwind(IndexList::<MAIN, BAD_TAG>::new).is_err());
        assert!(panic::catch_unwind(LinkState::linked::<BAD_TAG>).is_err());

        let mut link = Link::new();
        assert!(
            panic::catch_unwind(panic::AssertUnwindSafe(|| {
                link.link_between::<BAD_TAG>(None, None);
            }))
            .is_err()
        );
    }

    #[test]
    fn push_pop_remove_roundtrip() {
        let mut nodes = vec![Node::with_value(10), Node::with_value(20), Node::with_value(30)];

        let mut list = IndexList::<MAIN, ACTIVE>::new();

        list.try_push_back(&mut nodes, idx(0)).unwrap();
        list.try_push_back(&mut nodes, idx(1)).unwrap();
        list.try_push_back(&mut nodes, idx(2)).unwrap();

        assert_model(&list, &nodes, &VecDeque::from([0, 1, 2]));

        list.try_remove(&mut nodes, idx(1)).unwrap();
        assert_model(&list, &nodes, &VecDeque::from([0, 2]));
        assert_all_links_reset(&nodes, [1]);

        assert_eq!(list.pop_front(&mut nodes), Some(idx(0)));
        assert_eq!(list.pop_back(&mut nodes), Some(idx(2)));
        assert_eq!(list.pop_front(&mut nodes), None);
        assert!(list.is_empty());
        list.validate(&nodes).unwrap();
        assert_all_links_reset(&nodes, [0, 1, 2]);
    }

    #[test]
    fn push_front_and_reverse_iteration_preserve_order() {
        let mut nodes = make_nodes(3);
        let mut list = IndexList::<MAIN, ACTIVE>::new();

        list.try_push_front(&mut nodes, idx(0)).unwrap();
        list.try_push_front(&mut nodes, idx(1)).unwrap();
        list.try_push_front(&mut nodes, idx(2)).unwrap();

        assert_model(&list, &nodes, &VecDeque::from([2, 1, 0]));
    }

    #[test]
    fn iterator_contracts_are_exact_and_fused() {
        let (nodes, list) = build_main_list(&[0, 1, 2], 3);
        let mut iter = list.iter(&nodes);

        assert_eq!(iter.len(), 3);
        assert_eq!(iter.size_hint(), (3, Some(3)));
        assert_eq!(iter.next(), Some(idx(0)));
        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some(idx(1)));
        assert_eq!(iter.next(), Some(idx(2)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let values: Vec<u32> = list.iter_nodes(&nodes).map(|(_index, node)| node.value).collect();
        assert_eq!(values, vec![0, 1, 2]);
    }

    #[test]
    fn insert_before_and_after_cover_head_middle_and_tail() {
        let mut nodes = make_nodes(5);
        let mut list = IndexList::<MAIN, ACTIVE>::new();

        list.try_push_back(&mut nodes, idx(0)).unwrap();
        list.try_push_back(&mut nodes, idx(2)).unwrap();
        list.try_insert_after(&mut nodes, idx(0), idx(1)).unwrap();
        list.try_insert_before(&mut nodes, idx(0), idx(4)).unwrap();
        list.try_insert_after(&mut nodes, idx(2), idx(3)).unwrap();

        assert_model(&list, &nodes, &VecDeque::from([4, 0, 1, 2, 3]));
    }

    #[test]
    fn move_to_front_and_back_cover_edges_and_middle() {
        let mut nodes = make_nodes(4);
        let mut list = IndexList::<MAIN, ACTIVE>::new();
        let mut model = VecDeque::from([0, 1, 2, 3]);

        for i in 0..4 {
            list.try_push_back(&mut nodes, idx(i)).unwrap();
        }

        list.move_to_front(&mut nodes, idx(2));
        assert!(model_remove(&mut model, 2));
        model.push_front(2);
        assert_model(&list, &nodes, &model);

        list.move_to_back(&mut nodes, idx(0));
        assert!(model_remove(&mut model, 0));
        model.push_back(0);
        assert_model(&list, &nodes, &model);

        list.move_to_front(&mut nodes, idx(2));
        assert_model(&list, &nodes, &model);

        list.move_to_back(&mut nodes, idx(0));
        assert_model(&list, &nodes, &model);
    }

    #[test]
    fn append_handles_all_empty_and_non_empty_cases() {
        let mut nodes = make_nodes(6);
        let mut a = IndexList::<MAIN, ACTIVE>::new();
        let mut b = IndexList::<MAIN, ACTIVE>::new();

        a.append(&mut nodes, &mut b);
        assert_model(&a, &nodes, &VecDeque::new());
        assert_model(&b, &nodes, &VecDeque::new());

        a.try_push_back(&mut nodes, idx(0)).unwrap();
        a.try_push_back(&mut nodes, idx(1)).unwrap();
        a.append(&mut nodes, &mut b);
        assert_model(&a, &nodes, &VecDeque::from([0, 1]));
        assert_model(&b, &nodes, &VecDeque::new());

        b.try_push_back(&mut nodes, idx(2)).unwrap();
        b.try_push_back(&mut nodes, idx(3)).unwrap();
        let mut empty = IndexList::<MAIN, ACTIVE>::new();
        empty.append(&mut nodes, &mut b);
        assert_model(&empty, &nodes, &VecDeque::from([2, 3]));
        assert_model(&b, &nodes, &VecDeque::new());

        a.append(&mut nodes, &mut empty);
        assert_model(&a, &nodes, &VecDeque::from([0, 1, 2, 3]));
        assert_model(&empty, &nodes, &VecDeque::new());
    }

    #[test]
    fn retain_clear_and_for_each_mut_reset_links_correctly() {
        let mut nodes = vec![
            Node::with_value(1),
            Node::with_value(2),
            Node::with_value(3),
            Node::with_value(4),
        ];
        let mut list = IndexList::<MAIN, ACTIVE>::new();

        for i in 0..4 {
            list.try_push_back(&mut nodes, idx(i)).unwrap();
        }

        list.retain(&mut nodes, |_index, node| node.value % 2 == 0);
        assert_model(&list, &nodes, &VecDeque::from([1, 3]));
        assert_all_links_reset(&nodes, [0, 2]);

        list.for_each_mut(&mut nodes, |_index, node| node.value *= 10);
        assert_eq!(nodes[1].value, 20);
        assert_eq!(nodes[3].value, 40);
        assert_model(&list, &nodes, &VecDeque::from([1, 3]));

        list.clear(&mut nodes);
        assert_model(&list, &nodes, &VecDeque::new());
        assert_all_links_reset(&nodes, [0, 1, 2, 3]);
    }

    #[test]
    fn checked_operation_errors_are_precise() {
        let mut nodes = make_nodes(2);
        let mut list = IndexList::<MAIN, ACTIVE>::new();

        assert_eq!(
            list.try_push_back(&mut nodes, idx(3)),
            Err(ListError::IndexOutOfBounds { index: idx(3), len: 2 })
        );
        assert_eq!(
            list.try_remove(&mut nodes, idx(0)),
            Err(ListError::NotLinked { index: idx(0) })
        );

        list.try_push_back(&mut nodes, idx(0)).unwrap();
        assert_eq!(
            list.try_push_back(&mut nodes, idx(0)),
            Err(ListError::AlreadyLinked { index: idx(0), tag: Some(ACTIVE) })
        );

        let mut full = IndexList::<MAIN, ACTIVE>::new();
        full.len = u32::MAX;
        assert_eq!(full.try_push_back(&mut nodes, idx(1)), Err(ListError::LengthOverflow));
    }

    #[test]
    fn wrong_tag_and_wrong_same_tag_list_are_rejected_by_checked_remove() {
        let mut nodes = make_nodes(2);
        let mut active = IndexList::<MAIN, ACTIVE>::new();
        let mut free = IndexList::<MAIN, FREE>::new();
        let mut another_active = IndexList::<MAIN, ACTIVE>::new();

        active.try_push_back(&mut nodes, idx(0)).unwrap();

        assert_eq!(
            free.try_remove(&mut nodes, idx(0)),
            Err(ListError::WrongList {
                index: idx(0),
                expected_tag: FREE,
                actual_tag: Some(ACTIVE),
            })
        );
        assert_eq!(
            another_active.try_remove(&mut nodes, idx(0)),
            Err(ListError::WrongList {
                index: idx(0),
                expected_tag: ACTIVE,
                actual_tag: Some(ACTIVE),
            })
        );
    }

    #[test]
    fn same_node_can_live_in_two_link_slots_independently() {
        let mut nodes = make_nodes(3);
        let mut main = IndexList::<MAIN, ACTIVE>::new();
        let mut aux = IndexList::<AUX, FREE>::new();

        main.try_push_back(&mut nodes, idx(0)).unwrap();
        aux.try_push_back(&mut nodes, idx(0)).unwrap();
        main.try_push_back(&mut nodes, idx(1)).unwrap();
        aux.try_push_front(&mut nodes, idx(1)).unwrap();
        main.try_push_back(&mut nodes, idx(2)).unwrap();
        aux.try_push_front(&mut nodes, idx(2)).unwrap();

        assert_model(&main, &nodes, &VecDeque::from([0, 1, 2]));
        assert_model(&aux, &nodes, &VecDeque::from([2, 1, 0]));

        assert_eq!(main.pop_front(&mut nodes), Some(idx(0)));
        assert_eq!(aux.pop_back(&mut nodes), Some(idx(0)));
        assert_model(&main, &nodes, &VecDeque::from([1, 2]));
        assert_model(&aux, &nodes, &VecDeque::from([2, 1]));
    }

    #[test]
    fn exhaustive_shape_operations_for_small_lists() {
        const UNIVERSE: usize = 4;
        const EXTRA: usize = 4;

        for base in all_unique_orders(UNIVERSE) {
            let model = VecDeque::from(base.clone());
            let (nodes, list) = build_main_list(&base, UNIVERSE + 1);
            assert_model(&list, &nodes, &model);

            let (mut nodes, mut list) = build_main_list(&base, UNIVERSE + 1);
            list.clear(&mut nodes);
            assert_model(&list, &nodes, &VecDeque::new());
            assert_all_links_reset(&nodes, base.iter().copied());

            if let Some(&front) = base.first() {
                let (mut nodes, mut list) = build_main_list(&base, UNIVERSE + 1);
                assert_eq!(list.pop_front(&mut nodes), Some(idx(front)));
                let expected = VecDeque::from(base[1..].to_vec());
                assert_model(&list, &nodes, &expected);
                assert_all_links_reset(&nodes, [front]);
            }

            if let Some(&back) = base.last() {
                let (mut nodes, mut list) = build_main_list(&base, UNIVERSE + 1);
                assert_eq!(list.pop_back(&mut nodes), Some(idx(back)));
                let expected = VecDeque::from(base[..base.len() - 1].to_vec());
                assert_model(&list, &nodes, &expected);
                assert_all_links_reset(&nodes, [back]);
            }

            for &member in &base {
                let pos = base.iter().position(|&item| item == member).unwrap();

                let (mut nodes, mut list) = build_main_list(&base, UNIVERSE + 1);
                list.try_remove(&mut nodes, idx(member)).unwrap();
                let mut expected = base.clone();
                expected.remove(pos);
                assert_model(&list, &nodes, &VecDeque::from(expected));
                assert_all_links_reset(&nodes, [member]);

                let (mut nodes, mut list) = build_main_list(&base, UNIVERSE + 1);
                list.move_to_front(&mut nodes, idx(member));
                let mut expected = base.clone();
                let removed = expected.remove(pos);
                expected.insert(0, removed);
                assert_model(&list, &nodes, &VecDeque::from(expected));

                let (mut nodes, mut list) = build_main_list(&base, UNIVERSE + 1);
                list.move_to_back(&mut nodes, idx(member));
                let mut expected = base.clone();
                let removed = expected.remove(pos);
                expected.push(removed);
                assert_model(&list, &nodes, &VecDeque::from(expected));

                let (mut nodes, mut list) = build_main_list(&base, UNIVERSE + 1);
                list.try_insert_before(&mut nodes, idx(member), idx(EXTRA)).unwrap();
                let mut expected = base.clone();
                expected.insert(pos, EXTRA);
                assert_model(&list, &nodes, &VecDeque::from(expected));

                let (mut nodes, mut list) = build_main_list(&base, UNIVERSE + 1);
                list.try_insert_after(&mut nodes, idx(member), idx(EXTRA)).unwrap();
                let mut expected = base.clone();
                expected.insert(pos + 1, EXTRA);
                assert_model(&list, &nodes, &VecDeque::from(expected));
            }
        }
    }

    #[derive(Clone, Copy)]
    enum Op {
        PushBack,
        PushFront,
        Remove,
        PopFront,
        PopBack,
        MoveToFront,
        MoveToBack,
        Clear,
    }

    const OPS: [Op; 8] = [
        Op::PushBack,
        Op::PushFront,
        Op::Remove,
        Op::PopFront,
        Op::PopBack,
        Op::MoveToFront,
        Op::MoveToBack,
        Op::Clear,
    ];

    fn apply_model_op(
        op: Op,
        target: usize,
        list: &mut IndexList<MAIN, ACTIVE>,
        nodes: &mut [Node],
        model: &mut VecDeque<usize>,
        present: &mut [bool],
    ) {
        match op {
            Op::PushBack => {
                let result = list.try_push_back(nodes, idx(target));
                if present[target] {
                    assert!(matches!(result, Err(ListError::AlreadyLinked { .. })));
                } else {
                    result.unwrap();
                    present[target] = true;
                    model.push_back(target);
                }
            }
            Op::PushFront => {
                let result = list.try_push_front(nodes, idx(target));
                if present[target] {
                    assert!(matches!(result, Err(ListError::AlreadyLinked { .. })));
                } else {
                    result.unwrap();
                    present[target] = true;
                    model.push_front(target);
                }
            }
            Op::Remove => {
                let result = list.try_remove(nodes, idx(target));
                if present[target] {
                    result.unwrap();
                    assert!(model_remove(model, target));
                    present[target] = false;
                } else {
                    assert_eq!(result, Err(ListError::NotLinked { index: idx(target) }));
                }
            }
            Op::PopFront => {
                let expected = model.pop_front();
                assert_eq!(list.pop_front(nodes).map(Index::as_usize), expected);
                if let Some(removed) = expected {
                    present[removed] = false;
                }
            }
            Op::PopBack => {
                let expected = model.pop_back();
                assert_eq!(list.pop_back(nodes).map(Index::as_usize), expected);
                if let Some(removed) = expected {
                    present[removed] = false;
                }
            }
            Op::MoveToFront => {
                if present[target] {
                    list.move_to_front(nodes, idx(target));
                    assert!(model_remove(model, target));
                    model.push_front(target);
                }
            }
            Op::MoveToBack => {
                if present[target] {
                    list.move_to_back(nodes, idx(target));
                    assert!(model_remove(model, target));
                    model.push_back(target);
                }
            }
            Op::Clear => {
                list.clear(nodes);
                model.clear();
                present.fill(false);
            }
        }
    }

    #[derive(Clone, Copy)]
    struct XorShift64(u64);

    impl XorShift64 {
        fn new(seed: u64) -> Self {
            Self(seed.max(1))
        }

        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn usize(&mut self, max: usize) -> usize {
            (self.next() as usize) % max
        }
    }

    #[test]
    fn randomized_model_test_single_list() {
        const NODES: usize = 32;
        const STEPS: usize = 10_000;

        for seed in 1..=64u64 {
            let mut rng = XorShift64::new(seed);
            let mut nodes = make_nodes(NODES);
            let mut list = IndexList::<MAIN, ACTIVE>::new();
            let mut model = VecDeque::<usize>::new();
            let mut present = [false; NODES];

            for _step in 0..STEPS {
                let index = rng.usize(NODES);
                match rng.usize(11) {
                    0 => apply_model_op(
                        Op::PushBack,
                        index,
                        &mut list,
                        &mut nodes,
                        &mut model,
                        &mut present,
                    ),
                    1 => apply_model_op(
                        Op::PushFront,
                        index,
                        &mut list,
                        &mut nodes,
                        &mut model,
                        &mut present,
                    ),
                    2 => apply_model_op(
                        Op::Remove,
                        index,
                        &mut list,
                        &mut nodes,
                        &mut model,
                        &mut present,
                    ),
                    3 => apply_model_op(
                        Op::PopFront,
                        index,
                        &mut list,
                        &mut nodes,
                        &mut model,
                        &mut present,
                    ),
                    4 => apply_model_op(
                        Op::PopBack,
                        index,
                        &mut list,
                        &mut nodes,
                        &mut model,
                        &mut present,
                    ),
                    5 => apply_model_op(
                        Op::MoveToBack,
                        index,
                        &mut list,
                        &mut nodes,
                        &mut model,
                        &mut present,
                    ),
                    6 => apply_model_op(
                        Op::MoveToFront,
                        index,
                        &mut list,
                        &mut nodes,
                        &mut model,
                        &mut present,
                    ),
                    7 => {
                        if present[index] {
                            let replacement = rng.usize(NODES);
                            if !present[replacement] {
                                list.try_insert_after(&mut nodes, idx(index), idx(replacement))
                                    .unwrap();
                                let pos = model.iter().position(|&item| item == index).unwrap();
                                model.insert(pos + 1, replacement);
                                present[replacement] = true;
                            }
                        }
                    }
                    8 => {
                        if present[index] {
                            let replacement = rng.usize(NODES);
                            if !present[replacement] {
                                list.try_insert_before(&mut nodes, idx(index), idx(replacement))
                                    .unwrap();
                                let pos = model.iter().position(|&item| item == index).unwrap();
                                model.insert(pos, replacement);
                                present[replacement] = true;
                            }
                        }
                    }
                    9 => {
                        list.retain(&mut nodes, |index, _node| index.as_usize() % 2 == 0);
                        model.retain(|item| item % 2 == 0);
                        present.fill(false);
                        for &item in &model {
                            present[item] = true;
                        }
                    }
                    _ => apply_model_op(
                        Op::Clear,
                        index,
                        &mut list,
                        &mut nodes,
                        &mut model,
                        &mut present,
                    ),
                }

                assert_model(&list, &nodes, &model);
            }
        }
    }

    #[test]
    fn exhaustive_operation_sequences_for_three_nodes_depth_five() {
        const NODES: usize = 3;
        const DEPTH: usize = 5;
        let total_sequences = OPS.len().pow(DEPTH as u32);

        for mut sequence in 0..total_sequences {
            let mut decoded = [Op::Clear; DEPTH];
            for slot in &mut decoded {
                *slot = OPS[sequence % OPS.len()];
                sequence /= OPS.len();
            }

            for target_seed in 0..NODES.pow(DEPTH as u32) {
                let mut target_value = target_seed;
                let mut nodes = make_nodes(NODES);
                let mut list = IndexList::<MAIN, ACTIVE>::new();
                let mut model = VecDeque::new();
                let mut present = [false; NODES];

                for &op in &decoded {
                    let target = target_value % NODES;
                    target_value /= NODES;
                    apply_model_op(op, target, &mut list, &mut nodes, &mut model, &mut present);
                    assert_model(&list, &nodes, &model);
                }
            }
        }
    }

    #[test]
    fn randomized_model_test_append_two_lists() {
        const NODES: usize = 24;
        const STEPS: usize = 2_000;

        for seed in 100..116u64 {
            let mut rng = XorShift64::new(seed);
            let mut nodes = make_nodes(NODES);
            let mut a = IndexList::<MAIN, ACTIVE>::new();
            let mut b = IndexList::<MAIN, ACTIVE>::new();
            let mut model_a = VecDeque::<usize>::new();
            let mut model_b = VecDeque::<usize>::new();
            let mut owner = [0u8; NODES];

            for _step in 0..STEPS {
                let index = rng.usize(NODES);
                match rng.usize(9) {
                    0 => {
                        if owner[index] == 0 {
                            a.try_push_back(&mut nodes, idx(index)).unwrap();
                            model_a.push_back(index);
                            owner[index] = 1;
                        }
                    }
                    1 => {
                        if owner[index] == 0 {
                            b.try_push_back(&mut nodes, idx(index)).unwrap();
                            model_b.push_back(index);
                            owner[index] = 2;
                        }
                    }
                    2 => {
                        if owner[index] == 1 {
                            a.try_remove(&mut nodes, idx(index)).unwrap();
                            assert!(model_remove(&mut model_a, index));
                            owner[index] = 0;
                        }
                    }
                    3 => {
                        if owner[index] == 2 {
                            b.try_remove(&mut nodes, idx(index)).unwrap();
                            assert!(model_remove(&mut model_b, index));
                            owner[index] = 0;
                        }
                    }
                    4 => {
                        a.append(&mut nodes, &mut b);
                        for &item in &model_b {
                            owner[item] = 1;
                        }
                        model_a.extend(model_b.drain(..));
                    }
                    5 => {
                        b.append(&mut nodes, &mut a);
                        for &item in &model_a {
                            owner[item] = 2;
                        }
                        model_b.extend(model_a.drain(..));
                    }
                    6 => {
                        let removed = a.pop_front(&mut nodes).map(Index::as_usize);
                        assert_eq!(removed, model_a.pop_front());
                        if let Some(removed) = removed {
                            owner[removed] = 0;
                        }
                    }
                    7 => {
                        let removed = b.pop_back(&mut nodes).map(Index::as_usize);
                        assert_eq!(removed, model_b.pop_back());
                        if let Some(removed) = removed {
                            owner[removed] = 0;
                        }
                    }
                    _ => {
                        if owner[index] == 1 {
                            a.move_to_back(&mut nodes, idx(index));
                            assert!(model_remove(&mut model_a, index));
                            model_a.push_back(index);
                        } else if owner[index] == 2 {
                            b.move_to_front(&mut nodes, idx(index));
                            assert!(model_remove(&mut model_b, index));
                            model_b.push_front(index);
                        }
                    }
                }

                assert_model(&a, &nodes, &model_a);
                assert_model(&b, &nodes, &model_b);
            }
        }
    }

    #[test]
    fn validate_reports_corrupted_headers() {
        let nodes = make_nodes(2);

        let mut list = IndexList::<MAIN, ACTIVE>::new();
        list.head = Some(idx(0));
        assert_eq!(
            list.validate(&nodes),
            Err(ValidateError::EmptyListHasHead { head: Some(idx(0)) })
        );

        let mut list = IndexList::<MAIN, ACTIVE>::new();
        list.tail = Some(idx(0));
        assert_eq!(
            list.validate(&nodes),
            Err(ValidateError::EmptyListHasTail { tail: Some(idx(0)) })
        );

        let mut list = IndexList::<MAIN, ACTIVE>::new();
        list.len = 1;
        assert_eq!(list.validate(&nodes), Err(ValidateError::NonEmptyListMissingHead));

        let mut list = IndexList::<MAIN, ACTIVE>::new();
        list.len = 1;
        list.head = Some(idx(0));
        assert_eq!(list.validate(&nodes), Err(ValidateError::NonEmptyListMissingTail));

        let mut list = IndexList::<MAIN, ACTIVE>::new();
        list.len = 3;
        list.head = Some(idx(0));
        list.tail = Some(idx(1));
        assert_eq!(list.validate(&nodes), Err(ValidateError::LenExceedsNodes { len: 3, nodes: 2 }));
    }

    #[test]
    fn validate_reports_corrupted_links() {
        let (nodes, mut list) = build_main_list(&[0, 1, 2], 3);
        list.head = Some(idx(9));
        assert_eq!(
            list.validate(&nodes),
            Err(ValidateError::IndexOutOfBounds { index: idx(9), nodes: 3 })
        );

        let (mut nodes, list) = build_main_list(&[0, 1, 2], 3);
        nodes[1].links[MAIN].reset();
        assert_eq!(list.validate(&nodes), Err(ValidateError::UnlinkedNodeInList { index: idx(1) }));

        let (mut nodes, list) = build_main_list(&[0, 1, 2], 3);
        nodes[1].links[MAIN].state = LinkState::linked::<FREE>();
        assert_eq!(
            list.validate(&nodes),
            Err(ValidateError::WrongTag {
                index: idx(1),
                expected_tag: ACTIVE,
                actual_tag: Some(FREE),
            })
        );

        let (mut nodes, list) = build_main_list(&[0, 1, 2], 3);
        nodes[1].links[MAIN].prev = None;
        assert_eq!(
            list.validate(&nodes),
            Err(ValidateError::BrokenPrev { index: idx(1), expected: Some(idx(0)), actual: None })
        );

        let (mut nodes, list) = build_main_list(&[0, 1, 2], 3);
        nodes[1].links[MAIN].next = None;
        assert_eq!(
            list.validate(&nodes),
            Err(ValidateError::TailMismatch { expected: Some(idx(1)), actual: Some(idx(2)) })
        );

        let (mut nodes, list) = build_main_list(&[0, 1, 2], 3);
        nodes[2].links[MAIN].next = Some(idx(1));
        assert!(matches!(
            list.validate(&nodes),
            Err(ValidateError::CycleDetected | ValidateError::BrokenPrev { .. })
        ));

        let (nodes, mut list) = build_main_list(&[0, 1, 2], 3);
        list.len = 2;
        assert_eq!(
            list.validate(&nodes),
            Err(ValidateError::LengthMismatch { expected: 2, actual: 3 })
        );

        let (mut nodes, list) = build_main_list(&[0, 1, 2], 3);
        nodes[2].links[MAIN].next = Some(idx(9));
        assert_eq!(
            list.validate(&nodes),
            Err(ValidateError::IndexOutOfBounds { index: idx(9), nodes: 3 })
        );
    }
}
