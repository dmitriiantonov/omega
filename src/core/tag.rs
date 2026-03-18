/// A 64-bit metadata entry for a cache or hash table slot.
///
/// Layout:
/// - Bits 63-48: 16-bit Version ID (ABA protection)
/// - Bits 47-16: 32-bit Signature (Hashed key identifier)
/// - Bit 15:    Busy Flag (Concurrency lock)
/// - Bits 7-0:  8-bit Frequency (Access counter)
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Tag(u64);

impl Tag {
    const SEQUENCE_SHIFT: u64 = 48;
    const SIGNATURE_SHIFT: u64 = 16;
    const SIGNATURE_MASK: u64 = 0xFFFF_FFFF << Self::SIGNATURE_SHIFT;
    const BUSY_MASK: u64 = 1 << 15;
    const FREQUENCY_MASK: u64 = 0xFF;
    const SIGNATURE_C1: u64 = 0xff51afd7ed558ccd;
    const SIGNATURE_C2: u64 = 0xc4ceb9fe1a85ec53;

    /// Generates a non-zero 32-bit signature from a 64-bit hash.
    /// Uses a branchless bit-avalanche to ensure high entropy.
    #[inline]
    fn make_signature(hash: u64) -> u32 {
        let mut hash = hash;
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(Self::SIGNATURE_C1);
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(Self::SIGNATURE_C2);
        hash ^= hash >> 33;

        (hash as u32) | 1
    }

    #[inline]
    pub fn sequence(self) -> u16 {
        (self.0 >> Self::SEQUENCE_SHIFT) as u16
    }
    #[inline]
    pub fn signature(self) -> u32 {
        (self.0 >> Self::SIGNATURE_SHIFT) as u32
    }
    #[inline]
    pub fn is_busy(self) -> bool {
        (self.0 & Self::BUSY_MASK) != 0
    }
    #[inline]
    pub fn frequency(self) -> u8 {
        (self.0 & Self::FREQUENCY_MASK) as u8
    }

    #[inline]
    pub fn is_epoch_match(self, index: Index) -> bool {
        self.sequence() == index.id()
    }

    #[inline]
    pub fn is_hot(self) -> bool {
        self.frequency() > 0
    }

    /// Returns true if the slot is live, matches the version ID, and the signature.
    #[inline]
    pub fn is_match(self, index: Index, hash: u64) -> bool {
        self.sequence() == index.id()
            && !self.is_busy()
            && self.signature() == Self::make_signature(hash)
    }

    /// Creates a new Tag with a specific signature based on the provided hash.
    #[inline]
    pub fn with_signature(self, hash: u64) -> Self {
        let sig = Self::make_signature(hash);
        Self((self.0 & !Self::SIGNATURE_MASK) | (sig as u64) << Self::SIGNATURE_SHIFT)
    }

    /// Sets the busy bit for atomic/CAS operations.
    #[inline]
    pub fn busy(self) -> Self {
        Self(self.0 | Self::BUSY_MASK)
    }

    /// Increments the frequency counter, saturating at 255.
    #[inline]
    pub fn increment_frequency(self) -> Self {
        let frequency = self.frequency();
        if frequency < u8::MAX {
            Self((self.0 & !Self::FREQUENCY_MASK) | (frequency + 1) as u64)
        } else {
            self
        }
    }

    /// Decrements the frequency counter, saturating at 0.
    #[inline]
    pub fn decrement_frequency(self) -> Self {
        let frequency = self.frequency();
        if frequency > 0 {
            Self((self.0 & !Self::FREQUENCY_MASK) | (frequency - 1) as u64)
        } else {
            self
        }
    }

    /// Advances the state for the next inhabitant (ID++, Metadata Cleared).
    /// Returns a synchronized pair of Tag and Index.
    #[inline]
    pub fn advance(self, index: Index) -> (Self, Index) {
        let next_id = self.sequence().wrapping_add(1);
        let new_tag = Tag((next_id as u64) << Self::SEQUENCE_SHIFT);
        let new_index = index.with_id(next_id);
        (new_tag, new_index)
    }

    /// Resets the entry's frequency bits to zero.
    ///
    /// This is used during promotion or specific eviction cycles to clear the
    /// "hotness" of an entry. By masking out the frequency bits, the entry
    /// loses its "second chance" status and must be accessed again to
    /// build up its frequency.
    ///
    /// Returns a new `Tag` with the same metadata and busy status, but
    /// with a frequency of 0.
    #[inline]
    pub fn reset(&self) -> Tag {
        Tag(self.0 & !Self::FREQUENCY_MASK)
    }
}

impl Default for Tag {
    #[inline(always)]
    fn default() -> Self {
        const EMPTY: u64 = 0;
        Self(EMPTY)
    }
}

impl From<u64> for Tag {
    #[inline]
    fn from(raw: u64) -> Self {
        Self(raw)
    }
}

/// A 64-bit versioned pointer.
///
/// Combines a 16-bit version ID with a 48-bit slot index. This ensures that
/// an index pointing to an old version of a slot cannot be used after the
/// slot has been advanced.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Index(u64);

impl Index {
    const ID_SHIFT: u64 = 48;
    const INDEX_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

    /// Creates a new versioned Index.
    ///
    /// # Arguments
    /// * `id` - The 16-bit version identifier.
    /// * `slot_index` - The physical offset in the data array (max 48 bits).
    #[inline]
    pub fn new(id: u16, slot_index: usize) -> Self {
        let clean_slot = (slot_index as u64) & Self::INDEX_MASK;
        Self(((id as u64) << Self::ID_SHIFT) | clean_slot)
    }

    /// Returns the 16-bit version identifier.
    #[inline]
    pub fn id(self) -> u16 {
        (self.0 >> Self::ID_SHIFT) as u16
    }

    /// Returns the 48-bit physical slot index.
    #[inline]
    pub fn slot_index(self) -> usize {
        (self.0 & Self::INDEX_MASK) as usize
    }

    /// Returns a copy of the index with an updated version ID.
    ///
    /// This is typically used during an `advance` operation to synchronize
    /// the index with a new `Tag`.
    #[inline]
    pub fn with_id(self, id: u16) -> Self {
        let slot_part = self.0 & Self::INDEX_MASK;
        Self(((id as u64) << Self::ID_SHIFT) | slot_part)
    }
}

impl From<u64> for Index {
    #[inline]
    fn from(raw_index: u64) -> Self {
        Index(raw_index)
    }
}

impl From<Tag> for u64 {
    #[inline]
    fn from(tag: Tag) -> u64 {
        tag.0
    }
}

impl From<Index> for u64 {
    #[inline]
    fn from(index: Index) -> Self {
        index.0
    }
}
