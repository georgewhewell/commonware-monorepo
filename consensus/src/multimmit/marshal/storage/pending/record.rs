use super::super::blocks::BlockMeta;
use crate::{
    Epochable as _,
    multimmit::types::{ChainId, Height, TransactionBlockHeader},
    types::Epoch,
};
use bytes::BufMut;
use commonware_codec::{Buf, Error, FixedSize, Read, ReadExt as _, Write};
use commonware_cryptography::{Digest, crc32};
use commonware_storage::journal::segmented::oversized::Record;

/// Fixed-width metadata that locates one application block in segment value storage.
///
/// Pending frames are uncompressed, so their stored size is the encoded block length plus the
/// fixed CRC32 trailer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PendingRecord<D: Digest> {
    pub meta: BlockMeta<D>,
    pub offset: u64,
    pub size: u32,
}

impl<D: Digest> PendingRecord<D> {
    pub(super) const fn new(meta: BlockMeta<D>) -> Self {
        Self {
            meta,
            offset: 0,
            size: 0,
        }
    }
}

impl<D: Digest> Read for PendingRecord<D> {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let header = TransactionBlockHeader::new(
            Epoch::new(u64::read(buf)?),
            ChainId::new(u32::read(buf)?),
            Height::new(u64::read(buf)?),
            D::read(buf)?,
            D::read(buf)?,
        )
        .map_err(|_| Error::Invalid("PendingRecord", "height zero is synthetic"))?;
        let offset = u64::read(buf)?;
        let size = u32::read(buf)?;
        let encoded_len = u64::from(size)
            .checked_sub(crc32::Digest::SIZE as u64)
            .ok_or(Error::Invalid(
                "PendingRecord",
                "frame is smaller than CRC32",
            ))?;
        let meta = BlockMeta::new(header, encoded_len);
        Ok(Self { meta, offset, size })
    }
}

impl<D: Digest> Write for PendingRecord<D> {
    fn write(&self, buf: &mut impl BufMut) {
        let header = self.meta.header();
        header.epoch().get().write(buf);
        header.chain().get().write(buf);
        header.height().get().write(buf);
        header.parent().write(buf);
        header.body_digest().write(buf);
        self.offset.write(buf);
        self.size.write(buf);
    }
}

impl<D: Digest> FixedSize for PendingRecord<D> {
    const SIZE: usize = u64::SIZE + u32::SIZE + u64::SIZE + D::SIZE * 2 + u64::SIZE + u32::SIZE;
}

impl<D: Digest> Record for PendingRecord<D> {
    fn value_location(&self) -> (u64, u32) {
        (self.offset, self.size)
    }

    fn with_location(mut self, offset: u64, size: u32) -> Self {
        self.offset = offset;
        self.size = size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use commonware_codec::{DecodeExt as _, Encode as _};
    use commonware_cryptography::sha256::Digest as Sha256Digest;

    fn metadata(encoded_len: u64) -> BlockMeta<Sha256Digest> {
        let header = TransactionBlockHeader::new(
            Epoch::new(u64::MAX),
            ChainId::new(u32::MAX),
            Height::new(u64::MAX),
            Sha256Digest::from([0x55; 32]),
            Sha256Digest::from([0xaa; 32]),
        )
        .unwrap();
        BlockMeta::new(header, encoded_len)
    }

    #[test]
    fn round_trip_fixed_width_extremes() {
        let encoded_len = u64::from(u32::MAX) - crc32::Digest::SIZE as u64;
        let record = PendingRecord::new(metadata(encoded_len)).with_location(u64::MAX, u32::MAX);
        let encoded = record.encode();

        assert_eq!(PendingRecord::<Sha256Digest>::SIZE, 96);
        assert_eq!(encoded.len(), PendingRecord::<Sha256Digest>::SIZE);
        assert_eq!(PendingRecord::decode(encoded).unwrap(), record);
    }

    #[test]
    fn rejects_zero_height() {
        let mut encoded = BytesMut::with_capacity(PendingRecord::<Sha256Digest>::SIZE);
        1u64.write(&mut encoded);
        2u32.write(&mut encoded);
        0u64.write(&mut encoded);
        Sha256Digest::from([0x11; 32]).write(&mut encoded);
        Sha256Digest::from([0x22; 32]).write(&mut encoded);
        4u64.write(&mut encoded);
        5u32.write(&mut encoded);

        assert!(PendingRecord::<Sha256Digest>::decode(encoded.freeze()).is_err());
    }

    #[test]
    fn rejects_frame_smaller_than_checksum() {
        let record = PendingRecord::new(metadata(0)).with_location(0, 3);

        assert!(PendingRecord::<Sha256Digest>::decode(record.encode()).is_err());
    }

    #[test]
    fn location_update_preserves_metadata() {
        let meta = metadata(25);
        let record = PendingRecord::new(meta.clone()).with_location(17, 29);

        assert_eq!(record.meta, meta);
        assert_eq!(record.value_location(), (17, 29));
    }
}
