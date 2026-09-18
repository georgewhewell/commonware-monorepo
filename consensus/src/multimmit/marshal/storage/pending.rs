//! Shared temporary custody for producer blocks from every chain.
//!
//! Each independently reclaimable segment stores fixed metadata records and variable-sized bodies
//! in an Oversized journal. Records contain direct body locations. A durable manifest preserves
//! the global append coordinate and each chain's logical prune floor across crashes.

mod record;

use super::{
    archive::Shared,
    blocks::{BlockMeta, validated_reference},
};
use crate::{
    multimmit::{
        marshal::config::ArchiveConfig,
        types::{BlockRef, ChainId, Height, TransactionBlock, TransactionBlockHeader},
    },
    types::Epoch,
};
use bytes::BufMut;
use commonware_codec::{
    Buf, Codec, EncodeSize, Error as CodecError, FixedSize as _, RangeCfg, Read, ReadExt as _,
    Write,
};
use commonware_cryptography::{Digest, Digestible, Hasher, crc32};
use commonware_runtime::{Handle, ReadOptions};
use commonware_storage::{
    Context,
    journal::{
        self,
        segmented::{
            glob,
            oversized::{self, Oversized},
        },
    },
    metadata::{self, Metadata},
    translator::Translator,
};
use commonware_utils::sequence::Unit;
use futures::future::BoxFuture;
use record::PendingRecord;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    marker::PhantomData,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

type StoredBody<H, B> = Shared<TransactionBlock<H, B>>;
type BodySnapshot<E, H, B> =
    glob::Reader<<E as commonware_runtime::Storage>::Blob, StoredBody<H, B>>;
type Segment<E, H, B> = Oversized<E, PendingRecord<<H as Hasher>::Digest>, StoredBody<H, B>>;

const STATE_VERSION: u8 = 3;
/// Bounds file descriptors and filesystem operations used by one segment I/O wave.
pub(in crate::multimmit::marshal) const BODY_READ_CONCURRENCY: usize = 16;

#[derive(Clone)]
struct PendingState {
    version: u8,
    segment_capacity: u64,
    floors: Vec<Height>,
    segments: Vec<u64>,
}

#[derive(Clone, Copy)]
struct PendingStateCfg {
    chains: usize,
    max_segments: usize,
}

impl Read for PendingState {
    type Cfg = PendingStateCfg;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> Result<Self, CodecError> {
        Ok(Self {
            version: u8::read(buf)?,
            segment_capacity: u64::read(buf)?,
            floors: Vec::<Height>::read_cfg(buf, &(RangeCfg::exact(cfg.chains), ()))?,
            segments: Vec::<u64>::read_cfg(buf, &(RangeCfg::from(0..=cfg.max_segments), ()))?,
        })
    }
}

impl Write for PendingState {
    fn write(&self, buf: &mut impl BufMut) {
        self.version.write(buf);
        self.segment_capacity.write(buf);
        self.floors.write(buf);
        self.segments.write(buf);
    }
}

impl EncodeSize for PendingState {
    fn encode_size(&self) -> usize {
        STATE_VERSION.encode_size()
            + self.segment_capacity.encode_size()
            + self.floors.encode_size()
            + self.segments.encode_size()
    }
}

fn state_blob_size(state: &PendingState) -> Option<usize> {
    u64::SIZE
        .checked_add(Unit::SIZE)?
        .checked_add(state.encode_size())?
        .checked_add(crc32::Digest::SIZE)
}

/// Completed journal writes awaiting publication into the catalog's custody indexes.
pub(in crate::multimmit::marshal) struct Append<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
{
    position: u64,
    rows: Vec<PendingRecord<H::Digest>>,
    segment: Segment<E, H, B>,
}

/// A pending block store failed or recovered inconsistent data.
#[derive(Debug, thiserror::Error)]
pub(in crate::multimmit::marshal) enum Error {
    /// An underlying journal operation failed.
    #[error(transparent)]
    Storage(#[from] journal::Error),
    /// Durable custody state could not be read or written.
    #[error(transparent)]
    State(#[from] metadata::Error),
    /// Durable indexes disagree about a block's identity.
    #[error("pending block store is inconsistent: {0}")]
    Inconsistent(&'static str),
}

#[derive(Clone)]
struct Entry<D: Digest> {
    position: u64,
    reference: BlockRef<D>,
    meta: BlockMeta<D>,
    offset: u64,
    size: u32,
}

#[derive(Clone, Copy)]
pub(in crate::multimmit::marshal) struct BodyLocator<D: Digest> {
    position: u64,
    reference: BlockRef<D>,
    encoded_len: u64,
    offset: u64,
    size: u32,
}

/// The inputs needed to open one shared snapshot for a segment without a resident reader.
pub(in crate::multimmit::marshal) struct ColdSource<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
{
    context: E,
    config: glob::Config<B::Cfg>,
    segment: u64,
    segment_capacity: u64,
    _marker: PhantomData<H>,
}

impl<E, H, B> ColdSource<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
    B::Cfg: Clone,
{
    /// Opens an immutable value blob without replaying indexes or changing storage.
    pub(in crate::multimmit::marshal) async fn open(self) -> Result<BodyReader<E, H, B>, Error> {
        let reader = glob::Reader::open(&self.context, self.config, 0).await?;
        Ok(BodyReader::new(
            self.segment,
            self.segment_capacity,
            reader,
            true,
        ))
    }
}

/// An immutable segment source. Ready sources own a cloned snapshot; cold sources open one
/// on demand.
pub(in crate::multimmit::marshal) enum BodySource<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
{
    Ready(BodyReader<E, H, B>),
    Cold(ColdSource<E, H, B>),
}

impl<E, H, B> BodySource<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
    B::Cfg: Clone,
{
    #[cfg(test)]
    pub(in crate::multimmit::marshal) async fn open(self) -> Result<BodyReader<E, H, B>, Error> {
        match self {
            Self::Ready(reader) => Ok(reader),
            Self::Cold(source) => source.open().await,
        }
    }
}

pub(in crate::multimmit::marshal) struct BodyReader<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
{
    segment: u64,
    segment_capacity: u64,
    reader: BodySnapshot<E, H, B>,
    /// Whether the segment accepts no further appends and may enter the advisory reader cache.
    immutable: bool,
}

impl<E, H, B> Clone for BodyReader<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
{
    fn clone(&self) -> Self {
        Self {
            segment: self.segment,
            segment_capacity: self.segment_capacity,
            reader: self.reader.clone(),
            immutable: self.immutable,
        }
    }
}

impl<E, H, B> BodyReader<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
{
    const fn new(
        segment: u64,
        segment_capacity: u64,
        reader: BodySnapshot<E, H, B>,
        immutable: bool,
    ) -> Self {
        Self {
            segment,
            segment_capacity,
            reader,
            immutable,
        }
    }

    pub(in crate::multimmit::marshal) const fn segment(&self) -> u64 {
        self.segment
    }

    const fn local_position(&self, position: u64) -> Result<u64, Error> {
        let segment = position / self.segment_capacity;
        if segment != self.segment {
            return Err(Error::Inconsistent(
                "pending body locator names another segment",
            ));
        }
        Ok(position % self.segment_capacity)
    }

    fn contains(&self, locator: BodyLocator<H::Digest>) -> bool {
        self.local_position(locator.position).is_ok()
            && locator
                .offset
                .checked_add(u64::from(locator.size))
                .is_some_and(|end| end <= self.reader.size())
    }

    #[cfg(test)]
    pub(in crate::multimmit::marshal) async fn read(
        &self,
        locator: BodyLocator<H::Digest>,
    ) -> Result<Arc<TransactionBlock<H, B>>, Error> {
        self.local_position(locator.position)?;
        let block = self
            .reader
            .get(locator.offset, locator.size)
            .await?
            .into_inner();
        validate_body(&block, locator)?;
        Ok(block)
    }
}

/// One owned, single-segment materialization job. Entries retain their requested output indexes
/// and are sorted by storage position for one deduplicated batch of value reads.
pub(in crate::multimmit::marshal) struct BodyReadGroup<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
{
    source: BodySource<E, H, B>,
    entries: Vec<(usize, BodyLocator<H::Digest>)>,
    encoded_bytes: u64,
    prefetch: NonZeroUsize,
}

impl<E, H, B> BodyReadGroup<E, H, B>
where
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
    B::Cfg: Clone,
{
    fn new(
        source: BodySource<E, H, B>,
        entries: Vec<(usize, BodyLocator<H::Digest>)>,
        prefetch: NonZeroUsize,
    ) -> Result<Self, Error> {
        let encoded_bytes = Self::total_encoded_bytes(&entries)?;
        Ok(Self {
            source,
            entries,
            encoded_bytes,
            prefetch,
        })
    }

    pub(in crate::multimmit::marshal) const fn segment(&self) -> u64 {
        match &self.source {
            BodySource::Ready(reader) => reader.segment,
            BodySource::Cold(source) => source.segment,
        }
    }

    pub(in crate::multimmit::marshal) fn references(
        &self,
    ) -> impl Iterator<Item = BlockRef<H::Digest>> + '_ {
        self.entries.iter().map(|(_, locator)| locator.reference)
    }

    pub(in crate::multimmit::marshal) fn retain_references(
        &mut self,
        references: &BTreeSet<BlockRef<H::Digest>>,
    ) -> Result<bool, Error> {
        self.entries
            .retain(|(_, locator)| references.contains(&locator.reference));
        self.encoded_bytes = Self::total_encoded_bytes(&self.entries)?;
        Ok(!self.entries.is_empty())
    }

    fn total_encoded_bytes(entries: &[(usize, BodyLocator<H::Digest>)]) -> Result<u64, Error> {
        entries.iter().try_fold(0u64, |total, (_, locator)| {
            total
                .checked_add(locator.encoded_len)
                .ok_or(Error::Inconsistent("pending body read bytes overflow"))
        })
    }

    #[cfg(test)]
    pub(in crate::multimmit::marshal) async fn read(
        self,
    ) -> Result<Vec<(usize, Arc<TransactionBlock<H, B>>)>, Error> {
        let (source, read) = self.into_parts();
        read.read(source.open().await?).await
    }

    pub(in crate::multimmit::marshal) fn into_parts(self) -> (BodySource<E, H, B>, BodyRead<H>) {
        let segment = self.segment();
        (
            self.source,
            BodyRead {
                segment,
                entries: self.entries,
                encoded_bytes: self.encoded_bytes,
                prefetch: self.prefetch,
            },
        )
    }
}

/// An immutable batch that can share a segment reader with other batches.
pub(in crate::multimmit::marshal) struct BodyRead<H>
where
    H: Hasher,
{
    segment: u64,
    entries: Vec<(usize, BodyLocator<H::Digest>)>,
    encoded_bytes: u64,
    prefetch: NonZeroUsize,
}

impl<H> BodyRead<H>
where
    H: Hasher,
{
    pub(in crate::multimmit::marshal) const fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    pub(in crate::multimmit::marshal) const fn segment(&self) -> u64 {
        self.segment
    }

    pub(in crate::multimmit::marshal) async fn read<E, B>(
        self,
        reader: BodyReader<E, H, B>,
    ) -> Result<Vec<(usize, Arc<TransactionBlock<H, B>>)>, Error>
    where
        E: Context,
        B: Codec + Digestible<Digest = H::Digest>,
    {
        let requests = self
            .entries
            .chunk_by(|(_, left), (_, right)| left.position == right.position);
        let locations = requests
            .clone()
            .map(|requests| {
                let locator = requests[0].1;
                reader.local_position(locator.position)?;
                Ok((locator.offset, locator.size))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let stored = reader.reader.get_many(&locations, self.prefetch).await?;
        let mut results = Vec::new();
        for (requests, stored) in requests.zip(stored) {
            let block = stored.into_inner();
            for (output, locator) in requests {
                validate_body(&block, *locator)?;
                results.push((*output, Arc::clone(&block)));
            }
        }
        results.sort_unstable_by_key(|(output, _)| *output);
        Ok(results)
    }
}

fn validate_body<H, B>(
    block: &TransactionBlock<H, B>,
    locator: BodyLocator<H::Digest>,
) -> Result<(), Error>
where
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
{
    let encoded_len = u64::try_from(block.encode_size())
        .map_err(|_| Error::Inconsistent("encoded block length exceeds u64"))?;
    if block.reference() != locator.reference || encoded_len != locator.encoded_len {
        return Err(Error::Inconsistent(
            "pending body does not match its locator",
        ));
    }
    Ok(())
}

/// Catalog-owned pending producer blocks from every chain.
pub(in crate::multimmit::marshal) struct PendingBlocks<T, E, H, B>
where
    T: Translator,
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
{
    context: E,
    archive: ArchiveConfig<T>,
    prefix: String,
    body_codec_config: B::Cfg,
    state: Option<Metadata<E, Unit, PendingState>>,
    state_dirty: bool,
    segments: BTreeSet<u64>,
    /// An occupied empty entry lends the writer to an append; absent entries are cold.
    open_segments: BTreeMap<u64, Option<Segment<E, H, B>>>,
    active_readers: BTreeMap<u64, BodyReader<E, H, B>>,
    dirty_segments: BTreeSet<u64>,
    /// Full segments publishing recovery markers. Reclamation waits for these writes to finish.
    retiring: BTreeSet<u64>,
    by_digest: HashMap<H::Digest, Entry<H::Digest>>,
    by_position: BTreeMap<u64, H::Digest>,
    by_chain: Vec<BTreeMap<Height, Vec<H::Digest>>>,
    next_position: u64,
    floors: Vec<Height>,
    segment_capacity: u64,
    max_state_bytes: NonZeroUsize,
    epoch: Epoch,
}

impl<T, E, H, B> PendingBlocks<T, E, H, B>
where
    T: Translator,
    E: Context,
    H: Hasher,
    B: Codec + Digestible<Digest = H::Digest>,
    B::Cfg: Clone,
{
    /// Recovers each segment before publishing its metadata as local custody.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::multimmit::marshal) async fn init(
        context: E,
        archive: ArchiveConfig<T>,
        prefix: String,
        body_codec_config: B::Cfg,
        epoch: Epoch,
        chains: usize,
        segment_capacity: NonZeroU64,
        max_state_bytes: NonZeroUsize,
    ) -> Result<Self, Error> {
        let state_cfg = PendingStateCfg {
            chains,
            max_segments: max_state_bytes.get() / u64::SIZE,
        };
        let mut state = Metadata::init(
            context.child("state"),
            metadata::Config {
                partition: format!("{prefix}_state"),
                codec_config: state_cfg,
            },
        )
        .await?;
        if state.get(&Unit).is_none() {
            state = state
                .put_sync(
                    Unit,
                    PendingState {
                        version: STATE_VERSION,
                        segment_capacity: segment_capacity.get(),
                        floors: vec![Height::zero(); chains],
                        segments: Vec::new(),
                    },
                )
                .await?;
        }
        let persisted = state
            .get(&Unit)
            .expect("pending state was initialized")
            .clone();
        if persisted.version != STATE_VERSION {
            return Err(Error::Inconsistent("unsupported pending storage version"));
        }
        if persisted.segment_capacity != segment_capacity.get() {
            return Err(Error::Inconsistent(
                "pending segment capacity differs from storage",
            ));
        }
        if persisted.segments.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::Inconsistent(
                "pending segment manifest is not strictly ordered",
            ));
        }
        let mut store = Self {
            context,
            archive,
            prefix,
            body_codec_config,
            state: Some(state),
            state_dirty: false,
            segments: persisted.segments.iter().copied().collect(),
            open_segments: BTreeMap::new(),
            active_readers: BTreeMap::new(),
            dirty_segments: BTreeSet::new(),
            retiring: BTreeSet::new(),
            by_digest: HashMap::new(),
            by_position: BTreeMap::new(),
            by_chain: vec![BTreeMap::new(); chains],
            next_position: 0,
            floors: persisted.floors,
            segment_capacity: segment_capacity.get(),
            max_state_bytes,
            epoch,
        };

        let current = persisted.segments.last().copied();
        store.next_position = 0;
        for segment_id in persisted.segments {
            let segment_start = segment_id
                .checked_mul(store.segment_capacity)
                .ok_or(Error::Inconsistent("pending segment coordinate overflow"))?;

            let (mut segment, rows) = store.open_segment(segment_id).await?;
            let count = rows.len() as u64;
            for (local, row) in rows.into_iter().enumerate() {
                let position = segment_start
                    .checked_add(local as u64)
                    .ok_or(Error::Inconsistent("pending position overflow"))?;
                store.remember(position, row)?;
            }
            if Some(segment_id) == current {
                if count > 0 {
                    let reader;
                    (segment, reader) = segment.value_snapshot(0).await?;
                    store.active_readers.insert(
                        segment_id,
                        BodyReader::new(
                            segment_id,
                            store.segment_capacity,
                            reader,
                            count == store.segment_capacity,
                        ),
                    );
                }
                store.next_position = segment_start
                    .checked_add(count)
                    .ok_or(Error::Inconsistent("pending position overflow"))?;
                store.open_segments.insert(segment_id, Some(segment));
            } else {
                segment.close().await?;
            }
        }

        let empty = store
            .segments
            .iter()
            .copied()
            .filter(|segment| Some(*segment) != current && !store.segment_has_live_blocks(*segment))
            .collect::<Vec<_>>();
        store.reclaim_segments(&empty).await?;
        if current.is_none() {
            store.ensure_segment(0).await?;
            store.persist_state().await?;
        }
        Ok(store)
    }

    const fn segment_id(&self, position: u64) -> u64 {
        position / self.segment_capacity
    }

    fn segment_has_live_blocks(&self, segment: u64) -> bool {
        let Some(start) = segment.checked_mul(self.segment_capacity) else {
            return false;
        };
        let end = start.saturating_add(self.segment_capacity - 1);
        self.by_position.range(start..=end).next().is_some()
    }

    fn segment_prefix(&self, family: &str, segment: u64) -> String {
        format!("{}_{family}_{segment}", self.prefix)
    }

    fn body_config(&self, segment: u64) -> glob::Config<B::Cfg> {
        glob::Config {
            partition: self.segment_prefix("bodies", segment),
            compression: None,
            codec_config: self.body_codec_config.clone(),
            write_buffer: self.archive.value_write_buffer,
        }
    }

    fn body_context(&self, segment: u64) -> E {
        self.context
            .child("bodies")
            .with_attribute("segment", segment)
    }

    fn segment_config(&self, segment: u64) -> oversized::Config<B::Cfg> {
        oversized::Config {
            index_partition: self.segment_prefix("metadata", segment),
            value_partition: self.segment_prefix("bodies", segment),
            index_page_cache: self.archive.page_cache.clone(),
            index_write_buffer: self.archive.key_write_buffer,
            value_write_buffer: self.archive.value_write_buffer,
            replay_buffer: self.archive.replay_buffer,
            compression: None,
            codec_config: self.body_codec_config.clone(),
        }
    }

    fn segment_partitions(&self, segment: u64) -> [String; 3] {
        ["bodies", "metadata", "markers"].map(|family| self.segment_prefix(family, segment))
    }

    async fn remove_partitions(context: &E, partitions: [String; 3]) -> Result<(), Error> {
        for partition in partitions {
            match context.remove(&partition, None).await {
                Ok(()) | Err(commonware_runtime::Error::PartitionMissing(_)) => {}
                Err(err) => return Err(journal::Error::from(err).into()),
            }
        }
        Ok(())
    }

    fn segment_len(segment: &Segment<E, H, B>) -> Result<u64, Error> {
        match segment.size(0) {
            Ok(size) => Ok(size / PendingRecord::<H::Digest>::SIZE as u64),
            Err(journal::Error::SectionOutOfRange(_)) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    /// A manifest coordinate owns its partitions. Reuse removes unreachable residue before
    /// opening writers; recovery validates uncommitted bodies before their records become custody.
    #[allow(clippy::type_complexity)]
    fn open_segment(
        &self,
        segment: u64,
    ) -> impl Future<Output = Result<(Segment<E, H, B>, Vec<PendingRecord<H::Digest>>), Error>>
    + Send
    + use<T, E, H, B> {
        let context = self
            .context
            .child("segment")
            .with_attribute("segment", segment);
        let config = self.segment_config(segment);
        let partitions = self.segment_partitions(segment);
        let markers = partitions[2].clone();
        let exists = self.segments.contains(&segment);
        let capacity = self.segment_capacity;
        async move {
            if !exists {
                Self::remove_partitions(&context, partitions).await?;
            }
            let mut replay = Segment::<E, H, B>::init_with_metadata(
                &context,
                config,
                markers,
                ReadOptions::default(),
            )
            .await?;
            let mut rows = Vec::new();
            let mut end = 0u64;
            while let Some(row) = replay.next().await {
                let (section, position, row) = row?;
                if section != 0 || position != rows.len() as u64 || position >= capacity {
                    return Err(Error::Inconsistent("pending segment bounds are invalid"));
                }
                if row.offset != end {
                    return Err(Error::Inconsistent("pending body location is invalid"));
                }
                end = row
                    .offset
                    .checked_add(u64::from(row.size))
                    .ok_or(Error::Inconsistent("pending body location overflows"))?;
                rows.push(row);
            }
            Ok((replay.finish_tracked().await?, rows))
        }
    }

    fn state_snapshot(&self) -> Result<PendingState, Error> {
        let state = PendingState {
            version: STATE_VERSION,
            segment_capacity: self.segment_capacity,
            floors: self.floors.clone(),
            segments: self.segments.iter().copied().collect(),
        };
        if state_blob_size(&state).is_none_or(|size| size > self.max_state_bytes.get()) {
            return Err(Error::Inconsistent(
                "pending state exceeds configured bound",
            ));
        }
        Ok(state)
    }

    fn stage_state(&mut self) -> Result<(), Error> {
        let state = self.state_snapshot()?;
        self.state
            .as_mut()
            .expect("catalog owns pending state")
            .put(Unit, state);
        self.state_dirty = true;
        Ok(())
    }

    async fn persist_state(&mut self) -> Result<(), Error> {
        let state = self.state_snapshot()?;
        let metadata = self.state.take().expect("catalog owns pending state");
        self.state = Some(metadata.put_sync(Unit, state).await?);
        self.state_dirty = false;
        Ok(())
    }

    async fn ensure_segment(&mut self, segment: u64) -> Result<(), Error> {
        if self.open_segments.contains_key(&segment) {
            return Ok(());
        }
        let exists = self.segments.contains(&segment);
        let (opened, _) = self.open_segment(segment).await?;
        self.open_segments.insert(segment, Some(opened));
        if exists {
            Ok(())
        } else {
            self.segments.insert(segment);
            self.stage_state()
        }
    }

    async fn reclaim_segments(&mut self, segments: &[u64]) -> Result<Vec<u64>, Error> {
        let current = self.segments.last().copied();
        let segments = segments
            .iter()
            .copied()
            .filter(|segment| Some(*segment) != current)
            .collect::<Vec<_>>();
        if segments.is_empty() {
            return Ok(segments);
        }
        if segments
            .iter()
            .any(|segment| !self.segments.contains(segment))
        {
            return Err(Error::Inconsistent("pending segment manifest changed"));
        }
        for segment in &segments {
            self.open_segments.remove(segment);
            self.active_readers.remove(segment);
            self.segments.remove(segment);
        }
        // The manifest stops naming each segment before physical destruction, so a crash during
        // destruction leaves only unreachable residue, which reuse removes. Retired contents
        // are removed by name: they are never reopened or replayed.
        self.persist_state().await?;
        for batch in segments.chunks(BODY_READ_CONCURRENCY) {
            futures::future::try_join_all(batch.iter().map(|&segment| {
                Self::remove_partitions(&self.context, self.segment_partitions(segment))
            }))
            .await?;
        }
        Ok(segments)
    }

    fn remember(&mut self, position: u64, row: PendingRecord<H::Digest>) -> Result<(), Error> {
        let PendingRecord { meta, offset, size } = row;
        let digest = meta.header().digest::<H>();
        let reference = validated_reference::<H>(&meta, digest, self.epoch, self.by_chain.len())
            .ok_or(Error::Inconsistent(
                "block metadata has an invalid identity",
            ))?;
        let chain = reference.chain().get() as usize;
        if reference.height() < self.floors[chain] {
            return Ok(());
        }
        if let Some(existing) = self.by_digest.get(&digest) {
            if existing.position != position
                || existing.reference != reference
                || existing.meta != meta
                || existing.offset != offset
                || existing.size != size
            {
                return Err(Error::Inconsistent(
                    "one digest names multiple pending blocks",
                ));
            }
            return Ok(());
        }
        if self.by_position.insert(position, digest).is_some() {
            return Err(Error::Inconsistent(
                "one pending position names multiple blocks",
            ));
        }
        self.by_chain[chain]
            .entry(reference.height())
            .or_default()
            .push(digest);
        self.by_digest.insert(
            digest,
            Entry {
                position,
                reference,
                meta,
                offset,
                size,
            },
        );
        Ok(())
    }

    fn advance_floors(&mut self, floors: &[Option<Height>]) -> Result<bool, Error> {
        if floors.len() != self.by_chain.len() {
            return Err(Error::Inconsistent("pending prune chain count differs"));
        }
        let mut changed = false;
        for (chain, floor) in floors.iter().enumerate() {
            let Some(floor) = floor else { continue };
            let floor = (*floor).max(self.floors[chain]);
            if floor == self.floors[chain] {
                continue;
            }
            changed = true;
            self.floors[chain] = floor;
            let retained = self.by_chain[chain].split_off(&floor);
            let removed = std::mem::replace(&mut self.by_chain[chain], retained);
            for digest in removed.into_values().flatten() {
                if let Some(entry) = self.by_digest.remove(&digest) {
                    self.by_position.remove(&entry.position);
                }
            }
        }
        Ok(changed)
    }

    /// Returns the configured producer-chain count.
    pub(in crate::multimmit::marshal) const fn chain_count(&self) -> usize {
        self.by_chain.len()
    }

    /// Returns whether this reference remains eligible for temporary custody.
    pub(in crate::multimmit::marshal) fn admits(&self, reference: BlockRef<H::Digest>) -> bool {
        self.floors
            .get(reference.chain().get() as usize)
            .is_some_and(|floor| reference.height() >= *floor)
    }

    /// Buffers a complete block at one globally unique archive position.
    #[cfg(test)]
    pub(in crate::multimmit::marshal) async fn put(
        &mut self,
        reference: BlockRef<H::Digest>,
        block: Arc<TransactionBlock<H, B>>,
    ) -> Result<(), Error> {
        if let Some(append) = self.start_put(&mut [(reference, block)].into_iter())? {
            let append = append.await?;
            self.finish_put(append)?;
        }
        Ok(())
    }

    /// Consumes blocks through one segment boundary and lends its writer.
    /// Exact duplicates consume no positions; completed rows retain input order.
    ///
    /// Only immutable body reads may run until `finish_put` returns the writer. The occupied
    /// segment slot keeps an appendable segment from being mistaken for a cold immutable one.
    #[allow(clippy::type_complexity)]
    pub(in crate::multimmit::marshal) fn start_put(
        &mut self,
        blocks: &mut dyn Iterator<Item = (BlockRef<H::Digest>, Arc<TransactionBlock<H, B>>)>,
    ) -> Result<
        Option<impl Future<Output = Result<Append<E, H, B>, Error>> + Send + use<T, E, H, B>>,
        Error,
    > {
        let position = self.next_position;
        let segment_id = self.segment_id(position);
        let local = position % self.segment_capacity;
        let mut entries: Vec<(PendingRecord<H::Digest>, StoredBody<H, B>)> = Vec::new();
        let mut seen: HashMap<H::Digest, usize> = HashMap::new();
        while (entries.len() as u64) < self.segment_capacity - local {
            let Some((reference, block)) = blocks.next() else {
                break;
            };
            if !self.admits(reference) {
                return Err(Error::Inconsistent(
                    "pending block is outside the custody floor",
                ));
            }
            let digest = reference.digest();
            let meta = BlockMeta::new(
                block.header().clone(),
                u64::try_from(block.encode_size())
                    .map_err(|_| Error::Inconsistent("encoded block length exceeds u64"))?,
            );
            if block.reference() != reference
                || validated_reference::<H>(&meta, digest, self.epoch, self.by_chain.len())
                    != Some(reference)
            {
                return Err(Error::Inconsistent("pending block identity is invalid"));
            }
            if let Some(entry) = self.by_digest.get(&digest) {
                if entry.reference != reference || entry.meta != meta {
                    return Err(Error::Inconsistent("pending digest identity changed"));
                }
                continue;
            }
            if let Some(&index) = seen.get(&digest) {
                if entries[index].0.meta != meta {
                    return Err(Error::Inconsistent("pending digest identity changed"));
                }
                continue;
            }
            seen.insert(digest, entries.len());
            entries.push((PendingRecord::new(meta), Shared::new(block)));
        }
        if entries.is_empty() {
            return Ok(None);
        }
        let count = u64::try_from(entries.len())
            .map_err(|_| Error::Inconsistent("pending position overflow"))?;
        position
            .checked_add(count)
            .ok_or(Error::Inconsistent("pending position overflow"))?;
        let segment = self.open_segments.insert(segment_id, None);
        let opening = segment.is_none().then(|| self.open_segment(segment_id));
        Ok(Some(async move {
            let mut segment = match segment {
                Some(segment) => segment.expect("append owns pending segment"),
                None => opening.expect("a missing segment has open inputs").await?.0,
            };
            if Self::segment_len(&segment)? != local {
                return Err(Error::Inconsistent(
                    "pending segment does not match the append coordinate",
                ));
            }
            let last;
            let locations;
            (segment, last, locations) = segment.append_many(0, &entries).await?;
            if last != local + count - 1 {
                return Err(Error::Inconsistent(
                    "pending segment assigned an unexpected position",
                ));
            }
            let rows = entries
                .into_iter()
                .zip(locations)
                .map(|((mut row, _), (offset, size))| {
                    row.offset = offset;
                    row.size = size;
                    row
                })
                .collect();
            Ok(Append {
                position,
                rows,
                segment,
            })
        }))
    }

    /// Publishes one completed append; no other mutable operation may have intervened.
    pub(in crate::multimmit::marshal) fn finish_put(
        &mut self,
        append: Append<E, H, B>,
    ) -> Result<(), Error> {
        let Append {
            position,
            rows,
            segment,
        } = append;
        assert_eq!(position, self.next_position, "appends complete in order");
        let segment_id = self.segment_id(position);
        self.open_segments.insert(segment_id, Some(segment));
        if self.segments.insert(segment_id) {
            self.stage_state()?;
        }
        self.dirty_segments.insert(segment_id);
        for meta in rows {
            self.remember(self.next_position, meta)?;
            self.next_position += 1;
        }
        Ok(())
    }

    /// Starts one shared durability cut for every buffered producer block.
    pub(in crate::multimmit::marshal) async fn start_sync(
        &mut self,
    ) -> Result<Vec<Handle<()>>, Error> {
        let dirty = std::mem::take(&mut self.dirty_segments);
        let current = self.segments.last().copied();
        // The previous cut completed before this one starts. Its non-current readers can be
        // released; outstanding read plans own their snapshots and cold reads open only values.
        self.active_readers
            .retain(|segment, _| Some(*segment) == current);
        let mut cuts = Vec::with_capacity(dirty.len());
        for segment_id in dirty {
            let segment = self
                .open_segments
                .remove(&segment_id)
                .ok_or(Error::Inconsistent("dirty pending segment is missing"))?
                .expect("catalog owns pending segment");
            cuts.push(async move {
                let count = Self::segment_len(&segment)?;
                let (segment, handle, reader) = segment.start_sync_with_snapshot(0).await?;
                Ok::<_, Error>((segment_id, segment, handle, reader, count))
            });
        }

        let dirty_state = self.state_dirty;
        let state = dirty_state.then(|| self.state.take().expect("catalog owns pending state"));
        let (cuts, state) = futures::try_join!(futures::future::try_join_all(cuts), async move {
            let Some(state) = state else {
                return Ok::<_, Error>(None);
            };
            let (state, handle) = state.start_sync().await?;
            Ok(Some((state, handle)))
        },)?;

        let mut handles = Vec::with_capacity(cuts.len() + usize::from(dirty_state));
        for (segment_id, segment, handle, reader, count) in cuts {
            self.open_segments.insert(segment_id, Some(segment));
            handles.push(handle);
            let immutable = count == self.segment_capacity;
            self.active_readers.insert(
                segment_id,
                BodyReader::new(segment_id, self.segment_capacity, reader, immutable),
            );
        }
        // A full segment's final cut is in flight: keep its journals until the cut completes so
        // start_retire can persist their final checkpoints.
        let capacity = self.segment_capacity;
        self.open_segments.retain(|segment, journals| {
            Some(*segment) == current
                || Self::segment_len(journals.as_ref().expect("catalog owns pending segment"))
                    .expect("synced segment has a valid length")
                    == capacity
        });
        if let Some((state, handle)) = state {
            self.state = Some(state);
            handles.push(handle);
        }
        self.state_dirty = false;
        Ok(handles)
    }

    /// Closes full segments after their final admission cut, persisting recovery markers.
    /// The caller must return their identifiers through [`Self::finish_retire`] before they
    /// can be reclaimed. Dirty full segments wait for the cut covering their final appends.
    #[allow(clippy::type_complexity)]
    pub(in crate::multimmit::marshal) fn start_retire(
        &mut self,
    ) -> (Vec<u64>, Vec<BoxFuture<'static, Result<(), Error>>>) {
        let full = self
            .open_segments
            .iter()
            .filter(|(segment, journals)| {
                Self::segment_len(journals.as_ref().expect("catalog owns pending segment"))
                    .expect("synced segment has a valid length")
                    == self.segment_capacity
                    && !self.dirty_segments.contains(segment)
                    && !self.retiring.contains(segment)
            })
            .map(|(&segment, _)| segment)
            .collect::<Vec<_>>();
        let retirements = full
            .iter()
            .map(|segment_id| {
                let segment = self
                    .open_segments
                    .remove(segment_id)
                    .expect("full pending segment was just observed")
                    .expect("catalog owns pending segment");
                self.retiring.insert(*segment_id);
                Box::pin(async move { segment.close().await.map_err(Error::from) })
                    as BoxFuture<'static, Result<(), Error>>
            })
            .collect();
        (full, retirements)
    }

    /// Releases completed marker writers and reclaims any empty, unpinned segments that
    /// pruning deferred while their markers were being persisted.
    pub(in crate::multimmit::marshal) async fn finish_retire(
        &mut self,
        segments: Vec<u64>,
        pinned: &BTreeSet<u64>,
    ) -> Result<Vec<u64>, Error> {
        let current = self.segments.last().copied();
        for segment in segments {
            self.retiring.remove(&segment);
            if Some(segment) != current {
                self.active_readers.remove(&segment);
            }
        }
        if !self.dirty_segments.is_empty() || self.state_dirty {
            return Ok(Vec::new());
        }
        self.reclaim_unpinned(pinned).await
    }

    /// Clones snapshots whose segments cannot accept another append.
    pub(in crate::multimmit::marshal) fn immutable_body_readers(&self) -> Vec<BodyReader<E, H, B>> {
        self.active_readers
            .values()
            .filter(|reader| reader.immutable)
            .cloned()
            .collect()
    }

    const fn locator(entry: &Entry<H::Digest>) -> BodyLocator<H::Digest> {
        BodyLocator {
            position: entry.position,
            reference: entry.reference,
            encoded_len: entry.meta.encoded_len(),
            offset: entry.offset,
            size: entry.size,
        }
    }

    fn body_source(&self, segment: u64) -> BodySource<E, H, B> {
        self.active_readers.get(&segment).map_or_else(
            || {
                BodySource::Cold(ColdSource {
                    context: self.body_context(segment),
                    config: self.body_config(segment),
                    segment,
                    segment_capacity: self.segment_capacity,
                    _marker: PhantomData,
                })
            },
            |reader| BodySource::Ready(reader.clone()),
        )
    }

    /// Plans bounded, immutable reads for exact locally stored references.
    ///
    /// Positions beyond the latest snapshot remain unavailable until their admission cut starts.
    /// Non-current segments are safe to open independently because they can no longer be
    /// appended. Each group contains positions from one segment in ascending order. Byte-safe groups are
    /// split to expose up to `max_groups` jobs. Groups do not exceed an equal share of `max_bytes`,
    /// except that one individually oversized block is admitted alone so reads can make progress.
    /// A block is never split.
    pub(in crate::multimmit::marshal) fn body_read_groups(
        &self,
        references: impl IntoIterator<Item = (usize, BlockRef<H::Digest>)>,
        max_bytes: u64,
        max_groups: usize,
    ) -> Result<Vec<BodyReadGroup<E, H, B>>, Error> {
        debug_assert!(max_bytes > 0);
        debug_assert!(max_groups > 0);
        let mut by_segment = BTreeMap::<u64, Vec<_>>::new();
        let mut total_entries = 0usize;
        for (output, reference) in references {
            let Some(entry) = self.by_digest.get(&reference.digest()) else {
                continue;
            };
            if entry.reference != reference {
                continue;
            }
            let segment = self.segment_id(entry.position);
            let readable = self
                .active_readers
                .get(&segment)
                .is_some_and(|reader| reader.contains(Self::locator(entry)))
                || !self.open_segments.contains_key(&segment);
            if readable {
                total_entries = total_entries
                    .checked_add(1)
                    .ok_or(Error::Inconsistent("pending body read count overflow"))?;
                by_segment
                    .entry(segment)
                    .or_default()
                    .push((output, Self::locator(entry)));
            }
        }

        let budget_group_bytes = max_bytes
            .checked_div(u64::try_from(max_groups).unwrap_or(u64::MAX))
            .unwrap_or(0)
            .max(1);
        let mut byte_groups = Vec::new();
        for (segment, mut entries) in by_segment {
            entries.sort_unstable_by_key(|(_, locator)| locator.position);
            let mut chunk = Vec::new();
            let mut chunk_bytes = 0u64;
            for entry @ (_, locator) in entries {
                let next_bytes = chunk_bytes.checked_add(locator.encoded_len);
                if !chunk.is_empty() && next_bytes.is_none_or(|bytes| bytes > budget_group_bytes) {
                    byte_groups.push((segment, chunk_bytes, std::mem::take(&mut chunk)));
                    chunk_bytes = 0;
                }
                chunk_bytes = chunk_bytes
                    .checked_add(locator.encoded_len)
                    .ok_or(Error::Inconsistent("pending body read bytes overflow"))?;
                chunk.push(entry);
            }
            if !chunk.is_empty() {
                byte_groups.push((segment, chunk_bytes, chunk));
            }
        }

        let target_groups = max_groups.min(total_entries).max(byte_groups.len());
        let mut allocations = vec![1usize; byte_groups.len()];
        for _ in byte_groups.len()..target_groups {
            let index = (0..byte_groups.len())
                .filter(|&index| allocations[index] < byte_groups[index].2.len())
                .max_by_key(|&index| {
                    byte_groups[index]
                        .1
                        .div_ceil(u64::try_from(allocations[index]).unwrap_or(u64::MAX))
                })
                .expect("a body read group can be split toward its job target");
            allocations[index] += 1;
        }

        let mut groups = Vec::with_capacity(target_groups);
        for ((segment, mut remaining_bytes, entries), mut remaining_groups) in
            byte_groups.into_iter().zip(allocations)
        {
            let mut entries = entries.into_iter().peekable();
            let mut remaining_entries = entries.len();
            while remaining_groups > 0 {
                let target_bytes =
                    remaining_bytes.div_ceil(u64::try_from(remaining_groups).unwrap_or(u64::MAX));
                let mut chunk = Vec::new();
                let mut chunk_bytes = 0u64;
                while remaining_entries > remaining_groups - 1 {
                    let locator = &entries.peek().expect("a planned body entry exists").1;
                    let next_bytes = chunk_bytes.checked_add(locator.encoded_len);
                    if !chunk.is_empty() && next_bytes.is_none_or(|bytes| bytes > target_bytes) {
                        break;
                    }
                    chunk_bytes = next_bytes
                        .expect("a byte-safe body group cannot overflow its encoded length");
                    remaining_bytes = remaining_bytes
                        .checked_sub(locator.encoded_len)
                        .expect("a byte group covers its planned entries");
                    remaining_entries -= 1;
                    chunk.push(entries.next().expect("a peeked body read entry exists"));
                }
                groups.push(BodyReadGroup::new(
                    self.body_source(segment),
                    chunk,
                    self.archive.replay_buffer,
                )?);
                remaining_groups -= 1;
            }
        }
        Ok(groups)
    }

    /// Returns the exact pending reference named by a chain and header digest.
    pub(in crate::multimmit::marshal) fn reference_by_digest(
        &self,
        chain: ChainId,
        digest: H::Digest,
    ) -> Option<BlockRef<H::Digest>> {
        self.by_digest
            .get(&digest)
            .map(|entry| entry.reference)
            .filter(|reference| reference.chain() == chain)
    }

    #[cfg(test)]
    async fn read_live_body(
        &self,
        segment: u64,
        locator: BodyLocator<H::Digest>,
    ) -> Result<Arc<TransactionBlock<H, B>>, Error> {
        let block = self
            .open_segments
            .get(&segment)
            .ok_or(Error::Inconsistent("active pending segment is missing"))?
            .as_ref()
            .expect("catalog owns pending segment")
            .get_value(0, locator.offset, locator.size)
            .await?
            .into_inner();
        validate_body(&block, locator)?;
        Ok(block)
    }

    /// Returns a complete block only when its body survived storage recovery.
    #[cfg(test)]
    pub(in crate::multimmit::marshal) async fn block(
        &self,
        reference: BlockRef<H::Digest>,
    ) -> Result<Option<Arc<TransactionBlock<H, B>>>, Error> {
        let Some(entry) = self.by_digest.get(&reference.digest()) else {
            return Ok(None);
        };
        if entry.reference != reference {
            return Ok(None);
        }
        let segment = self.segment_id(entry.position);
        let locator = Self::locator(entry);
        if self
            .active_readers
            .get(&segment)
            .is_some_and(|reader| reader.contains(Self::locator(entry)))
        {
            let reader = self.body_source(segment).open().await?;
            return reader.read(locator).await.map(Some);
        }
        if self.open_segments.contains_key(&segment) {
            return self.read_live_body(segment, locator).await.map(Some);
        }
        let reader = self.body_source(segment).open().await?;
        reader.read(locator).await.map(Some)
    }

    #[cfg(test)]
    async fn blocks(
        &self,
        references: &[BlockRef<H::Digest>],
    ) -> Result<Vec<Option<Arc<TransactionBlock<H, B>>>>, Error> {
        let groups = self.body_read_groups(references.iter().copied().enumerate(), u64::MAX, 1)?;
        let mut blocks = vec![None; references.len()];
        for (output, block) in
            futures::future::try_join_all(groups.into_iter().map(BodyReadGroup::read))
                .await?
                .into_iter()
                .flatten()
        {
            blocks[output] = Some(block);
        }
        Ok(blocks)
    }

    /// Returns compact metadata for a complete stored body.
    pub(in crate::multimmit::marshal) fn custody_meta(
        &self,
        reference: BlockRef<H::Digest>,
    ) -> Option<BlockMeta<H::Digest>> {
        self.by_digest
            .get(&reference.digest())
            .filter(|entry| entry.reference == reference)
            .map(|entry| entry.meta.clone())
    }

    /// Returns a header recovered with its corresponding body.
    pub(in crate::multimmit::marshal) fn header(
        &self,
        reference: BlockRef<H::Digest>,
    ) -> Option<TransactionBlockHeader<H::Digest>> {
        self.by_digest
            .get(&reference.digest())
            .filter(|entry| entry.reference == reference)
            .map(|entry| entry.meta.header().clone())
    }

    async fn reclaim_unpinned(&mut self, pinned: &BTreeSet<u64>) -> Result<Vec<u64>, Error> {
        let empty = self
            .segments
            .iter()
            .copied()
            .filter(|segment| {
                !pinned.contains(segment)
                    && !self.retiring.contains(segment)
                    && !self.segment_has_live_blocks(*segment)
            })
            .collect::<Vec<_>>();
        self.reclaim_segments(&empty).await
    }

    /// Reclaims chain-local heights below each supplied floor and returns destroyed segments.
    pub(in crate::multimmit::marshal) async fn prune(
        &mut self,
        floors: &[Option<Height>],
        pinned: &BTreeSet<u64>,
    ) -> Result<Vec<u64>, Error> {
        if !self.dirty_segments.is_empty() || self.state_dirty {
            return Err(Error::Inconsistent(
                "pending prune crossed a durability cut",
            ));
        }
        let changed = self.advance_floors(floors)?;

        // Floors become authoritative before reclamation, so recovery can filter dead metadata
        // after a crash. Only whole non-current segments are destroyed; local positions never
        // move, and the retained current journal continues to own the append cursor.
        if changed {
            self.persist_state().await?;
        }
        self.reclaim_unpinned(pinned).await
    }
}

#[cfg(test)]
mod tests;
