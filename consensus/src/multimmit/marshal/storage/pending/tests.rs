use super::*;
use crate::{marshal::mocks::block::EmptyBlock, multimmit::marshal::config::ArchiveConfig};
use commonware_cryptography::{Sha256, sha256::Digest as Sha256Digest};
use commonware_runtime::{
    Blob as _, Metrics as _, Runner as _, Storage as _, Supervisor as _, WriteOptions,
    buffer::paged::CacheRef,
    deterministic::{self, Context as DeterministicContext},
};
use commonware_storage::{
    journal::segmented::{fixed, glob, oversized::Record as _},
    translator::TwoCap,
};
use commonware_utils::{NZU16, NZU64, NZUsize};
use rstest::rstest;

type TestBody = EmptyBlock<Sha256>;
type InnerTestStore = PendingBlocks<TwoCap, DeterministicContext, Sha256, TestBody>;

struct TestStore(InnerTestStore);

impl std::ops::Deref for TestStore {
    type Target = InnerTestStore;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for TestStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TestStore {
    async fn put(
        &mut self,
        reference: BlockRef<Sha256Digest>,
        block: Arc<TransactionBlock<Sha256, TestBody>>,
    ) -> Result<(), Error> {
        Box::pin(self.0.put(reference, block)).await
    }

    async fn put_many(&mut self, blocks: &[Arc<TransactionBlock<Sha256, TestBody>>]) {
        Box::pin(async {
            let mut blocks = blocks
                .iter()
                .map(|block| (block.reference(), block.clone()))
                .peekable();
            while blocks.peek().is_some() {
                if let Some(append) = self.0.start_put(&mut blocks).unwrap() {
                    let append = Box::pin(append).await.unwrap();
                    self.0.finish_put(append).unwrap();
                }
            }
        })
        .await
    }

    fn locator(entry: &Entry<Sha256Digest>) -> BodyLocator<Sha256Digest> {
        InnerTestStore::locator(entry)
    }
}

async fn open_with_capacity(
    context: &DeterministicContext,
    label: &'static str,
    prefix: &str,
    segment_capacity: NonZeroU64,
) -> TestStore {
    let config = ArchiveConfig::new(
        TwoCap,
        CacheRef::from_pooler(context, NZU16!(1024), NZUsize!(10)),
    );
    TestStore(
        Box::pin(InnerTestStore::init(
            context.child(label),
            config,
            prefix.to_string(),
            (),
            Epoch::new(7),
            2,
            segment_capacity,
            NZUsize!(1024 * 1024),
        ))
        .await
        .unwrap(),
    )
}

async fn open(context: &DeterministicContext, label: &'static str, prefix: &str) -> TestStore {
    open_with_capacity(context, label, prefix, NZU64!(2)).await
}

fn block(chain: u32, height: u64, nonce: u64) -> Arc<TransactionBlock<Sha256, TestBody>> {
    let body = TestBody::new(
        Sha256::hash(&[b"application parent"]),
        Height::new(height),
        nonce,
    );
    let header = TransactionBlockHeader::new(
        Epoch::new(7),
        ChainId::new(chain),
        Height::new(height),
        Sha256Digest::from([chain as u8; 32]),
        body.digest(),
    )
    .unwrap();
    Arc::new(TransactionBlock::new(header, body).unwrap())
}

/// One production-shaped durability round: the admission cut, then the retirements the
/// catalog starts once the cut completes and releases once they finish.
async fn sync(store: &mut TestStore) {
    futures::future::try_join_all(store.start_sync().await.unwrap())
        .await
        .unwrap();
    let (retiring, retirements) = store.start_retire();
    futures::future::try_join_all(retirements).await.unwrap();
    store
        .finish_retire(retiring, &BTreeSet::new())
        .await
        .unwrap();
}

/// Storage bytes read, writes, and syncs recorded so far.
fn storage_io(context: &DeterministicContext) -> (u64, u64, u64) {
    let encoded = context.encode();
    (
        counter(&encoded, "storage_read_bytes"),
        counter(&encoded, "storage_writes"),
        counter(&encoded, "storage_syncs"),
    )
}

#[test]
fn append_read_and_exact_idempotence() {
    deterministic::Runner::default().start(|context| async move {
        let block = block(0, 1, 1);
        let reference = block.reference();
        let mut store = open(&context, "store", "pending_append_read").await;
        store.put(reference, Arc::clone(&block)).await.unwrap();
        store.put(reference, Arc::clone(&block)).await.unwrap();
        assert_eq!(store.next_position, 1);
        let segment = store.open_segments.get(&0).unwrap().as_ref().unwrap();
        assert_eq!(InnerTestStore::segment_len(segment).unwrap(), 1);
        sync(&mut store).await;

        assert_eq!(
            store.block(reference).await.unwrap().as_deref(),
            Some(block.as_ref())
        );
        let blocks = store.blocks(&[reference, reference]).await.unwrap();
        assert_eq!(blocks[0].as_deref(), Some(block.as_ref()));
        assert_eq!(blocks[1].as_deref(), Some(block.as_ref()));

        let entry = store.by_digest.get(&reference.digest()).unwrap();
        let mut locator = TestStore::locator(entry);
        locator.encoded_len += 1;
        let reader = store.active_readers.get(&0).unwrap();
        assert!(reader.read(locator).await.is_err());
    });
}

#[test]
fn batched_appends_match_single_rows_across_segments_and_reopen() {
    deterministic::Runner::default().start(|context| async move {
        let mut single = open_with_capacity(&context, "single", "pending_single", NZU64!(3)).await;
        let mut batch = open_with_capacity(&context, "batch", "pending_batch", NZU64!(3)).await;
        let blocks = (0..7)
            .map(|i| block((i % 2) as u32, i + 1, i))
            .collect::<Vec<_>>();
        for store in [&mut single, &mut batch] {
            store
                .put(blocks[0].reference(), blocks[0].clone())
                .await
                .unwrap();
        }
        let input = [0, 1, 1, 2, 3, 3, 4, 5, 6, 6].map(|i| blocks[i].clone());
        for block in &input {
            single.put(block.reference(), block.clone()).await.unwrap();
        }
        batch.put_many(&[]).await;
        batch.put_many(&input).await;
        batch.put_many(&input).await;
        for store in [&mut single, &mut batch] {
            assert_eq!(store.next_position, blocks.len() as u64);
            for (&id, segment) in &store.open_segments {
                let segment = segment.as_ref().unwrap();
                let expected = (blocks.len() as u64 - id * 3).min(3);
                assert_eq!(InnerTestStore::segment_len(segment).unwrap(), expected);
            }
            sync(store).await;
        }
        let refs = blocks
            .iter()
            .map(|block| block.reference())
            .collect::<Vec<_>>();
        for reopened in [false, true] {
            if reopened {
                drop(single);
                drop(batch);
                single = open_with_capacity(&context, "single_reopen", "pending_single", NZU64!(3))
                    .await;
                batch =
                    open_with_capacity(&context, "batch_reopen", "pending_batch", NZU64!(3)).await;
            }
            assert_eq!(single.next_position, batch.next_position);
            assert_eq!(single.segments, batch.segments);
            for reference in &refs {
                assert_eq!(single.header(*reference), batch.header(*reference));
                assert_eq!(
                    single.by_digest[&reference.digest()].position,
                    batch.by_digest[&reference.digest()].position
                );
            }
            let expected = blocks.iter().cloned().map(Some).collect::<Vec<_>>();
            assert_eq!(single.blocks(&refs).await.unwrap(), expected);
            assert_eq!(batch.blocks(&refs).await.unwrap(), expected);
        }
    });
}

#[test]
fn owned_append_preserves_snapshot_and_publishes_only_on_completion() {
    deterministic::Runner::default().start(|context| async move {
        let mut store =
            open_with_capacity(&context, "store", "pending_owned_append", NZU64!(4)).await;
        let first = block(0, 1, 1);
        let second = block(0, 2, 2);
        let third = block(1, 1, 3);
        store.put(first.reference(), first.clone()).await.unwrap();
        sync(&mut store).await;

        let append = store
            .start_put(
                &mut [&second, &second, &third]
                    .into_iter()
                    .map(|block| (block.reference(), block.clone())),
            )
            .unwrap()
            .unwrap();
        assert!(matches!(store.open_segments.get(&0), Some(None)));
        let refs = [
            (0, first.reference()),
            (1, second.reference()),
            (2, third.reference()),
        ];
        let mut groups = store.body_read_groups(refs, u64::MAX, 1).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups.pop().unwrap().read().await.unwrap(),
            vec![(0, first.clone())]
        );
        assert_eq!(store.next_position, 1);
        for block in [&second, &third] {
            assert!(!store.by_digest.contains_key(&block.reference().digest()));
        }

        let append = Box::pin(append).await.unwrap();
        for block in [&second, &third] {
            assert!(!store.by_digest.contains_key(&block.reference().digest()));
        }
        store.finish_put(append).unwrap();
        assert!(matches!(store.open_segments.get(&0), Some(Some(_))));
        assert_eq!(store.next_position, 3);
        for block in [&second, &third] {
            assert!(store.by_digest.contains_key(&block.reference().digest()));
        }
        assert!(
            store
                .body_read_groups(
                    [(0, second.reference()), (1, third.reference())],
                    u64::MAX,
                    1
                )
                .unwrap()
                .is_empty()
        );
        sync(&mut store).await;
        let values = store
            .blocks(&[first.reference(), second.reference(), third.reference()])
            .await
            .unwrap();
        assert_eq!(values, vec![Some(first), Some(second), Some(third)]);
    });
}

#[test]
fn segment_rollover_owns_one_oversized_section_per_segment() {
    deterministic::Runner::default().start(|context| async move {
        let prefix = "pending_single_blob_segments";
        let mut store = open(&context, "store", prefix).await;
        let references = fill_first_segment(&mut store).await;

        for segment in 0..=1 {
            for family in ["bodies", "metadata"] {
                let partition = format!("{prefix}_{family}_{segment}");
                assert_eq!(
                    context.scan(&partition).await.unwrap(),
                    vec![0u64.to_be_bytes().to_vec()],
                    "pending segments own one oversized section: {partition}"
                );
            }
        }

        drop(store);
        let mut store = open(&context, "reopened", prefix).await;
        for reference in references {
            assert_eq!(
                store.block(reference).await.unwrap().unwrap().reference(),
                reference
            );
        }
        let next = block(1, 2, 4);
        let reference = next.reference();
        store.put(reference, next).await.unwrap();
        sync(&mut store).await;
        assert_eq!(store.next_position, 4);
        assert_eq!(
            store.block(reference).await.unwrap().unwrap().reference(),
            reference
        );
    });
}

#[test]
fn only_full_snapshots_are_exposed_as_immutable() {
    deterministic::Runner::default().start(|context| async move {
        let mut store = open(&context, "store", "pending_snapshot_bound").await;
        for height in 1..=5 {
            let block = block(0, height, height);
            store.put(block.reference(), block).await.unwrap();
        }

        // The completed cut is the catalog's offer point: every cut segment has a frozen
        // reader, and only the full ones are exposed as immutable.
        futures::future::try_join_all(store.start_sync().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            store.active_readers.keys().copied().collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            store
                .immutable_body_readers()
                .iter()
                .map(BodyReader::segment)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        // Retirement keeps full segments readable while it seals their markers. The next
        // admission cut may drop advisory readers because cold opens read immutable values.
        let (retiring, retirements) = store.start_retire();
        assert_eq!(retiring, vec![0, 1]);
        assert_eq!(
            store.active_readers.keys().copied().collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            store.open_segments.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );
        futures::future::try_join_all(store.start_sync().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            store.active_readers.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );
        let (_, writes, syncs) = storage_io(&context);
        futures::future::try_join_all(retirements).await.unwrap();
        let (_, writes_after, syncs_after) = storage_io(&context);
        assert_eq!(
            writes_after - writes,
            2,
            "one marker update per retired segment"
        );
        assert_eq!(syncs_after - syncs, 2, "no data fsync beyond the markers");
        store
            .finish_retire(retiring, &BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(
            store.active_readers.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );

        sync(&mut store).await;
        assert_eq!(
            store.active_readers.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );
        assert!(store.immutable_body_readers().is_empty());
    });
}

#[test]
fn retiring_segments_defer_reclamation_until_they_finish() {
    deterministic::Runner::default().start(|context| async move {
        let mut store = open(&context, "store", "pending_retiring_reclaim").await;
        for height in 1..=5 {
            let block = block(0, height, height);
            store.put(block.reference(), block).await.unwrap();
        }
        futures::future::try_join_all(store.start_sync().await.unwrap())
            .await
            .unwrap();
        let (retiring, retirements) = store.start_retire();
        assert_eq!(retiring, vec![0, 1]);

        // Pruning past every block must not destroy a segment whose checkpoints are still
        // being written; the retirement finishing reclaims the deferred segments itself.
        let reclaimed = store
            .prune(&[Some(Height::new(6)), None], &BTreeSet::new())
            .await
            .unwrap();
        assert!(reclaimed.is_empty());
        futures::future::try_join_all(retirements).await.unwrap();
        let reclaimed = store
            .finish_retire(retiring, &BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(reclaimed, vec![0, 1]);
    });
}

#[test]
fn asymmetric_prune_survives_reopen() {
    deterministic::Runner::default().start(|context| async move {
        let retained = block(0, 1, 1);
        let pruned = block(1, 1, 2);
        let also_pruned = block(1, 2, 3);
        let mut store = open(&context, "first", "pending_asymmetric").await;
        store
            .put(retained.reference(), Arc::clone(&retained))
            .await
            .unwrap();
        store
            .put(pruned.reference(), Arc::clone(&pruned))
            .await
            .unwrap();
        store
            .put(also_pruned.reference(), Arc::clone(&also_pruned))
            .await
            .unwrap();
        sync(&mut store).await;
        store
            .prune(&[None, Some(Height::new(3))], &BTreeSet::new())
            .await
            .unwrap();
        assert!(store.block(retained.reference()).await.unwrap().is_some());
        assert!(store.block(pruned.reference()).await.unwrap().is_none());
        assert!(
            store
                .block(also_pruned.reference())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.segments.iter().copied().collect::<Vec<_>>(),
            vec![0, 1]
        );
        drop(store);

        let store = open(&context, "reopen", "pending_asymmetric").await;
        assert!(store.block(retained.reference()).await.unwrap().is_some());
        assert!(store.block(pruned.reference()).await.unwrap().is_none());
        assert!(
            store
                .block(also_pruned.reference())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.segments.iter().copied().collect::<Vec<_>>(),
            vec![0, 1]
        );
    });
}

#[test]
fn fully_pruned_store_keeps_its_append_coordinate() {
    deterministic::Runner::default().start(|context| async move {
        let first = block(0, 1, 1);
        let second = block(0, 2, 2);
        let mut store = open(&context, "first", "pending_empty").await;
        store
            .put(first.reference(), Arc::clone(&first))
            .await
            .unwrap();
        sync(&mut store).await;
        store
            .prune(&[Some(Height::new(2)), None], &BTreeSet::new())
            .await
            .unwrap();
        drop(store);

        let mut store = open(&context, "reopen", "pending_empty").await;
        assert_eq!(store.next_position, 1);
        store
            .put(second.reference(), Arc::clone(&second))
            .await
            .unwrap();
        sync(&mut store).await;
        drop(store);

        let store = open(&context, "second_reopen", "pending_empty").await;
        assert!(store.block(second.reference()).await.unwrap().is_some());
    });
}

#[test]
fn durable_floor_precedes_physical_reclamation() {
    deterministic::Runner::default().start(|context| async move {
        let retained = block(0, 1, 1);
        let pruned = block(1, 1, 2);
        let mut store = open(&context, "first", "pending_prune_cut").await;
        store
            .put(retained.reference(), Arc::clone(&retained))
            .await
            .unwrap();
        store
            .put(pruned.reference(), Arc::clone(&pruned))
            .await
            .unwrap();
        sync(&mut store).await;

        assert!(store.advance_floors(&[None, Some(Height::new(2))]).unwrap());
        store.persist_state().await.unwrap();
        drop(store);

        let store = open(&context, "reopen", "pending_prune_cut").await;
        assert!(store.block(retained.reference()).await.unwrap().is_some());
        assert!(store.block(pruned.reference()).await.unwrap().is_none());
    });
}

#[test]
fn snapshot_survives_prune_and_segment_destruction() {
    deterministic::Runner::default().start(|context| async move {
        let first = block(0, 1, 1);
        let second = block(0, 2, 2);
        let current = block(1, 1, 3);
        let first_ref = first.reference();
        let current_ref = current.reference();
        let mut store = open(&context, "store", "pending_snapshot_prune").await;
        store.put(first_ref, Arc::clone(&first)).await.unwrap();
        store.put(second.reference(), second).await.unwrap();
        sync(&mut store).await;
        let reader = store.active_readers.get(&0).unwrap().clone();
        let locator = TestStore::locator(store.by_digest.get(&first_ref.digest()).unwrap());

        store.put(current_ref, current).await.unwrap();
        sync(&mut store).await;
        let reclaimed = store
            .prune(&[Some(Height::new(3)), None], &BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(reclaimed, vec![0]);
        assert!(!store.segments.contains(&0));
        assert_eq!(reader.read(locator).await.unwrap().reference(), first_ref);
        drop(store);

        let store = open(&context, "reopen", "pending_snapshot_prune").await;
        assert!(store.block(first_ref).await.unwrap().is_none());
        assert!(store.block(current_ref).await.unwrap().is_some());
    });
}

#[test]
fn planned_cold_read_pins_segment_until_materialized() {
    deterministic::Runner::default().start(|context| async move {
        let first = block(0, 1, 1);
        let second = block(0, 2, 2);
        let current = block(1, 1, 3);
        let first_ref = first.reference();
        let mut store = open(&context, "store", "pending_pinned_read").await;
        store.put(first_ref, first).await.unwrap();
        store.put(second.reference(), second).await.unwrap();
        sync(&mut store).await;
        store.put(current.reference(), current).await.unwrap();
        sync(&mut store).await;

        let group = store
            .body_read_groups([(0, first_ref)], u64::MAX, 1)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(group.segment(), 0);
        let reclaimed = store
            .prune(&[Some(Height::new(3)), None], &BTreeSet::from([0]))
            .await
            .unwrap();
        assert!(reclaimed.is_empty());
        assert!(store.segments.contains(&0));
        assert_eq!(group.read().await.unwrap()[0].1.reference(), first_ref);

        let reclaimed = store
            .prune(&[Some(Height::new(3)), None], &BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(reclaimed, vec![0]);
        assert!(!store.segments.contains(&0));
    });
}

#[test]
fn body_reads_fan_out_duplicate_positions_and_validate_each_locator() {
    deterministic::Runner::default().start(|context| async move {
        let mut store = open(&context, "store", "pending_duplicate_reads").await;
        let first = block(0, 1, 1);
        let second = block(0, 2, 2);
        for block in [&first, &second] {
            store
                .put(block.reference(), Arc::clone(block))
                .await
                .unwrap();
        }
        sync(&mut store).await;

        let mut groups = store
            .body_read_groups(
                [
                    (2, first.reference()),
                    (0, second.reference()),
                    (1, first.reference()),
                ],
                u64::MAX,
                1,
            )
            .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups.pop().unwrap().read().await.unwrap(),
            vec![
                (0, second),
                (1, Arc::clone(&first)),
                (2, Arc::clone(&first))
            ],
        );

        let mut group = store
            .body_read_groups(
                [(0, first.reference()), (1, first.reference())],
                u64::MAX,
                1,
            )
            .unwrap()
            .pop()
            .unwrap();
        group.entries[1].1.encoded_len += 1;
        assert!(matches!(group.read().await, Err(Error::Inconsistent(_))));
    });
}

#[test]
fn body_read_groups_preserve_contiguous_segment_run() {
    deterministic::Runner::default().start(|context| async move {
        let mut store =
            open_with_capacity(&context, "store", "pending_position_locality", NZU64!(8)).await;
        let blocks = (1..=6)
            .map(|height| block(0, height, height))
            .collect::<Vec<_>>();
        for block in &blocks {
            store
                .put(block.reference(), Arc::clone(block))
                .await
                .unwrap();
        }
        sync(&mut store).await;

        let request_order = [0, 3, 1, 4, 2, 5];
        let groups = store
            .body_read_groups(
                request_order
                    .into_iter()
                    .enumerate()
                    .map(|(output, block)| (output, blocks[block].reference())),
                u64::MAX,
                1,
            )
            .unwrap();
        let positions = groups
            .into_iter()
            .map(|group| {
                group
                    .into_parts()
                    .1
                    .entries
                    .into_iter()
                    .map(|(_, locator)| locator.position)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(positions, vec![vec![0, 1, 2, 3, 4, 5]]);
    });
}

#[test]
fn body_read_groups_split_at_byte_and_segment_boundaries() {
    deterministic::Runner::default().start(|context| async move {
        let mut store =
            open_with_capacity(&context, "store", "pending_group_boundaries", NZU64!(3)).await;
        let blocks = (1..=7)
            .map(|height| block(0, height, height))
            .collect::<Vec<_>>();
        for block in &blocks {
            store
                .put(block.reference(), Arc::clone(block))
                .await
                .unwrap();
        }
        sync(&mut store).await;

        let block_bytes = u64::try_from(blocks[0].encode_size()).unwrap();
        let max_bytes = block_bytes.checked_mul(2).unwrap();
        let request_order = [6, 2, 4, 1, 5, 0, 3];
        let groups = store
            .body_read_groups(
                request_order
                    .into_iter()
                    .enumerate()
                    .map(|(output, block)| (output, blocks[block].reference())),
                max_bytes,
                1,
            )
            .unwrap();
        let groups = groups
            .into_iter()
            .map(|group| {
                let (_, read) = group.into_parts();
                (
                    read.segment(),
                    read.encoded_bytes(),
                    read.entries
                        .into_iter()
                        .map(|(_, locator)| locator.position)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            groups,
            vec![
                (0, max_bytes, vec![0, 1]),
                (0, block_bytes, vec![2]),
                (1, max_bytes, vec![3, 4]),
                (1, block_bytes, vec![5]),
                (2, block_bytes, vec![6]),
            ]
        );
    });
}

#[test]
fn body_read_groups_do_not_exceed_request_job_target() {
    deterministic::Runner::default().start(|context| async move {
        const MAX_GROUPS: usize = 16;
        let mut store =
            open_with_capacity(&context, "store", "pending_group_target", NZU64!(64)).await;
        let blocks = (1..=31)
            .map(|height| block(0, height, height))
            .collect::<Vec<_>>();
        for block in &blocks {
            store
                .put(block.reference(), Arc::clone(block))
                .await
                .unwrap();
        }
        sync(&mut store).await;

        let block_bytes = u64::try_from(blocks[0].encode_size()).unwrap();
        let max_bytes = block_bytes
            .checked_mul(u64::try_from(MAX_GROUPS * 2).unwrap())
            .unwrap();
        let max_group_bytes = max_bytes / u64::try_from(MAX_GROUPS).unwrap();
        let groups = store
            .body_read_groups(
                blocks
                    .iter()
                    .enumerate()
                    .map(|(output, block)| (output, block.reference())),
                max_bytes,
                MAX_GROUPS,
            )
            .unwrap();

        assert_eq!(groups.len(), MAX_GROUPS);
        assert!(
            groups
                .iter()
                .all(|group| group.encoded_bytes <= max_group_bytes)
        );
        assert_eq!(
            groups
                .iter()
                .map(|group| group.entries.len())
                .collect::<Vec<_>>(),
            [vec![2; MAX_GROUPS - 1], vec![1]].concat()
        );
    });
}

#[test]
fn body_read_groups_fill_byte_capacity_before_exceeding_job_target() {
    deterministic::Runner::default().start(|context| async move {
        const MAX_GROUPS: usize = 16;
        let mut store =
            open_with_capacity(&context, "store", "pending_variable_groups", NZU64!(64)).await;
        let blocks = (1..=48)
            .map(|height| block(0, height, height))
            .collect::<Vec<_>>();
        for block in &blocks {
            store
                .put(block.reference(), Arc::clone(block))
                .await
                .unwrap();
        }
        sync(&mut store).await;

        for (index, block) in blocks.iter().enumerate() {
            let entry = store
                .by_digest
                .get_mut(&block.reference().digest())
                .unwrap();
            entry.meta =
                BlockMeta::new(entry.meta.header().clone(), if index < 46 { 1 } else { 5 });
        }
        let max_bytes = 160;
        let max_group_bytes = max_bytes / u64::try_from(MAX_GROUPS).unwrap();
        let groups = store
            .body_read_groups(
                blocks
                    .iter()
                    .enumerate()
                    .map(|(output, block)| (output, block.reference())),
                max_bytes,
                MAX_GROUPS,
            )
            .unwrap();

        assert_eq!(groups.len(), MAX_GROUPS);
        assert!(
            groups
                .iter()
                .all(|group| group.encoded_bytes <= max_group_bytes)
        );
        assert_eq!(
            groups
                .iter()
                .map(|group| group.entries.len())
                .sum::<usize>(),
            blocks.len()
        );
    });
}

#[test]
fn body_read_groups_allow_one_oversized_body() {
    deterministic::Runner::default().start(|context| async move {
        let mut store = open(&context, "store", "pending_oversized_group").await;
        let block = block(0, 1, 1);
        let block_bytes = u64::try_from(block.encode_size()).unwrap();
        store
            .put(block.reference(), Arc::clone(&block))
            .await
            .unwrap();
        sync(&mut store).await;

        let groups = store
            .body_read_groups(
                [(0, block.reference())],
                block_bytes.checked_sub(1).unwrap(),
                1,
            )
            .unwrap();
        assert_eq!(groups.len(), 1);
        let (_, read) = groups.into_iter().next().unwrap().into_parts();
        assert_eq!(read.encoded_bytes(), block_bytes);
        assert_eq!(read.entries.len(), 1);
    });
}

#[test]
fn state_version_two_is_rejected_without_touching_custody() {
    deterministic::Runner::default().start(|context| async move {
        let first = block(0, 1, 1);
        let second = block(0, 2, 2);
        let prefix = "pending_version_two";
        let mut store = open(&context, "first", prefix).await;
        for block in [&first, &second] {
            store
                .put(block.reference(), Arc::clone(block))
                .await
                .unwrap();
        }
        sync(&mut store).await;
        let custody = context.scan(&store.body_config(0).partition).await.unwrap();
        let state = store.state.take().unwrap();
        store.state = Some(
            state
                .put_sync(
                    Unit,
                    PendingState {
                        version: 2,
                        segment_capacity: store.segment_capacity,
                        floors: store.floors.clone(),
                        segments: store.segments.iter().copied().collect(),
                    },
                )
                .await
                .unwrap(),
        );
        drop(store);
        assert!(matches!(
            InnerTestStore::init(
                context.child("reopen"),
                ArchiveConfig::new(
                    TwoCap,
                    CacheRef::from_pooler(&context, NZU16!(1024), NZUsize!(10)),
                ),
                prefix.to_string(),
                (),
                Epoch::new(7),
                2,
                NZU64!(2),
                NZUsize!(1024 * 1024),
            )
            .await,
            Err(Error::Inconsistent("unsupported pending storage version"))
        ));
        assert_eq!(
            context.scan(&format!("{prefix}_bodies_0")).await.unwrap(),
            custody
        );
    });
}

#[rstest]
fn asymmetric_uncommitted_tail_recovers(#[values(false, true)] index_only: bool) {
    deterministic::Runner::default().start(|context| async move {
        let first = block(0, 1, 1);
        let tail = block(0, 2, 2);
        let prefix = "pending_asymmetric_suffix";
        let mut store = open_with_capacity(&context, "first", prefix, NZU64!(4)).await;
        store
            .put(first.reference(), Arc::clone(&first))
            .await
            .unwrap();
        sync(&mut store).await;
        let values_cfg = store.body_config(0);
        let cfg = store.segment_config(0);
        store
            .open_segments
            .remove(&0)
            .unwrap()
            .unwrap()
            .close()
            .await
            .unwrap();
        drop(store);

        if index_only {
            let index = fixed::Journal::<_, PendingRecord<Sha256Digest>>::init(
                context.child("missing_value"),
                fixed::Config {
                    partition: cfg.index_partition,
                    page_cache: cfg.index_page_cache,
                    write_buffer: cfg.index_write_buffer,
                },
            )
            .await
            .unwrap();
            let mut replay = index
                .replay(0, 0, cfg.replay_buffer, ReadOptions::default())
                .await
                .unwrap();
            while let Some(row) = replay.next().await {
                row.unwrap();
            }
            let index = replay.finish().unwrap();
            let missing = PendingRecord::new(BlockMeta::new(
                tail.header().clone(),
                tail.encode_size() as u64,
            ))
            .with_location(
                (first.encode_size() + crc32::Digest::SIZE) as u64,
                (tail.encode_size() + crc32::Digest::SIZE) as u32,
            );
            let (index, position) = index.append(0, &missing).await.unwrap();
            assert_eq!(position, 1);
            drop(index.sync_all().await.unwrap());
        } else {
            let values = glob::Glob::<_, StoredBody<Sha256, TestBody>>::init(
                context.child("orphan_values"),
                values_cfg,
            )
            .await
            .unwrap();
            let (values, _, _) = values
                .append(0, &Shared::new(Arc::clone(&tail)))
                .await
                .unwrap();
            drop(values.sync_all().await.unwrap());
        }

        let mut store = open_with_capacity(&context, "reopen", prefix, NZU64!(4)).await;
        assert_eq!(store.next_position, 1);
        assert_eq!(
            store.block(first.reference()).await.unwrap().as_deref(),
            Some(first.as_ref())
        );
        assert!(store.block(tail.reference()).await.unwrap().is_none());
        store
            .put(tail.reference(), Arc::clone(&tail))
            .await
            .unwrap();
        sync(&mut store).await;
        drop(store);
        let store = open_with_capacity(&context, "again", prefix, NZU64!(4)).await;
        assert_eq!(
            store.block(tail.reference()).await.unwrap().as_deref(),
            Some(tail.as_ref())
        );
    });
}

#[test]
fn invalid_uncommitted_middle_discards_a_valid_later_body() {
    deterministic::Runner::default().start(|context| async move {
        let prefix = "pending_middle_tail";
        let blocks = [block(0, 1, 1), block(0, 2, 2), block(1, 1, 3)];
        let mut store = open_with_capacity(&context, "first", prefix, NZU64!(4)).await;
        store
            .put(blocks[0].reference(), Arc::clone(&blocks[0]))
            .await
            .unwrap();
        sync(&mut store).await;
        store
            .open_segments
            .remove(&0)
            .unwrap()
            .unwrap()
            .close()
            .await
            .unwrap();
        drop(store);
        let mut store = open_with_capacity(&context, "append", prefix, NZU64!(4)).await;
        store.put_many(&blocks[1..]).await;
        let offset = store.by_digest[&blocks[1].reference().digest()].offset;
        let partition = store.body_config(0).partition;
        let segment = store.open_segments.remove(&0).unwrap().unwrap();
        drop(segment.sync_all().await.unwrap());
        drop(store);

        let (blob, _) = context.open(&partition, &0u64.to_be_bytes()).await.unwrap();
        let bytes = blob
            .read_at(offset, 1, ReadOptions::default())
            .await
            .unwrap()
            .coalesce();
        blob.write_at(offset, vec![bytes.as_ref()[0] ^ 0xff], WriteOptions::SYNC)
            .await
            .unwrap();
        drop(blob);

        let store = open_with_capacity(&context, "recover", prefix, NZU64!(4)).await;
        assert_eq!(store.next_position, 1);
        assert_eq!(
            store.block(blocks[0].reference()).await.unwrap().as_deref(),
            Some(blocks[0].as_ref())
        );
        for block in &blocks[1..] {
            assert!(store.header(block.reference()).is_none());
        }
    });
}

#[rstest]
fn unpublished_segment_is_reset_before_reuse(#[values(false, true)] corrupt: bool) {
    deterministic::Runner::default().start(|context| async move {
        let prefix = "pending_unpublished_segment";
        let blocks = [block(0, 1, 1), block(0, 2, 2), block(0, 3, 3)];
        let mut store = open(&context, "first", prefix).await;
        store.put_many(&blocks[..2]).await;
        sync(&mut store).await;
        store
            .put(blocks[2].reference(), Arc::clone(&blocks[2]))
            .await
            .unwrap();
        let segment = store.open_segments.remove(&1).unwrap().unwrap();
        segment.close().await.unwrap();
        let partitions = store.segment_partitions(1);
        drop(store);
        if corrupt {
            for partition in &partitions {
                drop(context.open(partition, b"invalid").await.unwrap());
            }
        }

        let mut store = open(&context, "reopen", prefix).await;
        assert_eq!(store.segments.iter().copied().collect::<Vec<_>>(), vec![0]);
        assert_eq!(store.next_position, 2);
        for block in &blocks[..2] {
            assert_eq!(
                store.block(block.reference()).await.unwrap().as_deref(),
                Some(block.as_ref())
            );
        }
        assert!(store.block(blocks[2].reference()).await.unwrap().is_none());
        store
            .put(blocks[2].reference(), Arc::clone(&blocks[2]))
            .await
            .unwrap();
        sync(&mut store).await;
        drop(store);
        let store = open(&context, "final", prefix).await;
        assert_eq!(
            store.block(blocks[2].reference()).await.unwrap().as_deref(),
            Some(blocks[2].as_ref())
        );
    });
}

#[test]
fn below_floor_admission_is_rejected() {
    deterministic::Runner::default().start(|context| async move {
        let stale = block(0, 1, 1);
        let mut store = open(&context, "store", "pending_below_floor").await;
        store
            .prune(&[Some(Height::new(2)), None], &BTreeSet::new())
            .await
            .unwrap();

        assert!(store.put(stale.reference(), stale).await.is_err());
    });
}

/// Extract a metric counter's value from encoded metrics output.
fn counter(buffer: &str, name: &str) -> u64 {
    buffer
        .lines()
        .find(|line| line.contains(name) && !line.starts_with('#'))
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse().ok())
        .expect("counter missing")
}

/// Buffers blocks that fill segment 0 and roll into segment 1.
async fn put_first_segment(store: &mut TestStore) -> Vec<BlockRef<Sha256Digest>> {
    let blocks = [block(0, 1, 1), block(0, 2, 2), block(1, 1, 3)];
    let references = blocks.iter().map(|block| block.reference()).collect();
    for block in blocks {
        store.put(block.reference(), block).await.unwrap();
    }
    references
}

/// Fills segment 0, rolls into segment 1, and runs production-shaped durability rounds.
async fn fill_first_segment(store: &mut TestStore) -> Vec<BlockRef<Sha256Digest>> {
    let references = put_first_segment(store).await;
    sync(store).await;
    // The next cut drops segment 0's admission-time reader, so later reads are cold.
    sync(store).await;
    references
}

/// Opens segment 0 cold and asserts the open wrote and synced nothing.
async fn cold_open_without_writes(
    context: &DeterministicContext,
    store: &TestStore,
) -> BodyReader<DeterministicContext, Sha256, TestBody> {
    let (_, writes, syncs) = storage_io(context);
    let reader = store.body_source(0).open().await.unwrap();
    let (_, writes_after, syncs_after) = storage_io(context);
    assert_eq!(writes_after, writes, "a retired cold open must not write");
    assert_eq!(syncs_after, syncs, "a retired cold open must not sync");
    reader
}

#[test]
fn retired_segment_cold_opens_without_replay_or_writes() {
    deterministic::Runner::default().start(|context| async move {
        let mut store = open(&context, "store", "pending_retired_cold").await;
        let references = fill_first_segment(&mut store).await;

        // A cold source opens the immutable value section directly without recovery I/O.
        let (read, _, _) = storage_io(&context);
        let reader = cold_open_without_writes(&context, &store).await;
        let (read_after, _, _) = storage_io(&context);
        let locator = TestStore::locator(store.by_digest.get(&references[0].digest()).unwrap());
        assert_eq!(read_after - read, 0, "cold open read storage data");
        assert_eq!(
            reader.read(locator).await.unwrap().reference(),
            references[0]
        );
    });
}

#[test]
fn cold_open_preserves_the_snapshot_extent() {
    deterministic::Runner::default().start(|context| async move {
        let mut store =
            open_with_capacity(&context, "store", "pending_snapshot_extent", NZU64!(4)).await;
        let first = block(0, 1, 1);
        let second = block(0, 2, 2);
        store
            .put(first.reference(), Arc::clone(&first))
            .await
            .unwrap();
        sync(&mut store).await;
        let reader = store.body_source(0).open().await.unwrap();
        store
            .put(second.reference(), Arc::clone(&second))
            .await
            .unwrap();
        sync(&mut store).await;
        let locator =
            TestStore::locator(store.by_digest.get(&second.reference().digest()).unwrap());
        assert!(reader.read(locator).await.is_err());
    });
}

#[test]
fn dirty_full_segment_defers_its_retirement_to_the_covering_cut() {
    deterministic::Runner::default().start(|context| async move {
        let mut store = open(&context, "store", "pending_dirty_retire").await;
        let first = block(0, 1, 1);
        let filler = block(0, 2, 2);
        let next = block(1, 1, 3);
        let filler_reference = filler.reference();
        store.put(first.reference(), first).await.unwrap();
        let cut = store.start_sync().await.unwrap();
        // The segment fills while its first cut is still in flight, so its tail is not
        // covered by that cut.
        store.put(filler_reference, filler).await.unwrap();
        store.put(next.reference(), next).await.unwrap();
        futures::future::try_join_all(cut).await.unwrap();

        // The catalog retires on cut completion; the dirty tail defers this segment's
        // retirement and keeps its journals appendable for the covering cut.
        assert!(store.start_retire().0.is_empty());
        sync(&mut store).await;
        sync(&mut store).await;

        let reader = cold_open_without_writes(&context, &store).await;
        let locator = TestStore::locator(store.by_digest.get(&filler_reference.digest()).unwrap());
        assert_eq!(
            reader.read(locator).await.unwrap().reference(),
            filler_reference
        );
    });
}

#[test]
fn unretired_full_segment_self_heals_on_reopen() {
    deterministic::Runner::default().start(|context| async move {
        let mut store = open(&context, "first", "pending_unretired_reopen").await;
        let references = put_first_segment(&mut store).await;
        // Crash window: the admission cut completes but the retirement never runs.
        futures::future::try_join_all(store.start_sync().await.unwrap())
            .await
            .unwrap();
        drop(store);

        // Startup validates the unproven suffix and seals the recovered segment before closing it.
        let store = open(&context, "reopen", "pending_unretired_reopen").await;
        for reference in &references {
            assert!(store.block(*reference).await.unwrap().is_some());
        }
        cold_open_without_writes(&context, &store).await;
    });
}

#[test]
fn active_and_cold_segments_batch_adjacent_runs() {
    deterministic::Runner::default().start(|context| async move {
        let mut store =
            open_with_capacity(&context, "store", "pending_replay_runs", NZU64!(4)).await;
        let blocks = (0..7)
            .map(|height| block(0, height + 1, height))
            .collect::<Vec<_>>();
        store.put_many(&blocks).await;
        sync(&mut store).await;
        sync(&mut store).await;
        assert!(!store.active_readers.contains_key(&0));

        // Positions 0, 1, and 3 of the retired segment: two adjacent runs, one hole. Each
        // run is one sequential replay, so the reads stay bounded by runs, not by pages.
        let reader = store.body_source(0).open().await.unwrap();
        assert!(reader.immutable);
        let requests = [
            blocks[3].reference(),
            blocks[0].reference(),
            blocks[1].reference(),
        ];
        let groups = store
            .body_read_groups(requests.iter().copied().enumerate(), u64::MAX, 1)
            .unwrap();
        assert_eq!(groups.len(), 1);
        let (_, read) = groups.into_iter().next().unwrap().into_parts();
        let reads = counter(&context.encode(), "storage_reads");
        let mut results = read.read(reader).await.unwrap();
        assert_eq!(
            counter(&context.encode(), "storage_reads") - reads,
            2,
            "two cold runs must issue two value reads"
        );
        results.sort_by_key(|(output, _)| *output);
        assert_eq!(results.len(), 3);
        for (output, block) in results {
            assert_eq!(block.reference(), requests[output]);
        }

        // The current segment uses its captured snapshot, but the same two runs still batch
        // into exactly two value reads.
        let current = store.active_readers.get(&1).unwrap();
        assert!(!current.immutable);
        let groups = store
            .body_read_groups(
                [(0, blocks[4].reference()), (1, blocks[6].reference())],
                u64::MAX,
                1,
            )
            .unwrap();
        let (source, read) = groups.into_iter().next().unwrap().into_parts();
        let reads = counter(&context.encode(), "storage_reads");
        let results = read.read(source.open().await.unwrap()).await.unwrap();
        assert_eq!(
            counter(&context.encode(), "storage_reads") - reads,
            2,
            "two active runs must issue two value reads"
        );
        assert_eq!(results.len(), 2);
        assert_eq!(
            store.block(blocks[6].reference()).await.unwrap().as_deref(),
            Some(blocks[6].as_ref())
        );
    });
}

#[test]
fn reclaimed_segments_are_removed_by_name_without_reads() {
    deterministic::Runner::default().start(|context| async move {
        let mut store = open(&context, "store", "pending_reclaim_by_name").await;
        let references = fill_first_segment(&mut store).await;

        // Retired contents need not be recoverable, including malformed blob names.
        let partition = store.body_config(0).partition;
        drop(context.open(&partition, b"invalid").await.unwrap());
        let reads = counter(&context.encode(), "storage_reads");
        let reclaimed = store
            .prune(&[Some(Height::new(3)), None], &BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(reclaimed, vec![0]);
        assert_eq!(counter(&context.encode(), "storage_reads"), reads);
        assert!(matches!(
            context.scan(&partition).await,
            Err(commonware_runtime::Error::PartitionMissing(_))
        ));
        drop(store);

        let store = open(&context, "reopen", "pending_reclaim_by_name").await;
        assert!(store.block(references[0]).await.unwrap().is_none());
        assert_eq!(
            store
                .block(references[2])
                .await
                .unwrap()
                .unwrap()
                .reference(),
            references[2]
        );
    });
}

#[test]
fn cold_segments_close_between_cuts() {
    deterministic::Runner::default().start(|context| async move {
        let first = block(0, 1, 1);
        let second = block(0, 2, 2);
        let third = block(1, 1, 3);
        let fourth = block(1, 2, 4);
        let fifth = block(0, 3, 5);
        let first_ref = first.reference();
        let second_ref = second.reference();
        let third_ref = third.reference();
        let mut store = open(&context, "store", "pending_cold_segments").await;

        store
            .put(first.reference(), Arc::clone(&first))
            .await
            .unwrap();
        store.put(second.reference(), second).await.unwrap();
        sync(&mut store).await;
        store.put(third.reference(), third).await.unwrap();
        store.put(fourth.reference(), fourth).await.unwrap();
        sync(&mut store).await;
        store.put(fifth.reference(), fifth).await.unwrap();
        sync(&mut store).await;

        assert_eq!(
            store.open_segments.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );
        store.put(first_ref, Arc::clone(&first)).await.unwrap();
        assert_eq!(
            store.open_segments.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );

        let blocks = store
            .blocks(&[first_ref, second_ref, third_ref])
            .await
            .unwrap();
        assert!(blocks.iter().all(Option::is_some));
        assert_eq!(
            store.open_segments.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );

        store
            .prune(&[Some(Height::new(2)), None], &BTreeSet::new())
            .await
            .unwrap();
        assert!(store.block(first_ref).await.unwrap().is_none());
        assert!(store.block(second_ref).await.unwrap().is_some());
        assert_eq!(
            store.open_segments.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );
    });
}
