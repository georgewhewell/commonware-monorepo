use super::Header;
use crate::{BufferPool, Error, telemetry::metrics::Registry};
use commonware_codec::Encode;
use commonware_formatting::{from_hex, hex};
#[cfg(unix)]
use std::path::Path;
use std::{
    fs,
    io::{Read as _, Write as _},
    ops::RangeInclusive,
    path::PathBuf,
    sync::Arc,
};
use tokio::{sync::Mutex, task::JoinHandle};

#[cfg(not(unix))]
mod fallback;
#[cfg(unix)]
mod unix;

/// Syncs a directory to ensure directory entry changes are durable.
/// On Unix, directory metadata (file creation/deletion) must be explicitly
/// fsynced.
#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<(), Error> {
    let dir = fs::File::open(path).map_err(|e| {
        Error::BlobOpenFailed(
            path.to_string_lossy().to_string(),
            "directory".to_string(),
            e.into(),
        )
    })?;
    dir.sync_all().map_err(|e| {
        Error::BlobSyncFailed(
            path.to_string_lossy().to_string(),
            "directory".to_string(),
            e.into(),
        )
    })
}

// The closure, including its guard, belongs to the blocking job. Cancelling a
// waiter or shutting down its runtime cannot release ownership around running I/O.
fn spawn_with_guard<G, T>(guard: G, operation: impl FnOnce() -> T + Send + 'static) -> JoinHandle<T>
where
    G: Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let result = operation();
        drop(guard);
        result
    })
}

fn missing_or_io(error: std::io::Error, missing: Error) -> Error {
    if error.kind() == std::io::ErrorKind::NotFound {
        missing
    } else {
        error.into()
    }
}

/// Configuration for filesystem storage on an existing Tokio runtime.
#[derive(Clone)]
pub struct Config {
    /// Root directory containing storage partitions.
    pub storage_directory: PathBuf,
    /// Maximum buffer size for file operations on non-Unix platforms.
    pub maximum_buffer_size: usize,
}

impl Config {
    /// Configure the storage root and maximum file-operation buffer size.
    pub const fn new(storage_directory: PathBuf, maximum_buffer_size: usize) -> Self {
        Self {
            storage_directory,
            maximum_buffer_size,
        }
    }
}

/// Filesystem storage driven by the caller's Tokio runtime.
///
/// Initialize with [`Self::init`] before opening blobs. Clones share filesystem
/// operation serialization and the buffer pool, but confer no exclusive writer
/// ownership over a partition or blob. The caller must provide that ownership.
/// This handle does not own a runtime. Keep the caller's runtime and writer
/// ownership alive until outstanding blob operations complete, including when
/// their waiting task is cancelled.
#[derive(Clone)]
pub struct Storage {
    lock: Arc<Mutex<()>>,
    cfg: Config,
    pool: BufferPool,
    #[cfg(unix)]
    guard: Option<Arc<dyn Send + Sync>>,
}

impl Storage {
    /// Create the root and make recovered bytes crash-durable before returning.
    ///
    /// Returns an error without a running Tokio runtime. Preparation runs on its
    /// blocking pool, without starting a second executor. If initialization is
    /// cancelled, preparation may finish, but no storage handle is published.
    ///
    /// Linux uses `syncfs(2)` on the root's filesystem (kernel >= 5.8 is required
    /// for reliable error reporting). The root must reside on one filesystem.
    /// Other platforms return [`std::io::ErrorKind::Unsupported`]: their current
    /// startup flush does not establish the [`crate::Storage`] durability contract.
    ///
    /// The buffer pool uses [`crate::BufferPoolConfig::for_storage`]; its metrics are
    /// registered with `registry` and shared by all clones of this storage.
    pub async fn init(cfg: Config, registry: &mut Registry) -> std::io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let directory = cfg.storage_directory.clone();
            let runtime = tokio::runtime::Handle::try_current().map_err(std::io::Error::other)?;
            runtime
                .spawn_blocking(move || {
                    std::fs::create_dir_all(&directory)?;
                    super::sync(&directory)
                })
                .await
                .map_err(std::io::Error::other)??;
            Ok(Self::new(
                cfg,
                BufferPool::new(crate::BufferPoolConfig::for_storage(), registry),
            ))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (cfg, registry);
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "durable storage initialization is not supported on this platform",
            ))
        }
    }

    pub(crate) fn new(cfg: Config, pool: BufferPool) -> Self {
        Self {
            lock: Arc::new(Mutex::new(())),
            cfg,
            pool,
            #[cfg(unix)]
            guard: None,
        }
    }

    /// Retain a caller-owned guard in this handle, its blobs and their native I/O.
    ///
    /// For example, pass an already-acquired journal lock. In-flight I/O keeps
    /// the guard even if its waiter is cancelled or the runtime shuts down.
    /// This does not acquire ownership itself or affect existing clones or blobs;
    /// attach the guard before opening the guarded journal's blobs.
    #[cfg(unix)]
    pub fn with_guard(mut self, guard: Arc<dyn Send + Sync>) -> Self {
        self.guard = Some(guard);
        self
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let lock = self.lock.clone().lock_owned().await;
        let storage = self.clone();
        spawn_with_guard(lock, move || operation(&storage))
            .await
            .map_err(|error| Error::from(std::io::Error::from(error)))?
    }
    /// Open an existing blob without creating, repairing or writing its contents.
    ///
    /// Uses [`crate::DEFAULT_BLOB_VERSION`]. An incomplete container header is
    /// corruption, not an interrupted create to repair. Returned handles reject
    /// mutation. Concurrent writers may change bytes during inspection; this is
    /// not a snapshot or a grant of exclusive ownership.
    pub async fn open_read_only(
        &self,
        partition: &str,
        name: &[u8],
    ) -> Result<(<Self as crate::Storage>::Blob, u64), Error> {
        let version = crate::DEFAULT_BLOB_VERSION;
        let (blob, size, _) = self
            .open_blob(partition, name, version..=version, false)
            .await?;
        Ok((blob, size))
    }

    async fn open_blob(
        &self,
        partition: &str,
        name: &[u8],
        versions: RangeInclusive<u16>,
        writable: bool,
    ) -> Result<(<Self as crate::Storage>::Blob, u64, u16), Error> {
        super::validate_partition_name(partition)?;
        let partition = partition.to_string();
        let name = name.to_vec();
        self.run(move |storage| {
            let partition = partition.as_str();
            let name = name.as_slice();
            // Construct the full path
            let path = storage
                .cfg
                .storage_directory
                .join(partition)
                .join(hex(name));
            let parent = match path.parent() {
                Some(parent) => parent,
                None => return Err(Error::PartitionCreationFailed(partition.into())),
            };

            // Check if partition exists before creating
            #[cfg(unix)]
            let parent_existed = parent.exists();

            // Create the partition directory, if it does not exist
            if writable {
                fs::create_dir_all(parent)
                    .map_err(|_| Error::PartitionCreationFailed(partition.into()))?;
            }

            // Open the file, creating it if it doesn't exist
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(writable)
                .create(writable)
                .truncate(false)
                .open(&path)
                .map_err(|e| Error::BlobOpenFailed(partition.into(), hex(name), e.into()))?;

            // Assume empty files are newly created. Existing empty files will be synced too; that's OK.
            let len = file.metadata().map_err(|_| Error::ReadFailed)?.len();
            let newly_created = writable && len == 0;

            // Only sync if we created a new file
            if newly_created {
                // Sync the file to ensure it is durable
                file.sync_all()
                    .map_err(|e| Error::BlobSyncFailed(partition.into(), hex(name), e.into()))?;

                // Windows doesn't have a notion of syncing a directory entry to ensure that it's
                // durably persisted. See https://github.com/commonwarexyz/monorepo/issues/2026.
                #[cfg(unix)]
                {
                    // Sync the parent directory to ensure the directory entry is durable.
                    sync_dir(parent)?;

                    // Sync storage directory if parent directory did not exist
                    if !parent_existed {
                        sync_dir(&storage.cfg.storage_directory)?;
                    }
                }
            }

            // Handle header: new/corrupted blobs get a fresh header written,
            // existing blobs have their header read.
            let (blob_version, logical_size) = if Header::missing(len) {
                if !writable {
                    return Err(Error::BlobCorrupt(
                        partition.into(),
                        hex(name),
                        "incomplete blob header".into(),
                    ));
                }
                // New or partially-created blob: reset it and write a fresh header. The
                // file grows only as header bytes are written, so a create interrupted by
                // a process crash leaves some prefix of the header, which the next open
                // resets here instead of rejecting as corrupt below.
                let (header, blob_version) = Header::new(&versions);
                file.set_len(0)
                    .map_err(|e| Error::BlobResizeFailed(partition.into(), hex(name), e.into()))?;
                file.write_all(&header.encode())
                    .map_err(|_| Error::WriteFailed)?;
                file.sync_all()
                    .map_err(|e| Error::BlobSyncFailed(partition.into(), hex(name), e.into()))?;
                (blob_version, 0)
            } else {
                // Existing blob - read and validate header
                let mut header_bytes = [0u8; Header::SIZE];
                file.read_exact(&mut header_bytes)
                    .map_err(|_| Error::ReadFailed)?;
                Header::from(header_bytes, len, &versions)
                    .map_err(|e| e.into_error(partition, name))?
            };

            #[cfg(unix)]
            {
                // Construct the blob
                Ok((
                    <Self as crate::Storage>::Blob::new(
                        partition.into(),
                        name,
                        file,
                        storage.pool.clone(),
                        storage.guard.clone(),
                    ),
                    logical_size,
                    blob_version,
                ))
            }
            #[cfg(not(unix))]
            {
                let mut file = tokio::fs::File::from_std(file);
                file.set_max_buf_size(storage.cfg.maximum_buffer_size);
                Ok((
                    <Self as crate::Storage>::Blob::new(
                        partition.into(),
                        name,
                        file,
                        storage.pool.clone(),
                    ),
                    logical_size,
                    blob_version,
                ))
            }
        })
        .await
    }
}

impl crate::Storage for Storage {
    #[cfg(unix)]
    type Blob = unix::Blob;
    #[cfg(not(unix))]
    type Blob = fallback::Blob;

    async fn open_versioned(
        &self,
        partition: &str,
        name: &[u8],
        versions: RangeInclusive<u16>,
    ) -> Result<(Self::Blob, u64, u16), Error> {
        self.open_blob(partition, name, versions, true).await
    }

    async fn remove(&self, partition: &str, name: Option<&[u8]>) -> Result<(), Error> {
        super::validate_partition_name(partition)?;
        let partition = partition.to_string();
        let name = name.map(<[u8]>::to_vec);
        self.run(move |storage| {
            let partition = partition.as_str();
            let name = name.as_deref();
            // Remove all related files
            let path = storage.cfg.storage_directory.join(partition);
            if let Some(name) = name {
                let blob_path = path.join(hex(name));
                fs::remove_file(blob_path).map_err(|error| {
                    missing_or_io(error, Error::BlobMissing(partition.into(), hex(name)))
                })?;

                // Sync the partition directory to ensure the removal is durable.
                // Windows doesn't have a notion of syncing a directory entry to ensure that it's
                // durably persisted. See https://github.com/commonwarexyz/monorepo/issues/2026.
                #[cfg(unix)]
                sync_dir(&path)?;
            } else {
                fs::remove_dir_all(&path).map_err(|error| {
                    missing_or_io(error, Error::PartitionMissing(partition.into()))
                })?;

                // Sync the storage directory to ensure the removal is durable.
                // Windows doesn't have a notion of syncing a directory entry to ensure that it's
                // durably persisted. See https://github.com/commonwarexyz/monorepo/issues/2026.
                #[cfg(unix)]
                sync_dir(&storage.cfg.storage_directory)?;
            }
            Ok(())
        })
        .await
    }

    async fn scan(&self, partition: &str) -> Result<Vec<Vec<u8>>, Error> {
        super::validate_partition_name(partition)?;
        let partition = partition.to_string();
        self.run(move |storage| {
            let partition = partition.as_str();
            // Scan the partition directory
            let path = storage.cfg.storage_directory.join(partition);
            let entries = fs::read_dir(path)
                .map_err(|error| missing_or_io(error, Error::PartitionMissing(partition.into())))?;
            let mut blobs = Vec::new();
            for entry in entries {
                let entry = entry.map_err(|_| Error::ReadFailed)?;
                let file_type = entry.file_type().map_err(|_| Error::ReadFailed)?;
                if !file_type.is_file() {
                    return Err(Error::PartitionCorrupt(partition.into()));
                }
                if let Some(name) = entry.file_name().to_str() {
                    // Reject anything that isn't canonical lowercase hex (no `0x`
                    // prefix, no whitespace) since `from_hex` is lenient and
                    // storage only ever writes the canonical form via `hex()`.
                    let decoded =
                        from_hex(name).ok_or(Error::PartitionCorrupt(partition.into()))?;
                    if hex(&decoded) != name {
                        return Err(Error::PartitionCorrupt(partition.into()));
                    }

                    blobs.push(decoded);
                }
            }
            Ok(blobs)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{Header, *};
    use crate::{
        Blob, BufferPoolConfig, Storage as _, storage::tests::run_storage_tests,
        telemetry::metrics::Registry,
    };
    use commonware_utils::sys_rng;
    use rand::RngExt as _;
    use std::env;

    fn test_pool() -> BufferPool {
        let mut registry = Registry::default();
        BufferPool::new(BufferPoolConfig::for_storage(), &mut registry)
    }

    fn random_suffix() -> u64 {
        let mut rng = sys_rng();
        rng.random()
    }

    #[tokio::test]
    async fn test_storage() {
        let mut rng = sys_rng();
        let storage_directory =
            env::temp_dir().join(format!("storage_tokio_{}", rng.random::<u64>()));
        let config = Config::new(storage_directory, 2 * 1024 * 1024);
        #[cfg(target_os = "linux")]
        let storage = Storage::init(config, &mut Registry::default())
            .await
            .unwrap();
        #[cfg(not(target_os = "linux"))]
        let storage = Storage::new(config, test_pool());
        run_storage_tests(storage).await;
    }

    /// Dropping the `start_sync` receiver must not break the blob: the handle stays
    /// usable and a later sync still persists data.
    #[tokio::test]
    async fn test_start_sync_dropped_receiver() {
        let mut rng = sys_rng();
        let storage_directory =
            env::temp_dir().join(format!("storage_tokio_start_sync_{}", rng.random::<u64>()));
        let config = Config::new(storage_directory, 2 * 1024 * 1024);
        let storage = Storage::new(config, test_pool());

        let (blob, _) = storage.open("partition", b"test_blob").await.unwrap();
        blob.write_at(0, b"hello world").await.unwrap();

        // Drop the completion receiver immediately.
        drop(blob.start_sync().await);

        // The blob remains usable, and a subsequent sync persists the data.
        blob.start_sync().await.await.unwrap();
        drop(blob);

        let (blob, len) = storage.open("partition", b"test_blob").await.unwrap();
        assert_eq!(len, 11);
        let read = blob.read_at(0, 11).await.unwrap().coalesce();
        assert_eq!(read.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn test_blob_header_handling() {
        let mut rng = sys_rng();
        let storage_directory =
            env::temp_dir().join(format!("storage_tokio_header_{}", rng.random::<u64>()));
        let config = Config::new(storage_directory.clone(), 2 * 1024 * 1024);
        let storage = Storage::new(config, test_pool());

        // Test 1: New blob returns logical size 0 and correct app version
        let (blob, size) = storage.open("partition", b"test").await.unwrap();
        assert_eq!(size, 0, "new blob should have logical size 0");

        // Verify raw file has 8 bytes (header only)
        let file_path = storage_directory.join("partition").join(hex(b"test"));
        let metadata = std::fs::metadata(&file_path).unwrap();
        assert_eq!(
            metadata.len(),
            Header::SIZE_U64,
            "raw file should have 8-byte header"
        );

        // Test 2: Logical offset handling - write at offset 0 stores at raw offset 8
        let data = b"hello world";
        blob.write_at(0, data).await.unwrap();
        blob.sync().await.unwrap();

        // Verify raw file size
        let metadata = std::fs::metadata(&file_path).unwrap();
        assert_eq!(metadata.len(), Header::SIZE_U64 + data.len() as u64);

        // Verify raw file layout
        let raw_content = std::fs::read(&file_path).unwrap();
        assert_eq!(&raw_content[..Header::MAGIC_LENGTH], &Header::MAGIC);
        // Header version (bytes 4-5) and App version (bytes 6-7)
        assert_eq!(
            &raw_content[Header::MAGIC_LENGTH..Header::MAGIC_LENGTH + Header::VERSION_LENGTH],
            &Header::RUNTIME_VERSION.to_be_bytes()
        );
        // Data should start at offset 8
        assert_eq!(&raw_content[Header::SIZE..], data);

        // Test 3: Read at logical offset 0 returns data from raw offset 8
        let read_buf = blob.read_at(0, data.len()).await.unwrap();
        assert_eq!(read_buf.coalesce(), data);

        // Test 4: Resize with logical length
        blob.resize(5).await.unwrap();
        blob.sync().await.unwrap();
        let metadata = std::fs::metadata(&file_path).unwrap();
        assert_eq!(
            metadata.len(),
            Header::SIZE_U64 + 5,
            "resize(5) should result in 13 raw bytes"
        );

        // resize(0) should leave only header
        blob.resize(0).await.unwrap();
        blob.sync().await.unwrap();
        let metadata = std::fs::metadata(&file_path).unwrap();
        assert_eq!(
            metadata.len(),
            Header::SIZE_U64,
            "resize(0) should leave only header"
        );

        // Test 5: Reopen existing blob preserves header and returns correct logical size
        blob.write_at(0, b"test data").await.unwrap();
        blob.sync().await.unwrap();
        drop(blob);

        let (blob2, size2) = storage.open("partition", b"test").await.unwrap();
        assert_eq!(size2, 9, "reopened blob should have logical size 9");
        let read_buf = blob2.read_at(0, 9).await.unwrap();
        assert_eq!(read_buf.coalesce(), b"test data");
        drop(blob2);

        // Test 6: Corrupted blob recovery (0 < raw_size < 8)
        // Manually create a corrupted file with only 4 bytes
        let corrupted_path = storage_directory.join("partition").join(hex(b"corrupted"));
        std::fs::write(&corrupted_path, vec![0u8; 4]).unwrap();

        // Opening should truncate and write fresh header
        let (blob3, size3) = storage.open("partition", b"corrupted").await.unwrap();
        assert_eq!(size3, 0, "corrupted blob should return logical size 0");

        // Verify raw file now has proper 8-byte header
        let metadata = std::fs::metadata(&corrupted_path).unwrap();
        assert_eq!(
            metadata.len(),
            Header::SIZE_U64,
            "corrupted blob should be reset to header-only"
        );

        // Cleanup
        drop(blob3);
        let _ = std::fs::remove_dir_all(&storage_directory);
    }

    #[tokio::test]
    async fn test_blob_magic_mismatch() {
        let storage_directory =
            env::temp_dir().join(format!("test_magic_mismatch_{}", random_suffix()));
        let storage = Storage::new(
            Config {
                storage_directory: storage_directory.clone(),
                maximum_buffer_size: 1024 * 1024,
            },
            test_pool(),
        );

        // Create the partition directory and a file with invalid magic bytes
        let partition_path = storage_directory.join("partition");
        std::fs::create_dir_all(&partition_path).unwrap();
        let bad_magic_path = partition_path.join(hex(b"bad_magic"));
        std::fs::write(&bad_magic_path, vec![0u8; Header::SIZE]).unwrap();

        // Opening should fail with corrupt error
        let result = storage.open("partition", b"bad_magic").await;
        assert!(
            matches!(result, Err(crate::Error::BlobCorrupt(_, _, reason)) if reason.contains("invalid magic"))
        );

        let _ = std::fs::remove_dir_all(&storage_directory);
    }

    /// Any file shorter than a header must reset to a valid, empty blob on open
    /// rather than fail as corrupt.
    #[tokio::test]
    async fn test_blob_partial_header_reset() {
        let storage_directory =
            env::temp_dir().join(format!("test_partial_header_reset_{}", random_suffix()));
        let storage = Storage::new(
            Config {
                storage_directory: storage_directory.clone(),
                maximum_buffer_size: 1024 * 1024,
            },
            test_pool(),
        );
        let partition_path = storage_directory.join("partition");
        std::fs::create_dir_all(&partition_path).unwrap();

        for prefix_len in 0..Header::SIZE {
            let name = format!("short_{prefix_len}");
            let path = partition_path.join(hex(name.as_bytes()));
            // Seed a file shorter than a full header.
            std::fs::write(&path, vec![0u8; prefix_len]).unwrap();

            let (blob, size) = storage
                .open("partition", name.as_bytes())
                .await
                .expect("interrupted create should recover, not fail");
            assert_eq!(size, 0, "recovered blob should be empty");
            drop(blob);

            // The recovered blob is a valid header-only file and reopens cleanly.
            let raw = std::fs::read(&path).unwrap();
            assert_eq!(
                raw.len(),
                Header::SIZE,
                "recovered blob should be header-only"
            );
            assert_eq!(&raw[..Header::MAGIC_LENGTH], &Header::MAGIC);
            storage
                .open("partition", name.as_bytes())
                .await
                .expect("reopen after recovery should succeed");
        }

        let _ = std::fs::remove_dir_all(&storage_directory);
    }

    #[tokio::test]
    async fn test_scan_rejects_non_canonical_hex_file_names() {
        // `commonware_formatting::from_hex` is lenient (strips `0x`/`0X` prefixes
        // and ASCII whitespace), but storage only ever writes filenames in the
        // canonical lowercase hex form produced by `hex()`. Verify that scans
        // reject any filename that decodes successfully but doesn't round-trip
        // to its canonical form.
        for bad_name in ["0x626c6f62", "0X626C6F62", " 626c6f62", "626C6F62"] {
            let storage_directory = env::temp_dir().join(format!(
                "test_scan_non_canonical_{}_{}",
                bad_name.replace([' ', '0', 'x', 'X'], "_"),
                random_suffix()
            ));
            let storage = Storage::new(
                Config {
                    storage_directory: storage_directory.clone(),
                    maximum_buffer_size: 1024 * 1024,
                },
                test_pool(),
            );

            let partition_path = storage_directory.join("partition");
            std::fs::create_dir_all(&partition_path).unwrap();
            std::fs::write(partition_path.join(bad_name), []).unwrap();

            let err = match storage.scan("partition").await {
                Ok(_) => panic!("scan should have failed for filename {bad_name:?}"),
                Err(err) => err,
            };
            assert_eq!(
                err.to_string(),
                "partition corrupt: partition",
                "filename {bad_name:?} should be rejected as corrupt",
            );

            let _ = std::fs::remove_dir_all(&storage_directory);
        }
    }
}
