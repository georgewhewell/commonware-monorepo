use super::{super::Storage, *};
use crate::{BufferPoolConfig, Storage as _, telemetry::metrics::Registry};
use std::{
    fs::{OpenOptions, TryLockError},
    sync::mpsc,
    time::Duration,
};

// Notify only after releasing the actual OS lock, so the test can distinguish
// completion from merely dropping an async waiter.
struct Lease {
    file: Option<File>,
    released: mpsc::Sender<()>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = self.released.send(());
    }
}

#[test]
fn native_io_retains_ownership_after_cancel_and_shutdown() {
    for metadata in [true, false] {
        let root =
            std::env::temp_dir().join(format!("storage_guard_{}_{}", std::process::id(), metadata));
        std::fs::create_dir(&root).unwrap();
        let lock_path = root.join("writer.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .unwrap();
        file.try_lock().unwrap();
        let (released_tx, released_rx) = mpsc::channel();
        let lease = Arc::new(Lease {
            file: Some(file),
            released: released_tx,
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let pool = BufferPool::new(BufferPoolConfig::for_storage(), &mut Registry::default());
        let observer = Storage::new(super::super::Config::new(root.clone(), 1024), pool);
        let storage = observer.clone().with_guard(lease);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let bound = Duration::from_secs(5);

        let task = if metadata {
            runtime.spawn(async move {
                storage
                    .run(move |storage| {
                        let _ = ready_tx.send(());
                        resume_rx.recv_timeout(bound).unwrap();
                        std::fs::write(storage.cfg.storage_directory.join("done"), b"done")?;
                        Ok(())
                    })
                    .await
            })
        } else {
            let (blob, _) = runtime
                .block_on(storage.open("partition", b"blob"))
                .unwrap();
            drop(storage);
            runtime.spawn(async move {
                let file = blob.file.clone();
                // Use the same job boundary as every native Blob I/O method,
                // pausing a real write after the blocking job has started.
                let pending = spawn_with_guard(blob.guard.clone(), move || {
                    let _ = ready_tx.send(());
                    resume_rx.recv_timeout(bound).unwrap();
                    file.write_all_at(b"done", Header::SIZE_U64)
                });
                drop(blob);
                pending.await.unwrap().map_err(Error::from)
            })
        };
        runtime.block_on(ready_rx).unwrap();
        task.abort();
        assert!(runtime.block_on(task).unwrap_err().is_cancelled());
        if metadata {
            assert!(observer.lock.try_lock().is_err());
        }
        runtime.shutdown_background();

        let contender = File::open(&lock_path).unwrap();
        assert!(matches!(
            contender.try_lock(),
            Err(TryLockError::WouldBlock)
        ));
        resume_tx.send(()).unwrap();
        released_rx.recv_timeout(bound).unwrap();
        contender.try_lock().unwrap();
        let bytes = if metadata {
            std::fs::read(root.join("done")).unwrap()
        } else {
            std::fs::read(root.join("partition").join(hex(b"blob"))).unwrap()[Header::SIZE..]
                .to_vec()
        };
        assert_eq!(bytes, b"done");
        drop(contender);
        std::fs::remove_dir_all(root).unwrap();
    }
}
