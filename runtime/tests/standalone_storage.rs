#![cfg(target_os = "linux")]

use commonware_runtime::{
    Blob, Storage as _,
    telemetry::metrics::Registry,
    tokio::{Storage, StorageConfig},
};

/// Exercise the public storage entry point without a Commonware runner. Reopening
/// must preserve prior process-visible writes, including a write without `sync`.
#[tokio::test(flavor = "current_thread")]
async fn reopen_on_existing_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!("standalone_storage_{}", std::process::id()));
    std::fs::create_dir(&root)?;
    let directory = root.join("nested").join("storage");
    let config = StorageConfig::new(directory.clone(), 1024 * 1024);
    let mut registry = Registry::default();
    let storage = Storage::init(config.clone(), &mut registry).await?;
    let (blob, size) = storage.open("partition", b"blob").await?;
    assert_eq!(size, 0);
    blob.write_at(0, b"recover").await?;
    drop(blob);
    drop(storage);

    let storage = Storage::init(config, &mut Registry::default()).await?;
    let (blob, size) = storage.open("partition", b"blob").await?;
    assert_eq!(size, 7);
    assert_eq!(blob.read_at(0, 7).await?.coalesce().as_ref(), b"recover");
    drop(blob);
    let (reader, size) = storage.open_read_only("partition", b"blob").await?;
    assert_eq!(size, 7);
    assert_eq!(reader.read_at(0, 7).await?.coalesce().as_ref(), b"recover");
    assert!(reader.write_at(0, b"changed").await.is_err());
    assert!(reader.resize(0).await.is_err());
    drop(reader);
    assert!(storage.open_read_only("absent", b"blob").await.is_err());
    assert!(!directory.join("absent").exists());
    let broken = directory
        .join("partition")
        .join(commonware_formatting::hex(b"broken"));
    std::fs::write(&broken, b"cw")?;
    assert!(
        storage
            .open_read_only("partition", b"broken")
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&broken)?, b"cw");
    std::fs::write(directory.join("not_a_partition"), b"keep")?;
    assert!(matches!(
        storage.scan("not_a_partition").await,
        Err(commonware_runtime::Error::Io(_))
    ));
    assert!(matches!(
        storage.remove("not_a_partition", Some(b"blob")).await,
        Err(commonware_runtime::Error::Io(_))
    ));
    drop(storage);

    // Initialization errors must be returned without constructing a storage handle.
    let file = root.join("file");
    std::fs::write(&file, [])?;
    let result = Storage::init(
        StorageConfig::new(file.join("storage"), 1024),
        &mut Registry::default(),
    )
    .await;
    assert!(result.is_err());
    std::fs::remove_dir_all(root)?;
    Ok(())
}
