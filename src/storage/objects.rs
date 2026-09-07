//! The bucket (spec §18).
//!
//! Content-addressable: blake3(payload) -> objects/ab/cd/<digest>.
//! Backed by `object_store` so the backend can later become S3/MinIO/R2
//! without Receipt semantics changing. Large bodies are streamed through a
//! staging path and renamed into place, so nothing is ever held whole in RAM.

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Bucket {
    store: Arc<dyn ObjectStore>,
    root: std::path::PathBuf,
}

/// objects/ab/cd/<digest> — two levels of fan-out keeps directories small.
pub fn object_path(digest: &str) -> ObjPath {
    ObjPath::from(format!("{}/{}/{}", &digest[0..2], &digest[2..4], digest))
}

impl Bucket {
    pub fn open(root: &Path) -> Result<Self> {
        std::fs::create_dir_all(root)
            .with_context(|| format!("creating bucket at {}", root.display()))?;
        std::fs::create_dir_all(root.join("staging")).context("creating bucket staging")?;
        let store = LocalFileSystem::new_with_prefix(root)
            .with_context(|| format!("opening object store at {}", root.display()))?;
        Ok(Self { store: Arc::new(store), root: root.to_path_buf() })
    }

    pub fn root(&self) -> &Path { &self.root }

    /// Put bytes already in memory. Returns the digest.
    pub async fn put_bytes(&self, data: Bytes) -> Result<String> {
        let digest = blake3::hash(&data).to_hex().to_string();
        let path = object_path(&digest);
        // Content-addressed: if it exists, the bytes are identical by definition.
        if self.store.head(&path).await.is_ok() {
            return Ok(digest);
        }
        self.store.put(&path, data.into()).await.context("writing object")?;
        Ok(digest)
    }

    /// Stage an incrementally-written file, then commit it under its digest.
    /// Used by the streaming blob ingress so a 256 MiB upload costs ~64 KiB of RAM.
    pub fn staging_path(&self, id: &str) -> std::path::PathBuf {
        self.root.join("staging").join(id)
    }

    pub async fn commit_staged(&self, staged: &std::path::Path, digest: &str) -> Result<()> {
        let dest = self.root.join(object_path(digest).to_string());
        if dest.exists() {
            let _ = std::fs::remove_file(staged);
            return Ok(());
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).context("creating object fan-out dir")?;
        }
        std::fs::rename(staged, &dest)
            .with_context(|| format!("committing object to {}", dest.display()))?;
        Ok(())
    }

    pub async fn get(&self, digest: &str) -> Result<Bytes> {
        let path = object_path(digest);
        let res = self.store.get(&path).await.context("reading object")?;
        Ok(res.bytes().await.context("collecting object bytes")?)
    }

    /// Read only the first `n` bytes — enough to sniff a type without
    /// pulling a large file into memory.
    pub fn head_bytes(&self, digest: &str, n: usize) -> Result<Vec<u8>> {
        use std::io::Read;
        let p = self.root.join(object_path(digest).to_string());
        let mut f = std::fs::File::open(&p).with_context(|| format!("opening {}", p.display()))?;
        let mut buf = vec![0u8; n];
        let read = f.read(&mut buf)?;
        buf.truncate(read);
        Ok(buf)
    }

    pub fn size_of(&self, digest: &str) -> Result<u64> {
        let p = self.root.join(object_path(digest).to_string());
        Ok(std::fs::metadata(&p)?.len())
    }
}
