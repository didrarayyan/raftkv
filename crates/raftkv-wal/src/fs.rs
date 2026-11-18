//! Minimal synchronous filesystem abstraction and in-memory fault-injecting implementation.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Open file handle used by [`Fs`] implementations.
pub trait FsFile {
    /// Writes all bytes in `buf` sequentially to the file.
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
    /// Flushes modified file data to stable storage (`fdatasync`).
    fn sync_data(&mut self) -> io::Result<()>;
    /// Returns the current byte length of the file.
    fn len(&self) -> io::Result<u64>;
    /// Returns `true` if the file contains zero bytes.
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
    /// Truncates or extends the file to `len` bytes.
    fn truncate(&mut self, len: u64) -> io::Result<()>;
}

/// Synchronous filesystem boundary enabling deterministic crash simulation.
pub trait Fs {
    /// Concrete file handle type opened by this filesystem.
    type File: FsFile;

    /// Recursively creates `path` and any missing parent directories.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// Lists immediate entries inside `path` in deterministic sorted order.
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
    /// Creates or truncates a file for sequential writing.
    fn create_file(&self, path: &Path) -> io::Result<Self::File>;
    /// Opens an existing file for appending and truncation recovery.
    fn open_append(&self, path: &Path) -> io::Result<Self::File>;
    /// Reads the entire contents of `path` into memory.
    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>>;
    /// Atomically renames `from` to `to`, replacing `to` if it exists.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Removes the file at `path`.
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    /// Persists pending directory metadata changes for `path`.
    fn sync_dir(&self, path: &Path) -> io::Result<()>;
}

/// Real OS filesystem backed by `std::fs`.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdFs;

/// Real file handle backed by [`std::fs::File`].
#[derive(Debug)]
pub struct StdFile(File);

impl FsFile for StdFile {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.write_all(buf)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.0.set_len(len)?;
        self.0.seek(SeekFrom::End(0))?;
        Ok(())
    }
}

impl Fs for StdFs {
    type File = StdFile;

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(path)? {
            out.push(entry?.path());
        }
        out.sort();
        Ok(out)
    }

    fn create_file(&self, path: &Path) -> io::Result<Self::File> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(StdFile(file))
    }

    fn open_append(&self, path: &Path) -> io::Result<Self::File> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(StdFile(file))
    }

    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            File::open(path)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let meta = fs::metadata(path)?;
            if !meta.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "path is not a directory",
                ));
            }
            Ok(())
        }
    }
}

#[cfg(any(test, feature = "fault-injection"))]
mod fault {
    use std::cell::RefCell;
    use std::collections::{BTreeMap, BTreeSet};
    use std::io;
    use std::path::{Component, Path, PathBuf};
    use std::rc::Rc;

    use rand::{Rng, RngExt};
    use rand_chacha::ChaCha8Rng;
    use rand_chacha::rand_core::SeedableRng;

    use super::{Fs, FsFile};

    #[derive(Debug, Clone, Default)]
    struct FaultInode {
        durable: Vec<u8>,
        pending: Vec<u8>,
    }

    #[derive(Debug, Clone)]
    enum PendingDirOp {
        CreateDir(String),
        CreateFile {
            path: String,
            inode: u64,
        },
        Rename {
            from: String,
            to: String,
            inode: u64,
        },
        Remove(String),
    }

    #[derive(Debug)]
    struct FaultFsInner {
        next_inode: u64,
        inodes: BTreeMap<u64, FaultInode>,
        durable_dirs: BTreeSet<String>,
        working_dirs: BTreeSet<String>,
        durable_entries: BTreeMap<String, u64>,
        working_entries: BTreeMap<String, u64>,
        pending_dir_ops: Vec<(String, PendingDirOp)>,
        eio_prob: f64,
        fail_next_io: bool,
        fail_next_sync: bool,
        rng: ChaCha8Rng,
    }

    /// In-memory filesystem supporting torn writes, unsynced directory losses, and `EIO` injection.
    #[derive(Debug, Clone)]
    pub struct FaultFs {
        inner: Rc<RefCell<FaultFsInner>>,
    }

    /// File handle backed by [`FaultFs`].
    #[derive(Debug)]
    pub struct FaultFile {
        inner: Rc<RefCell<FaultFsInner>>,
        inode: u64,
    }

    impl FaultFs {
        /// Creates a new empty in-memory fault-injecting filesystem seeded by `seed`.
        pub fn new(seed: u64) -> Self {
            let mut dirs = BTreeSet::new();
            dirs.insert("/".to_string());
            Self {
                inner: Rc::new(RefCell::new(FaultFsInner {
                    next_inode: 1,
                    inodes: BTreeMap::new(),
                    durable_dirs: dirs.clone(),
                    working_dirs: dirs,
                    durable_entries: BTreeMap::new(),
                    working_entries: BTreeMap::new(),
                    pending_dir_ops: Vec::new(),
                    eio_prob: 0.0,
                    fail_next_io: false,
                    fail_next_sync: false,
                    rng: ChaCha8Rng::seed_from_u64(seed),
                })),
            }
        }

        /// Configures the probability in `[0.0, 1.0]` of injecting `EIO` on `write_all` or `sync_data`.
        pub fn set_eio_probability(&self, prob: f64) {
            self.inner.borrow_mut().eio_prob = prob.clamp(0.0, 1.0);
        }

        /// Forces the next `write_all` or `sync_data` call to fail with `EIO`.
        pub fn fail_next_io(&self) {
            self.inner.borrow_mut().fail_next_io = true;
        }

        /// Forces the next `sync_data` call to fail with `EIO` after `write_all` has buffered pending bytes.
        pub fn fail_next_sync(&self) {
            self.inner.borrow_mut().fail_next_sync = true;
        }

        /// Simulates an abrupt power loss.
        ///
        /// Keeps a random prefix of unsynced directory operations and drops unsynced file bytes,
        /// optionally retaining a random torn prefix of pending appends when `torn_writes` is true.
        pub fn crash(&self, rng: &mut impl Rng, torn_writes: bool) {
            let mut inner = self.inner.borrow_mut();

            // 1. Apply a random prefix of unsynced directory ops to durable directory entries.
            let ops = std::mem::take(&mut inner.pending_dir_ops);
            let keep_ops = if ops.is_empty() {
                0
            } else {
                rng.random_range(0..=ops.len())
            };
            for (_, op) in ops.into_iter().take(keep_ops) {
                match op {
                    PendingDirOp::CreateDir(dir) => {
                        inner.durable_dirs.insert(dir);
                    }
                    PendingDirOp::CreateFile { path, inode } => {
                        inner.durable_entries.insert(path, inode);
                    }
                    PendingDirOp::Rename { from, to, inode } => {
                        inner.durable_entries.remove(&from);
                        inner.durable_entries.insert(to, inode);
                    }
                    PendingDirOp::Remove(path) => {
                        inner.durable_entries.remove(&path);
                    }
                }
            }

            // 2. Reconcile surviving inodes with durable state, optionally preserving torn tails.
            let live_inodes: BTreeSet<u64> = inner.durable_entries.values().copied().collect();
            let mut next_inodes = BTreeMap::new();
            for ino in live_inodes {
                if let Some(node) = inner.inodes.get(&ino) {
                    let final_bytes = if torn_writes && node.pending != node.durable {
                        if node.pending.len() >= node.durable.len()
                            && node.pending.starts_with(&node.durable)
                        {
                            let cut = rng.random_range(node.durable.len()..=node.pending.len());
                            node.pending[..cut].to_vec()
                        } else {
                            let cut = rng.random_range(0..=node.pending.len());
                            node.pending[..cut].to_vec()
                        }
                    } else {
                        node.durable.clone()
                    };
                    next_inodes.insert(
                        ino,
                        FaultInode {
                            durable: final_bytes.clone(),
                            pending: final_bytes,
                        },
                    );
                }
            }

            let inner = &mut *inner;
            inner.inodes = next_inodes;
            inner.working_dirs.clone_from(&inner.durable_dirs);
            inner.working_entries.clone_from(&inner.durable_entries);
        }

        /// Returns a direct copy of a file's current bytes for corruption/truncation tests.
        pub fn read_raw(&self, path: &Path) -> Option<Vec<u8>> {
            let key = normalize_path(path);
            let inner = self.inner.borrow();
            let ino = inner.working_entries.get(&key)?;
            inner.inodes.get(ino).map(|n| n.pending.clone())
        }

        /// Overwrites a file's durable and pending bytes directly for fault-injection tests.
        pub fn overwrite_durable(&self, path: &Path, data: Vec<u8>) {
            let key = normalize_path(path);
            let mut inner = self.inner.borrow_mut();
            let ino = if let Some(&existing) = inner.working_entries.get(&key) {
                existing
            } else {
                let id = inner.next_inode;
                inner.next_inode += 1;
                inner.working_entries.insert(key.clone(), id);
                inner.durable_entries.insert(key, id);
                id
            };
            inner.inodes.insert(
                ino,
                FaultInode {
                    durable: data.clone(),
                    pending: data,
                },
            );
        }
    }

    impl FaultFsInner {
        fn maybe_inject_eio(&mut self) -> io::Result<()> {
            if self.fail_next_io {
                self.fail_next_io = false;
                return Err(io::Error::other("injected EIO"));
            }
            if self.eio_prob > 0.0 && self.rng.random_bool(self.eio_prob) {
                return Err(io::Error::other("injected probabilistic EIO"));
            }
            Ok(())
        }
    }

    impl FsFile for FaultFile {
        fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
            let mut inner = self.inner.borrow_mut();
            inner.maybe_inject_eio()?;
            let node = inner
                .inodes
                .get_mut(&self.inode)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inode removed"))?;
            node.pending.extend_from_slice(buf);
            Ok(())
        }

        fn sync_data(&mut self) -> io::Result<()> {
            let mut inner = self.inner.borrow_mut();
            if inner.fail_next_sync {
                inner.fail_next_sync = false;
                return Err(io::Error::other("injected EIO on sync_data"));
            }
            inner.maybe_inject_eio()?;
            let node = inner
                .inodes
                .get_mut(&self.inode)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inode removed"))?;
            node.durable.clone_from(&node.pending);
            Ok(())
        }

        fn len(&self) -> io::Result<u64> {
            let inner = self.inner.borrow();
            let node = inner
                .inodes
                .get(&self.inode)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inode not found"))?;
            Ok(node.pending.len() as u64)
        }

        fn truncate(&mut self, len: u64) -> io::Result<()> {
            let mut inner = self.inner.borrow_mut();
            let node = inner
                .inodes
                .get_mut(&self.inode)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inode not found"))?;
            node.pending.truncate(len as usize);
            Ok(())
        }
    }

    impl Fs for FaultFs {
        type File = FaultFile;

        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            let mut inner = self.inner.borrow_mut();
            let norm = normalize_path(path);
            let mut accum = String::new();
            for part in norm.split('/').filter(|p| !p.is_empty()) {
                accum.push('/');
                accum.push_str(part);
                if inner.working_dirs.insert(accum.clone()) {
                    let parent = parent_key(&accum);
                    inner
                        .pending_dir_ops
                        .push((parent, PendingDirOp::CreateDir(accum.clone())));
                }
            }
            Ok(())
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            let inner = self.inner.borrow();
            let dir = normalize_path(path);
            if !inner.working_dirs.contains(&dir) {
                return Err(io::Error::new(io::ErrorKind::NotFound, "dir not found"));
            }
            let prefix = if dir == "/" {
                "/".to_string()
            } else {
                format!("{dir}/")
            };
            let mut entries = BTreeSet::new();
            for file in inner.working_entries.keys() {
                if let Some(rest) = file.strip_prefix(&prefix)
                    && !rest.is_empty()
                    && !rest.contains('/')
                {
                    entries.insert(PathBuf::from(file));
                }
            }
            Ok(entries.into_iter().collect())
        }

        fn create_file(&self, path: &Path) -> io::Result<Self::File> {
            let mut inner = self.inner.borrow_mut();
            let key = normalize_path(path);
            let parent = parent_key(&key);
            if !inner.working_dirs.contains(&parent) {
                return Err(io::Error::new(io::ErrorKind::NotFound, "parent missing"));
            }
            let inode = inner.next_inode;
            inner.next_inode += 1;
            inner.inodes.insert(inode, FaultInode::default());
            inner.working_entries.insert(key.clone(), inode);
            inner
                .pending_dir_ops
                .push((parent, PendingDirOp::CreateFile { path: key, inode }));
            Ok(FaultFile {
                inner: Rc::clone(&self.inner),
                inode,
            })
        }

        fn open_append(&self, path: &Path) -> io::Result<Self::File> {
            let inner = self.inner.borrow();
            let key = normalize_path(path);
            let &inode = inner
                .working_entries
                .get(&key)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file missing"))?;
            Ok(FaultFile {
                inner: Rc::clone(&self.inner),
                inode,
            })
        }

        fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
            let inner = self.inner.borrow();
            let key = normalize_path(path);
            let &inode = inner
                .working_entries
                .get(&key)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file missing"))?;
            let node = inner
                .inodes
                .get(&inode)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "inode missing"))?;
            Ok(node.pending.clone())
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            let mut inner = self.inner.borrow_mut();
            let from_key = normalize_path(from);
            let to_key = normalize_path(to);
            let inode = inner
                .working_entries
                .remove(&from_key)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "source missing"))?;
            inner.working_entries.insert(to_key.clone(), inode);
            let parent = parent_key(&to_key);
            inner.pending_dir_ops.push((
                parent,
                PendingDirOp::Rename {
                    from: from_key,
                    to: to_key,
                    inode,
                },
            ));
            Ok(())
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            let mut inner = self.inner.borrow_mut();
            let key = normalize_path(path);
            if inner.working_entries.remove(&key).is_none() {
                return Err(io::Error::new(io::ErrorKind::NotFound, "file missing"));
            }
            let parent = parent_key(&key);
            inner
                .pending_dir_ops
                .push((parent, PendingDirOp::Remove(key)));
            Ok(())
        }

        fn sync_dir(&self, path: &Path) -> io::Result<()> {
            let mut inner = self.inner.borrow_mut();
            let target = normalize_path(path);
            let mut remaining = Vec::new();
            let ops = std::mem::take(&mut inner.pending_dir_ops);
            for (parent, op) in ops {
                let matches = parent == target
                    || match &op {
                        PendingDirOp::CreateDir(d) => d == &target || target.starts_with(d),
                        _ => false,
                    };
                if matches {
                    match op {
                        PendingDirOp::CreateDir(d) => {
                            inner.durable_dirs.insert(d);
                        }
                        PendingDirOp::CreateFile { path, inode } => {
                            inner.durable_entries.insert(path, inode);
                        }
                        PendingDirOp::Rename { from, to, inode } => {
                            inner.durable_entries.remove(&from);
                            inner.durable_entries.insert(to, inode);
                        }
                        PendingDirOp::Remove(f) => {
                            inner.durable_entries.remove(&f);
                        }
                    }
                } else {
                    remaining.push((parent, op));
                }
            }
            inner.pending_dir_ops = remaining;
            Ok(())
        }
    }

    fn normalize_path(path: &Path) -> String {
        let mut parts: Vec<String> = Vec::new();
        for comp in path.components() {
            match comp {
                Component::Normal(s) => parts.push(s.to_string_lossy().into_owned()),
                Component::ParentDir => {
                    parts.pop();
                }
                Component::Prefix(p) => {
                    parts.push(p.as_os_str().to_string_lossy().replace(':', ""));
                }
                Component::RootDir | Component::CurDir => {}
            }
        }
        format!("/{}", parts.join("/"))
    }

    fn parent_key(norm: &str) -> String {
        match norm.rfind('/') {
            Some(0) | None => "/".to_string(),
            Some(pos) => norm[..pos].to_string(),
        }
    }
}

#[cfg(any(test, feature = "fault-injection"))]
pub use fault::{FaultFile, FaultFs};
