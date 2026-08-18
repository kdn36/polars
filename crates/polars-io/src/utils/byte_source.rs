use std::alloc::{Layout, alloc, dealloc};
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::ops::Range;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
//kdn TMP
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use polars_buffer::Buffer;
use polars_core::prelude::PlHashMap;
use polars_error::{PolarsResult, feature_gated, polars_err};
use polars_utils::_limit_path_len_io_err;
use polars_utils::aliases::InitHashMaps;
use polars_utils::mmap::MMapSemaphore;
use polars_utils::pl_path::PlRefPath;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::cloud::concurrency_config::{ConcurrencyStrategy, FetchConfig};
use crate::cloud::options::CloudOptions;
#[cfg(feature = "cloud")]
use crate::cloud::{
    CloudLocation, ObjectStorePath, PolarsObjectStore, build_object_store, object_path_from_str,
};
use crate::metrics::IOMetrics;

static IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
static PEAK_IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
static TOTAL_READS: AtomicU64 = AtomicU64::new(0);

struct InFlightGuard;

impl InFlightGuard {
    fn new() -> Self {
        let cur = IN_FLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
        PEAK_IN_FLIGHT.fetch_max(cur, Ordering::Relaxed);
        TOTAL_READS.fetch_add(1, Ordering::Relaxed);
        Self
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
    }
}

//kdn DEV STATICS
#[cfg(all(target_os = "linux", feature = "io_uring"))]
static PLDEV_OPEN_FADV: LazyLock<usize> = LazyLock::new(|| {
    let fadv = std::env::var("PLDEV_OPEN_FADV").map_or(0, |x| x.parse::<usize>().unwrap());
    match fadv {
        0 => {
            eprintln!("PLDEV_OPEN_FADV posix_adv_normal");
        }, //defaults to POSIX_FADV_NORMAL
        1 => {
            eprintln!("PLDEV_OPEN_FADV posix_adv_sequential");
        },
        2 => {
            eprintln!("PLDEV_OPEN_FADV posix_adv_random");
        },
        3 => {
            eprintln!("PLDEV_OPEN_FADV posix_adv_willneed");
        },
        _ => panic!("illegal value for PLDEV_OPEN_FADV: {}", fadv),
    };
    fadv
});

static PLDEV_MAX_BUF_SIZE: LazyLock<Option<usize>> = LazyLock::new(|| {
    let max_buf_size = std::env::var("PLDEV_MAX_BUF_SIZE")
        .map(|x| x.parse::<usize>().unwrap())
        .ok();
    eprintln!(
        "PLDEV_MAX_BUF_SIZE tokio::fs::File set_max_buf_size: {:?}",
        max_buf_size
    );
    max_buf_size
});

static PLDEV_MAX_FILE_OPEN: LazyLock<usize> = LazyLock::new(|| {
    let max_file_open =
        std::env::var("PLDEV_MAX_FILE_OPEN").map_or(32, |x| x.parse::<usize>().unwrap());
    eprintln!("PLDEV_MAX_FILE_OPEN max_file_open: {:?}", max_file_open);
    max_file_open
});

static PLDEV_PREAD: LazyLock<bool> = LazyLock::new(|| {
    let pread = std::env::var("PLDEV_PREAD").is_ok();
    eprintln!("PLDEV_PREAD pread spawn_blocking: {}", pread);
    pread
});

static PLDEV_PREAD_O_DIRECT: LazyLock<bool> = LazyLock::new(|| {
    let pread_o_direct = std::env::var("PLDEV_PREAD_O_DIRECT").is_ok();
    eprintln!("PLDEV_PREAD_O_DIRECT pread O_DIRECT: {}", pread_o_direct);
    pread_o_direct
});

#[allow(async_fn_in_trait)]
pub trait ByteSource: Send + Sync {
    async fn get_size(&self) -> PolarsResult<usize>;
    /// # Panics
    /// Panics if `range` is not in bounds.
    async fn get_range(&self, range: Range<usize>) -> PolarsResult<Buffer<u8>>;
    /// Note: This will mutably sort ranges for coalescing.
    async fn get_ranges(
        &self,
        ranges: &mut [Range<usize>],
    ) -> PolarsResult<PlHashMap<usize, Buffer<u8>>>;

    //kdn ADDED
    /// Advisory hint that `ranges` will be read soon. Non-blocking, best-effort.
    fn hint_will_need(&self, _ranges: &[Range<usize>]) {}
}

/// Byte source backed by a `Buffer`, which can potentially be memory-mapped.
pub struct BufferByteSource(pub Buffer<u8>);

impl BufferByteSource {
    async fn try_new_mmap_from_path(
        path: &Path,
        _cloud_options: Option<&CloudOptions>,
    ) -> PolarsResult<Self> {
        dbg!("start BufferByteSource::try_new_mmap_from_path"); //kdn
        dbg!(".. tokio::fs::file::open(path)"); //kdn
        let file = Arc::new(
            tokio::fs::File::open(path)
                .await
                .map_err(|err| _limit_path_len_io_err(path, err))?
                .into_std()
                .await,
        );

        Ok(Self(Buffer::from_owner(MMapSemaphore::new_from_file(
            &file,
        )?)))
    }
}

impl ByteSource for BufferByteSource {
    async fn get_size(&self) -> PolarsResult<usize> {
        Ok(self.0.as_ref().len())
    }

    async fn get_range(&self, range: Range<usize>) -> PolarsResult<Buffer<u8>> {
        let out = self.0.clone().sliced(range);
        Ok(out)
    }

    async fn get_ranges(
        &self,
        ranges: &mut [Range<usize>],
    ) -> PolarsResult<PlHashMap<usize, Buffer<u8>>> {
        Ok(ranges
            .iter()
            .map(|x| (x.start, self.0.clone().sliced(x.clone())))
            .collect())
    }
}

// kdn TODO comment
// kdn TODO pool of handles, size, path, IOmetrics
// kdn TODO TBD feature gating
// kdn TODO rename to TokioIoUringByteSource?
pub struct IoUringByteSource {
    /// Cache of idle handles. Grows lazily to peak concurrency, up to `cap`.
    // Note. This is a requirement from the tokio `AsyncRead` API, which holds
    // a cursor and is therefore stateful. Direct io_uring allows for exact
    // (start, stop) arguments and does not require mulitple file handles.
    free: Mutex<Vec<File>>,
    /// Track the number of open file handles to the specified max `cap``.
    permits: Semaphore,
    /// For opening additional handles on demand.
    path: PathBuf,
    // File size.
    size: u64,

    //kdn experiment: pread path
    std_file: Option<Arc<std::fs::File>>,
}

struct Lease<'a> {
    src: &'a IoUringByteSource,
    file: Option<File>,
    _permit: SemaphorePermit<'a>,
}

impl Lease<'_> {
    fn file(&mut self) -> &mut File {
        self.file.as_mut().expect("lease used after take")
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if let Some(f) = self.file.take() {
            self.src.free.lock().push(f);
        }
    }
}

//kdn for O_DIRECT
struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
    layout: Layout,
}

// Safety: exclusive ownership of a heap allocation; no interior mutability.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AsRef<[u8]> for AlignedBuf {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) }
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn _fadvise(file: &File) {
    // kdn TODO: benchmark
    // kdn TODO: only for non-sequential reads - see POSIX_FADV_SEQUENTIAL etc.
    // kdn TODO: investigate WILLNEED
    match *PLDEV_OPEN_FADV {
        0 => {}, //defaults to POSIX_FADV_NORMAL
        1 => unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        },
        2 => unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM);
        },
        3 => {
            unsafe {
                // makes little sense here
                libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_WILLNEED);
            }
        },
        _ => unreachable!("unexpected PLDEV_OPEN_FADV"),
    };
}

impl IoUringByteSource {
    //kdn TBD Path or PlRefPath
    async fn try_new_from_path(path: &Path) -> PolarsResult<Self> {
        let mut file = File::open(path).await?;
        if let Some(max_buf_size) = *PLDEV_MAX_BUF_SIZE {
            file.set_max_buf_size(max_buf_size); //kdn TRY
        }

        let size = file.metadata().await?.len();
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        _fadvise(&file);

        let cap = *PLDEV_MAX_FILE_OPEN;

        // kdn experiment pread
        //kdn TODO feature gate
        let std_file = if *PLDEV_PREAD {
            Some(Arc::new(if *PLDEV_PREAD_O_DIRECT {
                dbg!("IoUringByteSource::try_new_from_path open std_file with O_DIRECT");
                OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(path)?
            } else {
                dbg!("IoUringByteSource::try_new_from_path open std_file");
                std::fs::File::open(path)?
            }))
        } else {
            None
        };

        Ok(IoUringByteSource {
            free: Mutex::new(vec![file]),
            permits: Semaphore::new(cap),
            path: path.into(),
            size,

            std_file,
        })
    }

    async fn lease(&self) -> PolarsResult<Lease<'_>> {
        let permit = self.permits.acquire().await.unwrap();

        // TODO - investigate tokio::fs::File::try_clone()
        // `pop` enables ownership
        let existing = self.free.lock().pop();
        let file = Some(match existing {
            Some(f) => f,
            None => {
                let mut file = File::open(&self.path).await?;
                if let Some(max_buf_size) = *PLDEV_MAX_BUF_SIZE {
                    file.set_max_buf_size(max_buf_size); //kdn TRY
                }

                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                _fadvise(&file);
                file
            },
        });

        Ok(Lease {
            src: self,
            file,
            _permit: permit,
        })
    }
}

impl ByteSource for IoUringByteSource {
    async fn get_size(&self) -> PolarsResult<usize> {
        usize::try_from(self.size)
            .map_err(|_| polars_err!(ComputeError: "file size {} does not fit in usize", self.size))
    }

    async fn get_range(&self, range: Range<usize>) -> PolarsResult<Buffer<u8>> {
        assert!(range.end as u64 <= self.size);

        if *PLDEV_PREAD {
            let file = self.std_file.clone().unwrap();
            let offset = range.start as u64;
            let len = range.len();
            let size = self.size;
            let o_direct = *PLDEV_PREAD_O_DIRECT;

            // kdn TODO
            let _permit = self.permits.acquire().await.unwrap();

            let result = tokio::task::spawn_blocking(move || -> PolarsResult<Buffer<u8>> {
                let _g = InFlightGuard::new();
                if o_direct {
                    //kdn TODO: Get the alignment from logical_block_size.
                    const ALIGN: usize = 4096;
                    let lo = offset & !(ALIGN as u64 - 1);
                    // Clamp to EOF: read_exact_at fails on a short read past the end.
                    let hi = ((offset + len as u64).next_multiple_of(ALIGN as u64)).min(size);
                    let span = (hi - lo) as usize;
                    let pad = (offset - lo) as usize;

                    let layout = Layout::from_size_align(span, ALIGN).unwrap();
                    //kdn TODO investigate alloc and zero
                    let ptr = unsafe { alloc(layout) };
                    if ptr.is_null() {
                        std::alloc::handle_alloc_error(layout)
                    }
                    let raw = unsafe { std::slice::from_raw_parts_mut(ptr, span) };

                    if hi <= size {
                        file.read_exact_at(raw, lo)?;
                    } else {
                        // Last block: O_DIRECT rejects an unaligned length, and the aligned
                        // length runs past EOF. Read what exists, leave the tail untouched.
                        let mut filled = 0;
                        while (lo + filled as u64) < size {
                            let n = file.read_at(&mut raw[filled..], lo + filled as u64)?;
                            if n == 0 {
                                break;
                            }
                            filled += n;
                        }
                    }
                    // file.read_exact_at(raw, lo)?;

                    let owner = AlignedBuf {
                        ptr,
                        len: span,
                        layout,
                    };
                    Ok(Buffer::from_owner(owner).sliced(pad..pad + len))
                } else {
                    let mut buf = Vec::with_capacity(len);
                    unsafe { buf.set_len(len) };
                    file.read_exact_at(&mut buf, offset)?;
                    Ok(Buffer::from(buf))
                }
            })
            .await
            .expect("blocking task panicked");
            return result;
        }

        let mut lease = self.lease().await?;
        let file = lease.file();
        file.seek(SeekFrom::Start(range.start as u64)).await?;

        let mut buf = Vec::with_capacity(range.len());
        //kdn TODO
        unsafe {
            buf.set_len(range.len());
        }
        //kdn TODO RM guard
        {
            let _g = InFlightGuard::new();
            // let t0 = Instant::now();
            file.read_exact(&mut buf).await?;
            // eprintln!("read_exact: {:?}", t0.elapsed().as_micros());
        }

        // let mut buf = vec![0u8; range.len()];
        // let mut buf = Buffer::zeroed(range.len());
        // file.read_exact(&mut buf).await?;

        Ok(Buffer::from(buf))
    }

    async fn get_ranges(
        &self,
        ranges: &mut [Range<usize>],
    ) -> PolarsResult<PlHashMap<usize, Buffer<u8>>> {
        //kdn TODO coalesce when the gap == 0

        let mut out = PlHashMap::new();

        let mut futures: FuturesUnordered<_> = ranges
            .iter()
            .map(|range| async { (range.start, self.get_range(range.clone()).await) })
            .collect();

        while let Some((start, buf)) = futures.next().await {
            out.insert(start, buf?);
        }

        Ok(out)
    }

    //kdn TODO RM also trait
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    fn hint_will_need(&self, ranges: &[Range<usize>]) {
        // Any handle works: readahead populates the shared page cache,
        // even though the *window* setting is per-description.
        let Some(fd) = self.free.lock().first().map(|f| f.as_raw_fd()) else {
            return;
        };

        for r in ranges {
            let rc = unsafe {
                libc::posix_fadvise(fd, r.start as _, r.len() as _, libc::POSIX_FADV_WILLNEED)
            };
            if rc != 0 { /* advisory — log at debug and move on */ }
        }
    }
}

//kdn TODO RM
impl Drop for IoUringByteSource {
    fn drop(&mut self) {
        eprintln!(
            "reads={} peak_in_flight={}",
            TOTAL_READS.load(Ordering::Relaxed),
            PEAK_IN_FLIGHT.load(Ordering::Relaxed),
        );
    }
}

#[cfg(feature = "cloud")]
pub struct ObjectStoreByteSource {
    store: PolarsObjectStore,
    path: ObjectStorePath,
    config: FetchConfig,
}

#[cfg(feature = "cloud")]
impl ObjectStoreByteSource {
    async fn try_new_from_path(
        path: PlRefPath,
        cloud_options: Option<&CloudOptions>,
        io_metrics: Option<Arc<IOMetrics>>,
        config: FetchConfig,
    ) -> PolarsResult<Self> {
        let (CloudLocation { prefix, .. }, mut store) =
            build_object_store(path, cloud_options, false).await?;
        let path = object_path_from_str(&prefix)?;

        store.set_io_metrics(io_metrics);

        Ok(Self {
            store,
            path,
            config,
        })
    }

    #[allow(unused)]
    fn chunk_size(&self) -> usize {
        self.config.chunk_size
    }

    fn concurrency_strategy(&self) -> ConcurrencyStrategy {
        self.config.strategy
    }
}

#[cfg(feature = "cloud")]
impl ByteSource for ObjectStoreByteSource {
    async fn get_size(&self) -> PolarsResult<usize> {
        Ok(self
            .store
            .head(&self.path, ConcurrencyStrategy::Legacy)
            .await?
            .size as usize)
    }

    async fn get_range(&self, range: Range<usize>) -> PolarsResult<Buffer<u8>> {
        self.store.get_range(&self.path, range, self.config).await
    }

    async fn get_ranges(
        &self,
        ranges: &mut [Range<usize>],
    ) -> PolarsResult<PlHashMap<usize, Buffer<u8>>> {
        self.store
            .get_ranges_sort(&self.path, ranges, self.config)
            .await
    }
}

/// Dynamic dispatch to async functions.
pub enum DynByteSource {
    Buffer(BufferByteSource),
    IoUring(IoUringByteSource),
    #[cfg(feature = "cloud")]
    Cloud(ObjectStoreByteSource),
}

impl DynByteSource {
    pub fn variant_name(&self) -> &str {
        match self {
            Self::Buffer(_) => "Buffer",
            Self::IoUring(_) => "IoUring",
            #[cfg(feature = "cloud")]
            Self::Cloud(_) => "Cloud",
        }
    }

    pub fn is_cloud(&self) -> bool {
        match self {
            Self::Buffer(_) => false,
            Self::IoUring(_) => false,
            #[cfg(feature = "cloud")]
            Self::Cloud(_) => true,
        }
    }

    pub fn chunk_size(&self) -> Option<usize> {
        match self {
            Self::Buffer(_) => None,
            Self::IoUring(_) => None,
            #[cfg(feature = "cloud")]
            Self::Cloud(source) => Some(source.config.chunk_size),
        }
    }

    pub fn concurrency_strategy(&self) -> Option<ConcurrencyStrategy> {
        match self {
            Self::Buffer(_) => None,
            //kdn TBD
            Self::IoUring(_) => None,
            #[cfg(feature = "cloud")]
            Self::Cloud(source) => Some(source.concurrency_strategy()),
        }
    }
}

impl Default for DynByteSource {
    fn default() -> Self {
        Self::Buffer(BufferByteSource(Buffer::new()))
    }
}

impl ByteSource for DynByteSource {
    async fn get_size(&self) -> PolarsResult<usize> {
        match self {
            Self::Buffer(v) => v.get_size().await,
            Self::IoUring(v) => v.get_size().await,
            #[cfg(feature = "cloud")]
            Self::Cloud(v) => v.get_size().await,
        }
    }

    async fn get_range(&self, range: Range<usize>) -> PolarsResult<Buffer<u8>> {
        match self {
            Self::Buffer(v) => v.get_range(range).await,
            Self::IoUring(v) => v.get_range(range).await,
            #[cfg(feature = "cloud")]
            Self::Cloud(v) => v.get_range(range).await,
        }
    }

    async fn get_ranges(
        &self,
        ranges: &mut [Range<usize>],
    ) -> PolarsResult<PlHashMap<usize, Buffer<u8>>> {
        match self {
            Self::Buffer(v) => v.get_ranges(ranges).await,
            Self::IoUring(v) => v.get_ranges(ranges).await,
            #[cfg(feature = "cloud")]
            Self::Cloud(v) => v.get_ranges(ranges).await,
        }
    }
}

impl From<BufferByteSource> for DynByteSource {
    fn from(value: BufferByteSource) -> Self {
        Self::Buffer(value)
    }
}

impl From<IoUringByteSource> for DynByteSource {
    fn from(value: IoUringByteSource) -> Self {
        Self::IoUring(value)
    }
}

#[cfg(feature = "cloud")]
impl From<ObjectStoreByteSource> for DynByteSource {
    fn from(value: ObjectStoreByteSource) -> Self {
        Self::Cloud(value)
    }
}

impl From<Buffer<u8>> for DynByteSource {
    fn from(value: Buffer<u8>) -> Self {
        Self::Buffer(BufferByteSource(value))
    }
}

#[derive(Clone, Debug)]
pub enum DynByteSourceBuilder {
    Mmap,
    //kdn
    IoUring,
    /// Supports both cloud and local files, requires cloud feature.
    ObjectStore(FetchConfig),
}

impl DynByteSourceBuilder {
    pub async fn try_build_from_path(
        &self,
        path: PlRefPath,
        cloud_options: Option<&CloudOptions>,
        io_metrics: Option<Arc<IOMetrics>>,
    ) -> PolarsResult<DynByteSource> {
        dbg!("start DynByteSourceBuilder::try_build_from_path");
        Ok(match *self {
            Self::Mmap => {
                BufferByteSource::try_new_mmap_from_path(path.as_std_path(), cloud_options)
                    .await?
                    .into()
            },
            Self::IoUring => IoUringByteSource::try_new_from_path(path.as_std_path())
                .await?
                .into(),
            Self::ObjectStore(fetch_config) => feature_gated!("cloud", {
                ObjectStoreByteSource::try_new_from_path(
                    path,
                    cloud_options,
                    io_metrics,
                    fetch_config,
                )
                .await?
                .into()
            }),
        })
    }

    pub fn chunk_size(&self) -> Option<usize> {
        match self {
            Self::Mmap => None,
            Self::IoUring => None,
            Self::ObjectStore(fetch_config) => Some(fetch_config.chunk_size),
        }
    }

    pub fn concurrency_strategy(&self) -> Option<&ConcurrencyStrategy> {
        match self {
            Self::Mmap => None,
            Self::IoUring => None,
            Self::ObjectStore(fetch_config) => Some(&fetch_config.strategy),
        }
    }
}
