//! APIs to read from Avro format to arrow.
use std::io::Read;

use avro_schema::file::{Block, Compression, FileMetadata};
use avro_schema::read::fallible_streaming_iterator::FallibleStreamingIterator;
use avro_schema::read::{BlockStreamingIterator, block_iterator};
use avro_schema::schema::Field as AvroField;

mod deserialize;
pub use deserialize::deserialize;
use polars_error::{PolarsError, PolarsResult, polars_bail, polars_err};

mod nested;
mod schema;
mod util;

pub use schema::infer_schema;

use crate::array::Array;
use crate::datatypes::ArrowSchema;
use crate::record_batch::RecordBatchT;

/// The Avro codecs that can be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Null,
    Deflate,
    Snappy,
    Zstandard,
}

/// Reads the metadata and codec of an Avro file, like [`avro_schema::read::read_metadata`],
/// but errors on codecs that can't be decompressed.
///
/// `avro_schema` treats any codec other than `deflate` and `snappy` as uncompressed, which
/// would decode compressed blocks as raw records.
pub fn read_metadata<R: Read>(reader: &mut R) -> PolarsResult<(FileMetadata, Codec)> {
    let mut header = RecordingReader {
        reader,
        bytes: Vec::new(),
    };
    // Like `avro_schema`, treat a header cut short as an invalid file, not an IO error.
    let codec = read_codec(&mut header).map_err(|err| match err {
        PolarsError::IO { error, .. } if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            polars_err!(oos = "Avro header ends early")
        },
        err => err,
    })?;
    let codec = match codec.as_deref() {
        None | Some(b"null") => Codec::Null,
        Some(b"deflate") => Codec::Deflate,
        Some(b"snappy") => Codec::Snappy,
        Some(b"zstandard") => Codec::Zstandard,
        Some(codec) => polars_bail!(
            ComputeError: "unsupported Avro codec {:?}; supported codecs are null, deflate, snappy and zstandard",
            String::from_utf8_lossy(codec)
        ),
    };
    let metadata = avro_schema::read::read_metadata(&mut header.bytes.as_slice())?;
    Ok((metadata, codec))
}

/// Reads the magic, header and marker of an Avro file and returns its `avro.codec`.
fn read_codec<R: Read>(reader: &mut R) -> PolarsResult<Option<Vec<u8>>> {
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if magic != [b'O', b'b', b'j', 1u8] {
        polars_bail!(oos = "Avro file does not start with the Avro magic bytes");
    }

    let mut codec = None;
    loop {
        let len = util::zigzag_i64(reader)?;
        if len == 0 {
            break;
        }
        // `avro_schema` doesn't support header blocks with a byte size (negative counts).
        if len < 0 {
            polars_bail!(oos = "Avro header block with a negative count is not supported");
        }
        for _ in 0..len {
            let key = read_bytes(reader)?;
            let value = read_bytes(reader)?;
            if key == b"avro.codec" {
                codec = Some(value);
            }
        }
    }

    let mut marker = [0u8; 16];
    reader.read_exact(&mut marker)?;
    Ok(codec)
}

fn read_bytes<R: Read>(reader: &mut R) -> PolarsResult<Vec<u8>> {
    let len = util::zigzag_i64(reader)?;
    if len < 0 {
        polars_bail!(oos = "Avro header contains a negative length");
    }
    let mut bytes = Vec::new();
    reader.take(len as u64).read_to_end(&mut bytes)?;
    if bytes.len() != len as usize {
        polars_bail!(oos = "Avro header ends early");
    }
    Ok(bytes)
}

/// Keeps a copy of everything read through it.
struct RecordingReader<'a, R> {
    reader: &'a mut R,
    bytes: Vec<u8>,
}

impl<R: Read> Read for RecordingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.reader.read(buf)?;
        self.bytes.extend_from_slice(&buf[..n]);
        Ok(n)
    }
}

/// Decompresses a zstandard block into `decompressed`, reusing its buffer.
#[cfg(feature = "io_avro_compression")]
fn decompress_zstandard(block: &Block, decompressed: &mut Block) -> PolarsResult<()> {
    decompressed.number_of_rows = block.number_of_rows;
    decompressed.data.clear();
    zstd::Decoder::with_buffer(block.data.as_slice())
        .and_then(|mut decoder| decoder.read_to_end(&mut decompressed.data))
        .map_err(|err| polars_err!(ComputeError: "invalid zstandard Avro block: {err}"))?;
    Ok(())
}

#[cfg(not(feature = "io_avro_compression"))]
fn decompress_zstandard(_block: &Block, _decompressed: &mut Block) -> PolarsResult<()> {
    polars_bail!(ComputeError: "reading zstandard Avro files requires the `io_avro_compression` feature")
}

/// Single threaded, blocking reader of Avro; [`Iterator`] of [`RecordBatchT`].
pub struct Reader<R: Read> {
    iter: BlockStreamingIterator<R>,
    codec: Codec,
    decompressed: Block,
    avro_fields: Vec<AvroField>,
    fields: ArrowSchema,
    projection: Vec<bool>,
}

impl<R: Read> Reader<R> {
    /// Creates a new [`Reader`]. `codec` comes from [`read_metadata`] and overrides
    /// `metadata.compression`.
    pub fn new(
        reader: R,
        metadata: FileMetadata,
        codec: Codec,
        fields: ArrowSchema,
        projection: Option<Vec<bool>>,
    ) -> Self {
        let projection = projection.unwrap_or_else(|| fields.iter().map(|_| true).collect());
        let compression = match codec {
            Codec::Deflate => Some(Compression::Deflate),
            Codec::Snappy => Some(Compression::Snappy),
            // `avro_schema` can't decompress zstandard; `next` does.
            Codec::Null | Codec::Zstandard => None,
        };

        Self {
            iter: block_iterator(reader, compression, metadata.marker),
            codec,
            decompressed: Block::default(),
            avro_fields: metadata.record.fields,
            fields,
            projection,
        }
    }

    /// Deconstructs itself into its internal reader
    pub fn into_inner(self) -> R {
        self.iter.into_inner()
    }
}

impl<R: Read> Iterator for Reader<R> {
    type Item = PolarsResult<RecordBatchT<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let fields = &self.fields;
        let avro_fields = &self.avro_fields;
        let projection = &self.projection;
        let codec = self.codec;
        let decompressed = &mut self.decompressed;

        self.iter.next().transpose().map(|maybe_block| {
            let mut block = maybe_block?;
            if codec == Codec::Zstandard {
                decompress_zstandard(block, decompressed)?;
                block = decompressed;
            }
            deserialize(block, fields, avro_fields, projection)
        })
    }
}
