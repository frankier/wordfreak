use std::path::Path;
use std::io::{BufReader, Cursor, Read};
use std::fs::File;

use memmap::Mmap;
use itertools::Itertools;
use piz::{CompressionMethod, ZipArchive};
use piz::read::{read_direct, FileMetadata};


pub type MinEntry  = (usize, u32, usize, CompressionMethod, usize);
pub type MinEntries = Vec<MinEntry>;
pub type WholeEntryReader = Cursor<Box<[u8]>>;
pub type EntryBufReader<'a> = BufReader<Box<dyn Read + Send + 'a>>;
const READ_CHUNK_SIZE: usize = 64 * 1024;
pub const UNZIP_READERS: usize = 4;


pub(crate) fn mmap_file(path: &Path) -> Mmap {
    // XXX: This should be unsafe if this is a library
    let zip_file = File::open(path).unwrap();
    unsafe { Mmap::map(&zip_file).unwrap() }
}


pub fn filter_zip_entries(zip_reader: &ZipArchive, pred: fn(&FileMetadata) -> bool) -> MinEntries {
    zip_reader.entries().into_iter().filter_map(|entry| {
        if pred(entry) {
            Some((
                entry.header_offset,
                entry.crc32,
                entry.size,
                entry.compression_method,
                entry.compressed_size,
            ))
        } else {
            None
        }
    }).collect_vec()
}


pub fn open_piz(path: &Path, pred: fn(&FileMetadata) -> bool) -> (Mmap, MinEntries) {
    let mmap = mmap_file(path);
    let zip_reader: ZipArchive = ZipArchive::new(&mmap).unwrap();
    let entries = filter_zip_entries(&zip_reader, pred);
    (mmap, entries)
}


pub fn read_whole_file(mmap: &Mmap, entry: &MinEntry) -> WholeEntryReader {
    let (header_offset, crc32, size, compression_method, compressed_size) = entry;
    let mut contents = Vec::with_capacity(*size);
    read_direct(&mmap, *header_offset, *crc32, *compression_method, *compressed_size).unwrap().read_to_end(&mut contents).unwrap();
    Cursor::new(contents.into_boxed_slice())
}


pub fn read_buf<'a>(mmap: &'a Mmap, entry: &MinEntry) -> EntryBufReader<'a> {
    let (header_offset, crc32, size, compression_method, compressed_size) = entry;
    BufReader::with_capacity(READ_CHUNK_SIZE, read_direct(&mmap, *header_offset, *crc32, *compression_method, *compressed_size).unwrap())
}
