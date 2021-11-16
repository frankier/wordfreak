use std::path::Path;
use std::borrow::Borrow;
use std::fs::File;
use std::io::{BufReader, BufRead, Cursor};

use std::collections::BTreeMap;
use piz::ZipArchive;
use quick_xml::events::Event;
use memmap::Mmap;
use rayon::prelude::*;
use crate::termdocmat::VocabMap;
use piz::read::FileMetadata;
use std::ffi::OsStr;
use crossbeam_channel::bounded;
use rayon::{current_num_threads, Scope};
use itertools::Itertools;
use piz::CompressionMethod;
use piz::read::read_direct;

pub type MinEntries = Vec<(usize, u32, usize, CompressionMethod, usize)>;
type DocBow = (u32, BTreeMap<u32, u32>);
// Should probably be bigger than normal because deflate adds latency(?)
const READ_CHUNK_SIZE: usize = 64 * 1024;


pub fn mmap_file(path: &Path) -> Mmap {
    // XXX: This should be unsafe if this is a library
    let zip_file = File::open(path).unwrap();
    unsafe { Mmap::map(&zip_file).unwrap() }
}

pub fn is_xml_file(entry: &FileMetadata) -> bool {
    entry.is_file() && entry.path.extension().and_then(OsStr::to_str) == Some("xml")
}

pub fn filter_xml_entries(zip_reader: &ZipArchive) -> MinEntries {
    zip_reader.entries().into_iter().filter_map(|entry| {
        if is_xml_file(entry) {
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

pub fn iter_subtitles_enumerated<'a, 'b>(zip_reader: &'a ZipArchive<'b>) -> impl ParallelIterator<Item=(usize, quick_xml::Reader<BufReader<Box<dyn std::io::Read + Send + 'b>>>)> + 'a {
    zip_reader
        .entries()
        .into_iter()
        .filter(|x| is_xml_file(x))
        .enumerate()
        .par_bridge()
        .map(move |(idx, entry)| (
            idx,
            // TODO?: Reuse BufReaders somehow
            quick_xml::Reader::from_reader(
                BufReader::with_capacity(READ_CHUNK_SIZE, zip_reader.read(entry).unwrap())
            )
        ))
}

pub fn iter_subtitles<'a>(xml_entries: &'a MinEntries, mmap: &'a Mmap) -> impl ParallelIterator<Item=quick_xml::Reader<BufReader<Box<dyn std::io::Read + Send + 'a>>>> + 'a {
    xml_entries.par_iter().map(move |(header_offset, crc32, size, compression_method, compressed_size)| {
        quick_xml::Reader::from_reader(
            BufReader::with_capacity(READ_CHUNK_SIZE, read_direct(&mmap, *header_offset, *crc32, *compression_method, *compressed_size).unwrap())
        )
    })
}

pub fn iter_subtitles_whole_file<'a>(xml_entries: &'a MinEntries, mmap: &'a Mmap) -> impl ParallelIterator<Item=quick_xml::Reader<impl BufRead>> + 'a {
    xml_entries.par_iter().map(move |(header_offset, crc32, size, compression_method, compressed_size)| {
        let mut contents = Vec::with_capacity(*size);
        read_direct(&mmap, *header_offset, *crc32, *compression_method, *compressed_size).unwrap().read_to_end(&mut contents).unwrap();
        quick_xml::Reader::from_reader(Cursor::new(contents.into_boxed_slice()))
    })
}

pub fn mk_default_pool() -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new().build().unwrap()
}

pub fn next_opensubs_doc_token<R, F: FnMut(&[u8]) -> R, BR: BufRead>(
    buf: &mut Vec::<u8>,
    reader: &mut quick_xml::Reader<BR>,
    target_attr_key: &[u8],
    mut proc_token: F
) -> Option<R> {
    loop {
        match reader.read_event(buf) {
            Ok(Event::Start(ref e)) => {
                match e.name() {
                    b"w" => {
                        for attr in e.attributes().with_checks(false) {
                            let unwrapped_attr = attr.unwrap();
                            if unwrapped_attr.key == target_attr_key {
                                // XXX: Could just use the following if we were able to work with [u8]
                                let lemma_cow = unwrapped_attr.unescaped_value().unwrap();
                                return Some(proc_token(lemma_cow.borrow()));
                            }
                        }
                    }
                    _ => (),
                }
            },
            Ok(Event::Eof) => return None,
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => (),
        }
        buf.clear();
    }
}

pub fn iter_flat_tokens<'a>(xml_entries: &'a MinEntries, mmap: &'a Mmap, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=Box<[u8]>> + 'a {
    iter_subtitles_whole_file(xml_entries, mmap)
        .flat_map_iter(move |reader| {
            OpenSubsDoc::new(reader, target_attr_key)
        })
}


pub fn buffered_extract<'env>(pool: &Scope<'env>, xml_entries: &'env MinEntries, mmap: &'env Mmap) -> impl Iterator<Item=quick_xml::Reader<impl BufRead>> {
    // This turns out not be so good since par_bridge(...) doesn't perform any backpressure so
    // starts busy-waiting/busy-recursive work stealing as soon as there's not enough work to
    // feed the worker pool. See:
    // https://github.com/rayon-rs/rayon/issues/795
    let (snd, rcv) = bounded(1024);
    pool.spawn(move |_| {
        println!("Extracting zip entries using {} threads", current_num_threads());

        xml_entries.par_iter().for_each(|(header_offset, crc32, size, compression_method, compressed_size)| {
            let mut contents = Vec::with_capacity(*size);
            read_direct(&mmap, *header_offset, *crc32, *compression_method, *compressed_size).unwrap().read_to_end(&mut contents).unwrap();
            snd.send(quick_xml::Reader::from_reader(Cursor::new(contents.into_boxed_slice()))).unwrap();
        });
    });
    rcv.into_iter()
}


pub fn iter_flat_tokens_buf<'env, 'a>(extract_pool: &Scope<'env>, xml_entries: &'env MinEntries, mmap: &'env Mmap, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=Box<[u8]>> + 'a {
    let extracted_it = buffered_extract(extract_pool, xml_entries, mmap);
    extracted_it.par_bridge().flat_map_iter(move |reader| {
        OpenSubsDoc::new(reader, target_attr_key)
    })
}

pub fn map_xmls_to_doc_bows<'a, PI: 'a + ParallelIterator<Item=quick_xml::Reader<impl BufRead>>> (pit: PI, vocab: &'a VocabMap, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=DocBow> + 'a {
    pit.map_init(
        || Vec::<u8>::new(), move |xml_read_buf, mut reader| {
            let mut counts: BTreeMap<u32, u32> = BTreeMap::new();
            // XXX: Could have some kind of pool for these
            let mut doc_words: u32 = 0;
            loop {
                let got_some = next_opensubs_doc_token(xml_read_buf, &mut reader, target_attr_key, |lemma| {
                    let maybe_vocab_idx = vocab.get(lemma);
                    if let Some(vocab_idx) = maybe_vocab_idx {
                        *counts.entry(*vocab_idx).or_insert(0) += 1;
                        doc_words += 1;
                    }
                });
                if got_some == None {
                    break;
                }
            }
            (doc_words, counts)
        }
    )
}

pub fn iter_doc_bows<'a>(xml_entries: &'a MinEntries, mmap: &'a Mmap, vocab: &'a VocabMap, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=DocBow> + 'a {
    map_xmls_to_doc_bows(iter_subtitles_whole_file(&xml_entries, &mmap), vocab, target_attr_key)
}

pub fn iter_doc_bows_buf<'env, 'a>(extract_pool: &Scope<'env>, xml_entries: &'env MinEntries, mmap: &'env Mmap, vocab: &'a VocabMap, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=DocBow> + 'a
{
    map_xmls_to_doc_bows(buffered_extract(extract_pool, xml_entries, mmap).par_bridge(), vocab, target_attr_key)
}

pub struct OpenSubsDoc<'b, BR: BufRead> {
    buf: Vec::<u8>,
    reader: quick_xml::Reader<BR>,
    target_attr_key: &'b [u8]
}

impl<'b, BR: BufRead> OpenSubsDoc<'b, BR> {
    pub fn new(reader: quick_xml::Reader<BR>, target_attr_key: &'b [u8]) -> OpenSubsDoc<'b, BR> {
        OpenSubsDoc { buf: Vec::new(), reader, target_attr_key }
    }

    pub fn new_with_buf(buf: Vec::<u8>, reader: quick_xml::Reader<BR>, target_attr_key: &'b [u8]) -> OpenSubsDoc<'b, BR> {
        OpenSubsDoc { buf, reader, target_attr_key }
    }

    pub fn next_token<R, F: FnMut(&[u8]) -> R>(&mut self, proc_token: F) -> Option<R> {
        next_opensubs_doc_token(&mut self.buf, &mut self.reader, self.target_attr_key, proc_token)
    }
}


impl<'b, BR: BufRead> Iterator for OpenSubsDoc<'b, BR> {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_token(|x| x.to_owned().into_boxed_slice())
    }
}
