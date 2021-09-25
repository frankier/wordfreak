use std::path::Path;
use std::borrow::Borrow;
use std::fs::File;
use std::io::BufReader;
use std::io;

use std::collections::BTreeMap;
use piz::ZipArchive;
use quick_xml::events::Event;
use memmap::Mmap;
use rayon::prelude::*;
use crate::termdocmat::VocabMap;
use piz::read::FileMetadata;
use std::ffi::OsStr;

// Should probably be bigger than normal because deflate adds latency(?)
const READ_CHUNK_SIZE: usize = 64 * 1024;


pub fn mmap_file(path: &Path) -> Mmap {
    // XXX: This should be unsafe if this is a library
    let zip_file = File::open(path).unwrap();
    unsafe { Mmap::map(&zip_file).unwrap() }
}

fn is_xml_file(entry: &FileMetadata) -> bool {
    entry.is_file() && entry.path.extension().and_then(OsStr::to_str) == Some("xml")
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

pub fn iter_subtitles<'a, 'b>(zip_reader: &'a ZipArchive<'b>) -> impl ParallelIterator<Item=quick_xml::Reader<BufReader<Box<dyn std::io::Read + Send + 'b>>>> + 'a {
    zip_reader.entries().par_iter().filter_map(move |entry| {
        if is_xml_file(entry) {
            Some(quick_xml::Reader::from_reader(
                BufReader::with_capacity(READ_CHUNK_SIZE, zip_reader.read(entry).unwrap())
            ))
        } else {
            None
        }
    })
}

pub fn next_opensubs_doc_token<'a, R, F: FnMut(&[u8]) -> R>(
    buf: &mut Vec::<u8>,
    reader: &mut quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>,
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

pub fn iter_flat_tokens<'a>(zip_reader: &'a ZipArchive<'a>, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=Box<[u8]>> + 'a {
    iter_subtitles(&zip_reader)
        .flat_map_iter(move |reader| {
            OpenSubsDoc::new(reader, target_attr_key)
        })
}

pub fn iter_doc_bows<'a>(zip_reader: &'a ZipArchive<'a>, vocab: &'a VocabMap, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=(u32, BTreeMap<u32, u32>)> + 'a {
    iter_subtitles(&zip_reader).map_init(
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

pub struct OpenSubsDoc<'a, 'b> {
    buf: Vec::<u8>,
    reader: quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>,
    target_attr_key: &'b [u8]
}

impl<'a, 'b> OpenSubsDoc<'a, 'b> {
    pub fn new(reader: quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>, target_attr_key: &'b [u8]) -> OpenSubsDoc<'a, 'b> {
        OpenSubsDoc { buf: Vec::new(), reader, target_attr_key }
    }

    pub fn new_with_buf(buf: Vec::<u8>, reader: quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>, target_attr_key: &'b [u8]) -> OpenSubsDoc<'a, 'b> {
        OpenSubsDoc { buf, reader, target_attr_key }
    }

    pub fn next_token<R, F: FnMut(&[u8]) -> R>(&mut self, proc_token: F) -> Option<R> {
        next_opensubs_doc_token(&mut self.buf, &mut self.reader, self.target_attr_key, proc_token)
    }
}


impl<'a, 'b> Iterator for OpenSubsDoc<'a, 'b> {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_token(|x| x.to_owned().into_boxed_slice())
    }
}
