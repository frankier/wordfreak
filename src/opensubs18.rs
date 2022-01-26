use std::path::Path;
use std::borrow::Borrow;
use std::fs::File;
use std::io::{BufRead, Cursor};
use std::vec::Vec;
use std::collections::BTreeMap;
use piz::ZipArchive;
use quick_xml::events::Event;
use memmap::Mmap;
use crate::types::{Corpus, DocBow};
use crate::vocab::VocabMap;
use piz::read::FileMetadata;
use std::ffi::OsStr;
use crossbeam_channel::{unbounded, bounded, Receiver};
use crossbeam::thread::Scope;
use itertools::Itertools;
use piz::read::read_direct;
use crate::parallel::partition;
use crate::vocab::VocabBuilder;
use crate::zip::{MinEntries, open_piz, read_whole_file, UNZIP_READERS};


// Should probably be bigger than normal because deflate adds latency(?)
static LEMMA_KEY: &[u8] = b"lemma";


pub fn is_xml_file(entry: &FileMetadata) -> bool {
    entry.is_file() && entry.path.extension().and_then(OsStr::to_str) == Some("xml")
}

/*
pub fn iter_subtitles_enumerated<'a, 'b>(zip_reader: &'a ZipArchive<'b>) -> impl ParallelIterator<Item=(usize, quick_xml::Reader<BufReader<Box<dyn std::io::Read + Send + 'b>>>)> + 'a {
    zip_reader
        .entries()
        .into_iter()
        .filter(|x| is_xml_file(x))
        .enumerate()
        .map(move |(idx, entry)| (
            idx,
            // TODO?: Reuse BufReaders somehow
            quick_xml::Reader::from_reader(
                BufReader::with_capacity(READ_CHUNK_SIZE, zip_reader.read(entry).unwrap())
            )
        ))
}
*/

/*
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

*/

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

/*
pub fn iter_flat_tokens<'a>(xml_entries: &'a MinEntries, mmap: &'a Mmap, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=Box<[u8]>> + 'a {
    iter_subtitles_whole_file(xml_entries, mmap)
        .flat_map_iter(move |reader| {
            OpenSubsDoc::new(reader, target_attr_key)
        })
}
*/


pub fn buffered_extract<'env, F>(
    scope: &Scope<'env>,
    xml_entries: &'env MinEntries,
    mmap: &'env Mmap,
    cb: F
) -> ()
    where F: Fn(quick_xml::Reader<Cursor<Box<[u8]>>>) -> () + Send + Clone + 'env
{
    let entries_partitioned = partition(
        &xml_entries,
        UNZIP_READERS
    );
    println!("Extracting zip entries using {} threads", UNZIP_READERS);
    for entry_slice in entries_partitioned {
        let cb_clone = cb.clone();
        scope.spawn(move |_| {
            for entry in entry_slice {
                let contents = read_whole_file(mmap, entry);
                cb_clone(quick_xml::Reader::from_reader(contents));
            }
        });
    }
}

pub fn count_words<'env, 'a>(
    xml_entries: &'env MinEntries,
    mmap: &'env Mmap,
    target_attr_key: &'a [u8],
) -> VocabBuilder {
    crossbeam::scope(|scope| {
        let (snd, rcv) = unbounded();
        buffered_extract(scope, xml_entries, mmap, move |reader| {
            let mut vocab = VocabBuilder::new();
            let mut doc = OpenSubsDoc::new(reader, target_attr_key);
            doc.next_token(|t| vocab.add(t));
            snd.send(vocab).unwrap()
        });
        rcv.iter().reduce(|mut acc, other| {
            acc.merge(other);
            acc
        }).unwrap()
    }).unwrap()
}

pub fn xml_to_doc_bow<'a>(mut reader: quick_xml::Reader<impl BufRead>, vocab: &'a VocabMap, target_attr_key: &'a [u8]) -> DocBow {
    let mut xml_read_buf = Vec::<u8>::new();
    let mut counts: BTreeMap<u32, u32> = BTreeMap::new();
    // XXX: Could have some kind of pool for these
    let mut doc_words: u32 = 0;
    loop {
        let got_some = next_opensubs_doc_token(&mut xml_read_buf, &mut reader, target_attr_key, |lemma| {
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

/*
pub fn iter_doc_bows<'a>(xml_entries: &'a MinEntries, mmap: &'a Mmap, vocab: &'a VocabMap, target_attr_key: &'a [u8]) -> impl ParallelIterator<Item=DocBow> + 'a {
    map_xmls_to_doc_bows(iter_subtitles_whole_file(&xml_entries, &mmap), vocab, target_attr_key)
}
*/

pub fn iter_doc_bows_buf<'env, 'a>(
    scope: &Scope<'env>,
    xml_entries: &'env MinEntries,
    mmap: &'env Mmap,
    vocab: &'env VocabMap,
    target_attr_key: &'env [u8]
) -> Receiver<DocBow>
{
    let (snd, rcv) = bounded(1024);
    buffered_extract(scope, xml_entries, mmap, move |reader| {
        snd.send(xml_to_doc_bow(reader, vocab, target_attr_key)).unwrap();
    });
    rcv
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


pub struct OpenSubs18Corpus {
    xml_entries: MinEntries,
    mmap: Mmap,
    target_attr_key: Box<[u8]>
}

impl OpenSubs18Corpus {
    pub fn new(
        xml_entries: MinEntries,
        mmap: Mmap,
        target_attr_key: Box<[u8]>
    ) -> OpenSubs18Corpus {
        OpenSubs18Corpus {
            xml_entries, mmap, target_attr_key
        }
    }

    pub fn new_from_path(path: &Path) -> OpenSubs18Corpus {
        let (mmap, entries) = open_piz(path, is_xml_file);
        OpenSubs18Corpus::new(
            entries,
            mmap,
            Box::from(LEMMA_KEY)
        )
    }
}

impl Corpus for OpenSubs18Corpus {
    fn count_words(&self) -> (VocabBuilder, u32) {
        let vocab = count_words(&self.xml_entries, &self.mmap, &self.target_attr_key);
        (vocab, self.xml_entries.len() as u32)
    }

    fn gen_doc_bows<'env>(&'env self, scope: &Scope<'env>, vocab: &'env VocabMap) -> Receiver<DocBow> {
        iter_doc_bows_buf(scope, &self.xml_entries, &self.mmap, vocab, &self.target_attr_key)
    }
}
