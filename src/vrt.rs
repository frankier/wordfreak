use std::io::BufReader;
use std::io;
use std::path::Path;
use std::collections::BTreeMap;

use memmap::Mmap;
use quick_xml::events::Event;
use piz::read::FileMetadata;
use std::ffi::OsStr;
use crate::vocab::{VocabBuilder, VocabMap};
use crate::types::{Corpus, DocBow};
use crate::zip::{MinEntries, open_piz, EntryBufReader};
use crossbeam_channel::{unbounded, bounded, Receiver};
use crossbeam::thread::Scope;
use crate::zip::{read_buf, UNZIP_READERS};
use crate::parallel::partition;
use crate::conllu::grab_lemma;


fn is_vrt_file(entry: &FileMetadata) -> bool {
    if !entry.is_file() {
        return false;
    }
    if let Some(ext) = entry.path.extension().and_then(OsStr::to_str) {
        ext.to_lowercase() == "vrt"
    } else {
        false
    }
}


struct VrtFile<'a, 'b, F> {
    buf: Vec::<u8>,
    reader: &'b mut quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>,
    proc_doc: F
}

impl<'a, 'b, F> VrtFile<'a, 'b, F> {
    fn new(
        reader: &'b mut quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>,
        proc_doc: F
    ) -> VrtFile<'a, 'b, F> {
        VrtFile::new_with_buf(Vec::new(), reader, proc_doc)
    }

    fn new_with_buf(
        buf: Vec::<u8>,
        reader: &'b mut quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'a>>>,
        proc_doc: F
    ) -> VrtFile<'a, 'b, F> {
        VrtFile { buf, reader, proc_doc }
    }
}

impl<'a, 'b, R, F: FnMut(VrtText) -> Option<R>> Iterator for VrtFile<'a, 'b, F> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.reader.read_event(&mut self.buf) {
                Ok(Event::Start(ref e)) => {
                    match e.name() {
                        b"text" => {
                            return (self.proc_doc)(VrtText {
                                reader: self.reader,
                                buf: &mut self.buf
                            });
                        },
                        _ => {}
                    }
                },
                Ok(Event::Eof) => return None,
                Err(e) => panic!("Error at position {}: {:?}", self.reader.buffer_position(), e),
                _ => (),
            }
            self.buf.clear();
        }
    }
}

struct VrtText<'a, 'b> {
    reader: &'a mut quick_xml::Reader<BufReader<Box<dyn io::Read + Send + 'b>>>,
    buf: &'a mut Vec::<u8>,

}

impl<'a, 'b> VrtText<'a, 'b> {
    fn for_each<F>(self, mut f: F)
    where F: FnMut(&[u8])
    {
        let mut in_sent = false;
        loop {
            match self.reader.read_event(self.buf) {
                Ok(Event::Start(ref e)) => {
                    match e.name() {
                        b"sentence" => {
                            in_sent = true;
                        },
                        _ => {}
                    }
                },
                Ok(Event::End(ref e)) => {
                    match e.name() {
                        b"text" => {
                            return;
                        },
                        b"sentence" => {
                            in_sent = false
                        },
                        _ => {}
                    }
                },
                Ok(Event::Text(ref e)) => {
                    if in_sent {
                        let unescaped = e.unescaped().unwrap();
                        f(grab_lemma(unescaped.as_ref()));
                    }
                },
                Ok(Event::Eof) => {
                    // Filename might be useful
                    eprintln!(
                        "Premature end of VRT file at {}! Ended inside <text>.",
                        self.reader.buffer_position()
                    );
                    return;
                },
                Err(e) => panic!("Error at position {}: {:?}", self.reader.buffer_position(), e),
                _ => (),
            }
            self.buf.clear();
        }
    }
}

pub fn buffered_extract<'env, F>(
    scope: &Scope<'env>,
    vrt_entries: &'env MinEntries,
    mmap: &'env Mmap,
    cb: F
) -> ()
    where F: Fn(quick_xml::Reader<EntryBufReader>) -> () + Send + Clone + 'env
{
    let entries_partitioned = partition(
        &vrt_entries,
        UNZIP_READERS
    );
    println!("Extracting zip entries using {} threads", UNZIP_READERS);
    for entry_slice in entries_partitioned {
        let cb_clone = cb.clone();
        scope.spawn(move |_| {
            for entry in entry_slice {
                let contents = read_buf(mmap, entry);
                cb_clone(quick_xml::Reader::from_reader(contents));
            }
        });
    }
}

pub fn count_words<'env, 'a>(
    vrt_entries: &'env MinEntries,
    mmap: &'env Mmap
) -> VocabBuilder {
    crossbeam::scope(|scope| {
        let (snd, rcv) = unbounded();
        buffered_extract(scope, vrt_entries, mmap, move |mut reader| {
            let mut vocab = VocabBuilder::new();
            let mut it = VrtFile::new(&mut reader, |vrt_text: VrtText| -> Option<()> {
                vrt_text.for_each(|tok| {
                    vocab.add(tok);
                });
                Some(())
            });
            while it.next().is_some() {}
            println!("vocab len: {}", vocab.acc.len());
            snd.send(vocab).unwrap()
        });
        rcv.iter().reduce(|mut acc, other| {
            acc.merge(other);
            acc
        }).unwrap()
    }).unwrap()
}

pub fn make_doc_bows<'env, 'a>(
    scope: &Scope<'env>,
    vrt_entries: &'env MinEntries,
    mmap: &'env Mmap,
    vocab: &'env VocabMap
) -> Receiver<DocBow>
{
    let (snd, rcv) = bounded(1024);
    buffered_extract(scope, vrt_entries, mmap, move |mut reader| {
        for doc in VrtFile::new(&mut reader, |vrt_text: VrtText| {
            let mut counts: BTreeMap<u32, u32> = BTreeMap::new();
            let mut doc_count = 0;
            vrt_text.for_each(|tok| {
                let maybe_vocab_idx = vocab.get(tok);
                if let Some(vocab_idx) = maybe_vocab_idx {
                    *counts.entry(*vocab_idx).or_insert(0) += 1;
                    doc_count += 1
                }
            });
            Some((doc_count, counts))
        }) {
            snd.send(doc).unwrap();
        }
    });
    rcv
}

pub struct VrtCorpus {
    vrt_entries: MinEntries,
    mmap: Mmap
}

impl VrtCorpus {
    pub fn new(vrt_entries: MinEntries, mmap: Mmap) -> VrtCorpus {
        VrtCorpus {
            vrt_entries, mmap
        }
    }

    pub fn new_from_path(path: &Path) -> VrtCorpus {
        let (mmap, entries) = open_piz(path, is_vrt_file);
        println!("{} entires in {}", entries.len(), path.to_str().unwrap());
        if entries.len() == 0 {
            panic!("No VRT files found in {}", path.to_str().unwrap());
        }
        VrtCorpus::new(
            entries,
            mmap
        )
    }
}

impl Corpus for VrtCorpus {
    fn count_words(&self) -> (VocabBuilder, u32) {
        let vocab = count_words(&self.vrt_entries, &self.mmap);
        (vocab, self.vrt_entries.len() as u32)
    }

    fn gen_doc_bows<'env>(&'env self, scope: &Scope<'env>, vocab: &'env VocabMap) -> Receiver<DocBow> {
        make_doc_bows(scope, &self.vrt_entries, &self.mmap, vocab)
    }
}
