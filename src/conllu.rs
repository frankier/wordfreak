use crate::types::{Corpus, DocBow};
use crate::vocab::VocabMap;
use std::path::Path;
use std::io::{BufReader, BufRead};
use std::fs::File;
use std::collections::BTreeMap;
use crossbeam_channel::{Receiver, bounded};
use crossbeam::thread::Scope;
use crate::vocab::VocabBuilder;


pub fn grab_lemma(line: &[u8]) -> &[u8] {
    line.split(|chr| *chr == b'\t').nth(2).unwrap()
}


pub fn open_conllu(path: &Path) -> BufReader<File> {
    let file = File::open(path).unwrap();
    BufReader::new(file)
}

struct FlatTokenIter {
    buf_read: BufReader<File>,
    line_buf: Vec<u8>,
    doc_count: u32
}

impl<'a> FlatTokenIter {
    fn new(buf_read: BufReader<File>) -> FlatTokenIter {
        FlatTokenIter {
            buf_read,
            line_buf: Vec::with_capacity(200),
            doc_count: 0,
        }
    }

    pub fn next_token<R, F: FnMut(&[u8]) -> R>(&mut self, mut proc_token: F) -> Option<R> {
        loop {
            self.line_buf.clear();
            let line = self.buf_read.read_until(b'\n', &mut self.line_buf);
            let read = line.unwrap();
            if read == 0 {
                return None;
            } else if self.line_buf == b"# newdoc\n" {
                self.doc_count += 1;
            } else if self.line_buf[0] != b'#' && self.line_buf[0] != b'\n' {
                return Some(proc_token(grab_lemma(self.line_buf.as_slice())));
            }
        }
    }
}

impl<'a> Iterator for FlatTokenIter {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_token(|x| x.to_owned().into_boxed_slice())
    }
}

struct DocBowIter<'a> {
    buf_read: BufReader<File>,
    line_buf: Vec<u8>,
    is_first: bool,
    vocab: &'a VocabMap
}

impl<'a> DocBowIter<'a> {
    fn new(buf_read: BufReader<File>, vocab: &'a VocabMap) -> DocBowIter<'a> {
        DocBowIter {
            buf_read,
            line_buf: Vec::with_capacity(200),
            is_first: true,
            vocab,
        }
    }
}

impl<'a> Iterator for DocBowIter<'a> {
    type Item = DocBow;

    fn next(&mut self) -> Option<Self::Item> {
        let mut doc_words = 0;
        let mut counts: BTreeMap<u32, u32> = BTreeMap::new();

        loop {
            self.line_buf.clear();
            let line = self.buf_read.read_until(b'\n', &mut self.line_buf);
            let read = line.unwrap();
            if read == 0 {
                return None;
            } else if self.line_buf == b"# newdoc\n" {
                if self.is_first {
                    self.is_first = false
                } else {
                    return Some((doc_words, counts));
                }
            } else if self.line_buf[0] != b'#' && self.line_buf[0] != b'\n' {
                let lemma = grab_lemma(self.line_buf.as_slice());
                let maybe_vocab_idx = self.vocab.get(lemma);
                if let Some(vocab_idx) = maybe_vocab_idx {
                    *counts.entry(*vocab_idx).or_insert(0) += 1;
                    doc_words += 1;
                }
            }
        }
    }
}

pub struct ConlluCorpus {
    path: Box<Path>
}

impl ConlluCorpus {
    pub fn new(path: &Path) -> ConlluCorpus {
        ConlluCorpus {
            path: Box::from(path),
        }
    }

    fn open(&self) -> BufReader<File> {
        BufReader::new(File::open(&self.path).unwrap())
    }
}

impl Corpus for ConlluCorpus {
    fn count_words(&self) -> (VocabBuilder, u32) {
        let mut tokens = FlatTokenIter::new(self.open());
        let mut vocab = VocabBuilder::new();
        tokens.next_token(|tok| {
            vocab.add(tok);
        });
        (vocab, tokens.doc_count)
    }

    fn gen_doc_bows<'env>(&'env self, scope: &Scope<'env>, vocab: &'env VocabMap) -> Receiver<DocBow> {
        let (snd, rcv) = bounded(1024);
        let file = self.open();
        scope.spawn(move |_| {
            let iter = DocBowIter::new(file, vocab);
            for doc in iter {
                snd.send(doc).unwrap();
            }
        });
        rcv
    }
}
