use std::collections::BTreeMap;
use crossbeam_channel::Receiver;
use crate::vocab::{VocabBuilder, VocabMap};
use crossbeam::thread::Scope;


pub type DocBow = (u32, BTreeMap<u32, u32>);

pub trait Corpus {
    fn count_words(&self) -> (VocabBuilder, u32);
    fn gen_doc_bows<'env>(&'env self, scope: &Scope<'env>, vocab: &'env VocabMap) -> Receiver<DocBow>;
}
