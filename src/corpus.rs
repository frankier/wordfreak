use std::path::Path;
use std::str::FromStr;
use crate::opensubs18::OpenSubs18Corpus;
use crate::conllu::ConlluCorpus;
use crate::vrt::VrtCorpus;
use crate::types::Corpus;
use simple_error::SimpleError;


pub enum CorpusType {
    OpenSubtitles2018,
    NewsCrawlWMT18,
    Conllu,
    Vrt
}

impl FromStr for CorpusType {
    type Err = SimpleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "opensubs18" {
            Ok(CorpusType::OpenSubtitles2018)
        } else if s == "newscrawl" {
            Ok(CorpusType::NewsCrawlWMT18)
        } else if s == "conllu" {
            Ok(CorpusType::Conllu)
        } else if s == "vrt" {
            Ok(CorpusType::Vrt)
        } else {
            Err(SimpleError::new("Must be opensubs18 or newscrawl"))
        }
    }
}

pub fn get_corpus(corpus_path: &Path, corpus_type: CorpusType) -> Box<dyn Corpus> {
    match corpus_type {
        CorpusType::OpenSubtitles2018 => {
            Box::new(OpenSubs18Corpus::new_from_path(corpus_path))
        },
        CorpusType::NewsCrawlWMT18 => {
            panic!("NewsCrawl not supported yet");
        },
        CorpusType::Conllu => {
            Box::new(ConlluCorpus::new(corpus_path))
        },
        CorpusType::Vrt => {
            Box::new(VrtCorpus::new_from_path(corpus_path))
        },
    }
}
