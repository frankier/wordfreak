use std::path::Path;
use std::thread;
use crossbeam_channel::unbounded;
use wordfreak::termdocmat::TermDocMatWriter;
use wordfreak::vocab::get_numberbatch_vocab;
use argh::FromArgs;
use wordfreak::corpus::{CorpusType, get_corpus};
use wordfreak::types::Corpus;
use wordfreak::vocab::VocabMap;
use crossbeam::thread::Scope;


static LEMMA_KEY: &[u8] = b"lemma";


#[derive(FromArgs)]
/// add
struct MkTdMat {
    /// type of corpus to use
    #[argh(option)]
    corpus_type: Option<CorpusType>,

    /// vocab
    #[argh(option)]
    vocab: Option<String>,

    /// path
    #[argh(positional)]
    output: String,

    /// path
    #[argh(positional)]
    input: String,
}


fn vocab_from_corpus(corpus: &Box<dyn Corpus>) -> VocabMap {
    let (vocab_builder, _doc_count) = corpus.count_words();
    let (vocab, _word_freqs_indexed, _total_words) = vocab_builder.build();
    vocab
}


fn main() {
    let args: MkTdMat = argh::from_env();
    let corpus_path = Path::new(&args.input);
    let corpus = get_corpus(corpus_path, args.corpus_type.unwrap());
    let vocab = if let Some(vocab_path) = args.vocab {
        println!("Reading vocab");
        get_numberbatch_vocab(&vocab_path)
    } else {
        println!("Scanning vocab");
        vocab_from_corpus(&corpus)
    };
    println!("Vocab size: {}", vocab.len());

    println!("Reading and writing other files");
    let out_dir = Path::new(&args.output);

    let mut writer = TermDocMatWriter::new(out_dir, vocab.len() as u64);

    crossbeam::scope(|scope| {
        let rcv = corpus.gen_doc_bows(scope, &vocab);
        for (doc_words, counts) in rcv.into_iter() {
             writer.write_indexed_doc(doc_words as u64, &counts);
        }
        let (num_docs, vocab_len, num_values) = writer.close();
        println!("Vocab size: {}", vocab_len);
        println!("Num docs: {}", num_docs);
        println!("Num values: {}", num_values);
        println!("Density: {}", (num_values as f64) / ((num_docs * (vocab_len as u64))) as f64);
    }).unwrap();
}
