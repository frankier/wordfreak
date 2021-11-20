/* l = 50 (the length of the corpus in words)
 * n = 5 (the length of the corpus in parts)
 * s = (0.18, 0.2, 0.2, 0.2, 0.22) (the percentages of the n corpus part sizes)
 * f = 15 (the overall frequency of a in the corpus)
 * v = (1, 2, 3, 4, 5) (the frequencies of a in each corpus part 1-n)
 * p = ( 1 / 9 , 2 / 10 , 3 / 10 , 4 / 10 , 5 / 11 ) (the percentages a makes up of each corpus part 1-n)
 *
 *
 * d the length of the corpus part in words
 */

pub fn kl_div_elem(v: u32, f: u32, d: u32, l: u32) -> f64 {
    let v_by_f = (v as f64) / (f as f64);
    v_by_f * f64::log2(v_by_f * (l as f64) / (d as f64))
}


pub struct AccElement {
    kl_div: f64,
    occurences: u32,
    sd_v_acc: f64,
    sd_p_acc: f64,
}

impl AccElement {
    pub fn zero() -> AccElement {
        AccElement {
            kl_div: 0.0f64,
            occurences: 0u32,
            sd_v_acc: 0.0f64,
            sd_p_acc: 0.0f64,
        }
    }
}

pub fn acc_word(v: u32, f: u32, d: u32, l: u32, n: u32) -> AccElement {
    // Independent of document, could be factored out
    let mean_v = f as f64 / n as f64;
    let p = v as f64 / d as f64;
    AccElement {
        kl_div: kl_div_elem(v, f, d, l),
        occurences: (v > 0) as u32,
        sd_v_acc: (v as f64 - mean_v).powi(2),
        sd_p_acc: (p as f64 - mean_v).powi(2),
    }
}

pub fn reduce_word(left: &AccElement, right: &AccElement) -> AccElement {
    AccElement {
        kl_div: left.kl_div + right.kl_div,
        occurences: left.occurences + right.occurences,
        sd_v_acc: left.sd_v_acc + right.sd_v_acc,
        sd_p_acc: left.sd_p_acc + right.sd_p_acc
    }
}

pub struct FinalColumns {
    pub kl_div: Vec<f64>,
    pub idf: Vec<f64>,
    pub vc: Vec<f64>,
    //pub juillands_d: Vec<f64>,
    //pub carrols_d: Vec<f64>,
    pub zipf: Vec<f64>
}

impl FinalColumns {
    pub fn with_capacity(capacity: usize) -> FinalColumns {
        FinalColumns {
            kl_div: Vec::with_capacity(capacity),
            idf: Vec::with_capacity(capacity),
            vc: Vec::with_capacity(capacity),
            //juillands_d: Vec::with_capacity(capacity),
            //carrols_d: Vec::with_capacity(capacity),
            zipf: Vec::with_capacity(capacity),
        }
    }
}

pub fn norm_word(cols: &mut FinalColumns, elem: AccElement, f: u32, l: u32, n: u32) {
    // Independent of document, could be factored out
    let mean_v = f as f64 / n as f64;
    cols.kl_div.push(elem.kl_div);
    cols.idf.push((n as f64 / elem.occurences as f64).log10());
    cols.vc.push((elem.sd_v_acc / n as f64).sqrt() / mean_v);
    //cols.juillands_d.push();
    //cols.carrols_d.push();
    cols.zipf.push(((f as f64 * 1000000000.0f64) / l as f64).log10());
}
