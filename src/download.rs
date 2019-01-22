use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

const READ_LINES_NUM: u32 = 100;

pub fn run() {
    // let r = read_gz(Path::new("short.txt.gz"));
    // let mut w = write_gz(Path::new("testtest.txt.gz"));
    // for line in r.lines() {
    //     // writeln!(w, "{}", line.unwrap()).unwrap();
    // }

    let src = Path::new("short.txt.gz");
    let dst_dir = Path::new("./dst");
    fs::create_dir(dst_dir).unwrap();
    count_data(&src, &dst_dir);
}

/// open *.gz file
fn read_gz(path: &Path) -> BufReader<GzDecoder<BufReader<File>>> {
    let f = File::open(path).unwrap();
    let r = BufReader::new(f);
    let decoder = GzDecoder::new(r);
    BufReader::new(decoder)
}

/// create *.gz file
fn write_gz(path: &Path) -> BufWriter<GzEncoder<BufWriter<File>>> {
    let f = File::create(path).unwrap();
    let w = BufWriter::new(f);
    let encoder = GzEncoder::new(w, Compression::best());
    BufWriter::new(encoder)
}

fn count_data(src: &Path, dst_dir: &Path) {
    let r = read_gz(src);

    let mut entry_counter = EntryCounter::new();

    let mut file_num: u32 = 0;

    let mut lines = r.lines();

    loop {
        let mut dst_path = dst_dir.to_path_buf();
        dst_path.push(&format!("{:010}.gz", file_num));
        let mut w = write_gz(&dst_path);

        for _ in 0..READ_LINES_NUM {
            match lines.next() {
                Some(result) => match result {
                    Ok(line) => match Entry::from_raw_line(&line) {
                        Some(entry) => entry_counter.add(&entry),
                        None => continue,
                    },
                    Err(err) => panic!(err),
                },
                None => {
                    entry_counter.dump(&mut w);
                    return;
                }
            }
        }

        entry_counter.dump(&mut w);

        file_num += 1;
    }
}

fn merge_two_files(src1: &Path, src2: &Path, dst_dir: &Path) {
    let r1 = read_gz(src1);
    let r2 = read_gz(src2);

    let mut lines1 = r1.lines();
    let mut lines2 = r2.lines();

    let mut w = write_gz(dst_dir);

    let mut line1 = lines1.next();
    let mut line2 = lines2.next();

    let mut entry1 = Entry::from_raw_line(&line1.unwrap().unwrap());
    let mut entry2 = Entry::from_raw_line(&line1.unwrap().unwrap());

    loop {
        if line1.is_none() && line2.is_none() {
            return;
        }

        if line2.is_none() {}
    }
}

fn dump_line(w: &mut io::Write, line: &str) {}

/// The entry is a struct of each line of data.
#[derive(Debug)]
struct Entry {
    ngram: Vec<String>,
    match_count: u128,
}

impl Entry {
    /// Reads from a line of data.
    fn from_raw_line(line: &str) -> Option<Entry> {
        let mut elems = line.split("\t");

        let ngram = Entry::split_ngram_to_words(elems.next().unwrap());
        if ngram.is_none() {
            return None;
        }
        let ngram = ngram.unwrap();

        elems.next(); // Year

        let match_count = elems.next().unwrap().parse::<u128>().unwrap();

        Some(Entry { ngram, match_count })
    }

    /// Reads from a parsed line.
    fn from_parsed_line(line: &str) -> Entry {
        let mut elems = line.split("\t");
        let ngram: Vec<String> = elems
            .next()
            .unwrap()
            .split(" ")
            .map(|s| s.to_string())
            .collect();
        let match_count = elems.next().unwrap().parse::<u128>().unwrap();

        Entry { ngram, match_count }
    }

    /// Extracts valid word.
    fn valid_ngram_elem(elem: &str) -> Option<String> {
        if elem.starts_with("_") {
            None
        } else {
            Some(elem.split("_").next().unwrap().to_string())
        }
    }

    /// Splits s into valid words.
    fn split_ngram_to_words(s: &str) -> Option<Vec<String>> {
        let opt_words: Vec<Option<String>> = s
            .split(" ")
            .map(|word| Entry::valid_ngram_elem(word))
            .collect();

        if opt_words.contains(&None) {
            None
        } else {
            Some(opt_words.into_iter().map(|opt| opt.unwrap()).collect())
        }
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}\t{}", self.ngram.join(" "), self.match_count)
    }
}

/// EntryCount counts frequency of n-gram entry.
struct EntryCounter {
    data: BTreeMap<Vec<String>, u128>,
}

impl EntryCounter {
    fn new() -> EntryCounter {
        EntryCounter {
            data: BTreeMap::new(),
        }
    }

    fn add(&mut self, entry: &Entry) {
        self.data
            .entry(entry.ngram.clone())
            .and_modify(|cnt| *cnt += entry.match_count)
            .or_insert(entry.match_count);
    }

    fn dump(&self, w: &mut io::Write) {
        for (ngram, match_count) in &self.data {
            writeln!(w, "{}\t{}", ngram.join(" "), match_count).unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_valid_ngram_elem() {
        assert_eq!(Entry::valid_ngram_elem("word"), Some("word".to_string()));
        assert_eq!(
            Entry::valid_ngram_elem("word_NOUN"),
            Some("word".to_string())
        );
        assert_eq!(Entry::valid_ngram_elem("_NOUN_"), None);
    }

    #[test]
    fn test_split_ngram_to_words() {
        assert_eq!(
            Entry::split_ngram_to_words("a b c"),
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
        assert_eq!(
            Entry::split_ngram_to_words("a_NOUN b c"),
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
        assert_eq!(Entry::split_ngram_to_words("_NOUN_ b c"), None);
    }
}
