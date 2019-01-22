use std::error::Error;
use std::fmt;
use std::io;
use std::io::Write;

pub fn run() {
    println!("hello world!")
}

// Entry is a struct of each line of data.
#[derive(Debug, PartialEq)]
struct Entry {
    ngram: Vec<String>,
    match_count: u64,
}

// EntryOrd is the result of a comparison between two entries.
#[derive(Debug, PartialEq)]
enum EntryOrd {
    Less,
    Equal,
    Grater,
}

impl Entry {
    // Parses from raw line.
    fn from_raw_line(line: &str) -> Option<Entry> {
        let elems: Vec<&str> = line.split("\t").collect();

        if elems.len() != 2 {
            return None;
        }

        let ngram = Entry::split_ngram_to_words(elems[0]);
        if ngram.is_none() {
            return None;
        }
        let ngram = ngram.unwrap();

        let match_count = elems[1].parse::<u64>();
        if match_count.is_err() {
            return None;
        }
        let match_count = match_count.unwrap();

        Some(Entry { ngram, match_count })
    }

    // Splits s into valid words.
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

    // Extracts valid word.
    fn valid_ngram_elem(word: &str) -> Option<String> {
        if word.starts_with("_") {
            None
        } else {
            Some(word.split("_").next().unwrap().to_string())
        }
    }

    // Parses from sanity line.
    fn from_parsed_line(line: &str) -> Entry {
        let mut elems_iter = line.split("\t");

        let ngram: Vec<String> = elems_iter
            .next()
            .unwrap()
            .split(" ")
            .map(|word| word.to_string())
            .collect();

        let match_count = elems_iter.next().unwrap().parse::<u64>().unwrap();

        Entry { ngram, match_count }
    }

    // entry_cmp returns the original result of comparison.
    fn entry_cmp(&self, other: &Entry) -> EntryOrd {
        if self.ngram < other.ngram {
            EntryOrd::Less
        } else if self.ngram == other.ngram {
            EntryOrd::Equal
        } else {
            EntryOrd::Grater
        }
    }

    // merge merges two emtries which have the same entry.
    fn merge(&self, other: &Entry) -> Entry {
        Entry {
            ngram: self.ngram.clone(),
            match_count: self.match_count + other.match_count,
        }
    }

    // dump_to dumps entry to w.
    fn dump_to(&self, w: &mut Write) -> io::Result<()> {
        writeln!(w, "{}", self)
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}\t{}", self.ngram.join(" "), self.match_count)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_from_raw_line() {
        let query = vec!["a b\t1", "c_NOUN d\t2", "e_NOUN _f_\t3"];
        let ans = vec![
            Some(Entry {
                ngram: vec!["a".to_string(), "b".to_string()],
                match_count: 1,
            }),
            Some(Entry {
                ngram: vec!["c".to_string(), "d".to_string()],
                match_count: 2,
            }),
            None,
        ];

        for (q, a) in query.into_iter().zip(ans.into_iter()) {
            assert_eq!(Entry::from_raw_line(q), a);
        }
    }

    #[test]
    fn test_from_parsed_line() {
        assert_eq!(
            Entry::from_parsed_line("a b\t1"),
            Entry {
                ngram: vec!["a".to_string(), "b".to_string()],
                match_count: 1
            }
        )
    }

    #[test]
    fn test_entry_fmt() {
        assert_eq!(format!("{}", Entry::from_parsed_line("a b\t1")), "a b\t1\n")
    }

    #[test]
    fn test_entry_cmp() {
        let entry = Entry::from_parsed_line("b b\t1");
        let others = vec![
            Entry::from_parsed_line("b c\t6"),
            Entry::from_parsed_line("b b\t5"),
            Entry::from_parsed_line("a b\t4"),
        ];
        let answer = vec![EntryOrd::Less, EntryOrd::Equal, EntryOrd::Grater];

        for (other, ans) in others.into_iter().zip(answer.into_iter()) {
            assert_eq!(entry.entry_cmp(&other), ans);
        }
    }
}
