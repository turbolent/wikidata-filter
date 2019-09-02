use std::io::{BufRead, Read, BufWriter, Write};
use std::fs::File;
use std::env;
use std::io::BufReader;
use bzip2::bufread::BzDecoder;
use regex::Regex;
use std::collections::HashSet;
use std::time::Instant;
use std::thread;
use crossbeam_channel::{bounded, Receiver, Sender};
use bzip2::write::BzEncoder;
use bzip2::Compression;

const BATCH_SIZE: u64 = 100;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate lazy_static_include;

#[derive(Debug, PartialEq, Eq)]
pub enum Extra<'a> {
    None,
    Type(&'a str),
    Lang(&'a str)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Subject<'a> {
    IRI(&'a str),
    Blank(&'a str)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Object<'a> {
    IRI(&'a str),
    Blank(&'a str),
    Literal(&'a str, Extra<'a>)
}

#[derive(Debug, PartialEq, Eq)]
pub struct Statement<'a> {
    subject: Subject<'a>,
    predicate: &'a str,
    object: Object<'a>
}

pub enum Work {
    LINES(u64, Vec<String>),
    DONE
}

pub fn parse(line: u64, input: &str) -> Statement {
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"(?x)
            ^
            \s*

            # subject

            (?:

              # IRI
              (?:<([^>]*)>)

              |

              # Blank

              (?:_:([^\s]+))
            )

            \s*

            # predicate IRI
            <([^>]*)>

            \s*

            # object
            (?:

              # IRI
              (?:<([^>]*)>)

              |

              # Blank

              (?:_:([^\s]+))

              |

              # literal
              (?:

                "([^"]*)"

                # optional extra
                (?:

                  # language
                  (?:@([a-zA-Z]+(?:-[a-zA-Z0-9]+)*))

                  |

                  # data type
                  (?:\^\^<([^>]*)>)
                )?
              )
            )
            "#).unwrap();
    }
    let captures = RE.captures(input)
        .unwrap_or_else(|| panic!("Invalid line: {}: {:?}", line, input));

    let subject = captures.get(1)
        .map(|object| {
            Subject::IRI(object.as_str())
        })
        .or_else(|| {
            captures.get(2).map(|blank| {
                Subject::Blank(blank.as_str())
            })
        })
        .expect("failed to parse subject");

    let predicate = captures.get(3).expect("failed to parse predicate").as_str();

    let object = captures.get(4)
        .map(|object| {
            Object::IRI(object.as_str())
        })
        .or_else(|| {
            captures.get(5).map(|blank| {
                Object::Blank(blank.as_str())
            })
        })
        .unwrap_or_else(|| {
            let literal = captures.get(6).expect("failed to parse object").as_str();
            let extra = captures.get(7)
                .map(|lang| {
                    Extra::Lang(lang.as_str())
                })
                .or_else(|| {
                    captures.get(8).map(|data_type| {
                        Extra::Type(data_type.as_str())
                    })
                })
                .unwrap_or(Extra::None);
            Object::Literal(literal, extra)
        });
    Statement { subject, predicate, object }
}

lazy_static_include_str!(PROPERTIES_DATA, "properties");
lazy_static_include_str!(IDENTIFIER_PROPERTIES_DATA, "identifier-properties");
lazy_static_include_str!(LANGUAGES_DATA, "languages");

lazy_static! {
   static ref PROPERTIES: HashSet<&'static str> = line_set(&PROPERTIES_DATA);
}

lazy_static! {
   static ref IDENTIFIER_PROPERTIES: HashSet<String> =
       line_set(&IDENTIFIER_PROPERTIES_DATA)
           .iter()
           .flat_map(|id|
               vec![
                 format!("http://www.wikidata.org/prop/direct/P{}", id),
                 format!("http://www.wikidata.org/prop/direct-normalized/P{}", id)
               ])
           .collect();
}

lazy_static! {
   static ref LANGUAGES: HashSet<&'static str> = line_set(&LANGUAGES_DATA);
}

fn line_set(data: &str) -> HashSet<&str> {
   data.lines().collect()
}

fn ignored_subject(iri: &str) -> bool {
    iri.starts_with("https://www.wikidata.org/wiki/Special:EntityData")
}

fn produce<T: Read>(reader: T, s: &Sender<Work>) -> u64 {
    let mut total = 0;
    let mut buf_reader = BufReader::new(reader);

    let mut lines = Vec::new();

    loop {
        total += 1;
        let mut line = String::new();
        if buf_reader.read_line(&mut line).unwrap() == 0 {
            break
        }

        lines.push(line);

        if total % BATCH_SIZE == 0 {
            s.send(Work::LINES(total, lines)).unwrap();
            lines = Vec::new();
        }
    }

    if lines.len() > 0 {
        s.send(Work::LINES(total, lines)).unwrap();
    }

    total
}

fn consume(id: usize, r: Receiver<Work>) {
    let path = format!("{}.nt.bz2", id);
    let file = File::create(&path)
        .expect(format!("unable to create file: {}", &path).as_str());
    let mut encoder =
        BzEncoder::new(BufWriter::new(file), Compression::Default);

    loop {
        match r.recv().unwrap() {
            Work::LINES(number, lines) => {
                for line in lines {
                    handle(&mut encoder, number, line)
                }
            }
            Work::DONE => {
                encoder.try_finish().unwrap();
                return
            }
        }
    }
}

fn handle<T: Write>(writer: &mut T, number: u64, line: String) {
    let statement = parse(number, &line);

    if PROPERTIES.contains(statement.predicate)
        || IDENTIFIER_PROPERTIES.contains(statement.predicate)
    {
        return
    }

    match statement.subject {
        Subject::Blank(_) => return,
        Subject::IRI(iri) if ignored_subject(iri) => return,
        _ => (),
    }

    match statement.object {
        Object::Blank(_) => return,
        Object::Literal(_, Extra::Lang(lang)) if !LANGUAGES.contains(lang) => return,
        _ => (),
    }

    writer.write_all(&line.as_bytes()).unwrap();
}

fn main() {
    let path = env::args().nth(1)
        .expect("missing path");

    let file = File::open(&path)
        .expect("can't open file");

    let decoder = BzDecoder::new( BufReader::new(file));

    let start = Instant::now();

    let (s, r) = bounded::<Work>(0);

    let mut threads = Vec::new();
    for id in 1..=num_cpus::get() {
        let r = r.clone();
        threads.push(thread::spawn( move || { consume(id, r) }));
    }

    produce(decoder, &s);

    for _ in &threads {
        s.send(Work::DONE).unwrap();
    }

    for thread in threads {
        thread.join().unwrap();
    }

    // show duration
    let duration = start.elapsed();
    eprintln!("# took {:?}", duration);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_with_type() {
        let line =
            r#"<http://www.wikidata.org/entity/Q1644> <http://www.wikidata.org/prop/direct/P2043> "+1094.26"^^<http://www.w3.org/2001/XMLSchema#decimal> ."#;
        assert_eq!(parse(1, line), Statement {
            subject: Subject::IRI("http://www.wikidata.org/entity/Q1644"),
            predicate: "http://www.wikidata.org/prop/direct/P2043",
            object: Object::Literal("+1094.26", Extra::Type("http://www.w3.org/2001/XMLSchema#decimal"))
        });
    }

    #[test]
    fn test_literal_with_lang() {
        let line =
            r#"<http://www.wikidata.org/entity/Q177> <http://schema.org/name> "pizza"@en ."#;
        assert_eq!(parse(1, line), Statement {
            subject: Subject::IRI("http://www.wikidata.org/entity/Q177"),
            predicate: "http://schema.org/name",
            object: Object::Literal("pizza", Extra::Lang("en"))
        });
    }

    #[test]
    fn test_literal() {
        let line =
            r#"<http://www.wikidata.org/entity/Q177> <http://www.wikidata.org/prop/direct/P373> "Pizzas" ."#;
        assert_eq!(parse(1, line), Statement {
            subject: Subject::IRI("http://www.wikidata.org/entity/Q177"),
            predicate: "http://www.wikidata.org/prop/direct/P373",
            object: Object::Literal("Pizzas", Extra::None)
        });
    }

    #[test]
    fn test_blank_subject() {
        let line =
            r#"_:foo <bar> <baz>"#;
        assert_eq!(parse(1, line), Statement {
            subject: Subject::Blank("foo"),
            predicate: "bar",
            object: Object::IRI("baz")
        });
    }

    #[test]
    fn test_blank_object() {
        let line =
            r#"<foo> <bar> _:baz"#;
        assert_eq!(parse(1, line), Statement {
            subject: Subject::IRI("foo"),
            predicate: "bar",
            object: Object::Blank("baz")
        });
    }
}