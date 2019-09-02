use std::io::{BufRead, Write};
use std::fs::File;
use std::env;
use std::io::BufReader;
use bzip2::bufread::BzDecoder;
use regex::Regex;
use std::collections::HashSet;
use std::time::Instant;

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

pub fn parse(line: usize, input: &str) -> Statement {
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
    return iri.starts_with("https://www.wikidata.org/wiki/Special:EntityData")
}

fn main() {
    let path = env::args().nth(1)
        .expect("missing path");

    let file = File::open(&path)
        .expect("can't open file");

    let decoder = BzDecoder::new( BufReader::new(file));
    let mut reader = BufReader::new(decoder);

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    let mut kept = 0;
    let mut total = 0;

    let start = Instant::now();

    let mut line = String::new();
    loop {
        line.clear();

        if reader.read_line(&mut line).unwrap() <= 0 {
            break
        }

        total += 1;

        let statement = parse(total, &line);

        if PROPERTIES.contains(statement.predicate) {
            continue
        }

        if IDENTIFIER_PROPERTIES.contains(statement.predicate) {
            continue
        }

        match statement.subject {
            Subject::Blank(_) =>
                continue,
            Subject::IRI(iri) if ignored_subject(iri) =>
                continue,
            _ => (),
        }

        match statement.object {
            Object::Literal(_, Extra::Lang(lang)) if !LANGUAGES.contains(lang) =>
                continue,
            _ => (),
        }

        out.write_all(&line.as_bytes()).unwrap();

        kept += 1;
    }

    // show duration
    let duration = start.elapsed();
    writeln!(out, "# took {:?}", duration).unwrap();

    // show count of triples kept
    let percentage = ((kept as f64) * 100.0) / (total as f64);
    writeln!(out, "# kept: {} / {} = {}", kept, total, percentage).unwrap();

    out.flush().unwrap();
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