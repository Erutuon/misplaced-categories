use std::{
    collections::HashMap as Map,
    convert::TryFrom,
    env::args_os,
    ffi::OsString,
    io,
    path::{Path, PathBuf},
    string::String as StdString,
};

use parse_mediawiki_sql::{
    field_types::{PageNamespace, PageTitle},
    iterate_sql_insertions,
    schemas::{CategoryLink, Page},
    utils::{memory_map, Mmap, NamespaceMap},
};
use pico_args::Keys;
use rlua::{Lua, StdLib};
use smartstring::alias::String;
use thiserror::Error;

const PARTS_OF_SPEECH: &[&str] = &[
    "noun",
    "verb",
    "adjective",
    "numeral",
    "pronoun",
    "determiner",
];

// Checks that category is "Requests_for_<request_type>_in_<lang>_entries"
// or "Requests_for_inflections_in_<lang>_<pos>_entries".
fn is_request_category(category: &str, lang: &str) -> bool {
    if let Some(request_suffix) = category.strip_prefix("Requests_for_") {
        if let Some((request_type, lang_entries)) = request_suffix.split_once("_in_") {
            // match for instance "<lang>_entries" and "<lang>_<part_of_speech>_entries"
            lang_entries
                .strip_prefix(lang)
                .and_then(|entries| {
                    entries.strip_suffix("_entries").map(|pos| {
                        pos.is_empty()
                            || (request_type == "inflections"
                                && pos
                                    .strip_prefix('_')
                                    .map(|pos| PARTS_OF_SPEECH.contains(&pos))
                                    .unwrap_or(false))
                    })
                })
                .unwrap_or(false)
        } else {
            request_suffix.contains("_script_for_")
                || request_suffix.starts_with("attention")
                || request_suffix.starts_with("deletion")
        }
    } else {
        false
    }
}

#[test]
fn test_request_category() {
    assert!(is_request_category(
        "Requests_for_inflections_in_Proto-Anatolian_entries",
        "Proto-Anatolian"
    ));
    assert!(is_request_category(
        "Requests_for_inflections_in_Middle_Irish_entries",
        "Middle_Irish"
    ));
    assert!(!is_request_category(
        "Requests_for_inflections_in_Middle_Irish_entries",
        "Proto-Anatolian"
    ));
}

fn get_lang_code_prefix(category: &str) -> Option<&str> {
    category
        .split(':')
        .next()
        .filter(|prefix| prefix.chars().all(|c| c.is_ascii_lowercase() || c == '-'))
}

fn lang_code_to_name<'a>(languages: &'a Map<String, String>, code: &str) -> Option<&'a str> {
    languages.get(code).map(|s| std::ops::Deref::deref(s))
}

fn title_equals(title1: &str, title2: &str) -> bool {
    title1
        .bytes()
        .map(|c| if c == b' ' { b'_' } else { c })
        .eq(title2.bytes())
}

fn category_matches_lang(category: &str, lang: &str, languages: &Map<String, String>) -> bool {
    ["Undetermined_language_links", "Candidates_for_speedy_deletion", "Entries_missing_English_vernacular_names_of_taxa"].contains(&category)
        || [
            lang,
            "Terms_with_manual_transliterations_different_from_the_automated_ones",
            "Terms_with_redundant_transliterations",
            "Sort_key_tracking",
            "Requests_for_native_script",
            "EDSIL",
            "Entries_using_missing_taxonomic_name",
        ]
        .iter()
        .any(|prefix| category.starts_with(prefix))
        || ["term_requests","transliterations_containing_ambiguous_characters"].iter().any(|suffix| category.ends_with(suffix))
        || category.split('/').next().map(|first_word| first_word.ends_with("redlinks")).unwrap_or(false)
        // This seems to only match dialectal categories, such as "Vulgar_Latin" in a "Reconstruction:Latin/..." entry.
        || category.split('_').all(|word| word.chars().next().unwrap().is_uppercase())
        || is_request_category(category, lang)
        // Get language name for "<language code>:topic category" and compare it to the language name of the entry,
        // treating spaces as equal to underscores.
        || get_lang_code_prefix(category).and_then(|code| lang_code_to_name(languages, code)).map(|name| {
            title_equals(name, lang)
        }).unwrap_or(false)
        // Exclude tracking categories, which start with lowercase and don't contain ":".
        || (category.chars().next().map(|c| c.is_lowercase()).unwrap_or(false) && !category.contains(':'))
}

type LangCategoryMap<'a> = Map<(PageNamespace, PageTitle), (String, Vec<PageTitle>)>;

fn print_filtered_categories(
    categories: LangCategoryMap,
    namespaces: &NamespaceMap,
    languages: &Map<String, String>,
) -> anyhow::Result<()> {
    let categories: Map<_, _> = categories
        .into_iter()
        .filter_map(|((namespace, title), (lang, mismatched_categories))| {
            let filtered_categories: Vec<_> = mismatched_categories
                .into_iter()
                .filter(|PageTitle(category): &PageTitle| {
                    !category_matches_lang(&category, &lang, &languages)
                })
                .collect();
            if !filtered_categories.is_empty() {
                Some((
                    namespaces.readable_title(&title, &namespace),
                    (lang, filtered_categories),
                ))
            } else {
                None
            }
        })
        .collect();

    let mut categories = categories.into_iter().collect::<Vec<_>>();
    categories.sort();
    for (title, (_, categories)) in &categories {
        print!("{}", title);
        for PageTitle(category) in categories {
            print!("#{}", category);
        }
        println!();
    }
    Ok(())
}

fn print_reconstruction_categories<'a>(
    pages: impl IntoIterator<Item = Page<'a>>,
    categories: impl IntoIterator<Item = CategoryLink>,
) -> anyhow::Result<()> {
    let pages = pages
        .into_iter()
        .filter_map(
            |Page {
                 id,
                 namespace,
                 title,
                 ..
             }: Page| {
                if namespace == PageNamespace(118) {
                    if let Some(lang) = title.0.split_once('/').map(|(lang, _)| lang) {
                        return Some((id, (String::from(lang), namespace, title)));
                    }
                }
                None
            },
        )
        .collect::<Map<_, _>>();

    let categories = categories.into_iter().fold(
        LangCategoryMap::new(),
        |mut map, CategoryLink { from, to, .. }: CategoryLink| {
            if let Some((lang, namespace, title)) = pages.get(&from) {
                let (_, vec) = map
                    .entry((*namespace, title.clone()))
                    .or_insert_with(|| (lang.to_owned(), Vec::new()));
                vec.push(to);
            }
            map
        },
    );

    let stdout = io::stdout();
    let stdout = stdout.lock();
    bincode::serialize_into(stdout, &categories)?;

    Ok(())
}

#[derive(Debug, Error)]
enum Error {
    #[error("{0}")]
    PicoArgs(#[from] pico_args::Error),
    #[error("{0}")]
    UtilsError(#[from] parse_mediawiki_sql::utils::Error),
}

struct Arguments {
    args: pico_args::Arguments,
    dump_dir_keys: Keys,
    dump_dir: Option<PathBuf>,
}

type ArgResult<T> = std::result::Result<T, Error>;

impl Arguments {
    fn new<K: Into<Keys>>(args: Vec<OsString>, dump_dir_keys: K) -> Self {
        Self {
            args: pico_args::Arguments::from_vec(args),
            dump_dir: None,
            dump_dir_keys: dump_dir_keys.into(),
        }
    }

    fn dump_dir(&mut self) -> ArgResult<Option<&PathBuf>> {
        if self.dump_dir.is_none() {
            self.dump_dir = self.opt_path(self.dump_dir_keys)?;
        }
        Ok(self.dump_dir.as_ref())
    }

    fn opt_path<K: Into<Keys>>(&mut self, keys: K) -> ArgResult<Option<PathBuf>> {
        #[allow(clippy::redundant_closure)]
        self.args
            .opt_value_from_os_str(keys, |opt| PathBuf::try_from(opt))
            .map_err(Into::into)
    }

    fn path<K: Into<Keys>, P: AsRef<Path>>(&mut self, keys: K, default: P) -> ArgResult<PathBuf> {
        Ok(self
            .opt_path(keys)?
            .unwrap_or_else(|| default.as_ref().into()))
    }

    fn path_in_dir<K: Into<Keys>, P: AsRef<Path>>(
        &mut self,
        keys: K,
        default: P,
    ) -> ArgResult<PathBuf> {
        let file = self
            .opt_path(keys)?
            .unwrap_or_else(|| default.as_ref().into());
        Ok(self
            .dump_dir()?
            .cloned()
            .map(|mut dir| {
                dir.push(&file);
                dir
            })
            .unwrap_or(file))
    }
}

trait MmapPath {
    fn mmap(&self) -> ArgResult<Mmap>;
}

impl<P: AsRef<Path>> MmapPath for P {
    fn mmap(&self) -> ArgResult<Mmap> {
        unsafe { Ok(memory_map(&self)?) }
    }
}

/**
Lists categories on pages in the Reconstruction namespace whose titles contain a language name or code
that does not match the language code in the Reconstruction page title.
Requires pages-articles.xml, page.sql, categorylinks.sql, siteinfo-namespaces.json or siteinfo-namespaces.json.gz,
all from the English Wiktionary data dump for the same day, the first four decompressed from the files provided in the data dump,
for which see https://github.com/Erutuon/enwikt-dump-rs/blob/master/dump/download.sh.

Run three commands if the files have these exact names (with no date or wiki name appended):

cargo run --release -- categories --dump-dir your/dump/dir > categories.bincode # get categories for Reconstruction pages
cargo run --release -- langs --dump-dir your/dump/dir > code_to_name.bincode # get map from language code to language name
cargo run --release -- filter --categories categories.bincode --code-to-name code_to_name.bincode --dump-dir your/dump/dir > filtered.txt

Outputs a list in an ad-hoc format, where each line contains the Reconstruction title followed by the filtered categories.
*/
fn main() -> anyhow::Result<()> {
    let mut args = args_os().skip(1);
    let command = args
        .by_ref()
        .next()
        .ok_or_else(|| anyhow::anyhow!("expected subcommand"))?
        .into_string()
        .map_err(|_| pico_args::Error::NonUtf8Argument)?;
    let mut args = Arguments::new(args.collect(), ["-d", "--dump-dir"]);

    match command.as_str() {
        "langs" => {
            let xml_path = args.path_in_dir(["-x", "--xml"], "pages-articles.xml")?;
            const LANG_CODE_TO_NAME_MODULE: &str = "Module:languages/code to canonical name";

            if let Some(cbor_mediawiki_dump::Page { revisions, .. }) =
                cbor_mediawiki_dump::find_page(LANG_CODE_TO_NAME_MODULE, &xml_path.mmap()?)?
            {
                if let Some(cbor_mediawiki_dump::Revision { text, .. }) = revisions.get(0) {
                    let mut languages = None;
                    Lua::new_with(StdLib::empty()).context(|ctx| {
                        languages = Some(ctx.load(&text).eval::<Map<StdString, StdString>>());
                    });
                    if let Some(languages) = languages {
                        let stdout = io::stdout();
                        let stdout = stdout.lock();
                        bincode::serialize_into(stdout, &languages?)?;
                        return Ok(());
                    }
                }
            }

            Err(anyhow::anyhow!(
                "Did not find [[{}]] in {}",
                LANG_CODE_TO_NAME_MODULE,
                xml_path.display()
            ))
        }
        "filter" => {
            let serialized_categories = args.path(["-C", "--categories"], "categories.bincode")?;
            let map = bincode::deserialize(&serialized_categories.mmap()?)?;
            let namespaces = NamespaceMap::from_path(
                &args.path_in_dir(["-s", "--siteinfo-namespaces"], "siteinfo-namespaces.json")?,
            )?;
            let serialized_langs = args
                .path(["-c", "--code-to-name"], "code_to_name.bincode")?
                .mmap()?;
            let languages = bincode::deserialize(&serialized_langs);
            print_filtered_categories(map, &namespaces, &languages?)
        }
        "categories" => {
            let page_sql = args.path_in_dir(["-p", "--page"], "page.sql")?.mmap()?;
            let category_links_sql = args
                .path_in_dir(["-c", "--category-links"], "categorylinks.sql")?
                .mmap()?;

            print_reconstruction_categories(
                &mut iterate_sql_insertions(&page_sql),
                &mut iterate_sql_insertions(&category_links_sql),
            )?;

            Ok(())
        }
        _ => Err(anyhow::anyhow!("Expected a subcommand: langs")),
    }
}
