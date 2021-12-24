use std::fs::{self, File};
use std::process::Command;
use std::path::{Path, PathBuf};
use std::io::{BufReader, BufRead, Write};
use std::ffi::OsStr;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::collections::BTreeMap;
use std::collections::VecDeque;

use anyhow::{Context,Result};
use walkdir::{WalkDir, DirEntry};
use serde::{Serialize, Deserialize};
use rayon::prelude::*;
use indicatif::{ProgressBar};
use flate2::read::GzDecoder;
use tar::Archive;

const REPO_NAME: &str = "crates.io-index";

const CARGO_TOML_CACHE: &str = "toml_cache";

fn main() -> Result<()> {
    download_git_index()?;
    update_git_index()?;

    check_and_download_crates(parse_index(index_iterator().collect())?)?;

    let fs_iterator: Vec<_> = cargo_toml_iterator().collect();
    let macros = find_proc_macros(&fs_iterator)?;

    let weird_deps = find_weird_dependencies(macros);

    write_data(weird_deps)?;
    Ok(())
}

fn download_git_index() -> Result<()> {
    println!("Checking git index...");
    if fs::metadata(format!("{}/{}", REPO_NAME, ".git")).is_err() {
        println!("Cloning git index...");
        Command::new("git")
            .args([
                "clone",
                &format!("https://github.com/rust-lang/{}", REPO_NAME)
            ])
            .spawn()
            .context("git command failed to start")?
            .wait()
            .context("Failed to clone crates.io index")?;
    }

    Ok(())
}

fn update_git_index() -> Result<()> {
    if Path::new(CARGO_TOML_CACHE).exists() {
        println!("Skipping updating git index to ensure cache is synced");

        return Ok(());
    }
    println!("Updating git index...");
    Command::new("git")
        .arg("pull")
        .current_dir(REPO_NAME)
        .spawn()
        .context("git command failed to start")?
        .wait()
        .context("Failed to pull crates.io index")
        .map(|_| ())
}

fn index_iterator() -> impl Iterator<Item = DirEntry> {
    WalkDir::new(REPO_NAME)
        .into_iter()
        .filter_entry(|entry| {
            let name = entry.file_name().to_string_lossy();
            !name.starts_with('.') && name != "config.json"
        })
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.file_type().is_file() {
                Some(entry)
            } else {
                None
            }
        })
}

fn parse_index(iterator: Vec<DirEntry>) -> Result<Vec<GitIndexEntry>> {
    println!("Reading crate metadata from disk...");
    let output: Result<Vec<_>> = iterator.into_par_iter().map(|entry| {
        let mut entries = Vec::new();

        let file = File::open(entry.path()).context("Failed to read file")?;
        let mut contents = {
            let contents: VecDeque<_> = BufReader::new(file).lines().collect();
            contents.into_iter().rev()
        };

        while let Some(Ok(line)) = contents.next() {
            let entry: GitIndexEntry = serde_json::from_str(&line)
                .context("Failed to parse line")?;
            if !entry.yanked {
                entries.push(entry);
                break;
            }
        }

        Ok(entries)
    })
    .collect();

    let output: Vec<_> = output?
        .into_iter()
        .flatten()
        .collect();

    println!("Successfully parsed {} crates.", output.len());

    Ok(output)
}

// There are more fields, but we only care about a few.
#[derive(Deserialize, Debug)]
struct GitIndexEntry {
    name: String,
    vers: String,
    yanked: bool,
}

fn check_and_download_crates(index: Vec<GitIndexEntry>) -> Result<()> {
    println!("Checking and downloading crate manifest files...");
    let progress_bar = ProgressBar::new(index.len() as u64);
    index
        .into_par_iter()
        .map(|GitIndexEntry { name, vers, .. }| {
            let path = get_cache_name(&name)?;

            let toml_path = {
                let mut path = path.clone();
                path.push(format!("{}-{}", name, vers));
                path.push("Cargo.toml");
                path
            };
            if toml_path.exists() {
                // println!("Skipping {} {}", name, vers);
                progress_bar.inc(1);
                return Ok(());
            }
            let url = format!(
                "https://static.crates.io/crates/{name}/{name}-{version}.crate",
                name = name,
                version = vers
            );

            let tarball = reqwest::blocking::get(&url)
                .context("Failed to download crate")?
                .bytes()
                .context("Failed to read crate")?;
            let mut archive = Archive::new(GzDecoder::new(tarball.as_ref()));
            for entry in archive.entries()? {
                if let Ok(mut entry) = entry {
                    if let Some(file_name) = entry.path()?.file_name()  {
                        if file_name == OsStr::new("Cargo.toml") {
                            entry.unpack_in(path)?;
                            progress_bar.inc(1);
                            break;
                        }
                    }
                }
            }
            Ok(())
        }).collect::<Result<Vec<_>>>()?;

    progress_bar.finish();
    Ok(())
}

fn get_cache_name(crate_name: &str) -> Result<PathBuf> {
    let mut path = PathBuf::from(CARGO_TOML_CACHE);
    match crate_name.len() {
        1 => path.push("1"),
        2 => path.push("2"),
        3 => {
            path.push("3");
            path.push(crate_name.chars().next().unwrap().to_string());
        }
        _ => {
            path.push(crate_name.chars().take(2).collect::<String>());
            path.push(crate_name.chars().skip(2).take(2).collect::<String>());
        }
    }
    std::fs::create_dir_all(&path)?;
    Ok(path)
}

fn cargo_toml_iterator() -> impl Iterator<Item = DirEntry> {
    WalkDir::new(CARGO_TOML_CACHE)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.file_type().is_file() {
                Some(entry)
            } else {
                None
            }
        })
}

#[derive(Deserialize, Debug)]
struct CargoToml {
    #[serde(alias= "project")]
    package: TomlPackage,
    lib: Option<TomlLib>,
    dependencies: Option<BTreeMap<String, toml::Value>>,
}

#[derive(Deserialize, Debug)]
struct TomlPackage {
    name: String,
}


#[derive(Deserialize, Debug, Default)]
struct TomlLib {
    #[serde(alias = "proc-macro")]
    proc_macro: Option<bool>,
}


fn find_proc_macros(cargo_toml_files: &[DirEntry]) -> Result<BTreeMap<String, CargoToml>> {
    println!("Finding proc macros...");
    let progress_bar = ProgressBar::new(cargo_toml_files.len() as u64);
    let map: BTreeMap<String, CargoToml> = cargo_toml_files.par_iter().map(|entry| {
        progress_bar.inc(1);
        let toml_raw = fs::read_to_string(entry.path())?;
        let toml: CargoToml = match toml::from_str(&toml_raw) {
            Ok(toml) => toml,
            Err(e) => {
                progress_bar.println(format!("Got invalid manifest file at {}: {}", entry.path().to_string_lossy(), e));
                return Ok(None);
            }
        };

        if toml.lib.as_ref().map(|v| v.proc_macro).flatten().unwrap_or_default() {
            Ok(Some((toml.package.name.clone(), toml)))
        } else {
            Ok(None)
        }
    })
    .collect::<Result<Vec<_>>>()?
    .into_iter()
    .flatten()
    .collect();

    progress_bar.finish();

    println!("Found {} proc macros.", map.len());
    Ok(map)
}

const NORMAL_DEPS: &[&str] = &[
    "syn",
    "proc-macro2",
    "quote",
    "proc-macro-error",
    "proc-macro-crate",
    "proc-macro-hack",
    "darling",
    "heck",
    "lazy_static",
    "regex",
    "Inflector",
    "anyhow",
    "convert_case",
    "itertools",
    "once_cell",
    "rand", // ????
    "synstructure",
    "unicode-xid",
    "failure",
];

fn find_weird_dependencies(mapping: BTreeMap<String, CargoToml>) -> BTreeMap<String, CargoToml>{
    println!("Finding weird dependencies...");
    let res: BTreeMap<_, _> = mapping
        .into_par_iter()
        .filter_map(|(cargo_name, mut toml)| {
            let deps = toml.dependencies.as_mut()?;

            for dep in NORMAL_DEPS {
                deps.remove(&dep.to_string());
            }

            if deps.is_empty() {
                None
            } else {
                Some((cargo_name, toml))
            }
        })
        .collect();
    println!("Found {} proc macros with non-standard dependencies.", res.len());
    res
}

fn write_data(data: BTreeMap<String, CargoToml>) -> Result<()> {
    #[derive(Serialize)]
    struct Data {
        name: String,
        deps: Vec<String>,
    }

    let stats = dashmap::DashMap::new();

    let data = data.into_par_iter().filter_map(|(key, value)| {
        if value.dependencies.as_ref().map(|deps| deps.len()).unwrap_or_default() > 1 {
            Some((key, value))
        } else {
            None
        }
    }).collect::<BTreeMap<_, _>>();

    println!("Found {} proc macro crates with > 1 dependency, excluding 'standard' dependencies", data.len());

    let dependencies: Vec<_> = data.into_par_iter().map(|(name, value)| {
        let deps: Vec<_> = value.dependencies.unwrap().into_keys().collect();
        for dep in &deps {
            stats.entry(dep.clone()).or_insert(AtomicUsize::new(0)).fetch_add(1, Ordering::Relaxed);
        }
        Data {
            name,
            deps,
        }
    }).collect();
    let data = serde_json::to_string(&dependencies)?;
    File::create("data")?.write_all(data.as_bytes())?;

    println!("Found {} non-standard dependencies", stats.len());
    let stats = serde_json::to_string(&stats)?;
    File::create("stats")?.write_all(stats.as_bytes())?;

    Ok(())
}