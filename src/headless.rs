//! Headless processing: file collection (walkdir + glob), content hashing,
//! idempotent manifests, watch mode, parallel chunking, and output.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use bitvanes_core::arrow_io::batch::{chunks_to_batch, chunks_to_batch_with_embeddings};
use bitvanes_core::chunk::chunk_document;
use bitvanes_core::parse::parse_bytes;
use bitvanes_core::scrub::scrub_document;
use bitvanes_core::{
    BuiltInPattern, ChunkSpec, DocumentFormat, PipelineConfig, ScrubProfile, TokenizerKind,
};

use crate::Cli;

const SUPPORTED_EXTS: &[&str] = &["md", "markdown", "txt", "html", "htm", "pdf", "json"];

/// Entry point for headless mode.
pub fn run(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let config = build_config(cli)?;
    eprintln!(
        "Config: format={:?} tokenizer={:?} max_tokens={} strategy={:?}",
        config.format, config.chunk.tokenizer, config.chunk.max_tokens, config.chunk.strategy
    );

    let mut manifest = Manifest::load_or_default(cli.manifest.as_deref())?;
    let embedder = build_embedder(cli)?;

    if cli.watch {
        run_watch(cli, &config, &mut manifest, embedder.as_deref())?;
    } else {
        run_once(cli, &config, &mut manifest, embedder.as_deref(), true)?;
    }
    Ok(())
}

/// Processes a single batch of new/changed files and writes output.
/// Returns the number of files actually processed (post-manifest filter).
#[allow(clippy::too_many_arguments)]
fn run_once(
    cli: &Cli,
    config: &PipelineConfig,
    manifest: &mut Manifest,
    embedder: Option<&dyn bitvanes_core::Embedder>,
    write_output: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    let files = collect_files(cli, config)?;
    if files.is_empty() {
        eprintln!("No supported files found.");
        return Ok(0);
    }

    // Hash + manifest filter (sequential: cheap, and mutates the set).
    let force = cli.force;
    let mut pending: Vec<(PathBuf, String)> = Vec::new();
    let mut skipped = 0usize;
    for path in files {
        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("  ⚠ could not read {}: {e}", path.display());
                continue;
            }
        };
        let hash = hex_hash(&bytes);
        if !force && manifest.contains(&hash) {
            skipped += 1;
            continue;
        }
        pending.push((path, hash.clone()));
        manifest.insert(hash);
    }
    if skipped > 0 {
        eprintln!("Skipped {skipped} file(s) already in manifest (--force to reprocess).");
    }
    if pending.is_empty() {
        if write_output {
            eprintln!("Nothing new to process.");
        }
        return Ok(0);
    }
    eprintln!("Processing {} file(s)...", pending.len());

    let pb = ProgressBar::new(pending.len() as u64);
    pb.set_style(
        ProgressStyle::with_template("Processing {wide_bar} {pos}/{len}")
            .unwrap()
            .progress_chars("█░"),
    );

    // Parallel chunking. Each item carries its source hash alongside chunks.
    let processed: Vec<ProcessedChunk> = pending
        .par_iter()
        .flat_map(|(path, source_hash)| {
            let chunks = process_file(path, config);
            pb.inc(1);
            chunks
                .into_iter()
                .map(|spec| ProcessedChunk {
                    spec,
                    source_hash: source_hash.clone(),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    pb.finish_and_clear();

    // Persist the manifest now that we've committed to processing these files.
    manifest.save(cli.manifest.as_deref())?;

    if processed.is_empty() {
        eprintln!("\nNo chunks generated. Check that inputs are valid UTF-8 text.");
        return Ok(pending.len());
    }

    if write_output {
        let output = cli.output.as_deref().unwrap_or("output.json");
        write_output_inner(&processed, output, embedder)?;
        eprintln!(
            "\n{} chunk(s) from {} file(s) written to {output}",
            processed.len(),
            pending.len()
        );
    }
    Ok(pending.len())
}

/// Watch loop: repeatedly scan for new/changed files, process, persist.
fn run_watch(
    cli: &Cli,
    config: &PipelineConfig,
    manifest: &mut Manifest,
    embedder: Option<&dyn bitvanes_core::Embedder>,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!(
        "Watching for new files every {}s. Press Ctrl-C to stop.",
        cli.poll_interval
    );
    loop {
        let processed = run_once(cli, config, manifest, embedder, false)?;
        if processed > 0 {
            eprintln!("[watch] processed {processed} new file(s).");
        }
        std::thread::sleep(Duration::from_secs(cli.poll_interval.max(1)));
    }
}

/// A processed chunk paired with its source file's content hash.
struct ProcessedChunk {
    spec: ChunkSpec,
    source_hash: String,
}

/// Processes a single file through parse → scrub → chunk.
fn process_file(path: &Path, config: &PipelineConfig) -> Vec<ChunkSpec> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("  ⚠ could not read {}: {e}", path.display());
            return Vec::new();
        }
    };

    let cfg = PipelineConfig {
        source_label: Some(path.display().to_string()),
        ..config.clone()
    };
    let cfg = infer_format(path, cfg);

    match parse_bytes(&bytes, &cfg)
        .and_then(|doc| scrub_document(doc, &cfg.scrub).map(|(d, _)| d))
        .and_then(|doc| chunk_document(&doc, &cfg.chunk, cfg.source_label.as_deref()))
    {
        Ok(chunks) => chunks,
        Err(e) => {
            eprintln!("  ⚠ failed to process {}: {e}", path.display());
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

/// Writes chunks to the output file (TUI path; no hashes, no embeddings).
pub fn write_output(chunks: &[ChunkSpec], output: &str) -> Result<(), Box<dyn std::error::Error>> {
    let processed: Vec<ProcessedChunk> = chunks
        .iter()
        .map(|spec| ProcessedChunk {
            spec: spec.clone(),
            source_hash: String::new(),
        })
        .collect();
    write_output_inner(&processed, output, None)
}

/// Writes processed chunks in the format implied by the output extension.
/// Adds `source_hash`/`chunk_hash` to JSON, and fills the embedding column
/// for Arrow/CSV when an embedder is supplied.
fn write_output_inner(
    processed: &[ProcessedChunk],
    output: &str,
    embedder: Option<&dyn bitvanes_core::Embedder>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(output)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("json");

    let chunks: Vec<ChunkSpec> = processed.iter().map(|p| p.spec.clone()).collect();
    let source_hashes: Vec<String> = processed.iter().map(|p| p.source_hash.clone()).collect();

    // Embeddings (optional). All chunk texts in one batch.
    let embeddings: Option<(Vec<Vec<f32>>, usize)> = match embedder {
        Some(em) => {
            let texts: Vec<&str> = chunks.iter().map(|c| c.text.as_str()).collect();
            let vecs = em.embed(&texts)?;
            Some((vecs, em.dim()))
        }
        None => None,
    };

    match ext {
        "arrow" => {
            let ipc_bytes = match &embeddings {
                Some((vecs, dim)) => {
                    let batch = chunks_to_batch_with_embeddings(&chunks, vecs, *dim)?;
                    bitvanes_core::arrow_io::ipc::write_ipc_stream(&batch)?
                }
                None => {
                    let batch = chunks_to_batch(&chunks)?;
                    bitvanes_core::arrow_io::ipc::write_ipc_stream(&batch)?
                }
            };
            write_bytes(output, &ipc_bytes)?;
        }
        "csv" => {
            let batch = match &embeddings {
                Some((vecs, dim)) => chunks_to_batch_with_embeddings(&chunks, vecs, *dim)?,
                None => chunks_to_batch(&chunks)?,
            };
            let csv_text = bitvanes_core::arrow_io::csv::write_csv(&batch)?;
            write_text(output, &csv_text)?;
        }
        _ => {
            // JSON (default). Carries hashes; embeddings are omitted (use Arrow).
            let rows: Vec<JsonChunk> = chunks
                .iter()
                .zip(&source_hashes)
                .map(|(c, src)| JsonChunk::from_chunk(c, src))
                .collect();
            let json = serde_json::to_string_pretty(&rows)?;
            write_text(output, &json)?;
        }
    }
    Ok(())
}

fn write_bytes(output: &str, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    if output == "-" {
        use std::io::Write;
        std::io::stdout().write_all(bytes)?;
    } else {
        fs::write(output, bytes)?;
    }
    Ok(())
}

fn write_text(output: &str, text: &str) -> Result<(), Box<dyn std::error::Error>> {
    if output == "-" {
        print!("{text}");
    } else {
        fs::write(output, text)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// File collection (walkdir + glob)
// ---------------------------------------------------------------------------

fn collect_files(
    cli: &Cli,
    config: &PipelineConfig,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();

    if let Some(input) = &cli.input {
        out.extend(collect_input(input, cli, config)?);
    }
    for pattern in &cli.glob {
        for entry in glob::glob(pattern).map_err(|e| format!("bad glob '{pattern}': {e}"))? {
            match entry {
                Ok(p) => {
                    if let Some(canon) = canonical_if_supported(&p) {
                        if canon.is_file() && is_supported_ext(&canon) {
                            out.push(canon);
                        } else if canon.is_dir() {
                            out.extend(walk_dir(&canon, cli, config));
                        }
                    }
                }
                Err(e) => eprintln!("  ⚠ glob read error: {e}"),
            }
        }
    }

    // Dedupe (the same file may match --input and a --glob).
    let mut seen = HashSet::new();
    out.retain(|p| seen.insert(p.clone()));
    Ok(out)
}

fn collect_input(
    input: &Path,
    cli: &Cli,
    config: &PipelineConfig,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    if input.is_file() {
        return Ok(vec![canonical_or(input)]);
    }
    if !input.is_dir() {
        return Err(format!("{} is not a file or directory", input.display()).into());
    }
    Ok(walk_dir(input, cli, config))
}

fn walk_dir(root: &Path, cli: &Cli, config: &PipelineConfig) -> Vec<PathBuf> {
    let extensions: &[&str] = if cli.format.is_some() {
        match config.format {
            DocumentFormat::Markdown => &["md", "markdown"],
            DocumentFormat::Text => &["txt"],
            DocumentFormat::Html => &["html", "htm"],
            DocumentFormat::Pdf => &["pdf"],
            DocumentFormat::Json => &["json"],
        }
    } else {
        SUPPORTED_EXTS
    };

    WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| extensions.contains(&ext.to_lowercase().as_str()))
                .unwrap_or(false)
        })
        .map(|e| canonical_or(e.path()))
        .collect()
}

fn is_supported_ext(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| SUPPORTED_EXTS.contains(&e.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// canonicalize where possible (falls back to the path as-is on platforms
/// where the file is not yet statable through canonicalize).
fn canonical_or(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn canonical_if_supported(path: &Path) -> Option<PathBuf> {
    Some(path.canonicalize().unwrap_or_else(|_| path.to_path_buf()))
}

// ---------------------------------------------------------------------------
// Manifest (idempotency)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Default)]
struct Manifest {
    processed: Vec<String>,
}

impl Manifest {
    fn load_or_default(path: Option<&Path>) -> Result<Self, Box<dyn std::error::Error>> {
        match path {
            Some(p) if p.exists() => {
                let text = fs::read_to_string(p)?;
                let m: Manifest = serde_json::from_str(&text)?;
                Ok(m)
            }
            _ => Ok(Manifest::default()),
        }
    }

    fn contains(&self, hash: &str) -> bool {
        self.processed.iter().any(|h| h == hash)
    }

    fn insert(&mut self, hash: String) {
        if !self.contains(&hash) {
            self.processed.push(hash);
        }
    }

    fn save(&self, path: Option<&Path>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(p) = path {
            fs::write(p, serde_json::to_string_pretty(self)?)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.processed.len()
    }
}

/// Blake3 hash of bytes, lower-case hex.
fn hex_hash(bytes: &[u8]) -> String {
    let h = blake3::hash(bytes);
    h.to_hex().to_string()
}

// ---------------------------------------------------------------------------
// Config + embedding construction
// ---------------------------------------------------------------------------

/// Builds a `PipelineConfig` from a profile JSON and/or CLI flags.
pub(crate) fn build_config(cli: &Cli) -> Result<PipelineConfig, Box<dyn std::error::Error>> {
    let mut config = if let Some(path) = &cli.config {
        let json = fs::read_to_string(path)
            .map_err(|e| format!("could not read config {}: {e}", path.display()))?;
        serde_json::from_str(&json)?
    } else {
        PipelineConfig::default()
    };

    if let Some(fmt) = &cli.format {
        config.format = parse_format(fmt)?;
    }
    if let Some(tok) = &cli.tokenizer {
        config.chunk.tokenizer = parse_tokenizer(tok)?;
    }
    if let Some(mt) = cli.max_tokens {
        config.chunk.max_tokens = mt;
    }
    if let Some(scrub) = &cli.scrub {
        config.scrub = parse_scrub(scrub);
    }

    Ok(config)
}

#[cfg(feature = "embed")]
fn build_embedder(
    cli: &Cli,
) -> Result<Option<Box<dyn bitvanes_core::Embedder>>, Box<dyn std::error::Error>> {
    if let (Some(model), Some(tok)) = (cli.embed.as_deref(), cli.embed_tokenizer.as_deref()) {
        let em = bitvanes_core::OrtEmbedder::new(model, tok, cli.embed_dim, cli.embed_max_len)?;
        eprintln!(
            "Embeddings on: model={} dim={} max_len={}",
            model.display(),
            cli.embed_dim,
            cli.embed_max_len
        );
        return Ok(Some(Box::new(em)));
    }
    Ok(None)
}

#[cfg(not(feature = "embed"))]
fn build_embedder(
    _cli: &Cli,
) -> Result<Option<Box<dyn bitvanes_core::Embedder>>, Box<dyn std::error::Error>> {
    Ok(None)
}

/// Overrides the format based on file extension so each file is parsed by
/// the parser matching its type, regardless of the configured default.
pub(crate) fn infer_format(path: &Path, mut cfg: PipelineConfig) -> PipelineConfig {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase());
    match ext.as_deref() {
        Some("md") | Some("markdown") => cfg.format = DocumentFormat::Markdown,
        Some("txt") => cfg.format = DocumentFormat::Text,
        Some("html") | Some("htm") => cfg.format = DocumentFormat::Html,
        Some("pdf") => cfg.format = DocumentFormat::Pdf,
        Some("json") => cfg.format = DocumentFormat::Json,
        _ => {}
    }
    cfg
}

// --- Helpers ---

fn parse_format(s: &str) -> Result<DocumentFormat, Box<dyn std::error::Error>> {
    match s.to_lowercase().as_str() {
        "markdown" | "md" => Ok(DocumentFormat::Markdown),
        "text" | "txt" => Ok(DocumentFormat::Text),
        "html" | "htm" => Ok(DocumentFormat::Html),
        "json" => Ok(DocumentFormat::Json),
        "pdf" => Ok(DocumentFormat::Pdf),
        other => Err(format!("unknown format: {other}").into()),
    }
}

fn parse_tokenizer(s: &str) -> Result<TokenizerKind, Box<dyn std::error::Error>> {
    serde_json::from_str(&format!("\"{}\"", s.to_lowercase()))
        .map_err(|e| format!("unknown tokenizer '{s}': {e}").into())
}

fn parse_scrub(s: &str) -> ScrubProfile {
    let patterns: Vec<BuiltInPattern> = s
        .split(',')
        .filter_map(|p| serde_json::from_str(&format!("\"{}\"", p.trim())).ok())
        .collect();
    ScrubProfile {
        patterns,
        custom: vec![],
    }
}

/// JSON-serializable chunk wrapper with dedup hashes.
#[derive(Serialize)]
struct JsonChunk {
    chunk_index: u32,
    text: String,
    token_count: u16,
    source_path: String,
    heading_path: Vec<String>,
    section_kind: String,
    /// Blake3 of the source file's bytes (empty in the TUI path).
    source_hash: String,
    /// Blake3 of this chunk's text.
    chunk_hash: String,
}

impl JsonChunk {
    fn from_chunk(c: &ChunkSpec, source_hash: &str) -> Self {
        let chunk_hash = hex_hash(c.text.as_bytes());
        Self {
            chunk_index: c.chunk_index,
            text: c.text.clone(),
            token_count: c.token_count,
            source_path: c.source_path.clone(),
            heading_path: c.heading_path.clone(),
            section_kind: format!("{:?}", c.section_kind).to_lowercase(),
            source_hash: source_hash.to_string(),
            chunk_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_hash_is_stable_and_distinct() {
        let a = hex_hash(b"hello");
        let b = hex_hash(b"hello");
        let c = hex_hash(b"world");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 64); // blake3 hex = 64 chars
    }

    #[test]
    fn manifest_round_trips_and_dedups() {
        let tmp = format!(
            "/tmp/bv-manifest-{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = PathBuf::from(&tmp);
        let mut m = Manifest::default();
        m.insert("aaa".to_string());
        m.insert("aaa".to_string()); // dup ignored
        m.insert("bbb".to_string());
        assert_eq!(m.len(), 2);
        assert!(m.contains("aaa"));
        m.save(Some(&path)).unwrap();

        let loaded = Manifest::load_or_default(Some(&path)).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.contains("bbb"));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn infer_format_covers_all_extensions() {
        let base = PipelineConfig::default();
        assert_eq!(
            infer_format(Path::new("/x/a.md"), base.clone()).format,
            DocumentFormat::Markdown
        );
        assert_eq!(
            infer_format(Path::new("/x/a.json"), base.clone()).format,
            DocumentFormat::Json
        );
        assert_eq!(
            infer_format(Path::new("/x/a.pdf"), base.clone()).format,
            DocumentFormat::Pdf
        );
        assert_eq!(
            infer_format(Path::new("/x/a.xyz"), base).format,
            DocumentFormat::Markdown
        );
    }

    #[test]
    fn parse_scrub_filters_unknowns() {
        let p = parse_scrub("email, garbage ,ssn");
        assert_eq!(p.patterns.len(), 2);
        assert!(p.patterns.contains(&BuiltInPattern::Email));
        assert!(p.patterns.contains(&BuiltInPattern::Ssn));
    }

    #[test]
    fn json_chunk_carries_hashes() {
        let spec = ChunkSpec {
            chunk_index: 0,
            text: "sample text".to_string(),
            token_count: 2,
            source_path: "t.md".to_string(),
            heading_path: vec![],
            section_kind: bitvanes_core::SectionKind::Paragraph,
            char_offset_start: 0,
            char_offset_end: 11,
        };
        let jc = JsonChunk::from_chunk(&spec, "deadbeef");
        assert_eq!(jc.source_hash, "deadbeef");
        assert_eq!(jc.chunk_hash, hex_hash(b"sample text"));
    }

    #[test]
    fn collect_files_dedupes_overlap() {
        // Write two identical-path refs via a real dir to exercise dedup.
        let dir = format!(
            "/tmp/bv-collect-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        fs::create_dir_all(&dir).unwrap();
        fs::write(format!("{}/a.md", dir), "# hi").unwrap();
        let cli = Cli {
            input: Some(PathBuf::from(&dir)),
            glob: vec![format!("{}/a.md", dir)],
            config: None,
            no_tui: true,
            format: None,
            tokenizer: None,
            max_tokens: None,
            scrub: None,
            output: None,
            jobs: None,
            manifest: None,
            watch: false,
            poll_interval: 5,
            force: false,
            #[cfg(feature = "embed")]
            embed: None,
            #[cfg(feature = "embed")]
            embed_tokenizer: None,
            #[cfg(feature = "embed")]
            embed_dim: 384,
            #[cfg(feature = "embed")]
            embed_max_len: 256,
        };
        let files = collect_files(&cli, &PipelineConfig::default()).unwrap();
        let count = files
            .iter()
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("md"))
            .count();
        assert_eq!(
            count, 1,
            "a.md must appear once despite --input + --glob overlap"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
