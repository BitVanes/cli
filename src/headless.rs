//! Headless processing: directory scanning, parallel chunking, output.

use std::fs;
use std::path::{Path, PathBuf};

use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Serialize;
use walkdir::WalkDir;

use bitvanes_core::arrow_io::batch::chunks_to_batch;
use bitvanes_core::chunk::chunk_document;
use bitvanes_core::parse::parse_bytes;
use bitvanes_core::scrub::scrub_document;
use bitvanes_core::{
    BuiltInPattern, ChunkSpec, DocumentFormat, PipelineConfig, ScrubProfile, TokenizerKind,
};

use crate::Cli;

/// Entry point for headless mode.
pub fn run(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let input = cli
        .input
        .as_ref()
        .ok_or("no input specified — use --input <PATH>")?;

    // 1. Build pipeline config from profile + CLI flags.
    let config = build_config(cli)?;
    eprintln!(
        "Config: format={:?} tokenizer={:?} max_tokens={}",
        config.format, config.chunk.tokenizer, config.chunk.max_tokens
    );

    // 2. Collect input files.
    let files = collect_files(input, &config, cli)?;
    if files.is_empty() {
        return Err("no supported files found in the input path".into());
    }
    eprintln!("Found {} files", files.len());

    // 3. Process in parallel.
    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(
        ProgressStyle::with_template("Processing {wide_bar} {pos}/{len}")
            .unwrap()
            .progress_chars("█░"),
    );

    let all_chunks: Vec<ChunkSpec> = files
        .par_iter()
        .flat_map(|path| {
            let chunks = process_file(path, &config);
            pb.inc(1);
            chunks
        })
        .collect();

    pb.finish_and_clear();

    // 4. Print stats.
    let total_tokens: u64 = all_chunks.iter().map(|c| c.token_count as u64).sum();
    eprintln!();
    eprintln!("Results:");
    eprintln!("  Files processed:  {}", files.len());
    eprintln!("  Chunks generated: {}", all_chunks.len());
    eprintln!("  Total tokens:     {}", total_tokens);
    if !all_chunks.is_empty() {
        eprintln!(
            "  Avg tokens/chunk: {}",
            total_tokens / all_chunks.len() as u64
        );
    }

    if all_chunks.is_empty() {
        eprintln!("\nNo chunks generated. Check that your input files are valid UTF-8 text.");
        return Ok(());
    }

    // 5. Write output.
    let output = cli.output.as_deref().unwrap_or("output.json");
    write_output(&all_chunks, output)?;
    eprintln!("\nOutput written to {}", output);

    Ok(())
}

/// Builds a `PipelineConfig` from a profile JSON and/or CLI flags.
pub(crate) fn build_config(cli: &Cli) -> Result<PipelineConfig, Box<dyn std::error::Error>> {
    let mut config = if let Some(path) = &cli.config {
        let json = fs::read_to_string(path)
            .map_err(|e| format!("could not read config {}: {e}", path.display()))?;
        serde_json::from_str(&json)?
    } else {
        PipelineConfig::default()
    };

    // CLI flags override profile values.
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

/// Collects files from a path (single file or recursive directory walk).
fn collect_files(
    input: &Path,
    config: &PipelineConfig,
    cli: &Cli,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    if input.is_file() {
        return Ok(vec![input.canonicalize()?]);
    }

    if !input.is_dir() {
        return Err(format!("{} is not a file or directory", input.display()).into());
    }

    // Determine which extensions to look for. If format is explicitly given on CLI,
    // restrict to those extensions. Otherwise, collect all supported formats.
    let extensions: &[&str] = if cli.format.is_some() {
        match config.format {
            DocumentFormat::Markdown => &["md", "markdown"],
            DocumentFormat::Text => &["txt"],
            DocumentFormat::Html => &["html", "htm"],
            DocumentFormat::Pdf => &["pdf"],
            DocumentFormat::Json => &["json"],
        }
    } else {
        &["md", "markdown", "txt", "html", "htm", "pdf", "json"]
    };

    let files: Vec<PathBuf> = WalkDir::new(input)
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
        .map(|e| e.into_path())
        .collect();

    Ok(files)
}

/// Processes a single file through the full pipeline.
fn process_file(path: &Path, config: &PipelineConfig) -> Vec<ChunkSpec> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("  ⚠ Could not read {}: {}", path.display(), e);
            return Vec::new();
        }
    };

    // Override source_label with the file path.
    let cfg = PipelineConfig {
        source_label: Some(path.display().to_string()),
        ..config.clone()
    };

    // Try to infer format from extension if the config format doesn't match.
    let cfg = infer_format(path, cfg);

    match parse_bytes(&bytes, &cfg)
        .and_then(|doc| scrub_document(doc, &cfg.scrub).map(|(d, _)| d))
        .and_then(|doc| chunk_document(&doc, &cfg.chunk, cfg.source_label.as_deref()))
    {
        Ok(chunks) => chunks,
        Err(e) => {
            eprintln!("  ⚠ Failed to process {}: {}", path.display(), e);
            Vec::new()
        }
    }
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

/// Writes chunks to the output file in the specified format.
pub fn write_output(chunks: &[ChunkSpec], output: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(output)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("json");

    match ext {
        "arrow" => {
            let batch = chunks_to_batch(chunks)?;
            let ipc_bytes = bitvanes_core::arrow_io::ipc::write_ipc_stream(&batch)?;
            if output == "-" {
                use std::io::Write;
                std::io::stdout().write_all(&ipc_bytes)?;
            } else {
                fs::write(output, ipc_bytes)?;
            }
        }
        "csv" => {
            let batch = chunks_to_batch(chunks)?;
            let csv_text = bitvanes_core::arrow_io::csv::write_csv(&batch)?;
            if output == "-" {
                print!("{csv_text}");
            } else {
                fs::write(output, csv_text)?;
            }
        }
        _ => {
            // JSON output (default).
            let json_chunks: Vec<JsonChunk> = chunks.iter().map(JsonChunk::from).collect();
            let json = serde_json::to_string_pretty(&json_chunks)?;
            if output == "-" {
                println!("{json}");
            } else {
                fs::write(output, json)?;
            }
        }
    }

    Ok(())
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

/// JSON-serializable chunk wrapper.
#[derive(Serialize)]
struct JsonChunk {
    chunk_index: u32,
    text: String,
    token_count: u16,
    source_path: String,
    heading_path: Vec<String>,
    section_kind: String,
}

impl From<&ChunkSpec> for JsonChunk {
    fn from(c: &ChunkSpec) -> Self {
        Self {
            chunk_index: c.chunk_index,
            text: c.text.clone(),
            token_count: c.token_count,
            source_path: c.source_path.clone(),
            heading_path: c.heading_path.clone(),
            section_kind: format!("{:?}", c.section_kind).to_lowercase(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitvanes_core::SectionKind;

    fn chunk(text: &str) -> ChunkSpec {
        ChunkSpec {
            chunk_index: 0,
            text: text.to_string(),
            token_count: 1,
            source_path: "t.md".to_string(),
            heading_path: vec![],
            section_kind: SectionKind::Paragraph,
            char_offset_start: 0,
            char_offset_end: text.len() as u32,
        }
    }

    #[test]
    fn parse_format_recognizes_every_supported_format() {
        assert_eq!(parse_format("markdown").unwrap(), DocumentFormat::Markdown);
        assert_eq!(parse_format("md").unwrap(), DocumentFormat::Markdown);
        assert_eq!(parse_format("text").unwrap(), DocumentFormat::Text);
        assert_eq!(parse_format("HTML").unwrap(), DocumentFormat::Html);
        assert_eq!(parse_format("json").unwrap(), DocumentFormat::Json);
        assert_eq!(parse_format("pdf").unwrap(), DocumentFormat::Pdf);
        assert!(parse_format("docx").is_err());
    }

    #[test]
    fn parse_tokenizer_accepts_all_variants_case_insensitively() {
        for s in ["cl100k_base", "O200K_BASE", "r50k_base"] {
            assert!(parse_tokenizer(s).is_ok(), "{s} should parse");
        }
        assert!(parse_tokenizer("nope").is_err());
    }

    #[test]
    fn parse_scrub_collects_known_patterns() {
        let profile = parse_scrub("email,ssn, credit_card ,, unknown");
        assert_eq!(profile.patterns.len(), 3);
        assert!(profile.patterns.contains(&BuiltInPattern::Email));
        assert!(profile.patterns.contains(&BuiltInPattern::CreditCard));
        assert!(profile.custom.is_empty());
    }

    #[test]
    fn infer_format_routes_by_extension() {
        let base = PipelineConfig::default();
        assert_eq!(
            infer_format(std::path::Path::new("/x/a.md"), base.clone()).format,
            DocumentFormat::Markdown
        );
        assert_eq!(
            infer_format(std::path::Path::new("/x/a.json"), base.clone()).format,
            DocumentFormat::Json
        );
        assert_eq!(
            infer_format(std::path::Path::new("/x/a.pdf"), base.clone()).format,
            DocumentFormat::Pdf
        );
        assert_eq!(
            infer_format(std::path::Path::new("/x/a.htm"), base.clone()).format,
            DocumentFormat::Html
        );
        // Unknown extension keeps the configured format.
        assert_eq!(
            infer_format(std::path::Path::new("/x/a.xyz"), base).format,
            DocumentFormat::Markdown
        );
    }

    #[test]
    fn write_output_emits_all_three_formats() {
        let id = nanos();
        let chunks = vec![chunk("hello world")];

        let json_path = format!("/tmp/bv-pub-{id}.json");
        write_output(&chunks, &json_path).unwrap();
        assert!(
            std::fs::read_to_string(&json_path)
                .unwrap()
                .contains("hello world")
        );

        let csv_path = format!("/tmp/bv-pub-{id}.csv");
        write_output(&chunks, &csv_path).unwrap();
        let csv = std::fs::read_to_string(&csv_path).unwrap();
        assert!(csv.lines().next().unwrap().contains("chunk_index"));
        assert!(csv.contains("hello world"));

        let arrow_path = format!("/tmp/bv-pub-{id}.arrow");
        write_output(&chunks, &arrow_path).unwrap();
        let bytes = std::fs::read(&arrow_path).unwrap();
        // Arrow IPC stream begins with the continuation marker 0xFFFFFFFF.
        assert_eq!(&bytes[..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
        let _ = std::fs::remove_file(&json_path);
        let _ = std::fs::remove_file(&csv_path);
        let _ = std::fs::remove_file(&arrow_path);
    }

    fn nanos() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
