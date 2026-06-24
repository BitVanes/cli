//! `bitvanes` — zero-trust ETL for AI/RAG.
//!
//! Processes documents into BPE-aware chunks with structural context,
//! outputting Apache Arrow IPC, CSV, or JSON. Runs the same pipeline
//! as the web app, natively.

mod headless;
mod tui;

use clap::{CommandFactory, Parser};
use std::path::PathBuf;
use std::process::ExitCode;

/// Zero-trust ETL for AI/RAG — chunk documents for vector databases.
#[derive(Parser, Debug)]
#[command(
    name = "bitvanes",
    version,
    about = "Zero-trust document chunking for RAG",
    long_about = "Processes documents into BPE-aware chunks with structural context.\n\
                  Output formats: Arrow IPC (.arrow), CSV (.csv), JSON (.json).\n\
                  Supports profile replay from the web app."
)]
pub struct Cli {
    /// Input file or directory (recursive scan).
    #[arg(short, long, value_name = "PATH")]
    pub input: Option<PathBuf>,

    /// Glob pattern(s) instead of --input (may be repeated). e.g. "docs/**/*.md".
    #[arg(long, value_name = "PATTERN")]
    pub glob: Vec<String>,

    /// Pipeline profile JSON (exported from the web app).
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Headless mode — no TUI, just process and output. (Default if --input/--glob is given.)
    #[arg(long)]
    pub no_tui: bool,

    /// Document format. Inferred from file extension if omitted.
    #[arg(short, long, value_name = "FORMAT")]
    pub format: Option<String>,

    /// BPE tokenizer for chunk boundary calculation.
    #[arg(short, long, value_name = "NAME")]
    pub tokenizer: Option<String>,

    /// Maximum tokens per chunk.
    #[arg(short = 'm', long, value_name = "N")]
    pub max_tokens: Option<u32>,

    /// PII patterns to scrub (comma-separated). E.g. "email,ssn,credit_card".
    #[arg(long, value_name = "PATTERNS")]
    pub scrub: Option<String>,

    /// Output file. Supports .arrow, .csv, .json extensions.
    /// Use "-" for stdout (JSON only).
    #[arg(short, long, value_name = "FILE")]
    pub output: Option<String>,

    /// Limit parallelism to N file-processing threads (default: all cores).
    #[arg(long, value_name = "N")]
    pub jobs: Option<usize>,

    /// Persist processed file hashes to this JSON manifest and skip files
    /// already present in it (idempotent / incremental runs).
    #[arg(long, value_name = "FILE")]
    pub manifest: Option<PathBuf>,

    /// Watch mode: after the initial pass, keep scanning for new/changed
    /// files and process them until interrupted (Ctrl-C). Implies a manifest
    /// in memory when --manifest is not given.
    #[arg(long)]
    pub watch: bool,

    /// Seconds between watch scans (default: 5).
    #[arg(long, value_name = "SECS", default_value_t = 5)]
    pub poll_interval: u64,

    /// Re-process files even if their hash is in the manifest.
    #[arg(long)]
    pub force: bool,

    // --- Embeddings (on-device; requires `--features embed` at build time) ---
    /// ONNX model file for on-device embeddings (enables the embedding column).
    #[cfg(feature = "embed")]
    #[arg(long, value_name = "MODEL.onnx", requires = "embed_tokenizer")]
    pub embed: Option<PathBuf>,

    /// Tokenizer.json paired with --embed.
    #[cfg(feature = "embed")]
    #[arg(long, value_name = "FILE")]
    pub embed_tokenizer: Option<PathBuf>,

    /// Embedding dimension of the model (default: 384, MiniLM-L6-v2).
    #[cfg(feature = "embed")]
    #[arg(long, value_name = "N", default_value_t = 384)]
    pub embed_dim: usize,

    /// Max sequence length for the embedder (default: 256).
    #[cfg(feature = "embed")]
    #[arg(long, value_name = "N", default_value_t = 256)]
    pub embed_max_len: usize,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    // Cap rayon parallelism if requested (must precede any rayon use).
    if let Some(n) = cli.jobs {
        if let Err(e) = rayon::ThreadPoolBuilder::new()
            .num_threads(n.max(1))
            .build_global()
        {
            eprintln!("Warning: could not set thread count to {n}: {e}");
        }
    }

    let headless = cli.input.is_some() || !cli.glob.is_empty() || cli.no_tui;

    if !headless {
        return match tui::run(&cli) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("Error: {e}");
                ExitCode::FAILURE
            }
        };
    }

    if cli.input.is_none() && cli.glob.is_empty() {
        // `--no-tui` with nothing to read: show help.
        let _ = Cli::command().print_help();
        println!();
        return ExitCode::SUCCESS;
    }

    match headless::run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}
