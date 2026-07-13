# bitvanes-cli

Terminal ETL tool for [BitVanes](https://bitvanes.com) — zero-trust document
chunking for RAG. Runs the same pipeline as the web app, natively. Features
both an interactive TUI (ratatui) and headless mode (CI/CD piping).

## Install

### From source

```bash
cargo install --git https://github.com/BitVanes/cli.git
```

### From release

Download the latest binary from
[GitHub Releases](https://github.com/BitVanes/cli/releases):

```bash
# Linux/macOS
curl -L https://github.com/BitVanes/cli/releases/latest/download/bitvanes-v0.1.0-x86_64-linux.tar.gz | tar xz
sudo mv bitvanes /usr/local/bin/

# Verify
bitvanes --version
```

## Usage

### Process a file or directory

```bash
# Single file → JSON output
bitvanes -i document.md -o chunks.json

# Directory (recursive) → Arrow IPC
bitvanes -i ./docs/ -f markdown -m 512 -o chunks.arrow

# Multiple PII patterns
bitvanes -i ./docs/ --scrub email,ssn,credit_card -o chunks.csv
```

### Interactive TUI mode

Run `bitvanes` with no arguments to launch the terminal UI:

```
bitvanes
```

Four screens (press `?` anywhere for full keybinds):
1. **File Browser** — navigate directories, select files with Space
2. **Config** — adjust format, tokenizer, max tokens, PII patterns
3. **Results** — scrollable chunk preview table with stats; save to a path
4. **Help** — full keybind reference

Keybinds:
- `↑↓` / `jk` — navigate / scroll
- `Enter` — open directory / toggle selection / process
- `Space` — select file
- `Tab` — cycle screens (Browser → Config → Results)
- `m` — cycle max tokens (128 → 256 → 512 → 1024)
- `t` — cycle tokenizer
- `e/s/a` — toggle email/ssn/aws PII scrubbing (Config screen)
- `s` — **save** chunks to the output path (Results screen)
- `e` — **edit** the output path; type a new name, `Enter` confirms, `Esc` cancels.
  Output format is inferred from the extension: `.json` (default), `.csv`, `.arrow`
- `b` — back to file browser
- `?` — toggle help
- `q` / `Esc` — quit

Processing runs on a background thread, so the UI stays responsive on large
files (a spinner shows progress). CLI flags (`--format`, `--max-tokens`,
`--tokenizer`, `--scrub`, `--config`) are honoured when launching the TUI.

### Ingestion at scale

The CLI is built for automated, team-scale ingestion:

```bash
# Glob instead of a single path
bitvanes --no-tui --glob "docs/**/*.md" -o chunks.json

# Idempotent incremental runs: skip files already in the manifest
bitvanes --no-tui -i ./ingest/ --manifest .bitvanes-manifest.json -o chunks.json

# Hot-folder daemon: keep watching for new/changed files and process them
bitvanes --no-tui -i ./ingest/ --watch --manifest .bitvanes-manifest.json --poll-interval 10

# Cap parallelism
bitvanes --no-tui -i ./docs/ --jobs 4 -o chunks.json
```

JSON output carries dedup keys for downstream pipelines: `source_hash`
(blake3 of the source file) and `chunk_hash` (blake3 of the chunk text).

### On-device embeddings (optional)

Build with the `embed` feature to enable `--embed`, which fills the Arrow
`embedding` column with real vectors (requires glibc ≥ 2.38 to link ONNX
Runtime, so it is off in the prebuilt release binaries):

```bash
cargo build --release --features embed
bitvanes --no-tui -i ./docs/ --embed model.onnx --embed-tokenizer tokenizer.json \
    --embed-dim 384 -o chunks.arrow
```

### Profile replay (from web app)

Export a profile from the BitVanes web app, then replay it identically:

```bash
bitvanes -c profile.json -i ./documents/ -o output.arrow
```

### Unix piping

```bash
cat doc.md | bitvanes -f markdown -m 256 -o - | python process.py
```

### All options

```
bitvanes [OPTIONS]

Input:
  -i, --input <PATH>         File or directory (recursive scan)
  -c, --config <FILE>        Profile JSON from web export
      --no-tui               Headless mode

Pipeline:
  -f, --format <FORMAT>      markdown | text | html | json | pdf (auto-detected)
  -t, --tokenizer <NAME>     cl100k_base | o200k_base | r50k_base | ...
  -m, --max-tokens <N>       Max tokens per chunk
      --scrub <PATTERNS>     Comma-separated PII patterns
      --min-confidence <F>   Min confidence [0.0–1.0] for scrubbing (default: 0.0)
      --anchor-window <N>    Contextual anchor half-window in words (default: 7)
      --toml <FILE>          bitvanes.toml config (layered below CLI flags)

Output:
  -o, --output <FILE>        .arrow | .csv | .json | - (stdout)
```

## Output formats

| Format | Extension | Use case |
|--------|-----------|----------|
| Arrow IPC | `.arrow` | DuckDB / Polars / LanceDB direct ingestion |
| CSV | `.csv` | Spreadsheet import, human inspection |
| JSON | `.json` | Python / Node RAG pipelines |

## Supported document formats

| Format | Extensions | Parser |
|--------|------------|--------|
| Markdown | `.md` | `pulldown-cmark` |
| Text | `.txt` | Paragraph-based splitting |
| HTML | `.html` | `scraper` (html5ever) |
| JSON | `.json` | Structural (one chunk per object/leaf) |
| PDF | `.pdf` | `pdf-extract` (native, text-layer only) |
| **DOCX** | `.docx` | ZIP + quick-xml (headings, tables, lists) |
| **PPTX** | `.pptx` | Slide-by-slide text extraction |
| **XLSX** | `.xlsx` | `calamine` (memory-guarded for large sheets) |
| **EPUB** | `.epub` | OPF spine + HtmlParser per chapter |
| **RTF** | `.rtf` | `rtf-parser` |

Format is auto-detected from file extension. Override with `--format`.

## Features

- **10 document formats**: Markdown, Text, HTML, JSON, PDF, DOCX, PPTX, XLSX, EPUB, RTF
- **8 PII patterns**: email, SSN, phone, credit card (Luhn), routing number
  (ABA), AWS keys, GitHub PATs, JWTs — with confidence scoring + anchor windows
- **6 OpenAI tokenizers**: cl100k_base, o200k_base, r50k_base, p50k_base,
  p50k_edit, o200k_harmony (all embedded at compile time)
- **Parallel processing**: rayon multi-core batch + intra-doc parallel regex
- **Memory-mapped I/O**: zero-copy reads for files > 1 MB
- **Streaming output**: Arrow IPC to stdout for pipe-friendly ETL
- **Profile replay**: byte-for-byte identical output to the web app

## Build from source

```bash
git clone https://github.com/BitVanes/cli.git
cd cli
cargo build --release
./target/release/bitvanes --help
```

The CLI depends on [`bitvanes-core`](https://github.com/BitVanes/core) via a
git dependency (tag `v0.4.0`, with the `ipc`, `csv`, `cli-pdf`, `office`,
`mmap`, and `parallel` features). No manual checkout of the core repo needed.

## License

MIT OR Apache-2.0
