# AGENTS.md

Build commands for the `bitvanes-cli` crate.

## Quick verification

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo build --release
```

## Smoke test

```bash
echo '# Test\nHello world.' > /tmp/test.md
./target/release/bitvanes -i /tmp/test.md -f markdown -o /tmp/out.json
cat /tmp/out.json
```

## Architecture

Headless CLI that links `bitvanes-core` via git dependency (tag `v0.1.0`).

Entry point: `src/main.rs` (clap arg parsing → dispatch to headless or TUI).
Headless: `src/headless.rs` (directory scan → rayon parallel → output).
TUI: `src/tui/` (app state, event polling, ratatui rendering).

The four-stage pipeline runs inside `bitvanes-core`:
`parse → scrub → chunk → RecordBatch`.

Output formats: Arrow IPC, CSV, JSON (all from `bitvanes-core`'s `arrow_io`).

## Dependency on core

```toml
bitvanes-core = { git = "https://github.com/BitVanes/core.git", tag = "v0.1.1", features = ["ipc", "csv", "cli-pdf", "parallel"] }
```

After bumping the core version, update the tag here and in Cargo.toml.

## Release

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow (.github/workflows/release.yml) builds binaries for:
- x86_64 Linux
- aarch64 + x86_64 macOS
- x86_64 Windows

And creates a GitHub Release with download links + SHA256 checksums.

## Toolchain

- Rust stable (1.95+), pinned via `rust-toolchain.toml`.
- No wasm target needed — this is a native binary only.
