# Agent Guidelines for Apache DataFusion

This file provides guidance to CodeBuddy Code when working with code in this repository.

## Common Development Commands

```bash
# Build the entire workspace
cargo build

# Run a specific test
cargo test -p datafusion-optimizer -- test_name

# Run sqllogictests (recommended for SQL changes)
cargo test --profile=ci --test sqllogictests
cargo test --profile=ci --test sqllogictests -- aggregate.slt

# Run tests for core crates before PR
cargo test -p datafusion
cargo test -p datafusion-cli

# Alternative: use cargo-nextest for faster test execution
cargo nextest run -p datafusion

# Format code
cargo fmt --all

# Lint (must pass with no warnings)
cargo clippy --all-targets --all-features -- -D warnings

# Full lint suite (CI equivalent)
./dev/rust_lint.sh
# or auto-fix: ./dev/rust_lint.sh --write --allow-dirty

# Run doc tests
cargo test --doc

# Run benchmarks
cargo bench --bench parquet_query_sql

# Snapshot testing (when outputs change)
cargo insta review
```

## Build Profiles

Several Cargo profiles are available for different use cases:

```bash
# Faster builds during development (no LTO)
cargo build --profile release-nonlto

# Profiling with debug info preserved
cargo build --profile profiling

# CI profile (no debug info, smaller artifacts)
cargo test --profile=ci
```

## Architecture

DataFusion is an extensible query engine using Apache Arrow as its in-memory format. Key components:

- **SQL Parsing**: `datafusion-sql` (sqlparser)
- **Logical Planning**: `datafusion-optimizer` transforms SQL into logical plans
- **Physical Planning**: `datafusion-physical-plan` and `datafusion-physical-expr` convert to execution plans
- **Execution**: `datafusion-execution` runs queries in a columnar, streaming, vectorized manner
- **Data Sources**: `datafusion-datasource-*` (Parquet, CSV, JSON, Avro)

Key extension APIs for customization:
- `TableProvider` - custom data sources
- `OptimizerRule` - query optimization
- `ExecutionPlan` - custom physical operators

## Project Structure

```
datafusion/
├── core/          # Main DataFusion crate
├── sql/           # SQL parser
├── optimizer/     # Logical plan optimization
├── physical-* /   # Physical planning and execution
├── functions/     # Built-in functions (aggregate, window, scalar)
├── datasource-*/  # File format support (parquet, csv, json, avro)
├── expr/          # Expression representation
├── catalog/       # Catalog implementations
└── proto/         # Protobuf serialization
datafusion-cli/   # Command-line interface
datafusion-examples/  # Usage examples
```

## Before Committing

Before committing any changes, you MUST follow the instructions in
[Before Submitting a PR](docs/source/contributor-guide/index.md#before-submitting-a-pr)
and ensure the required checks listed there pass. Do not commit code that
fails any of those checks.

At a minimum, you MUST run and fix any errors from these commands before
committing:

```bash
# Format code
cargo fmt --all

# Lint (must pass with no warnings)
cargo clippy --all-targets --all-features -- -D warnings
```

You can also run the full lint suite used by CI:

```bash
./dev/rust_lint.sh
# or auto-fix: ./dev/rust_lint.sh --write --allow-dirty
```

When creating a PR, you MUST follow the [PR template](.github/pull_request_template.md).

## Testing

See the [Testing Quick Start](docs/source/contributor-guide/testing.md#testing-quick-start)
for the recommended pre-PR test commands.

## Prerequisites

- Rust 1.94.0 (pinned in rust-toolchain.toml)
- protoc 3.15+ (for protobuf compilation)
- Git submodules initialized: `git submodule update --init --recursive`

## Workspace Structure

This repository contains two related projects:

1. **DataFusion** (`datafusion/`) - The core query engine
2. **Ballista** (`datafusion-ballista/`) - Distributed query execution engine built on DataFusion

Ballista patches crates-io to use the local DataFusion version, so changes to DataFusion may affect Ballista.
