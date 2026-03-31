# solana-shred-perf

Multi-endpoint Solana shred benchmark runner for same-host comparisons such as:

- `Jito proxy` vs one or more commercial `ShredStream` feeds
- multiple regional / provider streams forwarded into different local UDP ports
- long-running pressure tests with periodic CSV snapshots and JSON summaries

The program listens on multiple local UDP ports, groups packets by `ShredId`, waits for a short settle window, then computes:

- per-endpoint winner count (`first_arrivals`)
- per-endpoint lag versus the fastest endpoint for the same shred
- per-endpoint missed shred count within the settle window
- pairwise win count and latency gap for every endpoint combination

## Build

```bash
cargo build --release
```

## Configuration

You can configure endpoints in either of two ways:

1. Repeated CLI flags: `--endpoint name:port`
2. `.env` file: `BENCH_ENDPOINTS=name0:port0,name1:port1,...`

CLI arguments override `.env`.

### Supported `.env` keys

```dotenv
BENCH_ENDPOINTS=jito_proxy:20001,provider_a:20002,provider_b:20003
BENCH_RUN_ID=jito-vs-commercial
BENCH_OUTPUT_DIR=./benchmark-results
BENCH_STATS_INTERVAL_SECS=10
BENCH_SETTLE_MS=500
BENCH_RETENTION_SECS=60
```

You can also define endpoints one per line:

```dotenv
BENCH_ENDPOINT_1=jito_proxy:20001
BENCH_ENDPOINT_2=provider_a:20002
BENCH_ENDPOINT_3=provider_b:20003
```

`BENCH_SETTLE_MS` is the comparison window per shred. Any endpoint that has not delivered the same shred within that window is counted as `missed_shreds`.

## Run

### Option 1: direct CLI

```bash
RUST_LOG=info cargo run --release -- \
  --endpoint jito_proxy:20001 \
  --endpoint provider_a:20002 \
  --endpoint provider_b:20003 \
  --run-id jito-vs-commercial \
  --settle-ms 500 \
  --stats-interval-secs 10 \
  --retention-secs 60 \
  --output-dir ./benchmark-results
```

### Option 2: `.env`

Create `.env` from `.env.example`, then run:

```bash
RUST_LOG=info cargo run --release
```

## Output

Each run writes these files under `BENCH_OUTPUT_DIR` / `--output-dir`:

- `<run-id>-endpoint-snapshots.csv`
- `<run-id>-pair-snapshots.csv`
- `<run-id>-latest.json`
- `<run-id>-final.json`

### Endpoint CSV columns

- `udp_packets`
- `valid_packets`
- `invalid_packets`
- `unique_shreds`
- `duplicate_packets`
- `late_packets`
- `first_arrivals`
- `missed_shreds`
- `avg_lag_ms`
- `p50_lag_ms`
- `p90_lag_ms`
- `p99_lag_ms`
- `max_lag_ms`

### Pair CSV columns

- `samples`
- `left_wins`
- `right_wins`
- `ties`
- `avg_gap_ms`
- `p50_gap_ms`
- `p90_gap_ms`
- `p99_gap_ms`
- `max_gap_ms`

## Same-host comparison workflow

Typical deployment is:

1. Run `Jito proxy` locally and forward it to one UDP port.
2. Run each commercial `ShredStream` forwarder locally and bind each one to a different UDP port.
3. Start `solana-shred-perf` on the same machine with all local ports configured.
4. Collect the CSV snapshots for long-term trend analysis and use the JSON summary for automation / dashboards.

Example:

```dotenv
BENCH_ENDPOINTS=jito_proxy:20001,syndica:20002,triton:20003,helius:20004
BENCH_RUN_ID=shredstream-host-a
BENCH_OUTPUT_DIR=./benchmark-results
BENCH_STATS_INTERVAL_SECS=10
BENCH_SETTLE_MS=500
BENCH_RETENTION_SECS=60
```

## Notes

- Endpoint names must be unique.
- Ports must be unique.
- At least 2 endpoints are required.
- The tool benchmarks local receive timing only. Upstream proxy / relay setup is outside this repository.
