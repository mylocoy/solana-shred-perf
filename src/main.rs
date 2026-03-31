use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use clap::Parser;
use csv::WriterBuilder;
use hdrhistogram::Histogram;
use log::{error, info};
use serde::Serialize;
use solana_ledger::shred::{Shred, ShredId};
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, MissedTickBehavior};

const DEFAULT_STATS_INTERVAL_SECS: u64 = 10;
const DEFAULT_SETTLE_MS: u64 = 500;
const DEFAULT_RETENTION_SECS: u64 = 60;
const DEFAULT_OUTPUT_DIR: &str = "benchmark-results";
const DEFAULT_ENV_FILE: &str = ".env";

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Multi-endpoint Solana shred benchmark runner"
)]
struct Args {
    #[arg(long = "endpoint", value_name = "NAME:PORT")]
    endpoints: Vec<String>,
    #[arg(long, default_value = DEFAULT_ENV_FILE)]
    env_file: PathBuf,
    #[arg(long)]
    output_dir: Option<PathBuf>,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long)]
    stats_interval_secs: Option<u64>,
    #[arg(long)]
    settle_ms: Option<u64>,
    #[arg(long)]
    retention_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct EndpointConfig {
    id: usize,
    name: String,
    port: u16,
}

#[derive(Clone, Debug, Serialize)]
struct RuntimeConfig {
    run_id: String,
    output_dir: PathBuf,
    endpoints: Vec<EndpointConfig>,
    stats_interval_secs: u64,
    settle_ms: u64,
    retention_secs: u64,
}

impl RuntimeConfig {
    fn from_args(args: Args) -> Result<Self> {
        load_env_file_if_present(&args.env_file)?;

        let endpoints = if args.endpoints.is_empty() {
            load_endpoints_from_env()?
        } else {
            parse_endpoints(&args.endpoints)?
        };

        if endpoints.len() < 2 {
            bail!("at least 2 endpoints are required");
        }

        validate_endpoint_uniqueness(&endpoints)?;

        let stats_interval_secs = args
            .stats_interval_secs
            .or_else(|| read_env_u64("BENCH_STATS_INTERVAL_SECS"))
            .unwrap_or(DEFAULT_STATS_INTERVAL_SECS);
        let settle_ms = args
            .settle_ms
            .or_else(|| read_env_u64("BENCH_SETTLE_MS"))
            .unwrap_or(DEFAULT_SETTLE_MS);
        let retention_secs = args
            .retention_secs
            .or_else(|| read_env_u64("BENCH_RETENTION_SECS"))
            .unwrap_or(DEFAULT_RETENTION_SECS);

        if stats_interval_secs == 0 {
            bail!("stats interval must be greater than 0 seconds");
        }
        if settle_ms == 0 {
            bail!("settle window must be greater than 0 milliseconds");
        }
        if retention_secs == 0 {
            bail!("retention must be greater than 0 seconds");
        }

        let run_id = args
            .run_id
            .or_else(|| read_env_string("BENCH_RUN_ID"))
            .unwrap_or_else(default_run_id);
        let output_dir = args
            .output_dir
            .or_else(|| read_env_string("BENCH_OUTPUT_DIR").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_DIR));

        Ok(Self {
            run_id,
            output_dir,
            endpoints,
            stats_interval_secs,
            settle_ms,
            retention_secs,
        })
    }

    fn stats_interval(&self) -> Duration {
        Duration::from_secs(self.stats_interval_secs)
    }

    fn settle_window(&self) -> Duration {
        Duration::from_millis(self.settle_ms)
    }

    fn retention_window(&self) -> Duration {
        Duration::from_secs(self.retention_secs)
    }

    fn file_safe_run_id(&self) -> String {
        sanitize_for_filename(&self.run_id)
    }
}

#[derive(Debug)]
enum ProcessorEvent {
    ValidPacket {
        endpoint_id: usize,
        shred_id: ShredId,
        received_at: Instant,
    },
    InvalidPacket {
        endpoint_id: usize,
    },
    MaintenanceTick,
    StatsTick,
}

#[derive(Clone, Copy, Debug)]
struct ArrivalRecord {
    received_at: Instant,
}

#[derive(Debug)]
struct ShredObservation {
    earliest_received_at: Instant,
    latest_received_at: Instant,
    arrivals: BTreeMap<usize, ArrivalRecord>,
}

#[derive(Debug)]
struct EndpointStats {
    udp_packets: u64,
    valid_packets: u64,
    invalid_packets: u64,
    unique_shreds: u64,
    duplicate_packets: u64,
    late_packets: u64,
    first_arrivals: u64,
    missed_shreds: u64,
    lag_samples: u64,
    total_lag_micros: u128,
    max_lag_micros: u64,
    lag_histogram: Histogram<u64>,
}

impl EndpointStats {
    fn new(max_trackable_micros: u64) -> Result<Self> {
        Ok(Self {
            udp_packets: 0,
            valid_packets: 0,
            invalid_packets: 0,
            unique_shreds: 0,
            duplicate_packets: 0,
            late_packets: 0,
            first_arrivals: 0,
            missed_shreds: 0,
            lag_samples: 0,
            total_lag_micros: 0,
            max_lag_micros: 0,
            lag_histogram: new_histogram(max_trackable_micros)?,
        })
    }

    fn record_lag(&mut self, lag: Duration) -> Result<()> {
        let micros = duration_to_micros(lag);
        self.lag_samples += 1;
        self.total_lag_micros += micros as u128;
        self.max_lag_micros = self.max_lag_micros.max(micros);
        self.lag_histogram
            .record(encode_histogram_value(micros))
            .context("record endpoint lag")?;
        Ok(())
    }
}

#[derive(Debug)]
struct PairStats {
    samples: u64,
    left_wins: u64,
    right_wins: u64,
    ties: u64,
    total_gap_micros: u128,
    max_gap_micros: u64,
    gap_histogram: Histogram<u64>,
}

impl PairStats {
    fn new(max_trackable_micros: u64) -> Result<Self> {
        Ok(Self {
            samples: 0,
            left_wins: 0,
            right_wins: 0,
            ties: 0,
            total_gap_micros: 0,
            max_gap_micros: 0,
            gap_histogram: new_histogram(max_trackable_micros)?,
        })
    }

    fn record_gap(&mut self, left_arrival: Instant, right_arrival: Instant) -> Result<()> {
        let gap = abs_duration(left_arrival, right_arrival);
        let micros = duration_to_micros(gap);

        self.samples += 1;
        self.total_gap_micros += micros as u128;
        self.max_gap_micros = self.max_gap_micros.max(micros);
        self.gap_histogram
            .record(encode_histogram_value(micros))
            .context("record pair gap")?;

        if left_arrival < right_arrival {
            self.left_wins += 1;
        } else if right_arrival < left_arrival {
            self.right_wins += 1;
        } else {
            self.ties += 1;
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct EndpointReport {
    endpoint: String,
    port: u16,
    udp_packets: u64,
    valid_packets: u64,
    invalid_packets: u64,
    unique_shreds: u64,
    duplicate_packets: u64,
    late_packets: u64,
    first_arrivals: u64,
    missed_shreds: u64,
    lag_samples: u64,
    avg_lag_ms: f64,
    p50_lag_ms: f64,
    p90_lag_ms: f64,
    p99_lag_ms: f64,
    max_lag_ms: f64,
}

#[derive(Debug, Serialize)]
struct PairReport {
    left_endpoint: String,
    left_port: u16,
    right_endpoint: String,
    right_port: u16,
    samples: u64,
    left_wins: u64,
    right_wins: u64,
    ties: u64,
    avg_gap_ms: f64,
    p50_gap_ms: f64,
    p90_gap_ms: f64,
    p99_gap_ms: f64,
    max_gap_ms: f64,
}

#[derive(Debug, Serialize)]
struct RunReport {
    phase: String,
    generated_at: String,
    run_id: String,
    uptime_secs: u64,
    finalized_shreds: u64,
    inflight_shreds: usize,
    recent_shred_cache_size: usize,
    config: RuntimeConfig,
    endpoints: Vec<EndpointReport>,
    pairs: Vec<PairReport>,
}

#[derive(Debug, Serialize)]
struct EndpointSnapshotRow {
    phase: String,
    generated_at: String,
    run_id: String,
    uptime_secs: u64,
    finalized_shreds: u64,
    inflight_shreds: usize,
    endpoint: String,
    port: u16,
    udp_packets: u64,
    valid_packets: u64,
    invalid_packets: u64,
    unique_shreds: u64,
    duplicate_packets: u64,
    late_packets: u64,
    first_arrivals: u64,
    missed_shreds: u64,
    lag_samples: u64,
    avg_lag_ms: f64,
    p50_lag_ms: f64,
    p90_lag_ms: f64,
    p99_lag_ms: f64,
    max_lag_ms: f64,
}

#[derive(Debug, Serialize)]
struct PairSnapshotRow {
    phase: String,
    generated_at: String,
    run_id: String,
    uptime_secs: u64,
    finalized_shreds: u64,
    inflight_shreds: usize,
    left_endpoint: String,
    left_port: u16,
    right_endpoint: String,
    right_port: u16,
    samples: u64,
    left_wins: u64,
    right_wins: u64,
    ties: u64,
    avg_gap_ms: f64,
    p50_gap_ms: f64,
    p90_gap_ms: f64,
    p99_gap_ms: f64,
    max_gap_ms: f64,
}

struct ReportWriter {
    endpoint_csv_path: PathBuf,
    pair_csv_path: PathBuf,
    latest_json_path: PathBuf,
    final_json_path: PathBuf,
    endpoint_writer: csv::Writer<File>,
    pair_writer: csv::Writer<File>,
}

impl ReportWriter {
    fn new(config: &RuntimeConfig) -> Result<Self> {
        fs::create_dir_all(&config.output_dir)
            .with_context(|| format!("create output dir {}", config.output_dir.display()))?;

        let run_id = config.file_safe_run_id();
        let endpoint_csv_path = config
            .output_dir
            .join(format!("{run_id}-endpoint-snapshots.csv"));
        let pair_csv_path = config
            .output_dir
            .join(format!("{run_id}-pair-snapshots.csv"));
        let latest_json_path = config.output_dir.join(format!("{run_id}-latest.json"));
        let final_json_path = config.output_dir.join(format!("{run_id}-final.json"));

        let endpoint_writer = open_csv_writer(&endpoint_csv_path)?;
        let pair_writer = open_csv_writer(&pair_csv_path)?;

        Ok(Self {
            endpoint_csv_path,
            pair_csv_path,
            latest_json_path,
            final_json_path,
            endpoint_writer,
            pair_writer,
        })
    }

    fn write_snapshot(&mut self, report: &RunReport) -> Result<()> {
        for endpoint in &report.endpoints {
            self.endpoint_writer
                .serialize(EndpointSnapshotRow {
                    phase: report.phase.clone(),
                    generated_at: report.generated_at.clone(),
                    run_id: report.run_id.clone(),
                    uptime_secs: report.uptime_secs,
                    finalized_shreds: report.finalized_shreds,
                    inflight_shreds: report.inflight_shreds,
                    endpoint: endpoint.endpoint.clone(),
                    port: endpoint.port,
                    udp_packets: endpoint.udp_packets,
                    valid_packets: endpoint.valid_packets,
                    invalid_packets: endpoint.invalid_packets,
                    unique_shreds: endpoint.unique_shreds,
                    duplicate_packets: endpoint.duplicate_packets,
                    late_packets: endpoint.late_packets,
                    first_arrivals: endpoint.first_arrivals,
                    missed_shreds: endpoint.missed_shreds,
                    lag_samples: endpoint.lag_samples,
                    avg_lag_ms: endpoint.avg_lag_ms,
                    p50_lag_ms: endpoint.p50_lag_ms,
                    p90_lag_ms: endpoint.p90_lag_ms,
                    p99_lag_ms: endpoint.p99_lag_ms,
                    max_lag_ms: endpoint.max_lag_ms,
                })
                .context("write endpoint snapshot row")?;
        }

        for pair in &report.pairs {
            self.pair_writer
                .serialize(PairSnapshotRow {
                    phase: report.phase.clone(),
                    generated_at: report.generated_at.clone(),
                    run_id: report.run_id.clone(),
                    uptime_secs: report.uptime_secs,
                    finalized_shreds: report.finalized_shreds,
                    inflight_shreds: report.inflight_shreds,
                    left_endpoint: pair.left_endpoint.clone(),
                    left_port: pair.left_port,
                    right_endpoint: pair.right_endpoint.clone(),
                    right_port: pair.right_port,
                    samples: pair.samples,
                    left_wins: pair.left_wins,
                    right_wins: pair.right_wins,
                    ties: pair.ties,
                    avg_gap_ms: pair.avg_gap_ms,
                    p50_gap_ms: pair.p50_gap_ms,
                    p90_gap_ms: pair.p90_gap_ms,
                    p99_gap_ms: pair.p99_gap_ms,
                    max_gap_ms: pair.max_gap_ms,
                })
                .context("write pair snapshot row")?;
        }

        self.endpoint_writer.flush().context("flush endpoint csv")?;
        self.pair_writer.flush().context("flush pair csv")?;
        write_json_file(&self.latest_json_path, report)?;

        Ok(())
    }

    fn write_final(&mut self, report: &RunReport) -> Result<()> {
        self.write_snapshot(report)?;
        write_json_file(&self.final_json_path, report)?;
        Ok(())
    }
}

struct BenchmarkProcessor {
    config: RuntimeConfig,
    report_writer: ReportWriter,
    started_at: Instant,
    inflight_shreds: HashMap<ShredId, ShredObservation>,
    finalized_recent_shreds: HashMap<ShredId, Instant>,
    endpoint_stats: Vec<EndpointStats>,
    pair_stats: BTreeMap<(usize, usize), PairStats>,
    finalized_shreds: u64,
}

impl BenchmarkProcessor {
    fn new(config: RuntimeConfig) -> Result<Self> {
        let max_trackable_micros = config
            .settle_window()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX.saturating_sub(1))
            .max(1_000_000)
            .saturating_add(1_000_000);

        let mut endpoint_stats = Vec::with_capacity(config.endpoints.len());
        for _ in &config.endpoints {
            endpoint_stats.push(EndpointStats::new(max_trackable_micros)?);
        }

        let mut pair_stats = BTreeMap::new();
        for left in 0..config.endpoints.len() {
            for right in (left + 1)..config.endpoints.len() {
                pair_stats.insert((left, right), PairStats::new(max_trackable_micros)?);
            }
        }

        let report_writer = ReportWriter::new(&config)?;

        Ok(Self {
            config,
            report_writer,
            started_at: Instant::now(),
            inflight_shreds: HashMap::new(),
            finalized_recent_shreds: HashMap::new(),
            endpoint_stats,
            pair_stats,
            finalized_shreds: 0,
        })
    }

    fn handle_valid_packet(
        &mut self,
        endpoint_id: usize,
        shred_id: ShredId,
        received_at: Instant,
    ) -> Result<()> {
        let endpoint_stats = self
            .endpoint_stats
            .get_mut(endpoint_id)
            .ok_or_else(|| anyhow!("unknown endpoint id {endpoint_id}"))?;
        endpoint_stats.udp_packets += 1;
        endpoint_stats.valid_packets += 1;

        if self.finalized_recent_shreds.contains_key(&shred_id) {
            endpoint_stats.late_packets += 1;
            return Ok(());
        }

        let observation =
            self.inflight_shreds
                .entry(shred_id)
                .or_insert_with(|| ShredObservation {
                    earliest_received_at: received_at,
                    latest_received_at: received_at,
                    arrivals: BTreeMap::new(),
                });

        if observation.arrivals.contains_key(&endpoint_id) {
            endpoint_stats.duplicate_packets += 1;
            observation.latest_received_at = observation.latest_received_at.max(received_at);
            return Ok(());
        }

        endpoint_stats.unique_shreds += 1;
        observation
            .arrivals
            .insert(endpoint_id, ArrivalRecord { received_at });
        observation.earliest_received_at = observation.earliest_received_at.min(received_at);
        observation.latest_received_at = observation.latest_received_at.max(received_at);

        Ok(())
    }

    fn handle_invalid_packet(&mut self, endpoint_id: usize) -> Result<()> {
        let endpoint_stats = self
            .endpoint_stats
            .get_mut(endpoint_id)
            .ok_or_else(|| anyhow!("unknown endpoint id {endpoint_id}"))?;
        endpoint_stats.udp_packets += 1;
        endpoint_stats.invalid_packets += 1;
        Ok(())
    }

    fn finalize_due_shreds(&mut self, now: Instant, force_all: bool) -> Result<()> {
        let mut due_ids = Vec::new();
        for (shred_id, observation) in &self.inflight_shreds {
            let ready = force_all
                || now.duration_since(observation.earliest_received_at)
                    >= self.config.settle_window();
            if ready {
                due_ids.push(shred_id.clone());
            }
        }

        for shred_id in due_ids {
            if let Some(observation) = self.inflight_shreds.remove(&shred_id) {
                self.finalize_observation(shred_id, observation, now)?;
            }
        }

        Ok(())
    }

    fn finalize_observation(
        &mut self,
        shred_id: ShredId,
        observation: ShredObservation,
        finalized_at: Instant,
    ) -> Result<()> {
        if observation.arrivals.is_empty() {
            return Ok(());
        }

        let mut arrivals: Vec<(usize, ArrivalRecord)> = observation
            .arrivals
            .iter()
            .map(|(endpoint_id, arrival)| (*endpoint_id, *arrival))
            .collect();
        arrivals.sort_by_key(|(endpoint_id, arrival)| (arrival.received_at, *endpoint_id));

        let winner_endpoint_id = arrivals[0].0;
        self.endpoint_stats[winner_endpoint_id].first_arrivals += 1;

        let winner_arrival = arrivals[0].1.received_at;
        for endpoint_id in 0..self.config.endpoints.len() {
            match observation.arrivals.get(&endpoint_id) {
                Some(arrival) => self.endpoint_stats[endpoint_id]
                    .record_lag(abs_duration(arrival.received_at, winner_arrival))?,
                None => self.endpoint_stats[endpoint_id].missed_shreds += 1,
            }
        }

        for left_index in 0..arrivals.len() {
            for right_index in (left_index + 1)..arrivals.len() {
                let (left_id, left_arrival) = arrivals[left_index];
                let (right_id, right_arrival) = arrivals[right_index];
                let key = if left_id < right_id {
                    (left_id, right_id)
                } else {
                    (right_id, left_id)
                };
                let pair_stats = self
                    .pair_stats
                    .get_mut(&key)
                    .ok_or_else(|| anyhow!("missing pair stats for {key:?}"))?;

                if key == (left_id, right_id) {
                    pair_stats.record_gap(left_arrival.received_at, right_arrival.received_at)?;
                } else {
                    pair_stats.record_gap(right_arrival.received_at, left_arrival.received_at)?;
                }
            }
        }

        self.finalized_shreds += 1;
        self.finalized_recent_shreds.insert(shred_id, finalized_at);

        Ok(())
    }

    fn cleanup_recent_shreds(&mut self, now: Instant) {
        let retention = self.config.retention_window();
        self.finalized_recent_shreds
            .retain(|_, finalized_at| now.duration_since(*finalized_at) < retention);
    }

    fn build_report(&self, phase: &str) -> RunReport {
        let generated_at = Utc::now().to_rfc3339();
        let uptime_secs = self.started_at.elapsed().as_secs();

        let endpoints = self
            .config
            .endpoints
            .iter()
            .enumerate()
            .map(|(index, endpoint)| {
                let stats = &self.endpoint_stats[index];
                EndpointReport {
                    endpoint: endpoint.name.clone(),
                    port: endpoint.port,
                    udp_packets: stats.udp_packets,
                    valid_packets: stats.valid_packets,
                    invalid_packets: stats.invalid_packets,
                    unique_shreds: stats.unique_shreds,
                    duplicate_packets: stats.duplicate_packets,
                    late_packets: stats.late_packets,
                    first_arrivals: stats.first_arrivals,
                    missed_shreds: stats.missed_shreds,
                    lag_samples: stats.lag_samples,
                    avg_lag_ms: average_ms(stats.total_lag_micros, stats.lag_samples),
                    p50_lag_ms: histogram_quantile_ms(&stats.lag_histogram, 0.50),
                    p90_lag_ms: histogram_quantile_ms(&stats.lag_histogram, 0.90),
                    p99_lag_ms: histogram_quantile_ms(&stats.lag_histogram, 0.99),
                    max_lag_ms: micros_to_ms(stats.max_lag_micros),
                }
            })
            .collect();

        let pairs = self
            .pair_stats
            .iter()
            .map(|(&(left_id, right_id), stats)| {
                let left = &self.config.endpoints[left_id];
                let right = &self.config.endpoints[right_id];
                PairReport {
                    left_endpoint: left.name.clone(),
                    left_port: left.port,
                    right_endpoint: right.name.clone(),
                    right_port: right.port,
                    samples: stats.samples,
                    left_wins: stats.left_wins,
                    right_wins: stats.right_wins,
                    ties: stats.ties,
                    avg_gap_ms: average_ms(stats.total_gap_micros, stats.samples),
                    p50_gap_ms: histogram_quantile_ms(&stats.gap_histogram, 0.50),
                    p90_gap_ms: histogram_quantile_ms(&stats.gap_histogram, 0.90),
                    p99_gap_ms: histogram_quantile_ms(&stats.gap_histogram, 0.99),
                    max_gap_ms: micros_to_ms(stats.max_gap_micros),
                }
            })
            .collect();

        RunReport {
            phase: phase.to_string(),
            generated_at,
            run_id: self.config.run_id.clone(),
            uptime_secs,
            finalized_shreds: self.finalized_shreds,
            inflight_shreds: self.inflight_shreds.len(),
            recent_shred_cache_size: self.finalized_recent_shreds.len(),
            config: self.config.clone(),
            endpoints,
            pairs,
        }
    }

    fn emit_snapshot(&mut self, phase: &str) -> Result<()> {
        let report = self.build_report(phase);
        self.report_writer.write_snapshot(&report)?;
        log_report(&report);
        Ok(())
    }

    fn emit_final(&mut self) -> Result<()> {
        let report = self.build_report("final");
        self.report_writer.write_final(&report)?;
        log_report(&report);
        info!(
            "Wrote endpoint CSV: {} | pair CSV: {} | latest JSON: {} | final JSON: {}",
            self.report_writer.endpoint_csv_path.display(),
            self.report_writer.pair_csv_path.display(),
            self.report_writer.latest_json_path.display(),
            self.report_writer.final_json_path.display(),
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let args = Args::parse();
    let config = RuntimeConfig::from_args(args)?;

    info!(
        "run_id={} endpoints={} stats_interval={}s settle={}ms retention={}s output_dir={}",
        config.run_id,
        config
            .endpoints
            .iter()
            .map(|endpoint| format!("{}:{}", endpoint.name, endpoint.port))
            .collect::<Vec<_>>()
            .join(","),
        config.stats_interval_secs,
        config.settle_ms,
        config.retention_secs,
        config.output_dir.display()
    );

    let (processor_tx, processor_rx) = mpsc::channel(8192);
    let (shutdown_tx, _) = broadcast::channel(1);

    let mut listener_tasks = Vec::with_capacity(config.endpoints.len());
    for endpoint in config.endpoints.clone() {
        listener_tasks.push(start_port_listener(
            endpoint,
            processor_tx.clone(),
            shutdown_tx.subscribe(),
        ));
    }

    let timer_task = start_timer(
        processor_tx.clone(),
        config.stats_interval(),
        maintenance_interval(config.settle_ms),
        shutdown_tx.subscribe(),
    );

    let processor_task = tokio::spawn(run_processor(config, processor_rx));

    tokio::signal::ctrl_c()
        .await
        .context("wait for ctrl-c shutdown signal")?;
    info!("Ctrl-C received, shutting down");

    let _ = shutdown_tx.send(());
    drop(processor_tx);

    for task in listener_tasks {
        if let Err(join_error) = task.await {
            error!("listener task join error: {}", join_error);
        }
    }

    if let Err(join_error) = timer_task.await {
        error!("timer task join error: {}", join_error);
    }

    processor_task
        .await
        .context("join processor task")?
        .context("run processor")?;

    Ok(())
}

fn start_port_listener(
    endpoint: EndpointConfig,
    sender: mpsc::Sender<ProcessorEvent>,
    mut shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let bind_addr = format!("0.0.0.0:{}", endpoint.port);
        let socket = match UdpSocket::bind(&bind_addr).await {
            Ok(socket) => socket,
            Err(error) => {
                error!(
                    "[{}] failed to bind UDP listener on {}: {}",
                    endpoint.name, bind_addr, error
                );
                return;
            }
        };

        info!("[{}] listening on {}", endpoint.name, bind_addr);

        let mut buffer = [0u8; 4096];
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("[{}] listener stopped", endpoint.name);
                    break;
                }
                recv = socket.recv_from(&mut buffer) => {
                    match recv {
                        Ok((size, _peer)) => {
                            let received_at = Instant::now();
                            let packet = buffer[..size].to_vec();
                            let event = match Shred::new_from_serialized_shred(packet) {
                                Ok(shred) => ProcessorEvent::ValidPacket {
                                    endpoint_id: endpoint.id,
                                    shred_id: shred.id(),
                                    received_at,
                                },
                                Err(_) => ProcessorEvent::InvalidPacket {
                                    endpoint_id: endpoint.id,
                                },
                            };

                            if sender.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(error) => error!("[{}] UDP receive error: {}", endpoint.name, error),
                    }
                }
            }
        }
    })
}

fn start_timer(
    sender: mpsc::Sender<ProcessorEvent>,
    stats_interval: Duration,
    maintenance_interval: Duration,
    mut shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stats_tick = time::interval(stats_interval);
        stats_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        stats_tick.tick().await;

        let mut maintenance_tick = time::interval(maintenance_interval);
        maintenance_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        maintenance_tick.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                _ = maintenance_tick.tick() => {
                    if sender.send(ProcessorEvent::MaintenanceTick).await.is_err() {
                        break;
                    }
                }
                _ = stats_tick.tick() => {
                    if sender.send(ProcessorEvent::StatsTick).await.is_err() {
                        break;
                    }
                }
            }
        }
    })
}

async fn run_processor(
    config: RuntimeConfig,
    mut receiver: mpsc::Receiver<ProcessorEvent>,
) -> Result<()> {
    let mut processor = BenchmarkProcessor::new(config)?;

    while let Some(event) = receiver.recv().await {
        match event {
            ProcessorEvent::ValidPacket {
                endpoint_id,
                shred_id,
                received_at,
            } => processor.handle_valid_packet(endpoint_id, shred_id, received_at)?,
            ProcessorEvent::InvalidPacket { endpoint_id } => {
                processor.handle_invalid_packet(endpoint_id)?
            }
            ProcessorEvent::MaintenanceTick => {
                let now = Instant::now();
                processor.finalize_due_shreds(now, false)?;
                processor.cleanup_recent_shreds(now);
            }
            ProcessorEvent::StatsTick => {
                let now = Instant::now();
                processor.finalize_due_shreds(now, false)?;
                processor.cleanup_recent_shreds(now);
                processor.emit_snapshot("snapshot")?;
            }
        }
    }

    let now = Instant::now();
    processor.finalize_due_shreds(now, true)?;
    processor.cleanup_recent_shreds(now);
    processor.emit_final()?;

    Ok(())
}

fn load_env_file_if_present(path: &Path) -> Result<()> {
    if path.exists() {
        dotenvy::from_path(path).with_context(|| format!("load env file {}", path.display()))?;
        info!("Loaded environment from {}", path.display());
    }
    Ok(())
}

fn load_endpoints_from_env() -> Result<Vec<EndpointConfig>> {
    if let Some(raw) = read_env_string("BENCH_ENDPOINTS") {
        let values: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        return parse_endpoints(&values);
    }

    let mut indexed = std::env::vars()
        .filter_map(|(key, value)| {
            let suffix = key.strip_prefix("BENCH_ENDPOINT_")?;
            let index = suffix.parse::<usize>().ok()?;
            Some((index, value))
        })
        .collect::<Vec<_>>();
    indexed.sort_by_key(|(index, _)| *index);

    if indexed.is_empty() {
        bail!(
            "no endpoints configured. Use repeated --endpoint NAME:PORT or set BENCH_ENDPOINTS / BENCH_ENDPOINT_1..N in .env"
        );
    }

    let values = indexed
        .into_iter()
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    parse_endpoints(&values)
}

fn parse_endpoints(values: &[String]) -> Result<Vec<EndpointConfig>> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| parse_endpoint(index, value))
        .collect()
}

fn parse_endpoint(index: usize, value: &str) -> Result<EndpointConfig> {
    let (name, port_str) = value
        .trim()
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("invalid endpoint '{value}', expected NAME:PORT"))?;

    if name.trim().is_empty() {
        bail!("endpoint name cannot be empty in '{value}'");
    }

    let port = port_str
        .trim()
        .parse::<u16>()
        .with_context(|| format!("invalid port in endpoint '{value}'"))?;

    Ok(EndpointConfig {
        id: index,
        name: name.trim().to_string(),
        port,
    })
}

fn validate_endpoint_uniqueness(endpoints: &[EndpointConfig]) -> Result<()> {
    let mut seen_names = HashMap::new();
    let mut seen_ports = HashMap::new();

    for endpoint in endpoints {
        if let Some(existing) = seen_names.insert(endpoint.name.clone(), endpoint.port) {
            bail!(
                "duplicate endpoint name '{}' configured on ports {} and {}",
                endpoint.name,
                existing,
                endpoint.port
            );
        }
        if let Some(existing_name) = seen_ports.insert(endpoint.port, endpoint.name.clone()) {
            bail!(
                "duplicate endpoint port '{}' configured for '{}' and '{}'",
                endpoint.port,
                existing_name,
                endpoint.name
            );
        }
    }

    Ok(())
}

fn read_env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_env_u64(key: &str) -> Option<u64> {
    read_env_string(key)?.parse().ok()
}

fn default_run_id() -> String {
    format!("bench-{}", Utc::now().format("%Y%m%dT%H%M%SZ"))
}

fn sanitize_for_filename(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();

    if sanitized.is_empty() {
        "benchmark".to_string()
    } else {
        sanitized
    }
}

fn maintenance_interval(settle_ms: u64) -> Duration {
    let interval_ms = ((settle_ms + 1) / 2).clamp(50, 1_000);
    Duration::from_millis(interval_ms)
}

fn open_csv_writer(path: &Path) -> Result<csv::Writer<File>> {
    let already_exists = path.exists();
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open csv {}", path.display()))?;
    let should_write_headers = !already_exists || file.metadata()?.len() == 0;

    Ok(WriterBuilder::new()
        .has_headers(should_write_headers)
        .from_writer(file))
}

fn write_json_file(path: &Path, report: &RunReport) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create json {}", path.display()))?;
    serde_json::to_writer_pretty(file, report)
        .with_context(|| format!("write json {}", path.display()))?;
    Ok(())
}

fn new_histogram(max_trackable_micros: u64) -> Result<Histogram<u64>> {
    Histogram::new_with_bounds(1, max_trackable_micros.saturating_add(1), 3)
        .context("create histogram")
}

fn duration_to_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

fn abs_duration(left: Instant, right: Instant) -> Duration {
    if left >= right {
        left.duration_since(right)
    } else {
        right.duration_since(left)
    }
}

fn encode_histogram_value(micros: u64) -> u64 {
    micros.saturating_add(1)
}

fn decode_histogram_value(encoded: u64) -> u64 {
    encoded.saturating_sub(1)
}

fn average_ms(total_micros: u128, samples: u64) -> f64 {
    if samples == 0 {
        0.0
    } else {
        (total_micros as f64 / samples as f64) / 1_000.0
    }
}

fn histogram_quantile_ms(histogram: &Histogram<u64>, quantile: f64) -> f64 {
    if histogram.is_empty() {
        0.0
    } else {
        micros_to_ms(decode_histogram_value(
            histogram.value_at_quantile(quantile),
        ))
    }
}

fn micros_to_ms(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

fn log_report(report: &RunReport) {
    info!(
        "[{}] run_id={} uptime={}s finalized={} inflight={} recent_cache={}",
        report.phase,
        report.run_id,
        report.uptime_secs,
        report.finalized_shreds,
        report.inflight_shreds,
        report.recent_shred_cache_size,
    );

    for endpoint in &report.endpoints {
        info!(
            "[{}] endpoint={} port={} valid={} unique={} first={} missed={} dup={} late={} avg_lag_ms={:.3} p99_lag_ms={:.3}",
            report.phase,
            endpoint.endpoint,
            endpoint.port,
            endpoint.valid_packets,
            endpoint.unique_shreds,
            endpoint.first_arrivals,
            endpoint.missed_shreds,
            endpoint.duplicate_packets,
            endpoint.late_packets,
            endpoint.avg_lag_ms,
            endpoint.p99_lag_ms,
        );
    }

    for pair in &report.pairs {
        if pair.samples == 0 {
            continue;
        }
        info!(
            "[{}] pair={} vs {} samples={} wins={}/{} ties={} avg_gap_ms={:.3} p99_gap_ms={:.3}",
            report.phase,
            pair.left_endpoint,
            pair.right_endpoint,
            pair.samples,
            pair.left_wins,
            pair.right_wins,
            pair.ties,
            pair.avg_gap_ms,
            pair.p99_gap_ms,
        );
    }
}
