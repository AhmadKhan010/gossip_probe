//! gossip-probe: joins Solana gossip as an unstaked node for a fixed time and reports how much gossip
//! data arrived, what it cost (CPU, memory, network), and how much of it was leader contact-info
//! changes: the only part the sidecar needs.
//!
//! Sources:
//! - leader identities: one RPC call at startup (`schedule`);
//! - accepted contact info: gossip's contact-info hook (`contacts`);
//! - gossip's own per-type counters: its metrics datapoints, caught from the log (`datapoints`);
//! - cost: `/proc` and optional nftables counters (`proc_stats`).

mod contacts;
mod datapoints;
mod proc_stats;
mod schedule;

use {
    anyhow::{Context, Result, anyhow, bail},
    clap::{Parser, ValueEnum},
    proc_stats::{Bytes, CpuSample},
    solana_gossip::gossip_service::make_node,
    solana_keypair::Keypair,
    solana_net_utils::SocketAddrSpace,
    std::{
        fs::File,
        io::{BufWriter, Write},
        net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// Advertise a public gossip address: peers push to us, as they would to a sidecar member.
    Gossip,
    /// Pull only (no reachable address): what `solana-gossip spy` sees.
    Spy,
}

#[derive(Parser, Debug)]
#[command(about = "Measure Solana gossip traffic against the leader contact-info changes we need")]
struct Args {
    /// Gossip entrypoint, host:port. Repeat for more than one.
    #[arg(long = "entrypoint", default_values = [
        "entrypoint.mainnet-beta.solana.com:8001",
        "entrypoint2.mainnet-beta.solana.com:8001",
        "entrypoint3.mainnet-beta.solana.com:8001",
    ])]
    entrypoints: Vec<String>,

    /// JSON-RPC URL, used once at startup for the leader schedule.
    #[arg(long, default_value = "https://api.mainnet-beta.solana.com")]
    rpc_url: String,

    #[arg(long, value_enum, default_value_t = Mode::Gossip)]
    mode: Mode,

    /// UDP port gossip binds in gossip mode. Open it in the firewall.
    #[arg(long, default_value_t = 8101)]
    gossip_port: u16,

    /// Public IP to advertise in gossip mode. Default: asked from the first entrypoint.
    #[arg(long)]
    public_ip: Option<IpAddr>,

    /// Cluster shred version. Default: asked from the first entrypoint.
    #[arg(long)]
    shred_version: Option<u16>,

    /// How long to listen.
    #[arg(long, default_value_t = 7200)]
    duration_secs: u64,

    /// Seconds between progress lines.
    #[arg(long, default_value_t = 60)]
    report_secs: u64,

    /// Interface to read byte counters from (/proc/net/dev; includes other traffic on the host).
    #[arg(long)]
    iface: Option<String>,

    /// nftables table (family inet) with counters `gossip_in` / `gossip_out` (see README).
    #[arg(long)]
    nft_table: Option<String>,

    /// CSV of every address change and removal (and leader restarts).
    #[arg(long, default_value = "gossip-changes.csv")]
    changes_csv: String,

    /// CSV of every gossip datapoint field: sum, last value, count.
    #[arg(long, default_value = "gossip-datapoints.csv")]
    datapoints_csv: String,

    /// Also print gossip's warnings (errors are always printed).
    #[arg(long)]
    verbose: bool,
}

/// Gossip gauges: report the last value, not the sum.
const GAUGES: &[&str] = &[
    "table_size",
    "num_nodes",
    "num_nodes_staked",
    "num_pubkeys",
    "purged_values_size",
    "failed_inserts_size",
];

/// Counter readings taken at the same moment, for differences.
struct Reading {
    at: Instant,
    cpu: CpuSample,
    iface: Option<Bytes>,
    nft: Option<Bytes>,
    points: datapoints::Table,
}

impl Reading {
    fn take(args: &Args) -> Self {
        Self {
            at: Instant::now(),
            cpu: proc_stats::cpu_sample(),
            iface: args.iface.as_deref().and_then(proc_stats::iface),
            nft: args.nft_table.as_deref().and_then(proc_stats::nft),
            points: datapoints::snapshot(),
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    datapoints::install(args.verbose).map_err(|e| anyhow!("logger: {e}"))?;

    let entrypoints = resolve(&args.entrypoints)?;
    let first = entrypoints[0];

    let leaders = schedule::fetch(&args.rpc_url)?;
    println!(
        "leader schedule: epoch {} (+ next), {} leader identities",
        leaders.epoch,
        leaders.pubkeys.len()
    );

    let shred_version = match args.shred_version {
        Some(version) => version,
        None => solana_net_utils::get_cluster_shred_version(&first)
            .map_err(|e| anyhow!("shred version from {first}: {e}"))?,
    };
    let gossip_addr = match args.mode {
        Mode::Spy => None,
        Mode::Gossip => {
            let ip = match args.public_ip {
                Some(ip) => ip,
                None => solana_net_utils::get_public_ip_addr_with_binding(
                    &first,
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                )
                .map_err(|e| anyhow!("public IP from {first}: {e:?}"))?,
            };
            Some(SocketAddr::new(ip, args.gossip_port))
        }
    };

    let exit = Arc::new(AtomicBool::new(false));
    {
        let exit = Arc::clone(&exit);
        ctrlc::set_handler(move || exit.store(true, Ordering::Relaxed))
            .context("Ctrl-C handler")?;
    }

    let start = Reading::take(&args);
    let (gossip_service, _ip_echo, cluster_info) = make_node(
        Keypair::new(), // throwaway, unstaked
        &entrypoints,
        Arc::clone(&exit),
        gossip_addr.as_ref(),
        shred_version,
        false,
        SocketAddrSpace::Global,
    );
    // Attached right after start: the table is empty, so nothing accepted is missed.
    let (contact_tx, contact_rx) = crossbeam_channel::bounded(1 << 16);
    cluster_info.set_contact_info_sender(contact_tx);
    let (contacts, contacts_thread) = contacts::spawn(
        contact_rx,
        leaders.pubkeys.clone(),
        &args.changes_csv,
        Arc::clone(&exit),
    )?;

    println!(
        "joined gossip: id {} mode {:?} addr {} shred_version {shred_version} entrypoints {entrypoints:?}",
        cluster_info.id(),
        args.mode,
        gossip_addr.map_or_else(|| "none (spy)".to_string(), |a| a.to_string()),
    );
    println!("running {} s; Ctrl-C to stop early", args.duration_secs);

    let deadline = start.at + Duration::from_secs(args.duration_secs);
    let mut last = Reading::take(&args);
    while !exit.load(Ordering::Relaxed) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(250));
        if last.at.elapsed() >= Duration::from_secs(args.report_secs) {
            let now = Reading::take(&args);
            let crds_len = cluster_info.gossip.crds.read().map_or(0, |crds| crds.len());
            progress(&start, &last, &now, crds_len, &contacts);
            last = now;
        }
    }

    let end = Reading::take(&args);
    let crds_len = cluster_info.gossip.crds.read().map_or(0, |crds| crds.len());
    final_report(
        &args,
        &start,
        &end,
        crds_len,
        leaders.pubkeys.len(),
        &contacts,
    );
    write_datapoints(&args.datapoints_csv, &end.points)?;

    exit.store(true, Ordering::Relaxed);
    let _ = contacts_thread.join();
    if gossip_service.join().is_err() {
        eprintln!("a gossip thread panicked during shutdown");
    }
    Ok(())
}

fn resolve(entrypoints: &[String]) -> Result<Vec<SocketAddr>> {
    let mut out = Vec::new();
    for entrypoint in entrypoints {
        let addr = entrypoint
            .to_socket_addrs()
            .with_context(|| format!("resolve {entrypoint}"))?
            .find(SocketAddr::is_ipv4)
            .ok_or_else(|| anyhow!("{entrypoint}: no IPv4 address"))?;
        out.push(addr);
    }
    if out.is_empty() {
        bail!("no entrypoint given");
    }
    Ok(out)
}

/// Accepted CRDS values (push + pull, every type) in a datapoint table.
fn accepted(points: &datapoints::Table) -> f64 {
    datapoints::crds_by_type(points, "cluster_info_crds_stats")
        .iter()
        .map(|(_, push, pull)| push + pull)
        .sum()
}

fn rejected(points: &datapoints::Table) -> f64 {
    datapoints::crds_by_type(points, "cluster_info_crds_stats_fails")
        .iter()
        .map(|(_, push, pull)| push + pull)
        .sum()
}

fn progress(
    start: &Reading,
    last: &Reading,
    now: &Reading,
    crds_len: usize,
    contacts: &contacts::ContactStats,
) {
    let secs = now.at.duration_since(last.at).as_secs_f64().max(1e-9);
    let packets = datapoints::sum_field(&now.points, "packets_received_count")
        - datapoints::sum_field(&last.points, "packets_received_count");
    let cpu = proc_stats::process_secs(&last.cpu, &now.cpu) / secs * 100.0;
    let (events, _) = contacts.events.get();
    let (changes, leader_changes) = contacts.addr_changes.get();
    let (rss, _) = proc_stats::rss_mib();
    let net = match (&now.nft, &last.nft, &now.iface, &last.iface) {
        (Some(n), Some(l), _, _) | (None, None, Some(n), Some(l)) => {
            let d = n.since(l);
            format!(" net_in {:.2} Mbit/s", d.rx_bytes as f64 * 8.0 / secs / 1e6)
        }
        _ => String::new(),
    };
    println!(
        "[{:>5.0}s] crds {crds_len} | pkts_rx {:.0}/s | crds accepted {:.0}/s rejected {:.0}/s | contact-info {events} total, addr changes {changes} (leaders {leader_changes}) | cpu {cpu:.1}% rss {rss:.0} MiB{net}",
        now.at.duration_since(start.at).as_secs_f64(),
        packets / secs,
        (accepted(&now.points) - accepted(&last.points)) / secs,
        (rejected(&now.points) - rejected(&last.points)) / secs,
    );
}

fn final_report(
    args: &Args,
    start: &Reading,
    end: &Reading,
    crds_len: usize,
    leader_count: usize,
    contacts: &contacts::ContactStats,
) {
    let secs = end.at.duration_since(start.at).as_secs_f64().max(1e-9);
    let points = &end.points;
    let per_sec = |v: f64| v / secs;
    println!("\n================ gossip-probe report ================");
    println!(
        "mode {:?}, ran {:.0} s ({:.2} h), {leader_count} leader identities",
        args.mode,
        secs,
        secs / 3600.0
    );

    println!("\n-- network --");
    for (label, start_bytes, end_bytes) in [
        ("gossip port (nft)", start.nft, end.nft),
        ("interface (whole host)", start.iface, end.iface),
    ] {
        if let (Some(s), Some(e)) = (start_bytes, end_bytes) {
            let d = e.since(&s);
            println!(
                "{label}: in {:.1} MiB / {} pkts ({:.2} Mbit/s), out {:.1} MiB / {} pkts ({:.2} Mbit/s)",
                d.rx_bytes as f64 / 1048576.0,
                d.rx_packets,
                d.rx_bytes as f64 * 8.0 / secs / 1e6,
                d.tx_bytes as f64 / 1048576.0,
                d.tx_packets,
                d.tx_bytes as f64 * 8.0 / secs / 1e6,
            );
        }
    }
    let packets_rx = datapoints::sum_field(points, "packets_received_count");
    println!(
        "gossip packets received (gossip's own count): {packets_rx:.0} ({:.0}/s)",
        per_sec(packets_rx)
    );

    println!("\n-- CRDS values by type (accepted = new to us; rejected = duplicate or old) --");
    println!(
        "{:<28} {:>14} {:>14} {:>14} {:>14}",
        "type", "accepted push", "accepted pull", "rejected push", "rejected pull"
    );
    let fails = datapoints::crds_by_type(points, "cluster_info_crds_stats_fails");
    for (kind, push, pull) in datapoints::crds_by_type(points, "cluster_info_crds_stats") {
        let (fail_push, fail_pull) = fails
            .iter()
            .find(|(k, _, _)| *k == kind)
            .map_or((0.0, 0.0), |(_, p, q)| (*p, *q));
        if push + pull + fail_push + fail_pull > 0.0 {
            println!("{kind:<28} {push:>14.0} {pull:>14.0} {fail_push:>14.0} {fail_pull:>14.0}");
        }
    }
    let total_accepted = accepted(points);
    println!(
        "total accepted {total_accepted:.0} ({:.0}/s), rejected {:.0}",
        per_sec(total_accepted),
        rejected(points)
    );
    println!("CRDS table entries now: {crds_len}");
    for gauge in GAUGES {
        if let Some(value) = datapoints::last_field(points, gauge) {
            println!("{gauge}: {value:.0}");
        }
    }

    println!("\n-- contact info (what the sidecar needs) --");
    println!("{:<34} {:>12} {:>12}", "", "all nodes", "leaders");
    let row = |label: &str, counts: &contacts::Counts| {
        let (all, leaders) = counts.get();
        println!("{label:<34} {all:>12} {leaders:>12}");
    };
    row("accepted contact-info entries", &contacts.events);
    row("  first seen (table fill)", &contacts.first_seen);
    row("  address changed", &contacts.addr_changes);
    row("  restart, same addresses", &contacts.restarts);
    row("  republish, nothing changed", &contacts.republishes);
    row("removed", &contacts.removed);
    println!(
        "nodes known now: {}",
        contacts.nodes.load(Ordering::Relaxed)
    );
    let (events, _) = contacts.events.get();
    let (_, leader_changes) = contacts.addr_changes.get();
    let (_, leader_removed) = contacts.removed.get();
    let useful = (leader_changes + leader_removed) as f64;
    println!(
        "useful after the table filled: {useful:.0} leader address changes/removals = {:.6} % of accepted CRDS values, {:.6} % of accepted contact-info entries",
        pct(useful, total_accepted),
        pct(useful, events as f64),
    );
    println!("details: {}", args.changes_csv);

    println!("\n-- cost --");
    let process = proc_stats::process_secs(&start.cpu, &end.cpu);
    let (rss, peak) = proc_stats::rss_mib();
    println!(
        "process CPU {process:.1} s = {:.2} % of one core on average; RSS {rss:.0} MiB (peak {peak:.0} MiB)",
        process / secs * 100.0
    );
    println!("CPU by thread (s, % of one core):");
    for (name, thread_secs) in proc_stats::secs_by_thread(&start.cpu, &end.cpu)
        .iter()
        .take(20)
    {
        let ours = if name == contacts::THREAD_NAME {
            "  (probe's own counting)"
        } else {
            ""
        };
        println!(
            "  {name:<20} {thread_secs:>9.1} {:>7.2}%{ours}",
            thread_secs / secs * 100.0
        );
    }
    println!("all datapoint fields: {}", args.datapoints_csv);
    println!("=====================================================");
}

fn pct(part: f64, whole: f64) -> f64 {
    if whole > 0.0 {
        part / whole * 100.0
    } else {
        0.0
    }
}

fn write_datapoints(path: &str, points: &datapoints::Table) -> Result<()> {
    let mut out = BufWriter::new(File::create(path).with_context(|| format!("create {path}"))?);
    writeln!(out, "measurement,field,sum,last,reports")?;
    for (key, agg) in points {
        let (measurement, field) = key.rsplit_once('.').unwrap_or((key.as_str(), ""));
        writeln!(
            out,
            "{measurement},{field},{},{},{}",
            agg.sum, agg.last, agg.n
        )?;
    }
    out.flush()?;
    Ok(())
}
