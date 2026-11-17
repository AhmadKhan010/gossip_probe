//! The `probeContacts` thread. Gossip hands it every contact-info entry it accepts (through
//! `ClusterInfo::set_contact_info_sender`). Each one is sorted into: first seen, address change,
//! restart (same addresses, new `outset`), republish (only `wallclock` moved) or removed. Address
//! changes and removals are also written to a CSV, so they can be checked afterwards.

use {
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_gossip::contact_info_notifier::{ContactInfoEvent, ContactInfoSnapshot},
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        fs::File,
        io::{BufWriter, Write},
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        thread::{self, JoinHandle},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

pub const THREAD_NAME: &str = "probeContacts";

/// One counter for every node, one for the nodes in the leader schedule.
#[derive(Debug, Default)]
pub struct Counts {
    all: AtomicU64,
    leaders: AtomicU64,
}

impl Counts {
    fn inc(&self, leader: bool) {
        self.all.fetch_add(1, Ordering::Relaxed);
        if leader {
            self.leaders.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `(all nodes, leaders)`.
    pub fn get(&self) -> (u64, u64) {
        (
            self.all.load(Ordering::Relaxed),
            self.leaders.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug, Default)]
pub struct ContactStats {
    /// Every accepted contact-info entry.
    pub events: Counts,
    pub first_seen: Counts,
    /// gossip, tpu_quic or tpu_forwards_quic changed: the only thing the sidecar must act on.
    pub addr_changes: Counts,
    /// New `outset` (the validator restarted) with the same addresses.
    pub restarts: Counts,
    /// Nothing we track changed (the 7.5 s refresh).
    pub republishes: Counts,
    pub removed: Counts,
    /// Distinct nodes currently known.
    pub nodes: AtomicU64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Endpoints {
    gossip: Option<SocketAddr>,
    tpu_quic: Option<SocketAddr>,
    tpu_forwards_quic: Option<SocketAddr>,
}

impl From<&ContactInfoSnapshot> for Endpoints {
    fn from(snapshot: &ContactInfoSnapshot) -> Self {
        Self {
            gossip: snapshot.gossip,
            tpu_quic: snapshot.tpu_quic,
            tpu_forwards_quic: snapshot.tpu_forwards_quic,
        }
    }
}

struct Known {
    endpoints: Endpoints,
    outset: u64,
}

pub fn spawn(
    events: Receiver<ContactInfoEvent>,
    leaders: HashSet<Pubkey>,
    csv_path: &str,
    exit: Arc<AtomicBool>,
) -> std::io::Result<(Arc<ContactStats>, JoinHandle<()>)> {
    let mut csv = BufWriter::new(File::create(csv_path)?);
    writeln!(
        csv,
        "unix_ms,pubkey,leader,kind,old_gossip,new_gossip,old_tpu_quic,new_tpu_quic,old_tpu_fwd_quic,new_tpu_fwd_quic"
    )?;
    let stats = Arc::new(ContactStats::default());
    let handle = thread::Builder::new().name(THREAD_NAME.into()).spawn({
        let stats = Arc::clone(&stats);
        move || run(&events, &leaders, &mut csv, &stats, &exit)
    })?;
    Ok((stats, handle))
}

fn run(
    events: &Receiver<ContactInfoEvent>,
    leaders: &HashSet<Pubkey>,
    csv: &mut BufWriter<File>,
    stats: &ContactStats,
    exit: &AtomicBool,
) {
    let mut known: HashMap<Pubkey, Known> = HashMap::new();
    while !exit.load(Ordering::Relaxed) {
        let event = match events.recv_timeout(Duration::from_millis(200)) {
            Ok(event) => event,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        match event {
            ContactInfoEvent::Updated(snapshot) => {
                on_update(&snapshot, leaders, &mut known, csv, stats);
            }
            ContactInfoEvent::Removed(pubkey) => {
                let leader = leaders.contains(&pubkey);
                stats.removed.inc(leader);
                let old = known.remove(&pubkey);
                stats.nodes.store(known.len() as u64, Ordering::Relaxed);
                write_row(
                    csv,
                    &pubkey,
                    leader,
                    "removed",
                    old.map(|k| k.endpoints),
                    None,
                );
            }
        }
    }
    let _ = csv.flush();
}

fn on_update(
    snapshot: &ContactInfoSnapshot,
    leaders: &HashSet<Pubkey>,
    known: &mut HashMap<Pubkey, Known>,
    csv: &mut BufWriter<File>,
    stats: &ContactStats,
) {
    let leader = leaders.contains(&snapshot.pubkey);
    stats.events.inc(leader);
    let endpoints = Endpoints::from(snapshot);
    let old = known.insert(
        snapshot.pubkey,
        Known {
            endpoints,
            outset: snapshot.outset,
        },
    );
    match old {
        None => {
            stats.first_seen.inc(leader);
            stats.nodes.store(known.len() as u64, Ordering::Relaxed);
        }
        Some(old) if old.endpoints != endpoints => {
            stats.addr_changes.inc(leader);
            write_row(
                csv,
                &snapshot.pubkey,
                leader,
                "addr",
                Some(old.endpoints),
                Some(endpoints),
            );
        }
        Some(old) if old.outset != snapshot.outset => {
            stats.restarts.inc(leader);
            if leader {
                write_row(
                    csv,
                    &snapshot.pubkey,
                    leader,
                    "restart",
                    Some(old.endpoints),
                    Some(endpoints),
                );
            }
        }
        Some(_) => stats.republishes.inc(leader),
    }
}

fn write_row(
    csv: &mut BufWriter<File>,
    pubkey: &Pubkey,
    leader: bool,
    kind: &str,
    old: Option<Endpoints>,
    new: Option<Endpoints>,
) {
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let addr = |e: Option<Endpoints>, pick: fn(&Endpoints) -> Option<SocketAddr>| {
        e.as_ref()
            .and_then(pick)
            .map_or_else(String::new, |a| a.to_string())
    };
    let _ = writeln!(
        csv,
        "{unix_ms},{pubkey},{leader},{kind},{},{},{},{},{},{}",
        addr(old, |e| e.gossip),
        addr(new, |e| e.gossip),
        addr(old, |e| e.tpu_quic),
        addr(new, |e| e.tpu_quic),
        addr(old, |e| e.tpu_forwards_quic),
        addr(new, |e| e.tpu_forwards_quic),
    );
}
