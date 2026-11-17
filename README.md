# gossip-probe

Joins Solana gossip as an unstaked node for a fixed time (default 2 h) and reports:

- **how much arrives:** packets and bytes, and CRDS values accepted or rejected per type (ContactInfo,
  Vote, EpochSlots, …);
- **what it costs:** CPU per thread, memory, network;
- **how much of it we need:** leader contact-info changes (address changed or node removed), next to the
  republishes, restarts and first-seen entries.

It answers one question: should the sidecar join gossip, or get contact-info changes another way (RPC
`getClusterNodes`, or a Spark topic fed by the validator's own contact-info hook)?

Nothing here has been compiled yet. It was written against the `solana-gossip` 4.4.1 sources from
crates.io.

## How it works

| Source | What |
|---|---|
| RPC, once at startup | `getEpochInfo` + `getLeaderSchedule` for this and the next epoch → the leader identities |
| `solana_gossip::gossip_service::make_node` | a real gossip node, with a throwaway keypair, using the same code a validator runs |
| `ClusterInfo::set_contact_info_sender` | every contact-info entry gossip accepts (the same hook the validator uses for geyser plugins) |
| gossip's metrics datapoints | per-type accepted/rejected counts and packet counters. `solana-metrics` logs each datapoint, and the probe's logger catches them, so no InfluxDB is needed |
| `/proc/self/task/*/stat`, `/proc/self/status` | CPU seconds per thread, RSS |
| `/proc/net/dev` or nftables counters | bytes and packets |

Each accepted contact-info entry is sorted into one of:

- **first seen**: the table filling up after start;
- **address changed**: `gossip`, `tpu_quic` or `tpu_forwards_quic` differ. This is what the sidecar must act on;
- **restart**: new `outset`, same addresses;
- **republish**: only `wallclock` moved (every node re-sends about every 7.5 s);
- **removed**: gossip dropped the node.

## Build (on the remote machine)

It needs Rust 1.98.1 (pinned in `rust-toolchain.toml`) and the usual Agave build packages:
`solana-gossip` pulls in `solana-ledger`, so RocksDB is compiled too.

```bash
sudo apt-get install -y build-essential clang cmake pkg-config libssl-dev libudev-dev protobuf-compiler
```

```bash
cd gossip-probe && cargo build --release
```

`solana-gossip`, `solana-net-utils` and `solana-rpc-client` are pinned to `=4.4.1` (mainnet Jito). For
another release, change all three together.

## Run

**Gossip mode** (default). This is what a sidecar joining gossip would see: we advertise a public UDP
port, so peers push to us. Open that port to the internet first:

```bash
sudo ufw allow 8101/udp
```

**Optional exact byte counts** for the gossip port only, using nftables (needs root, and the probe must
then run with permission to call `nft`):

```bash
sudo nft add table inet gossip_probe
sudo nft add counter inet gossip_probe gossip_in
sudo nft add counter inet gossip_probe gossip_out
sudo nft 'add chain inet gossip_probe input { type filter hook input priority -10; policy accept; }'
sudo nft 'add chain inet gossip_probe output { type filter hook output priority -10; policy accept; }'
sudo nft add rule inet gossip_probe input udp dport 8101 counter name gossip_in
sudo nft add rule inet gossip_probe output udp sport 8101 counter name gossip_out
```

**Two-hour run:**

```bash
sudo ./target/release/gossip-probe --rpc-url https://api.mainnet-beta.solana.com --gossip-port 8101 --duration-secs 7200 --iface eth0 --nft-table gossip_probe | tee probe-gossip.txt
```

- Without `sudo`, leave out `--nft-table`. `--iface` reads the whole interface, which includes other
  traffic on the host.
- Use your own RPC node with `--rpc-url` if you have one. It's used only once.
- The default entrypoints are mainnet's. `--shred-version` and `--public-ip` are asked from the
  entrypoint unless you pass them.

**Spy mode, for comparison.** No reachable address, so peers don't push to us; this is what
`solana-gossip spy` sees:

```bash
./target/release/gossip-probe --mode spy --duration-secs 7200 --iface eth0 | tee probe-spy.txt
```

Stop early with Ctrl-C; the report is still printed. Remove the nftables table afterwards:

```bash
sudo nft delete table inet gossip_probe
```

## Output

- A progress line every 60 s: CRDS size, packets/s, CRDS accepted and rejected per second, contact-info
  totals, address changes, CPU %, RSS, and network Mbit/s.
- **The final report** (stdout):
  - network in/out;
  - a per-type table of accepted and rejected CRDS values;
  - gossip gauges;
  - the contact-info breakdown for all nodes and for leaders, and the useful share;
  - process CPU and CPU per thread;
  - memory.
- `gossip-changes.csv`: every address change and removal (and leader restarts), with old and new
  addresses.
- `gossip-datapoints.csv`: every gossip datapoint field with its sum, last value and report count, for
  anything the report doesn't print.

## Reading the result

- **Useful share** = leader address changes + leader removals, divided by all accepted CRDS values. Expect
  a very small number; the table makes clear where the rest went (votes, epoch slots, republishes, …).
- The **CPU per thread** list shows gossip's threads (`solGossip…`, `solRspndr…`, the receiver/consumer
  threads) separately from `probeContacts`, which is the probe's own counting.
- The **first minutes are the table filling** (`first seen`). Compare rates after that, using the progress lines.
- **An unstaked node is pushed to less than a staked validator**, so these numbers are what a sidecar
  would get. A validator handles more.
