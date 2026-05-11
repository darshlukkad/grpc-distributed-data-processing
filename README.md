# CMPE 275 — Mini 2: Distributed Scatter-Gather Query System

A 9-process distributed system that partitions the NYC 311 Service Request dataset (20M rows) across two physical machines and answers range queries via a gRPC scatter-gather tree.

---

## Architecture

### Tree Topology

```
            A  (root — public facing)
           /|\ \
          B  H  G  I
         /|\
        C  D  E
              |
              F
```

| Node | Language | Machine | Port |
|------|----------|---------|------|
| A | C++ | mac1 | 50051 |
| B | C++ | mac1 | 50052 |
| C | C++ | mac1 | 50053 |
| D | C++ | mac1 | 50054 |
| E | C++ | mac1 | 50055 |
| F | C++ | mac2 | 50056 |
| G | C++ | mac2 | 50057 |
| H | C++ | mac2 | 50058 |
| I | Python | mac2 | 50059 |

**Directed peer edges (who calls whom):**
- A → B, H, G, I
- B → C, D, E
- E → F
- C, D, F, G, H, I → nobody (leaves)

Clients always talk to **node A only**. A scatters the query down the tree, collects results from all nodes, and returns them to the client.

---

## Dataset

**NYC 311 Service Requests 2020–2026**
- 20,129,232 data rows, 44 columns
- File: `dataset/nyc_311_2020_2026.csv` (~12 GB)

### Data Partition (equal row counts per node)

| Node | row_start | row_end |
|------|-----------|---------|
| A | 0 | 2,236,580 |
| B | 2,236,581 | 4,473,161 |
| C | 4,473,162 | 6,709,742 |
| D | 6,709,743 | 8,946,323 |
| E | 8,946,324 | 11,182,904 |
| F | 11,182,905 | 13,419,485 |
| G | 13,419,486 | 15,656,066 |
| H | 15,656,067 | 17,892,647 |
| I | 17,892,648 | 20,129,232 |

Each node loads only its assigned slice at startup.

---

## How It Works

### Phase 1 — Dataset Loading

Each node starts by loading its row slice into memory:

1. **mmap** the full 12 GB CSV into virtual memory (OS lazy-loads pages on demand)
2. **Direct byte seek** — jump to approximate byte position using `(row_start / total_rows) × file_size`, then snap forward to the next newline — O(1), no scanning
3. **4 OpenMP threads** parse the byte range in parallel, each thread writing into its own thread-local Structure-of-Arrays (SoA)
4. **StringPool** — pre-allocated 400 MB arena for variable-length string fields; allocation is a single atomic `fetch_add` (lock-free)
5. **StringRegistry** — categorical fields (agency, borough, status) interned to 1-byte codes using `shared_mutex`
6. **Merge** — thread-local SoA arrays copied sequentially into the global DataStore in thread order (preserving row sequence)

**Memory layout (SoA):**
```
incident_zip_:  [10001, 10002, 10065, ...]   ← 2.2M uint32s, contiguous
created_date_:  [1631499370, 1631484828, ...] ← 2.2M uint32s, contiguous
latlon_:        [{40.76,-73.95}, ...]         ← lat+lon together, contiguous
```
Storing each field as a separate array means search only reads the relevant field — maximizing cache efficiency.

---

### Phase 2 — Query Submission (Submit RPC)

```
Client → A.Submit(query) → request_id
```

1. Client builds a `Query` proto (ZIP_RANGE / DATE_RANGE / BBOX) and calls `Submit` on node A
2. Node A fires **parallel `std::async`** calls to all its peers (B, H, G, I) simultaneously
3. Node A **simultaneously** searches its own local data with **4 OpenMP threads**
4. Each peer (B, H, G, I) receives a `Forward` RPC and repeats the same pattern recursively:
   - B fires async to C, D, E + searches local in parallel
   - E fires async to F + searches local in parallel
   - Leaves (C, D, F, G, H, I) just search local and return
5. Results bubble back up the tree to A
6. A merges all peer results + its own results into one `vector<ServiceRecord>`
7. Stores in **ChunkManager** (mutex-protected `unordered_map<request_id, vector<ServiceRecord>>`)
8. Returns `request_id` to client

---

### Phase 3 — Chunked Fetch (Fetch RPC)

No gRPC streaming. Client drives the pull loop with **dynamic chunk sizing**:

```
Client → A.Fetch(request_id, offset=0,    chunk_size=500)   → 500 records
Client → A.Fetch(request_id, offset=500,  chunk_size=1000)  → 1000 records
Client → A.Fetch(request_id, offset=1500, chunk_size=2000)  → 2000 records
...
Client → A.Fetch(request_id, offset=N,    chunk_size=50000) → remaining, is_last=true
```

- Chunk size starts at **500**, doubles each round-trip, caps at **50,000**
- `chunk_idx` field is reused as a **record offset** (not a sequential counter), enabling variable chunk sizes
- Max gRPC message size: **64 MB**
- Client loops until `is_last == true`
- A 3.8M-record result set goes from ~7,757 Fetch calls (fixed 500) to ~80 calls (dynamic)

---

## Query Types

| Type | Parameters | Example |
|------|-----------|---------|
| ZIP_RANGE | zip_min, zip_max (uint32) | 10001–10099 |
| DATE_RANGE | date_min, date_max (unix epoch uint32) | 1672531200–1704067199 |
| BBOX | lat_min, lat_max, lon_min, lon_max (double) | 40.57–40.74, -74.04– -73.83 |

Dates are stored and queried as **unix epoch** (seconds since Jan 1 1970). The CSV format `MM/DD/YYYY HH:MM:SS AM/PM` is parsed to epoch at load time.

---

## gRPC Protocol

```protobuf
service NodeService {
    rpc Submit(Query)         returns (SubmitResponse);   // Client → A
    rpc Fetch(FetchRequest)   returns (FetchResponse);    // Client → A
    rpc Forward(Query)        returns (ForwardResponse);  // Node → peer node
    rpc Cancel(CancelRequest) returns (CancelResponse);   // Client → A
}
```

All RPCs are **unary** (no streaming).

---

## File Structure

```
mini2/
├── proto/
│   └── mini2.proto                  ← gRPC service + message definitions
├── config/
│   └── topology.json                ← node hosts/ports/row ranges + peer edges
├── cpp/
│   ├── CMakeLists.txt
│   ├── server/
│   │   ├── data_store.hpp/cpp       ← CSV load + 3 search queries (SoA, OpenMP)
│   │   ├── chunk_manager.hpp/cpp    ← result buffer per request_id
│   │   ├── node_service.hpp/cpp     ← gRPC service implementation
│   │   └── main.cpp                 ← entry point, reads config, starts server
│   └── client/
│       └── main.cpp                 ← submit + fetch loop + timing
├── python/
│   ├── node_server.py               ← node I (Python gRPC, same logic as C++)
│   └── requirements.txt
├── benchmark.sh                     ← runs 30 test queries, reports timing
└── run_mac2.sh                      ← setup + start script for mac2
```

---

## Prerequisites

### Mac 1 (your machine — nodes A, B, C, D, E)
```bash
brew install grpc nlohmann-json libomp
```

### Mac 2 (other machine — nodes F, G, H, I)
```bash
# Handled automatically by run_mac2.sh
```

---

## Build

```bash
cd cpp
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(sysctl -n hw.logicalcpu)
```

Produces two binaries in `cpp/build/`:
- `node_server` — gRPC server (used for nodes A–H)
- `node_client` — query client

---

## Running

### Mac 1 — Start nodes A, B, C, D, E

```bash
cd cpp/build
pkill -f node_server 2>/dev/null
for id in A B C D E; do
  ./node_server --id $id --config ../../config/topology.json > /tmp/node_${id}.log 2>&1 &
done

# Wait for all to be ready
for id in A B C D E; do
  while ! grep -q "Listening on port" /tmp/node_${id}.log 2>/dev/null; do sleep 2; done
  echo "Node $id ready"
done
```

### Mac 2 — Start nodes F, G, H, I

```bash
# On mac2, from the project root:
bash run_mac2.sh
```

This script will:
1. Install dependencies (grpc, nlohmann-json, libomp via Homebrew)
2. Build C++ binaries
3. Generate Python proto stubs
4. Start nodes F, G, H in background
5. Start node I (Python) in foreground

---

## Running Queries

```bash
cd cpp/build

# ZIP range
./node_client --config ../../config/topology.json \
  --query zip --zip-min 10001 --zip-max 10099

# Date range (unix epoch)
./node_client --config ../../config/topology.json \
  --query date --date-min 1672531200 --date-max 1704067199

# Bounding box (Manhattan)
./node_client --config ../../config/topology.json \
  --query bbox --lat-min 40.57 --lat-max 40.74 --lon-min -74.04 --lon-max -73.83
```

Output format (stdout):
```
unique_key,created_date,incident_zip,latitude,longitude
51841122,1631499370,10065,40.7634,-73.9592
...
total_records=3878159 chunks=7757 dt_ms=7038
```

---

## Benchmark

Runs 10 test cases per query type (30 total), reports per-run timing and averages:

```bash
bash benchmark.sh
```

Sample output:
```
=== ZIP (10 runs) ===
Run   Parameters                           Records    Chunks        ms
----------------------------------------------------------------------
1     zip 10001-10099                      1673769      3348    5980ms
...
AVG                                                              6218ms

=== DATE (10 runs) ===
...
AVG                                                               906ms

=== BBOX (10 runs) ===
...
AVG                                                              4787ms

======================================================================
  SUMMARY
======================================================================
  ZIP avg        6218ms
  DATE avg        906ms
  BBOX avg       4787ms
  ------------------
  OVERALL avg    3970ms
======================================================================
```

---

## Configuration

`config/topology.json`:
```json
{
  "csv_path": "../dataset/nyc_311_2020_2026.csv",
  "chunk_size": 500,
  "nodes": {
    "A": { "host": "10.0.0.180", "port": 50051, "row_start": 0, "row_end": 2236580 },
    ...
  },
  "peers": {
    "A": ["B", "H", "G", "I"],
    "B": ["C", "D", "E"],
    "E": ["F"]
  }
}
```

- `csv_path` — relative to the `config/` directory
- `chunk_size` — records per Fetch response
- `peers` — defines the scatter tree (omit a node or leave `[]` for leaves)

---

## Design Constraints

- No gRPC async or streaming APIs — all RPCs are unary
- No hardcoded node identity, host, or port in source code
- No shared memory for responses between nodes
- Each node runs as its own OS process in its own shell
- C++ for nodes A–H, Python for node I
- Realistic typed storage: `uint32`, `uint64`, `float`, `double` — not raw strings

---

## Hardware

- 2× Apple M-series Mac (ARM64, macOS)
- Connected via RJ45 LAN through D-Link switch
- 16 GB RAM each
- Compiler: Apple Clang, C++17