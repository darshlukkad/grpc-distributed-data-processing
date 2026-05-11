# CMPE 275 — Mini 2: Distributed Scatter-Gather Query System
## Project Report

---

## 1. Overview

This project implements a 9-process distributed query system that partitions the NYC 311 Service Request dataset (20M rows, ~12 GB) across two physical machines and answers range queries via a gRPC scatter-gather tree. Clients submit queries to a single root node (A), which fans them out to the tree, gathers results, and returns them in dynamically sized chunks.

---

## 2. System Architecture

### Tree Topology

```
            A  (root — public facing, mac1)
           /|\ \
          B  H  G  I
         /|\
        C  D  E
              |
              F
```

Nine processes span two Apple M-series Macs connected via RJ45 through a D-Link switch:

| Node | Language | Machine | IP | Port | Rows |
|------|----------|---------|-----|------|------|
| A | C++ | mac1 | 10.0.0.180 | 50051 | 0 – 2,236,580 |
| B | C++ | mac1 | 10.0.0.180 | 50052 | 2,236,581 – 4,473,161 |
| C | C++ | mac1 | 10.0.0.180 | 50053 | 4,473,162 – 6,709,742 |
| D | C++ | mac1 | 10.0.0.180 | 50054 | 6,709,743 – 8,946,323 |
| E | C++ | mac1 | 10.0.0.180 | 50055 | 8,946,324 – 11,182,904 |
| F | C++ | mac2 | 10.0.0.100 | 50056 | 11,182,905 – 13,419,485 |
| G | C++ | mac2 | 10.0.0.100 | 50057 | 13,419,486 – 15,656,066 |
| H | C++ | mac2 | 10.0.0.100 | 50058 | 15,656,067 – 17,892,647 |
| I | Python | mac2 | 10.0.0.100 | 50059 | 17,892,648 – 20,129,232 |

All inter-node communication uses gRPC unary RPCs (no streaming API).

---

## 3. Dataset

**NYC 311 Service Requests 2020–2026**
- 20,129,232 data rows, 44 columns, ~12 GB CSV
- Fields used: `unique_key`, `created_date`, `incident_zip`, `latitude`, `longitude`
- Each node loads only its assigned row slice at startup

---

## 4. Implementation

### Phase 1 — Dataset Loading

**Goal:** Load ~2.2M rows per node as fast as possible into a search-optimized in-memory layout.

**Approach:**

1. **mmap** the full CSV into virtual address space — OS lazy-loads pages on demand, no upfront I/O cost
2. **O(1) byte-offset seek** — instead of scanning millions of newlines to reach `row_start`, we estimate the byte position as:
   ```
   offset = (row_start / 20,129,232) × file_size
   ```
   Then snap forward to the next `\n` using `memchr`. This replaces a O(row_start) scan with an O(1) seek + tiny local correction.
3. **4 OpenMP threads** parse the assigned byte range in parallel, each writing into its own thread-local Structure-of-Arrays (TLSoA)
4. **StringPool** — a pre-allocated 400 MB arena for variable-length strings; allocation uses `std::atomic::fetch_add` (lock-free)
5. **StringRegistry** — categorical fields (agency, borough, complaint type) are interned to 1-byte integer codes using a `shared_mutex` (multiple concurrent readers, exclusive writers)
6. **Sequential merge** — thread-local arrays are merged into the global DataStore in thread order, preserving row sequence

**Memory layout (Structure-of-Arrays):**
```
incident_zip_:  [10001, 10002, ...]   ← contiguous uint32 array
created_date_:  [1631499370, ...]     ← contiguous uint32 array (unix epoch)
latlon_:        [{lat,lon}, ...]      ← lat+lon packed together, contiguous
```

Each field is a separate array so a query reads only the relevant field, maximizing CPU cache utilization.

---

### Phase 2 — Query Submission (Submit RPC)

**Flow:**

```
Client → A.Submit(query) → request_id
```

1. Client sends a `Query` proto (ZIP_RANGE / DATE_RANGE / BBOX) to node A
2. Node A launches its own local search as `std::async` simultaneously with peer forwarding
3. Node A fires parallel `std::async` calls to all direct peers (B, H, G, I)
4. Each peer repeats the pattern recursively — forward to its children while searching locally
5. Results bubble back up to A
6. A merges all results and stores them in `ChunkManager` (mutex-protected `unordered_map<request_id, records>`)
7. Returns `request_id` to client

**Key parallelism:** local search and all peer RPC calls run concurrently. No node waits for peers before starting its own search.

---

### Phase 3 — Dynamic Chunked Fetch (Fetch RPC)

No gRPC streaming. Client drives the pull loop with **adaptive chunk sizing**:

```
Fetch(offset=0,    chunk_size=500)   → 500 records
Fetch(offset=500,  chunk_size=1000)  → 1000 records
Fetch(offset=1500, chunk_size=2000)  → 2000 records
...
Fetch(offset=N,    chunk_size=50000) → remaining, is_last=true
```

The client starts at 500 records per call, doubles the chunk size each round-trip, and caps at 50,000. The `chunk_idx` field is repurposed as an absolute record offset so the server can honor variable sizes without tracking state.

**Impact:** A 3.8M-record result set requires ~80 Fetch RPCs with dynamic sizing vs. ~7,757 with a fixed 500-record chunk — the same bytes transferred with 97% fewer round-trips.

---

## 5. Search Implementation

All three query types use 4 OpenMP threads with thread-local result vectors merged under a critical section:

| Query Type | Fields Scanned | Condition |
|-----------|---------------|-----------|
| ZIP_RANGE | `incident_zip_[]` | `zip_min ≤ zip ≤ zip_max` |
| DATE_RANGE | `created_date_[]` | `date_min ≤ date ≤ date_max` (unix epoch) |
| BBOX | `latlon_[]` | lat and lon within bounding box |

---

## 6. gRPC Protocol

```protobuf
service NodeService {
    rpc Submit  (Query)         returns (SubmitResponse);   // Client → A only
    rpc Fetch   (FetchRequest)  returns (FetchResponse);    // Client → A only
    rpc Forward (Query)         returns (ForwardResponse);  // Node → peer node
    rpc Cancel  (CancelRequest) returns (CancelResponse);   // Client → A only
}

message FetchRequest {
    string request_id = 1;
    int32  chunk_idx  = 2;  // used as record offset for dynamic chunk sizing
    int32  chunk_size = 3;  // requested records; 0 = use server default (500)
}
```

Max gRPC message size: **64 MB** on all nodes.

---

## 7. Design Decisions

### No hardcoded configuration
All node identity, host, port, row range, and peer edges are read from `config/topology.json`. Source code has no hardcoded values.

### No shared memory between nodes
Each node is a standalone OS process. Nodes communicate exclusively via gRPC. Results are not passed through shared memory or files.

### Typed storage, not raw strings
Fields are stored as `uint32` (zip, date), `float` (lat/lon) — not strings. Dates are parsed from `MM/DD/YYYY HH:MM:SS AM/PM` to unix epoch at load time.

### Mixed language
Nodes A–H are C++ (performance-critical, OpenMP parallel search). Node I is Python (demonstrates language interoperability over gRPC; same scatter-gather logic implemented with `concurrent.futures.ThreadPoolExecutor`).

### O(1) row seek vs. O(N) scan
The naive approach scans every newline from the file start to reach `row_start`. For node H (row 15.6M), that's 15.6M newline scans. The byte-offset estimation reduces this to a single arithmetic operation + one short `memchr` scan to align to a row boundary.

---

## 8. Benchmark Results

30 test queries (10 per type) across the full 9-node cluster:

| Query Type | Avg Latency |
|-----------|-------------|
| ZIP_RANGE | ~6,218 ms |
| DATE_RANGE | ~906 ms |
| BBOX | ~4,787 ms |
| **Overall** | **~3,970 ms** |

DATE_RANGE is fastest because date values are uniformly distributed — most queries return a small slice. ZIP and BBOX return large result sets (up to 3.8M records) dominated by fetch time.

---

## 9. Challenges and Solutions

| Challenge | Solution |
|-----------|----------|
| Stale CMakeCache on mac2 (paths from mac1 zip) | Added `rm -rf cpp/build` to `run_mac2.sh` before cmake |
| csv_path resolved from wrong directory | Resolved relative to config file directory using `std::filesystem` (C++) and `os.path` (Python) |
| gRPC/Protobuf CMake target conflict | Load `find_package(gRPC)` before `find_package(Protobuf)` |
| OpenMP not found on macOS | Used `brew --prefix libomp` to locate headers and library explicitly |
| Row sequence not preserved with parallel parse | Each thread writes to its own TLSoA; sequential merge in thread-order after parse |
| benchmark.sh avg capture broken | Shell `$()` captured all output including table rows; wrote avg to `/tmp/_bench_avg` file instead |

---

## 10. Hardware

- 2× Apple M-series Mac (ARM64, macOS), 16 GB RAM each
- Connected via RJ45 through a D-Link switch (~1 Gbps)
- Compiler: Apple Clang, C++17, `-O2`
- Python 3.x with `grpcio`, `grpcio-tools`