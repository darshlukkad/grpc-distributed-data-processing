# Mini 2 — What's New vs. Mini 1

---

## What Mini 1 Had
- Single-process C++ application
- Local in-memory data store
- Basic CSV parsing
- No networking

---

## What Mini 2 Added

### 1. Distributed 9-Node gRPC System
- 9 OS processes across **2 physical machines** connected via RJ45 LAN
- A tree topology: root A fans out to B/H/G/I, B fans out to C/D/E, E fans out to F
- All inter-node communication via **gRPC unary RPCs** (no streaming)
- Every node is config-driven — no hardcoded host, port, or identity in source code

### 2. mmap-Based CSV Loading
- The 12 GB CSV is **memory-mapped** (`mmap`) instead of read with `fread`
- OS lazy-loads pages on demand — startup doesn't wait for 12 GB to be read
- Each node maps the full file but only touches its assigned byte range

### 3. O(1) Row Partitioning
- **Problem:** Node H starts at row 15.6M — naively scanning 15.6M newlines is slow
- **Solution:** Estimate byte offset as `(row_start / 20,129,232) × file_size`, then snap forward one row using `memchr`
- What was O(N) scanning became O(1) arithmetic + one short scan

### 4. Parallel CSV Parsing with OpenMP
- 4 OpenMP threads parse each node's byte range in parallel
- Each thread writes into its own **thread-local Structure-of-Arrays (TLSoA)** — no locking during parse
- After parse, arrays are merged sequentially in thread order to preserve row sequence

### 5. Structure-of-Arrays Memory Layout
- Fields stored as separate contiguous arrays: `incident_zip_[]`, `created_date_[]`, `latlon_[]`
- A ZIP query only reads `incident_zip_[]` — the CPU cache never loads latitude/longitude data
- Maximizes cache efficiency vs. Array-of-Structs

### 6. Lock-Free StringPool
- Variable-length string fields (agency name, complaint type, etc.) go into a **pre-allocated 400 MB arena**
- Allocation is a single `std::atomic::fetch_add` — completely lock-free under concurrent writes

### 7. StringRegistry — Categorical Field Interning
- High-cardinality string fields (borough, status) are interned to **1-byte integer codes**
- Uses `std::shared_mutex` — multiple threads can read simultaneously, writers take exclusive lock

### 8. 4-Thread OpenMP Parallel Search
- All three query types (ZIP, DATE, BBOX) search with 4 OpenMP threads
- Each thread accumulates matches in a thread-local vector, merged at the end under `#pragma omp critical`
- Scales search across all CPU cores

### 9. Concurrent Local Search + Peer Calls
- **Problem:** Node A was searching its own 2.2M rows *after* waiting for peer results
- **Solution:** Local search launched as `std::async` simultaneously with peer gRPC calls
- Every node in the tree (A, B, E) now overlaps its local search with network I/O

### 10. Parallel Peer RPC Calls
- Each node fires all peer `Forward` RPCs simultaneously using `std::async(std::launch::async)`
- B calls C, D, E in parallel — not sequentially
- A calls B, H, G, I in parallel

### 11. Chunked Fetch Protocol
- Results are stored server-side after `Submit`; client pulls them with repeated `Fetch` calls
- Avoids a single massive gRPC response (64 MB limit per message)
- Client drives the loop until `is_last == true`

### 12. Dynamic Chunk Sizing
- Chunk size starts at **500 records**, doubles each round-trip: 500 → 1,000 → 2,000 → ... → 50,000
- `chunk_idx` field repurposed as an **absolute record offset** to support variable sizes
- A 3.8M-record result: ~7,757 Fetch calls (fixed 500) → ~80 calls (dynamic) — 97% fewer round-trips
- Directly uses the LAN bandwidth instead of wasting it on RPC overhead

### 13. Python Node (Node I)
- Node I is implemented in **Python** using `grpcio`
- Same scatter-gather logic as C++ nodes — uses `concurrent.futures.ThreadPoolExecutor` for parallel peer calls and concurrent local search
- Demonstrates gRPC language interoperability: Python and C++ nodes talk to each other seamlessly

### 14. Cross-Machine Deployment
- mac1 (10.0.0.180): nodes A, B, C, D, E
- mac2 (10.0.0.100): nodes F, G, H, I
- `run_mac2.sh` automates dependency install, build, proto stub generation, and node startup on mac2
- `config/topology.json` is the single source of truth for all IPs, ports, row ranges, and peer edges

---

## Performance (Benchmark — 30 queries)

| Query Type | Avg Latency |
|-----------|-------------|
| ZIP_RANGE | ~6,218 ms |
| DATE_RANGE | ~906 ms |
| BBOX | ~4,787 ms |
| **Overall** | **~3,970 ms** |

Queries touch all 9 nodes across 2 machines simultaneously, returning up to 3.8M records over the LAN.

---

## Summary Table

| Feature | Mini 1 | Mini 2 |
|---------|--------|--------|
| Processes | 1 | 9 |
| Machines | 1 | 2 |
| Networking | None | gRPC over LAN |
| Data loading | Sequential fread | mmap + 4-thread OpenMP |
| Row seek | O(N) scan | O(1) byte-offset estimation |
| Memory layout | — | Structure-of-Arrays |
| Search | Sequential | 4-thread OpenMP parallel |
| Peer calls | — | Parallel std::async |
| Local + peer overlap | — | Yes (concurrent std::async) |
| Result delivery | — | Dynamic chunked fetch (500→50,000) |
| Languages | C++ | C++ (A–H) + Python (I) |
| Config | Hardcoded | topology.json driven |