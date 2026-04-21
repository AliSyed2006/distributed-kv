```markdown
# Distributed Log-Structured Storage Engine

A high-performance, concurrent Key-Value storage engine built in Go. It implements a custom Log-Structured Merge (LSM) tree architecture designed for maximum write throughput, zero-data-loss crash recovery, and low-latency disk reads.

## Core Architecture

This engine is built from scratch without external database dependencies, utilizing several enterprise-grade optimizations:

* **LSM Tree Core:** Write-optimized storage utilizing a MemTable (SkipList) and immutable Sorted String Tables (SSTables).
* **Decoupled WAL with Group Commit:** Write-Ahead Log guarantees durability. Concurrent writes are batched and flushed to the NVMe/disk in a single fsync via a Group Commit loop, preventing I/O death spirals.
* **Immutable MemTable Rotation:** Zero-downtime flushes. When the active MemTable fills, it is atomically swapped to an "Immutable" state and flushed to disk in the background, allowing incoming writes to continue uninterrupted.
* **Backpressure Valve:** Safely throttles incoming client requests if background disk flushes cannot keep pace with memory intake, preventing OOM crashes under extreme load.
* **Zero-Copy Block Reads:** `SSTableReader` utilizes `ReadAt` syscalls to pull specific 4KB blocks directly from disk into memory without loading the entire file, acting at near-RAM speeds.
* **K-Way Compaction:** A background worker uses a Min-Heap to continuously merge overlapping SSTables, purging tombstones and optimizing read paths.
* **Integrated Bloom Filters:** Every SSTable writes a serialized probabilistic Bloom filter to its footer, eliminating disk seeks for missing keys.

## Performance Benchmarks

Tested on local hardware via a concurrent gRPC client utilizing a Two-Phase stress test (Write/Read).

**Test Parameters:**
* Payload Size: 8 KB per request
* Concurrency: 100 Workers
* Total Operations: 200,000 PUTs followed by 200,000 GETs

**Results:**
* **Write Throughput:** ~5,300 Req/sec 
* **Write Latency:** ~0.18 ms
* **Read Throughput:** ~74,000 Req/sec
* **Read Latency:** ~0.01 ms
* **Data Loss:** 0 (100% Consistency)
```

## Usage & Benchmarking

You can interact with the engine and run the built-in Two-Phase stress test using the CLI client.

**1. Start the Server**
Ensure your working directory is clean, then boot the engine:
```bash
go run cmd/server/main.go
```

**2. Start the Client**
In a separate terminal, launch the interactive gRPC client:
```bash
go run cmd/client/main.go
```

**3. Run the Workload**
Inside the client prompt, you can run the enterprise stress test by passing the number of concurrent workers and the total request count:
```text
kv-client> STRESS 100 200000
```

You can also interact with the engine manually using standard commands:
```text
kv-client> SET mykey myvalue
kv-client> GET mykey
kv-client> STATS
```

## Project Structure

* `/cmd`: Entry points for the gRPC Server and Client tunnel benchmarks.
* `/internal/storage`: The core LSM engine (WAL, MemTable, SSTables, Compactor).
* `/api/proto`: Protocol Buffer definitions for node-to-node and client-to-node communication.

## Next Phase: Distributed 
The engine is currently being extended from a single-node into a Distributed "Pair VM" Architecture, utilizing Decoupled WAL replication (Primary-Backup) over gRPC for immediate failover.
