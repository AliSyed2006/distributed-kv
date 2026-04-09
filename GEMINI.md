# Distributed KV Store
**State of the System - April 2026**

## 1. System Architecture (LSM-Tree)
High-performance, disk-persistent Key-Value store modeled after industry standards (RocksDB/LevelDB).

### **The Write Path (Persistence & Speed)**
1.  **gRPC Gateway:** Entry point for client requests.
2.  **WAL (Write-Ahead Log):** Optimized via **Group Commits**. Collects writes into batches of **200 entries** or a **10ms timeout** before calling `fsync`. This amortizes disk seek costs.
3.  **MemTable:** Thread-safe **SkipList** in RAM. Threshold: **64MB**.
4.  **Flush:** Upon reaching the threshold, the MemTable is frozen and written to disk as a Level-0 SSTable.

### **The Read Path (Hierarchy)**
1.  **MemTable Search:** $O(\log N)$ search in the active SkipList.
2.  **SSTable Search (Newest to Oldest):**
    * **Bloom Filter:** $O(1)$ check in RAM to skip files that definitely don't contain the key.
    * **Sparse Index:** Binary search in RAM to locate the 4KB data block offset.
    * **Disk Seek:** Exactly one targeted read of the specific 4KB block.

---

## 2. File Manifest (The "Map")

| Path | Responsibility | Key Logic |
| :--- | :--- | :--- |
| `api/proto/kv.proto` | **IDL Contract** | Service definition (Get/Put/Del/Stats) and message buffers. |
| `cmd/server/main.go` | **The Brain** | Engine init, background worker boot, gRPC listener (:50051). |
| `cmd/client/main.go` | **The Hammer** | Benchmarking tool (STRESS command) + Interactive REPL. |
| `internal/storage/engine.go` | **The Conductor** | Manages lifecycle, background worker, and **Atomic Swaps**. |
| `internal/storage/wal.go` | **The Vault** | Group Commit batcher logic using channels and timers. |
| `internal/storage/memtable.go` | **The Workspace** | Concurrent SkipList implementation. |
| `internal/storage/sstable.go` | **The Library** | 4KB block storage with binary Footers and Sparse Index. |
| `internal/storage/bloom.go` | **The Gatekeeper** | Probabilistic bitset for $O(1)$ negative lookups. |
| `internal/storage/compaction.go` | **The Janitor** | **K-Way Merge Sort** to merge L0 files and purge tombstones. |
| `internal/network/server.go` | **The Bridge** | Maps gRPC service to `storage.StorageEngine` methods. |

---

## 3. Technical Specifications

* **Concurrency:** `sync.RWMutex` at the Engine level; dedicated goroutines for Compaction and WAL Batching.
* **Binary Contract:** `binary.LittleEndian` used throughout for cross-platform compatibility.
* **Storage Geometry:** 4KB data blocks; 24-byte Footer for metadata pointers (Bloom/Index offsets).
* **Deletion Strategy:** `nil` values act as "Tombstones." Physical eviction occurs only during the Compaction merge.

---

## 4. Current Roadmap
- [x] **Phase 1.1:** Core LSM primitives (WAL, MemTable, SSTable).
- [x] **Phase 1.2:** Performance Tuning (Bloom Filters, Group Commits).
- [x] **Phase 1.3:** Automation (Background Compaction at 4+ files).
- [x] **Phase 1.4:** Network Layer (gRPC + High-speed Benchmarking Client).
- [ ] **Phase 2.1: The Manifest File** (Persisting SSTable metadata to avoid directory scanning on boot).
- [ ] **Phase 2.2: Replication (Raft)** (Moving from a single node to a distributed cluster).

---

### **Engineering Principles for this Project**
* **Zero-Copy Focus:** Aim for minimal byte allocation to reduce GC pressure.
* **HFT Ready:** Every millisecond counts; prefer binary serialization over JSON/Text.
* **Safety:** The WAL is the source of truth. A request is only successful if it is physically synced to disk.