# Merkle DAG Store

A local Merkle DAG store with content-addressable block storage in Python. Provides immutable, deterministic, block-addressed object storage with DAG linking, snapshotting, and garbage collection.

## Features

- **Content Addressing**: Multiple hash algorithms (SHA256, SHA512, BLAKE2b)
- **DAG Linking**: Create parent-child relationships between blocks
- **Snapshotting**: Tag and version DAG states for time-travel
- **Garbage Collection**: Remove unreachable blocks from storage
- **Integrity Verification**: Recursive DAG verification
- **DAG Traversal**: BFS traversal to discover reachable blocks
- **SQLite Index**: Efficient metadata indexing and access statistics
- **Sharded Storage**: Filesystem sharding for performance at scale

## Installation

```bash
pip install merkle-dag-store
```

## Usage

```python
from merkle_dag_store import MerkleDAGStore

store = MerkleDAGStore("./my_dag")

# Store blocks
cid1 = store.put(b"Hello, World!")
cid2 = store.put(b"Linked data")

# Create linked block
cid3 = store.put(b"Parent", links=[str(cid1), str(cid2)])

# Create snapshot
store.snapshot(str(cid3), tag="v1.0")

# Verify integrity
assert store.verify(str(cid3), recursive=True)
```

## License

MIT
