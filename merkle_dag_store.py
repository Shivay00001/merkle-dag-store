"""
Local Merkle DAG Store - Content-Addressable Block Storage
A minimal, embeddable, deterministic block-addressed local object store.
"""

import hashlib
import json
import os
import sqlite3
import threading
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Set
from enum import Enum


class CIDVersion(Enum):
    V0 = 0
    V1 = 1


class HashAlgorithm(Enum):
    SHA256 = "sha256"
    SHA512 = "sha512"
    BLAKE2B = "blake2b"


@dataclass
class CID:
    """Content Identifier"""
    version: int
    codec: str
    hash_algo: str
    digest: bytes
    
    def __str__(self) -> str:
        """Generate human-readable CID string"""
        if self.version == 0:
            # V0: simple base58-encoded SHA256
            return f"Qm{self.digest.hex()[:46]}"
        else:
            # V1: multibase encoded
            prefix = f"ba{self.codec}"
            return f"{prefix}{self.digest.hex()}"
    
    @staticmethod
    def from_bytes(data: bytes, algo: HashAlgorithm = HashAlgorithm.SHA256) -> 'CID':
        """Generate CID from raw bytes"""
        if algo == HashAlgorithm.SHA256:
            digest = hashlib.sha256(data).digest()
        elif algo == HashAlgorithm.SHA512:
            digest = hashlib.sha512(data).digest()
        else:  # BLAKE2B
            digest = hashlib.blake2b(data).digest()
        
        return CID(
            version=1,
            codec="raw",
            hash_algo=algo.value,
            digest=digest
        )


@dataclass
class Block:
    """Immutable content block"""
    cid: CID
    data: bytes
    size: int
    links: List[str]  # CIDs of linked blocks
    created_at: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'cid': str(self.cid),
            'size': self.size,
            'links': self.links,
            'created_at': self.created_at.isoformat()
        }


@dataclass
class Snapshot:
    """Versioned snapshot of DAG state"""
    root_cid: str
    timestamp: datetime
    metadata: Dict[str, Any]
    tag: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'root_cid': self.root_cid,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata,
            'tag': self.tag
        }


class MerkleDAGStore:
    """
    Local Merkle DAG Store with content-addressing and immutable blocks.
    Thread-safe, crash-resistant, with atomic operations.
    """
    
    def __init__(self, storage_path: str, hash_algo: HashAlgorithm = HashAlgorithm.SHA256):
        self.storage_path = Path(storage_path)
        self.hash_algo = hash_algo
        self.lock = threading.RLock()
        
        # Create storage directories
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.blocks_path = self.storage_path / "blocks"
        self.blocks_path.mkdir(exist_ok=True)
        
        # Initialize SQLite index
        self.db_path = self.storage_path / "index.db"
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database for metadata and indexing"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS blocks (
                    cid TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    hash_algo TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    access_count INTEGER DEFAULT 0,
                    last_accessed TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS links (
                    parent_cid TEXT NOT NULL,
                    child_cid TEXT NOT NULL,
                    link_index INTEGER NOT NULL,
                    PRIMARY KEY (parent_cid, child_cid, link_index),
                    FOREIGN KEY (parent_cid) REFERENCES blocks(cid),
                    FOREIGN KEY (child_cid) REFERENCES blocks(cid)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    root_cid TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    tag TEXT UNIQUE,
                    metadata TEXT,
                    FOREIGN KEY (root_cid) REFERENCES blocks(cid)
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_links_parent ON links(parent_cid)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_links_child ON links(child_cid)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_tag ON snapshots(tag)")
            conn.commit()
    
    def _block_file_path(self, cid: str) -> Path:
        """Generate file path for block storage using sharding"""
        # Shard blocks into subdirectories for filesystem performance
        prefix = cid[:4]
        shard_dir = self.blocks_path / prefix
        shard_dir.mkdir(exist_ok=True)
        return shard_dir / cid
    
    def put(self, data: bytes, links: Optional[List[str]] = None) -> CID:
        """
        Store a block with content-addressing.
        Returns CID for the stored block.
        """
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")
        
        # Generate CID
        cid = CID.from_bytes(data, self.hash_algo)
        cid_str = str(cid)
        
        with self.lock:
            # Check if block already exists (deduplication)
            if self._block_exists(cid_str):
                return cid
            
            # Store block data
            block_path = self._block_file_path(cid_str)
            with open(block_path, 'wb') as f:
                f.write(data)
                # Ensure durability
                os.fsync(f.fileno())
            
            # Update index
            now = datetime.utcnow().isoformat()
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    INSERT INTO blocks (cid, size, hash_algo, created_at)
                    VALUES (?, ?, ?, ?)
                """, (cid_str, len(data), self.hash_algo.value, now))
                
                # Store links if provided
                if links:
                    for idx, child_cid in enumerate(links):
                        if not self._block_exists(child_cid):
                            raise ValueError(f"Linked block {child_cid} does not exist")
                        conn.execute("""
                            INSERT INTO links (parent_cid, child_cid, link_index)
                            VALUES (?, ?, ?)
                        """, (cid_str, child_cid, idx))
                
                conn.commit()
        
        return cid
    
    def get(self, cid: str) -> Optional[bytes]:
        """Retrieve block data by CID"""
        with self.lock:
            if not self._block_exists(cid):
                return None
            
            # Update access statistics
            now = datetime.utcnow().isoformat()
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    UPDATE blocks 
                    SET access_count = access_count + 1,
                        last_accessed = ?
                    WHERE cid = ?
                """, (now, cid))
                conn.commit()
            
            # Read block data
            block_path = self._block_file_path(cid)
            try:
                with open(block_path, 'rb') as f:
                    return f.read()
            except FileNotFoundError:
                return None
    
    def link(self, parent_cid: str, child_cid: str) -> bool:
        """Create a link between parent and child blocks"""
        with self.lock:
            if not self._block_exists(parent_cid):
                raise ValueError(f"Parent block {parent_cid} does not exist")
            if not self._block_exists(child_cid):
                raise ValueError(f"Child block {child_cid} does not exist")
            
            with sqlite3.connect(str(self.db_path)) as conn:
                # Get current max link index
                cursor = conn.execute("""
                    SELECT COALESCE(MAX(link_index), -1) + 1
                    FROM links
                    WHERE parent_cid = ?
                """, (parent_cid,))
                next_index = cursor.fetchone()[0]
                
                conn.execute("""
                    INSERT OR IGNORE INTO links (parent_cid, child_cid, link_index)
                    VALUES (?, ?, ?)
                """, (parent_cid, child_cid, next_index))
                conn.commit()
            
            return True
    
    def get_links(self, cid: str) -> List[str]:
        """Get all child CIDs linked from a block"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT child_cid
                FROM links
                WHERE parent_cid = ?
                ORDER BY link_index
            """, (cid,))
            return [row[0] for row in cursor.fetchall()]
    
    def get_block_info(self, cid: str) -> Optional[Dict[str, Any]]:
        """Get metadata about a block"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT cid, size, hash_algo, created_at, access_count, last_accessed
                FROM blocks
                WHERE cid = ?
            """, (cid,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return {
                'cid': row[0],
                'size': row[1],
                'hash_algo': row[2],
                'created_at': row[3],
                'access_count': row[4],
                'last_accessed': row[5],
                'links': self.get_links(cid)
            }
    
    def snapshot(self, root_cid: str, metadata: Optional[Dict[str, Any]] = None, tag: Optional[str] = None) -> int:
        """Create a versioned snapshot of the DAG"""
        if not self._block_exists(root_cid):
            raise ValueError(f"Root block {root_cid} does not exist")
        
        with self.lock:
            now = datetime.utcnow().isoformat()
            metadata_json = json.dumps(metadata or {})
            
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute("""
                    INSERT INTO snapshots (root_cid, timestamp, tag, metadata)
                    VALUES (?, ?, ?, ?)
                """, (root_cid, now, tag, metadata_json))
                conn.commit()
                return cursor.lastrowid
    
    def get_snapshot(self, tag: str) -> Optional[Snapshot]:
        """Retrieve snapshot by tag"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT root_cid, timestamp, metadata, tag
                FROM snapshots
                WHERE tag = ?
            """, (tag,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return Snapshot(
                root_cid=row[0],
                timestamp=datetime.fromisoformat(row[1]),
                metadata=json.loads(row[2]),
                tag=row[3]
            )
    
    def list_snapshots(self, limit: int = 100) -> List[Snapshot]:
        """List recent snapshots"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT root_cid, timestamp, metadata, tag
                FROM snapshots
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            
            return [
                Snapshot(
                    root_cid=row[0],
                    timestamp=datetime.fromisoformat(row[1]),
                    metadata=json.loads(row[2]),
                    tag=row[3]
                )
                for row in cursor.fetchall()
            ]
    
    def verify(self, cid: str, recursive: bool = False) -> bool:
        """
        Verify block integrity by recalculating hash.
        If recursive=True, verify entire DAG from this root.
        """
        data = self.get(cid)
        if data is None:
            return False
        
        # Recalculate CID
        recalculated_cid = CID.from_bytes(data, self.hash_algo)
        if str(recalculated_cid) != cid:
            return False
        
        if recursive:
            # Verify all linked blocks
            links = self.get_links(cid)
            for child_cid in links:
                if not self.verify(child_cid, recursive=True):
                    return False
        
        return True
    
    def traverse_dag(self, root_cid: str) -> Set[str]:
        """
        Traverse entire DAG from root and return all reachable CIDs.
        Uses breadth-first search.
        """
        visited = set()
        queue = [root_cid]
        
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            
            visited.add(current)
            links = self.get_links(current)
            queue.extend(links)
        
        return visited
    
    def gc(self, keep_snapshots: int = 10) -> int:
        """
        Garbage collect unreachable blocks.
        Keeps blocks reachable from recent snapshots.
        Returns number of blocks removed.
        """
        with self.lock:
            # Get recent snapshot roots
            snapshots = self.list_snapshots(limit=keep_snapshots)
            protected_cids = set()
            
            for snapshot in snapshots:
                protected_cids.update(self.traverse_dag(snapshot.root_cid))
            
            # Find all blocks
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute("SELECT cid FROM blocks")
                all_cids = {row[0] for row in cursor.fetchall()}
            
            # Determine garbage
            garbage_cids = all_cids - protected_cids
            
            # Remove garbage blocks
            removed_count = 0
            for cid in garbage_cids:
                if self._remove_block(cid):
                    removed_count += 1
            
            return removed_count
    
    def _block_exists(self, cid: str) -> bool:
        """Check if block exists in store"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("SELECT 1 FROM blocks WHERE cid = ?", (cid,))
            return cursor.fetchone() is not None
    
    def _remove_block(self, cid: str) -> bool:
        """Remove a block from storage (used by GC)"""
        try:
            # Remove from filesystem
            block_path = self._block_file_path(cid)
            if block_path.exists():
                block_path.unlink()
            
            # Remove from database
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("DELETE FROM links WHERE parent_cid = ? OR child_cid = ?", (cid, cid))
                conn.execute("DELETE FROM blocks WHERE cid = ?", (cid,))
                conn.commit()
            
            return True
        except Exception:
            return False
    
    def stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as block_count,
                    SUM(size) as total_size,
                    AVG(size) as avg_size
                FROM blocks
            """)
            block_stats = cursor.fetchone()
            
            cursor = conn.execute("SELECT COUNT(*) FROM links")
            link_count = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM snapshots")
            snapshot_count = cursor.fetchone()[0]
        
        return {
            'block_count': block_stats[0] or 0,
            'total_size_bytes': block_stats[1] or 0,
            'avg_block_size': block_stats[2] or 0,
            'link_count': link_count,
            'snapshot_count': snapshot_count,
            'hash_algorithm': self.hash_algo.value
        }


# Example usage and tests
if __name__ == "__main__":
    # Initialize store
    store = MerkleDAGStore("./test_dag_store")
    
    # Store some blocks
    cid1 = store.put(b"Hello, World!")
    print(f"Stored block: {cid1}")
    
    cid2 = store.put(b"Merkle DAG is awesome!")
    print(f"Stored block: {cid2}")
    
    # Create linked block
    cid3 = store.put(b"Parent block", links=[str(cid1), str(cid2)])
    print(f"Stored linked block: {cid3}")
    
    # Retrieve data
    data = store.get(str(cid1))
    print(f"Retrieved: {data.decode()}")
    
    # Get block info
    info = store.get_block_info(str(cid3))
    print(f"Block info: {info}")
    
    # Create snapshot
    snapshot_id = store.snapshot(str(cid3), metadata={"version": "1.0"}, tag="v1.0")
    print(f"Created snapshot: {snapshot_id}")
    
    # Verify integrity
    is_valid = store.verify(str(cid3), recursive=True)
    print(f"DAG verification: {is_valid}")
    
    # Traverse DAG
    reachable = store.traverse_dag(str(cid3))
    print(f"Reachable blocks: {reachable}")
    
    # Get stats
    stats = store.stats()
    print(f"Store stats: {stats}")
