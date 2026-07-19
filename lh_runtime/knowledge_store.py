"""Local, version-aware FTS5 retrieval for LH code and documentation."""
from __future__ import annotations

import hashlib
import re
import sqlite3
import time
from pathlib import Path
from typing import Any


class KnowledgeStore:
    """A rebuildable knowledge index; it never stores run state or leases."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "knowledge.sqlite3"
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS documents (
                    document_id TEXT PRIMARY KEY,
                    source_uri TEXT NOT NULL,
                    revision TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    active INTEGER NOT NULL,
                    indexed_at REAL NOT NULL,
                    UNIQUE(source_uri, revision, content_hash)
                );
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id TEXT PRIMARY KEY,
                    document_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                );
                CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
                    chunk_id UNINDEXED,
                    text,
                    tokenize='unicode61'
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _digest(value: str) -> str:
        return "sha256:" + hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def _chunks(content: str, limit: int = 800) -> list[str]:
        paragraphs = [item.strip() for item in re.split(r"\n\s*\n", content) if item.strip()]
        result: list[str] = []
        for paragraph in paragraphs:
            while len(paragraph) > limit:
                cut = paragraph.rfind(" ", 0, limit)
                cut = cut if cut > limit // 2 else limit
                result.append(paragraph[:cut].strip())
                paragraph = paragraph[cut:].strip()
            if paragraph:
                result.append(paragraph)
        return result or [content.strip()] if content.strip() else []

    def ingest(self, *, source_uri: str, revision: str, content: str) -> dict[str, Any]:
        if not source_uri or not revision or not content.strip():
            raise ValueError("source_uri, revision and non-empty content are required")
        content_hash = self._digest(content)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT document_id FROM documents WHERE source_uri = ? AND revision = ? AND content_hash = ? AND active = 1", (source_uri, revision, content_hash)).fetchone()
            if existing is not None:
                conn.execute("COMMIT")
                return {"status": "reused", "document_id": existing["document_id"], "content_hash": content_hash, "chunks": 0}
            prior_ids = [row["document_id"] for row in conn.execute("SELECT document_id FROM documents WHERE source_uri = ? AND active = 1", (source_uri,))]
            for document_id in prior_ids:
                chunk_ids = [row["chunk_id"] for row in conn.execute("SELECT chunk_id FROM chunks WHERE document_id = ?", (document_id,))]
                for chunk_id in chunk_ids:
                    conn.execute("DELETE FROM chunks_fts WHERE chunk_id = ?", (chunk_id,))
                conn.execute("DELETE FROM chunks WHERE document_id = ?", (document_id,))
            conn.execute("UPDATE documents SET active = 0 WHERE source_uri = ?", (source_uri,))
            document_id = self._digest(f"{source_uri}\0{revision}\0{content_hash}")
            conn.execute("INSERT INTO documents VALUES (?, ?, ?, ?, 1, ?)", (document_id, source_uri, revision, content_hash, time.time()))
            chunks = self._chunks(content)
            for ordinal, text in enumerate(chunks, 1):
                chunk_hash = self._digest(text)
                chunk_id = self._digest(f"{document_id}\0{ordinal}\0{chunk_hash}")
                conn.execute("INSERT INTO chunks VALUES (?, ?, ?, ?, ?)", (chunk_id, document_id, ordinal, text, chunk_hash))
                conn.execute("INSERT INTO chunks_fts(chunk_id, text) VALUES (?, ?)", (chunk_id, text))
            conn.execute("COMMIT")
        return {"status": "indexed", "document_id": document_id, "content_hash": content_hash, "chunks": len(chunks)}

    def ingest_or_revise(self, *, source_uri: str, revision: str, content: str) -> dict[str, Any]:
        """Ingest, but restamp instead of rebuild when the content is unchanged.

        An indexer that tags every file with the repo HEAD would otherwise
        re-chunk every file on every commit.  When the active document for a
        source_uri already holds the same content_hash under an older revision,
        only its revision label moves forward; the chunks stay put.
        """
        if not source_uri or not revision or not content.strip():
            raise ValueError("source_uri, revision and non-empty content are required")
        content_hash = self._digest(content)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            active = conn.execute(
                "SELECT document_id, revision FROM documents WHERE source_uri = ? AND content_hash = ? AND active = 1",
                (source_uri, content_hash)).fetchone()
            if active is not None and active["revision"] != revision:
                conflict = conn.execute(
                    "SELECT document_id FROM documents WHERE source_uri = ? AND revision = ? AND content_hash = ? AND active = 0",
                    (source_uri, revision, content_hash)).fetchone()
                if conflict is not None:
                    conn.execute("DELETE FROM documents WHERE document_id = ?", (conflict["document_id"],))
                conn.execute("UPDATE documents SET revision = ?, indexed_at = ? WHERE document_id = ?",
                             (revision, time.time(), active["document_id"]))
                conn.execute("COMMIT")
                return {"status": "revised", "document_id": active["document_id"], "content_hash": content_hash, "chunks": 0}
            conn.execute("COMMIT")
        return self.ingest(source_uri=source_uri, revision=revision, content=content)

    @staticmethod
    def _match_query(query: str) -> str:
        terms = re.findall(r"[^\W_]+", query, flags=re.UNICODE)
        if not terms:
            raise ValueError("query must contain searchable text")
        return " OR ".join(f'"{term.replace(chr(34), "")}"' for term in terms[:12])

    def search(self, query: str, *, revision: str | None = None, max_results: int = 5) -> list[dict[str, Any]]:
        if not 1 <= max_results <= 10:
            raise ValueError("max_results must be an integer from 1 to 10")
        where = ["chunks_fts MATCH ?", "d.active = 1"]
        params: list[Any] = [self._match_query(query)]
        if revision is not None:
            where.append("d.revision = ?")
            params.append(revision)
        params.append(max_results)
        sql = """
            SELECT c.chunk_id, c.ordinal, c.text, c.content_hash, d.source_uri, d.revision,
                   d.content_hash AS document_hash, bm25(chunks_fts) AS score
            FROM chunks_fts
            JOIN chunks c ON c.chunk_id = chunks_fts.chunk_id
            JOIN documents d ON d.document_id = c.document_id
            WHERE """ + " AND ".join(where) + " ORDER BY score, d.source_uri, c.ordinal LIMIT ?"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            {"chunk_id": row["chunk_id"], "source_uri": row["source_uri"], "revision": row["revision"],
             "document_hash": row["document_hash"], "chunk_hash": row["content_hash"], "ordinal": row["ordinal"],
             "score": row["score"], "text": row["text"]}
            for row in rows
        ]

    def summary(self) -> dict[str, int]:
        with self._connect() as conn:
            documents = conn.execute("SELECT COUNT(*) FROM documents WHERE active = 1").fetchone()[0]
            chunks = conn.execute("SELECT COUNT(*) FROM chunks c JOIN documents d ON d.document_id = c.document_id WHERE d.active = 1").fetchone()[0]
        return {"active_documents": documents, "active_chunks": chunks}
