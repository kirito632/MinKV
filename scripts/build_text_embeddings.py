#!/usr/bin/env python3
"""Build a real text-embedding dataset, from this repository or an external tree.

Two sources:

  --source repo     the repository's own markdown (small; the original experiment)
  --source stdlib   the Python standard library sources and docstrings
                    (independent of MinKV, ~30 MB of technical English)

Chunking is 400-character windows with a 200-character stride, which is what a
RAG pipeline does. Every tenth chunk becomes a query, so queries are real text
the index has never seen. Vectors are L2-normalised, so L2 ordering equals
cosine ordering - the metric the graph layer uses.

When --max-chunks is set the pool is deterministically shuffled before the
query split, so a 10K set is a prefix of a 50K set built from the same seed and
the two sizes are nested rather than independent samples.

Output format: <int32 rows><int32 dim><float32 payload>.
"""
import argparse
import glob
import os
import struct
import sys

os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
os.environ.setdefault("HF_HUB_DISABLE_XET", "1")

import numpy as np
from model2vec import StaticModel

WINDOW = 400
STRIDE = 200
MAX_QUERIES = 200


def repo_files(root):
    patterns = ["*.md", "docs/**/*.md", "plans/*.md", "mcp_server/*.md", ".instructions/*.md"]
    paths = []
    for pattern in patterns:
        paths.extend(glob.glob(os.path.join(root, pattern), recursive=True))
    return sorted(set(paths))


def stdlib_files(prefix):
    return sorted(glob.glob(os.path.join(prefix, "**", "*.py"), recursive=True))


def chunk_file(path, window, stride):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            text = handle.read()
    except OSError:
        return []
    text = " ".join(text.split())
    out = []
    for start in range(0, max(0, len(text) - window // 2), stride):
        piece = text[start:start + window]
        if len(piece.strip()) >= window // 2:
            out.append(piece)
    return out


def dump(path, matrix):
    with open(path, "wb") as handle:
        handle.write(struct.pack("<ii", matrix.shape[0], matrix.shape[1]))
        handle.write(matrix.astype(np.float32).tobytes())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["repo", "stdlib"], default="repo")
    parser.add_argument("--root", default="/home/robinson/cppworks/MinKV")
    parser.add_argument("--stdlib-prefix", default="/usr/lib/python3.12")
    parser.add_argument("--max-chunks", type=int, default=0,
                        help="0 keeps everything; otherwise deterministically subsample")
    parser.add_argument("--out-prefix", default="/tmp/text")
    args = parser.parse_args()

    if args.source == "repo":
        paths = repo_files(args.root)
    else:
        paths = stdlib_files(args.stdlib_prefix)
    print(f"scanning {len(paths)} files from {args.source}")

    chunks = []
    for path in paths:
        chunks.extend(chunk_file(path, WINDOW, STRIDE))
    print(f"chunks before subsampling: {len(chunks)}")
    if not chunks:
        sys.exit("no text found")

    # Deterministic order, so a smaller size is a prefix of a larger one.
    rng = np.random.default_rng(20261026)
    order = rng.permutation(len(chunks))
    chunks = [chunks[i] for i in order]

    if args.max_chunks and len(chunks) > args.max_chunks:
        # Reserve room for the held-out queries.
        chunks = chunks[: int(args.max_chunks * 10 / 9)]
    print(f"chunks used: {len(chunks)}")

    queries = [c for i, c in enumerate(chunks) if i % 10 == 0][:MAX_QUERIES]
    corpus = [c for i, c in enumerate(chunks) if i % 10 != 0]
    print(f"corpus={len(corpus)} queries={len(queries)}")

    model = StaticModel.from_pretrained("minishlab/potion-base-8M")
    corpus_vecs = model.encode(corpus, show_progress_bar=False)
    query_vecs = model.encode(queries, show_progress_bar=False)
    corpus_vecs /= np.clip(np.linalg.norm(corpus_vecs, axis=1, keepdims=True), 1e-8, None)
    query_vecs /= np.clip(np.linalg.norm(query_vecs, axis=1, keepdims=True), 1e-8, None)
    print("dim =", corpus_vecs.shape[1])

    dump(f"{args.out_prefix}_corpus.bin", corpus_vecs)
    dump(f"{args.out_prefix}_query.bin", query_vecs)
    print(f"wrote {args.out_prefix}_corpus.bin {corpus_vecs.shape} "
          f"and {args.out_prefix}_query.bin {query_vecs.shape}")


main()
