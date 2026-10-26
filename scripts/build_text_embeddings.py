#!/usr/bin/env python3
"""Build a real text-embedding dataset from the repository's own prose.

Corpus: every markdown file in the repo (README, AGENTS, docs/, plans/, mcp_server/).
Chunking: 400-character windows with a 200-character stride, which is what a RAG
pipeline actually does (overlapping chunks). Every tenth chunk is held out and
used as a query, so queries are real text the index has never seen but which
has genuinely similar neighbours in the corpus.

Embedding: minishlab/potion-base-8M (static, 256-d) via hf-mirror. Vectors are
L2-normalised, so L2 ordering equals cosine ordering - matching the metric the
graph layer uses.

Output: <int32 rows><int32 dim><float32 payload>, the format
`benchmark_hnsw real` reads.
"""
import glob
import os
import struct
import sys

os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
os.environ.setdefault("HF_HUB_DISABLE_XET", "1")

import numpy as np
from model2vec import StaticModel

ROOT = "/home/robinson/cppworks/MinKV"
WINDOW = 400
STRIDE = 200
MAX_QUERIES = 200


def collect_text():
    patterns = [
        "*.md",
        "docs/**/*.md",
        "plans/*.md",
        "mcp_server/*.md",
        ".instructions/*.md",
        "docs/tests*/*.md",
    ]
    paths = []
    for pattern in patterns:
        paths.extend(glob.glob(os.path.join(ROOT, pattern), recursive=True))
    paths = sorted(set(paths))
    chunks = []
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as handle:
                text = handle.read()
        except OSError:
            continue
        text = " ".join(text.split())
        for start in range(0, max(0, len(text) - WINDOW // 2), STRIDE):
            piece = text[start:start + WINDOW].strip()
            if len(piece) >= WINDOW // 2:
                chunks.append(piece)
    return paths, chunks


def dump(path, matrix):
    with open(path, "wb") as handle:
        handle.write(struct.pack("<ii", matrix.shape[0], matrix.shape[1]))
        handle.write(matrix.astype(np.float32).tobytes())


def main():
    paths, chunks = collect_text()
    print(f"read {len(paths)} markdown files -> {len(chunks)} chunks")
    if len(chunks) < 500:
        sys.exit("corpus too small")

    queries = [chunks[i] for i in range(len(chunks)) if i % 10 == 0][:MAX_QUERIES]
    corpus = [c for i, c in enumerate(chunks) if i % 10 != 0]
    print(f"corpus={len(corpus)} queries={len(queries)}")

    model = StaticModel.from_pretrained("minishlab/potion-base-8M")
    corpus_vecs = model.encode(corpus, show_progress_bar=False)
    query_vecs = model.encode(queries, show_progress_bar=False)
    corpus_vecs = corpus_vecs / np.clip(np.linalg.norm(corpus_vecs, axis=1, keepdims=True), 1e-8, None)
    query_vecs = query_vecs / np.clip(np.linalg.norm(query_vecs, axis=1, keepdims=True), 1e-8, None)
    print("embedding dim =", corpus_vecs.shape[1])

    dump("/tmp/text_corpus.bin", corpus_vecs)
    dump("/tmp/text_query.bin", query_vecs)
    print("wrote /tmp/text_corpus.bin", corpus_vecs.shape, "and /tmp/text_query.bin", query_vecs.shape)


main()
