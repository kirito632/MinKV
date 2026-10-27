#!/usr/bin/env python3
"""Turn a BEIR-style dataset into the .bin files benchmark_hnsw reads, plus qrels.

Input (SciFact layout):
  <dir>/corpus.jsonl          {"_id", "title", "text"}
  <dir>/queries.jsonl         {"_id", "text"}
  <dir>/qrels/test.tsv        query-id <tab> corpus-id <tab> score

Output:
  <out>_corpus.bin            <int32 rows><int32 dim><float32>, L2-normalised
  <out>_query.bin             same, one row per query that has a judgement
  <out>_qrels.txt             "<query_row> <doc_row> <grade>", row indices in
                              the order of the two .bin files

Embedding model: minishlab/potion-base-8M (256-d, static) via hf-mirror. The
same model must encode both sides - the evaluation is of model+index together,
not of HNSW alone.
"""
import argparse
import json
import os
import struct
import sys

os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
os.environ.setdefault("HF_HUB_DISABLE_XET", "1")

import numpy as np
from model2vec import StaticModel


def dump(path, matrix):
    with open(path, "wb") as handle:
        handle.write(struct.pack("<ii", matrix.shape[0], matrix.shape[1]))
        handle.write(matrix.astype(np.float32).tobytes())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default="/tmp/scifact_dir/scifact")
    parser.add_argument("--out", default="/tmp/scifact")
    args = parser.parse_args()

    with open(os.path.join(args.dir, "corpus.jsonl"), encoding="utf-8") as handle:
        corpus = [json.loads(line) for line in handle if line.strip()]
    with open(os.path.join(args.dir, "queries.jsonl"), encoding="utf-8") as handle:
        queries = {row["_id"]: row["text"] for line in handle if line.strip()
                   for row in [json.loads(line)]}

    qrels = []
    with open(os.path.join(args.dir, "qrels/test.tsv"), encoding="utf-8") as handle:
        next(handle)  # header
        for line in handle:
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 3:
                qrels.append((parts[0], parts[1], int(parts[2])))

    doc_row = {row["_id"]: i for i, row in enumerate(corpus)}
    qids = sorted({qid for qid, _, _ in qrels if qid in queries})
    query_row = {qid: i for i, qid in enumerate(qids)}
    print(f"corpus={len(corpus)} queries_with_qrels={len(qids)} judgements={len(qrels)}")

    corpus_texts = [(row.get("title", "") + ". " + row.get("text", "")).strip()
                    for row in corpus]
    query_texts = [queries[qid] for qid in qids]

    model = StaticModel.from_pretrained("minishlab/potion-base-8M")
    corpus_vecs = model.encode(corpus_texts, show_progress_bar=False)
    query_vecs = model.encode(query_texts, show_progress_bar=False)
    corpus_vecs /= np.clip(np.linalg.norm(corpus_vecs, axis=1, keepdims=True), 1e-8, None)
    query_vecs /= np.clip(np.linalg.norm(query_vecs, axis=1, keepdims=True), 1e-8, None)

    dump(f"{args.out}_corpus.bin", corpus_vecs)
    dump(f"{args.out}_query.bin", query_vecs)

    kept = 0
    with open(f"{args.out}_qrels.txt", "w", encoding="utf-8") as handle:
        for qid, did, score in qrels:
            if qid in query_row and did in doc_row:
                handle.write(f"{query_row[qid]} {doc_row[did]} {score}\n")
                kept += 1
    print(f"wrote {args.out}_corpus.bin {corpus_vecs.shape}, {args.out}_query.bin "
          f"{query_vecs.shape}, {kept} judgements")


main()
