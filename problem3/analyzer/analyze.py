#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import re
import time
from datetime import datetime, timezone
from itertools import combinations
from collections import Counter

def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def tokenize(text: str):
    return re.findall(r"\b[\w-]+\b", text, flags=re.UNICODE)

def jaccard_similarity(doc1_words, doc2_words):
    set1, set2 = set(doc1_words), set(doc2_words)
    inter = set1.intersection(set2)
    union = set1.union(set2)
    return (len(inter) / len(union)) if union else 0.0

def ngrams(tokens, n):
    return [" ".join(tokens[i:i+n]) for i in range(0, max(0, len(tokens)-n+1))]

def avg(nums):
    return (sum(nums) / len(nums)) if nums else 0.0

def main():
    shared = "/shared"
    processed_dir = os.path.join(shared, "processed")
    analysis_dir = os.path.join(shared, "analysis")
    status_dir = os.path.join(shared, "status")

    os.makedirs(analysis_dir, exist_ok=True)
    os.makedirs(status_dir, exist_ok=True)

    proc_done = os.path.join(status_dir, "process_complete.json")
    while not os.path.exists(proc_done):
        print(f"[{utc_now()}] Analyzer waiting for {proc_done} ...", flush=True)
        time.sleep(2)

    files = sorted([fn for fn in os.listdir(processed_dir) if fn.lower().endswith(".json")])
    docs = []
    all_tokens = []
    doc_word_sets = {}
    total_words = 0

    doc_avg_sentence_len = []
    doc_avg_word_len = []

    for fn in files:
        path = os.path.join(processed_dir, fn)
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        docs.append((fn, obj))
        text = obj.get("text", "")
        stats = obj.get("statistics", {})
        tokens = tokenize(text)
        all_tokens.extend(tokens)
        total_words += len(tokens)
        doc_word_sets[fn] = set(t.lower() for t in tokens)

        sentences = re.split(r"[.!?]+", text)
        sentences = [s.strip() for s in sentences if s.strip()]
        sent_lens = [len(tokenize(s)) for s in sentences]
        doc_avg_sentence_len.append(avg(sent_lens))

        doc_avg_word_len.append(avg([len(t) for t in tokens]))

    unique_words = len(set(t.lower() for t in all_tokens))

    counter = Counter(t.lower() for t in all_tokens)
    top_100 = counter.most_common(100)
    top_100_words = []
    for w, cnt in top_100:
        freq = cnt / total_words if total_words else 0.0
        top_100_words.append({"word": w, "count": int(cnt), "frequency": float(round(freq, 6))})

    similarity = []
    for (fn1, _), (fn2, _) in combinations(docs, 2):
        sim = jaccard_similarity(doc_word_sets.get(fn1, []), doc_word_sets.get(fn2, []))
        similarity.append({"doc1": fn1, "doc2": fn2, "similarity": float(round(sim, 6))})

    bigram_counts = Counter()
    trigram_counts = Counter()
    lower_all = [t.lower() for t in all_tokens]
    for bg in ngrams(lower_all, 2):
        bigram_counts[bg] += 1
    for tg in ngrams(lower_all, 3):
        trigram_counts[tg] += 1
    top_bigrams = [{"bigram": k, "count": int(v)} for k, v in bigram_counts.most_common(100)]
    top_trigrams = [{"trigram": k, "count": int(v)} for k, v in trigram_counts.most_common(100)]

    avg_sentence_length = avg(doc_avg_sentence_len)
    avg_word_length = avg(doc_avg_word_len)
    complexity_score = avg_sentence_length * (avg_word_length / 5.0)

    report = {
        "processing_timestamp": utc_now(),
        "documents_processed": len(files),
        "total_words": int(total_words),
        "unique_words": int(unique_words),
        "top_100_words": top_100_words,
        "document_similarity": similarity,
        "top_bigrams": top_bigrams,
        "top_trigrams": top_trigrams,
        "readability": {
            "avg_sentence_length": float(round(avg_sentence_length, 3)),
            "avg_word_length": float(round(avg_word_length, 3)),
            "complexity_score": float(round(complexity_score, 6))
        }
    }

    out_path = os.path.join(analysis_dir, "final_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    with open(os.path.join(status_dir, "analyze_complete.json"), "w", encoding="utf-8") as f:
        json.dump({"timestamp": utc_now(), "output": "analysis/final_report.json"}, f, ensure_ascii=False, indent=2)

    print(f"[{utc_now()}] Analyzer complete", flush=True)

if __name__ == "__main__":
    main()
