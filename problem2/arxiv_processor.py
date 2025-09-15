#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
import json
import re
import time
from datetime import datetime, timezone
from urllib import request as urlrequest
import xml.etree.ElementTree as ET

# Constants & stopwords
BASE_URL = "http://export.arxiv.org/api/query"
ATOM = "{http://www.w3.org/2005/Atom}"

STOPWORDS = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
             'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
             'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
             'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
             'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it',
             'we', 'they', 'what', 'which', 'who', 'when', 'where', 'why', 'how',
             'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other', 'some',
             'such', 'as', 'also', 'very', 'too', 'only', 'so', 'than', 'not'}

# Small utilities
def utc_now_iso() -> str:
    """UTC ISO-8601 with 'Z' suffix, millisecond precision."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def log_line(log_path: str, msg: str):
    line = f"[{utc_now_iso()}] {msg}"
    # append to log file
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    # also print to stdout for docker logs
    print(line, flush=True)

def percent_encode_min(s: str) -> str:
    """
    Minimal percent-encoding without urllib.parse (since it's not listed).
    Keep safe chars: A-Z a-z 0-9 -_.~:/
    """
    safe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~:/"
    out = []
    for c in s:
        out.append(c if c in safe else "%%%02X" % ord(c))
    return "".join(out)

def tokenize_words(text: str):
    """
    Tokenize words (unicode letters/digits + allow hyphen).
    Case-insensitive stats use .lower() on tokens.
    """
    return re.findall(r"\b[\w-]+\b", text, flags=re.UNICODE)

def sentence_split(text: str):
    parts = re.split(r"[.!?]+", text)
    return [p.strip() for p in parts if p.strip()]

def extract_terms_upper(text: str):
    return set(re.findall(r"\b(?=[\w-]*[A-Z])[\w-]+\b", text))

def extract_terms_numeric(text: str):
    return set(re.findall(r"\b(?=[\w-]*\d)[\w-]+\b", text))

def extract_terms_hyphen(text: str):
    return set(re.findall(r"\b\w+(?:-\w+)+\b", text))

def avg(nums):
    return (sum(nums) / len(nums)) if nums else 0.0

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

# HTTP with rate limiting
def rate_limited_get(url: str, headers: dict, timeout: int, log_path: str, max_attempts: int = 3):
    """
    Perform GET with at least 3s between attempts.
    On HTTP 429: sleep 3s and retry (up to max_attempts).
    On other errors, raise; caller will handle and exit 1.
    """
    last_attempt = 0.0
    for attempt in range(1, max_attempts + 1):
        now = time.time()
        wait = max(0.0, 3.0 - (now - last_attempt)) if last_attempt > 0 else 0.0
        if wait > 0:
            time.sleep(wait)
        last_attempt = time.time()

        req = urlrequest.Request(url, headers=headers)
        try:
            with urlrequest.urlopen(req, timeout=timeout) as resp:
                status = getattr(resp, "status", None) or resp.getcode()
                content = resp.read()
                return status, dict(resp.headers.items()), content
        except Exception as ex:
            code = getattr(ex, "code", None)  # HTTPError has .code
            if code == 429 and attempt < max_attempts:
                log_line(log_path, f"Received 429 (rate limit). Retrying after 3s... (attempt {attempt}/{max_attempts})")
                time.sleep(3.0)
                continue
            # non-429 (or attempts exhausted): re-raise
            raise


# Per-abstract stats
def abstract_stats(abstract_text: str):
    tokens = tokenize_words(abstract_text)
    tokens_lower = [t.lower() for t in tokens]

    total_words = len(tokens_lower)
    unique_words = len(set(tokens_lower))
    avg_word_length = avg([len(t) for t in tokens_lower])

    sentences = sentence_split(abstract_text)
    sent_word_counts = [len(tokenize_words(s)) for s in sentences]
    total_sentences = len(sentences)
    avg_words_per_sentence = avg(sent_word_counts)

    # Top-20 words (excluding stopwords) - computed to satisfy Part B,
    # but NOT emitted in papers.json (schema disallows extra fields).
    from collections import Counter
    cnt = Counter(t for t in tokens_lower if t not in STOPWORDS)
    top20 = cnt.most_common(20)

    # Longest/shortest sentence - computed (not emitted in papers.json)
    longest_len = max(sent_word_counts) if sent_word_counts else 0
    shortest_len = min(sent_word_counts) if sent_word_counts else 0

    stats_for_output = {
        "total_words": int(total_words),
        "unique_words": int(unique_words),
        "total_sentences": int(total_sentences),
        "avg_words_per_sentence": float(round(avg_words_per_sentence, 3)),
        "avg_word_length": float(round(avg_word_length, 3)),
    }

    # Return extra items for corpus aggregation if needed
    return stats_for_output, tokens_lower, top20, longest_len, shortest_len


# Main
def main():
    # args:
    if len(sys.argv) != 4:
        print("Usage: arxiv_processor.py <query> <max_results 1..100> <output_dir>", file=sys.stderr)
        sys.exit(2)

    query = sys.argv[1]
    try:
        max_results = int(sys.argv[2])
    except ValueError:
        print("Error: max_results must be an integer", file=sys.stderr)
        sys.exit(2)
    if not (1 <= max_results <= 100):
        print("Error: max_results must be between 1 and 100", file=sys.stderr)
        sys.exit(2)

    output_dir = sys.argv[3]

    if output_dir is None:
        print("Error: missing output_dir", file=sys.stderr)
        sys.exit(2)

    ensure_dir(output_dir)
    papers_json_path = os.path.join(output_dir, "papers.json")
    corpus_json_path = os.path.join(output_dir, "corpus_analysis.json")
    log_path = os.path.join(output_dir, "processing.log")

    t0 = time.time()
    log_line(log_path, f"Starting ArXiv query: {query}")


    qs = f"search_query={percent_encode_min(query)}&start=0&max_results={max_results}"
    url = BASE_URL + "?" + qs
    headers = {
        "User-Agent": "EE547-ArXivProcessor/1.0",
        "Accept": "application/atom+xml",
    }

    try:
        status, resp_headers, content = rate_limited_get(url, headers, timeout=15, log_path=log_path)
    except Exception as ex:
        log_line(log_path, f"Network error: {ex}")
        sys.exit(1)

    if status != 200:
        log_line(log_path, f"HTTP status {status} from ArXiv API")
        sys.exit(1)

    # parse XML
    try:
        root = ET.fromstring(content)
    except ET.ParseError as ex:
        # whole feed invalid -> cannot continue per-entry; exit 1
        log_line(log_path, f"Invalid XML: {ex}")
        sys.exit(1)

    entries = root.findall(ATOM + "entry")
    log_line(log_path, f"Fetched {len(entries)} results from ArXiv API")

    # iterate entries
    papers_out = []

    # corpus accumulators
    total_words_all = 0
    abstract_lengths = []
    global_vocab = set()
    df_counts = {}   # document frequency
    tf_counts = {}   # term frequency (stopwords excluded for top-N)
    uppercase_terms = set()
    numeric_terms = set()
    hyphen_terms = set()
    category_distribution = {}

    for entry in entries:
        try:
            def get_text(tag):
                el = entry.find(ATOM + tag)
                return el.text if (el is not None and el.text) else None

            eid_full = get_text("id")
            title = get_text("title")
            summary = get_text("summary")
            published = get_text("published")
            updated = get_text("updated")

            # authors
            authors = []
            for a in entry.findall(ATOM + "author"):
                name_el = a.find(ATOM + "name")
                if name_el is not None and name_el.text:
                    authors.append(name_el.text.strip())

            # categories + distribution
            categories = []
            for c in entry.findall(ATOM + "category"):
                term = c.attrib.get("term")
                if term:
                    term = term.strip()
                    categories.append(term)
                    category_distribution[term] = category_distribution.get(term, 0) + 1

            # required fields
            if not eid_full or not summary or not title:
                log_line(log_path, f"Warning: missing fields, skipping one paper (id={eid_full})")
                continue

            arxiv_id = eid_full.rsplit("/", 1)[-1]
            log_line(log_path, f"Processing paper: {arxiv_id}")

            title_norm = normalize_ws(title)
            abstract_norm = normalize_ws(summary)

            # per-abstract stats
            stats_out, tokens_lower, top20_local, longest_len, shortest_len = abstract_stats(abstract_norm)

            # corpus-level vocab / counts
            total_words_all += stats_out["total_words"]
            abstract_lengths.append(stats_out["total_words"])
            # doc freq
            for t in set(tokens_lower):
                df_counts[t] = df_counts.get(t, 0) + 1
            # term freq (exclude stopwords for top-N)
            for t in tokens_lower:
                if t not in STOPWORDS:
                    tf_counts[t] = tf_counts.get(t, 0) + 1
            global_vocab.update(tokens_lower)

            # technical terms (preserve original case in corpus output)
            uppercase_terms.update(extract_terms_upper(abstract_norm))
            numeric_terms.update(extract_terms_numeric(abstract_norm))
            hyphen_terms.update(extract_terms_hyphen(abstract_norm))

            # emit paper object (schema-limited)
            papers_out.append({
                "arxiv_id": arxiv_id,
                "title": title_norm,
                "authors": authors,
                "abstract": abstract_norm,
                "categories": categories,
                "published": (published or "").replace("+00:00", "Z"),
                "updated": (updated or "").replace("+00:00", "Z"),
                "abstract_stats": stats_out
            })

        except Exception as ex_entry:
            # Any per-entry parse/logic error -> skip this paper
            log_line(log_path, f"Warning: error processing an entry; skipping. Details: {ex_entry}")
            continue

    # ---- write papers.json (even if empty list) ----
    with open(papers_json_path, "w", encoding="utf-8") as f:
        json.dump(papers_out, f, ensure_ascii=False, indent=2)

    # ---- corpus_analysis.json ----
    papers_processed = len(papers_out)
    if papers_processed == 0:
        corpus = {
            "query": query,
            "papers_processed": 0,
            "processing_timestamp": utc_now_iso(),
            "corpus_stats": {
                "total_abstracts": 0,
                "total_words": 0,
                "unique_words_global": 0,
                "avg_abstract_length": 0.0,
                "longest_abstract_words": 0,
                "shortest_abstract_words": 0
            },
            "top_50_words": [],
            "technical_terms": {
                "uppercase_terms": [],
                "numeric_terms": [],
                "hyphenated_terms": []
            },
            "category_distribution": {}
        }
    else:
        avg_abs_len = total_words_all / papers_processed
        longest = max(abstract_lengths) if abstract_lengths else 0
        shortest = min(abstract_lengths) if abstract_lengths else 0

        # top 50 global words (exclude stopwords), include doc freq
        items = sorted(tf_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:50]
        top_50_words = [
            {"word": w, "frequency": int(cnt), "documents": int(df_counts.get(w, 0))}
            for w, cnt in items
        ]

        corpus = {
            "query": query,
            "papers_processed": papers_processed,
            "processing_timestamp": utc_now_iso(),
            "corpus_stats": {
                "total_abstracts": papers_processed,
                "total_words": int(total_words_all),
                "unique_words_global": int(len(global_vocab)),
                "avg_abstract_length": float(round(avg_abs_len, 3)),
                "longest_abstract_words": int(longest),
                "shortest_abstract_words": int(shortest)
            },
            "top_50_words": top_50_words,
            "technical_terms": {
                "uppercase_terms": sorted(uppercase_terms),
                "numeric_terms": sorted(numeric_terms),
                "hyphenated_terms": sorted(hyphen_terms)
            },
            "category_distribution": dict(sorted((k, int(v)) for k, v in category_distribution.items()))
        }

    with open(corpus_json_path, "w", encoding="utf-8") as f:
        json.dump(corpus, f, ensure_ascii=False, indent=2)

    # ---- done ----
    dt = time.time() - t0
    log_line(log_path, f"Completed processing: {papers_processed} papers in {dt:.2f} seconds")


if __name__ == "__main__":
    main()
