#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import re
import time
from datetime import datetime, timezone

def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

LINK_RE = re.compile(r'href=[\'"]?([^\'" >]+)', re.IGNORECASE)
IMG_RE  = re.compile(r'src=[\'"]?([^\'" >]+)', re.IGNORECASE)

def strip_html(html_content: str):
    links = LINK_RE.findall(html_content)
    images = IMG_RE.findall(html_content)
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>',   '', html_content, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', html_content)
    text = re.sub(r'\s+', ' ', text).strip()
    return text, links, images

def sentence_split(text: str):
    parts = re.split(r"[.!?]+", text)
    return [p.strip() for p in parts if p.strip()]

def tokenize(text: str):
    return re.findall(r"\b[\w-]+\b", text, flags=re.UNICODE)

def avg(nums):
    return (sum(nums) / len(nums)) if nums else 0.0

def paragraph_count_from_html(html: str):
    p_tags = re.findall(r'</p\s*>', html, flags=re.IGNORECASE)
    if p_tags:
        return len(p_tags)
    blocks = re.split(r'\n\s*\n', html)
    return max(1, len([b for b in blocks if b.strip()]))

def main():
    shared = "/shared"
    raw_dir = os.path.join(shared, "raw")
    processed_dir = os.path.join(shared, "processed")
    status_dir = os.path.join(shared, "status")

    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(status_dir, exist_ok=True)
    fetch_done = os.path.join(status_dir, "fetch_complete.json")
    while not os.path.exists(fetch_done):
        print(f"[{utc_now()}] Processor waiting for {fetch_done} ...", flush=True)
        time.sleep(2)
    files = sorted([fn for fn in os.listdir(raw_dir) if fn.lower().endswith(".html")])
    processed = []

    for fn in files:
        src_path = os.path.join(raw_dir, fn)
        with open(src_path, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()

        text, links, images = strip_html(html)

        words = tokenize(text)
        sentences = sentence_split(text)
        sent_lens = [len(tokenize(s)) for s in sentences]
        para_count = paragraph_count_from_html(html)

        out_obj = {
            "source_file": fn,
            "text": text,
            "statistics": {
                "word_count": int(len(words)),
                "sentence_count": int(len(sentences)),
                "paragraph_count": int(para_count),
                "avg_word_length": float(round(avg([len(w) for w in words]) if words else 0.0, 3))
            },
            "links": links,
            "images": images,
            "processed_at": utc_now()
        }

        out_name = os.path.splitext(fn)[0] + ".json"
        out_path = os.path.join(processed_dir, out_name)
        with open(out_path, "w", encoding="utf-8") as wf:
            json.dump(out_obj, wf, ensure_ascii=False, indent=2)

        processed.append(out_name)
        print(f"[{utc_now()}] Processor wrote {out_name}", flush=True)

    status = {
        "timestamp": utc_now(),
        "processed_files": processed
    }
    with open(os.path.join(status_dir, "process_complete.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    print(f"[{utc_now()}] Processor complete", flush=True)

if __name__ == "__main__":
    main()

