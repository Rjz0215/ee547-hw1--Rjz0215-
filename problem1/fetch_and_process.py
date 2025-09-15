#!/usr/bin/env python3
# fetch_and_process.py
# Usage: python fetch_and_process.py <input_urls_file> <output_directory>

import sys
import os
import json
import time
import re
from datetime import datetime, timezone
from urllib import request, error

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

WORD_RE = re.compile(r"[0-9A-Za-z]+")

def is_text_content(content_type_value):
    if not content_type_value:
        return False
    return "text" in content_type_value.lower()

def ensure_output_dir(path):
    os.makedirs(path, exist_ok=True)

def read_urls(path):
    urls = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if u:
                urls.append(u)
    return urls

def count_words_from_bytes(body_bytes, content_type, headers):
    if not is_text_content(content_type):
        return None
    charset = None
    try:
        if hasattr(headers, "get_content_charset"):
            charset = headers.get_content_charset()
    except Exception:
        charset = None
    if not charset:
        m = re.search(r"charset=([^\s;]+)", content_type or "", flags=re.I)
        if m:
            charset = m.group(1).strip().strip('"').strip("'")
    if not charset:
        charset = "utf-8"

    try:
        text = body_bytes.decode(charset, errors="replace")
    except Exception:
        text = body_bytes.decode("utf-8", errors="replace")
    return len(WORD_RE.findall(text))

def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def append_error_log(path, timestamp_iso, url, msg):
    line = f"[{timestamp_iso}] [{url}]: {msg}\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)

def main():
    if len(sys.argv) != 3:
        print("Usage: fetch_and_process.py <input_urls_file> <output_directory>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    if not os.path.isfile(input_file):
        print(f"Error: Input file does not exist: {input_file}")
        sys.exit(1)

    ensure_output_dir(output_dir)

    responses_path = os.path.join(output_dir, "responses.json")
    summary_path = os.path.join(output_dir, "summary.json")
    errors_path = os.path.join(output_dir, "errors.log")

    urls = read_urls(input_file)

    responses = []
    status_dist = {}
    total_bytes = 0
    total_resp_time_ms_accum = 0.0
    successful = 0
    failed = 0

    processing_start = utc_now_iso()

    for url in urls:
        record = {
            "url": url,
            "status_code": None,
            "response_time_ms": None,
            "content_length": 0,
            "word_count": None,
            "timestamp": utc_now_iso(),
            "error": None
        }

        req = request.Request(url, method="GET")
        start = time.perf_counter()
        try:
            with request.urlopen(req, timeout=10) as resp:
                body = resp.read()
                end = time.perf_counter()

                status = resp.getcode() or 0
                ct = resp.headers.get("Content-Type", "")
                content_len = len(body)
                wc = count_words_from_bytes(body, ct, resp.headers) if is_text_content(ct) else None

                record["status_code"] = int(status)
                record["response_time_ms"] = (end - start) * 1000.0
                record["content_length"] = int(content_len)
                record["word_count"] = None if wc is None else int(wc)

                total_resp_time_ms_accum += record["response_time_ms"]
                total_bytes += content_len
                status_dist[str(status)] = status_dist.get(str(status), 0) + 1

                if 200 <= status <= 299:
                    successful += 1
                else:
                    failed += 1
                    record["error"] = f"HTTP {status}: Non-2xx response"
                    append_error_log(errors_path, record["timestamp"], url, record["error"])

        except error.HTTPError as e:
            try:
                body = e.read()
            except Exception:
                body = b""
            end = time.perf_counter()

            status = int(getattr(e, "code", 0) or 0)
            content_len = len(body)
            try:
                ct = e.headers.get("Content-Type", "")
            except Exception:
                ct = ""
            wc = count_words_from_bytes(body, ct, e.headers) if is_text_content(ct) else None

            record["status_code"] = status
            record["response_time_ms"] = (end - start) * 1000.0
            record["content_length"] = int(content_len)
            record["word_count"] = None if wc is None else int(wc)
            record["error"] = f"HTTPError {status}: {e.reason}"

            total_resp_time_ms_accum += record["response_time_ms"]
            total_bytes += content_len
            status_dist[str(status)] = status_dist.get(str(status), 0) + 1
            failed += 1
            append_error_log(errors_path, record["timestamp"], url, record["error"])

        except error.URLError as e:
            end = time.perf_counter()
            record["status_code"] = 0
            record["response_time_ms"] = (end - start) * 1000.0
            record["content_length"] = 0
            record["word_count"] = None
            reason = getattr(e, "reason", e)
            record["error"] = f"URLError: {reason}"
            total_resp_time_ms_accum += record["response_time_ms"]
            failed += 1
            append_error_log(errors_path, record["timestamp"], url, record["error"])

        except Exception as e:
            end = time.perf_counter()
            record["status_code"] = 0
            record["response_time_ms"] = (end - start) * 1000.0
            record["content_length"] = 0
            record["word_count"] = None
            record["error"] = f"Exception: {e}"
            total_resp_time_ms_accum += record["response_time_ms"]
            failed += 1
            append_error_log(errors_path, record["timestamp"], url, record["error"])

        responses.append(record)

    processing_end = utc_now_iso()

    total_urls = len(urls)
    avg_resp_ms = (total_resp_time_ms_accum / total_urls) if total_urls > 0 else 0.0

    summary = {
        "total_urls": int(total_urls),
        "successful_requests": int(successful),
        "failed_requests": int(failed),
        "average_response_time_ms": float(avg_resp_ms),
        "total_bytes_downloaded": int(total_bytes),
        "status_code_distribution": {k: int(v) for (k, v) in sorted(status_dist.items(), key=lambda kv: int(kv[0]))},
        "processing_start": processing_start,
        "processing_end": processing_end
    }

    write_json(responses_path, responses)
    write_json(summary_path, summary)

if __name__ == "__main__":
    main()
