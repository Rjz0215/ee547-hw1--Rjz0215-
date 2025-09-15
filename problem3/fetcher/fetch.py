#!/usr/bin/env python3
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

def utc_z():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def main():
    print(f"[{utc_z()}] Fetcher starting", flush=True)
    os.makedirs("/shared/input", exist_ok=True)
    os.makedirs("/shared/raw", exist_ok=True)
    os.makedirs("/shared/status", exist_ok=True)

    input_file = "/shared/input/urls.txt"
    while not os.path.exists(input_file):
        print(f"[{utc_z()}] Waiting for {input_file}...", flush=True)
        time.sleep(2)

    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        urls = [ln.strip() for ln in f if ln.strip()]

    results = []
    for i, url in enumerate(urls, 1):
        dst = f"/shared/raw/page_{i}.html"
        try:
            print(f"[{utc_z()}] Fetching {url} ...", flush=True)
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; EE547-HW/1.0)"}
            )
            t0 = time.perf_counter()
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read()
                status = resp.getcode() or 0
            dt_ms = (time.perf_counter() - t0) * 1000.0
            with open(dst, "wb") as wf:
                wf.write(body)
            results.append({
                "url": url,
                "file": os.path.basename(dst),
                "size": len(body),
                "status_code": int(status),
                "response_time_ms": dt_ms,
                "status": "success" if 200 <= status < 300 else "non-2xx"
            })
        except urllib.error.HTTPError as e:
            try:
                body = e.read()
                size = len(body or b"")
            except Exception:
                size = 0
            results.append({
                "url": url,
                "file": None,
                "size": int(size),
                "status_code": int(getattr(e, "code", 0) or 0),
                "error": f"HTTPError {getattr(e, 'code', 0)}: {getattr(e, 'reason', '')}",
                "status": "failed"
            })
        except urllib.error.URLError as e:
            results.append({
                "url": url,
                "file": None,
                "size": 0,
                "status_code": 0,
                "error": f"URLError: {getattr(e, 'reason', e)}",
                "status": "failed"
            })
        except Exception as e:
            results.append({
                "url": url,
                "file": None,
                "size": 0,
                "status_code": 0,
                "error": f"Exception: {e}",
                "status": "failed"
            })
        time.sleep(1)

    status = {
        "timestamp": utc_z(),
        "urls_processed": len(urls),
        "successful": sum(1 for r in results if r["status"] == "success"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
        "results": results
    }
    with open("/shared/status/fetch_complete.json", "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    print(f"[{utc_z()}] Fetcher complete", flush=True)

if __name__ == "__main__":
    main()
