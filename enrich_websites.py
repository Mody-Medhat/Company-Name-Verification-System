# -*- coding: utf-8 -*-
"""Enrich company names with likely official websites."""

import os
import time
import logging
import requests
import pandas as pd
from ddgs import DDGS
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BATCH_DIR = "./enrichment_artifacts/batches"
OUTPUT_DIR = "./enrichment_results"
SEARCH_RESULTS = 5
CONFIDENCE_THRESHOLD = 70
SLEEP_BETWEEN_QUERIES = 0

SKIP_DOMAINS = [
    "linkedin.com",
    "facebook.com",
    "crunchbase.com",
    "bloomberg.com",
    "wikipedia.org",
    "youtube.com",
]

logging.basicConfig(level=logging.WARNING, format="%(levelname)s:%(message)s")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_homepage_text(url, retries=2, backoff=2):
    """Fetch title, meta, and h1 text from homepage."""
    for attempt in range(retries):
        try:
            resp = requests.get(
                url, timeout=10, headers={"User-Agent": "Mozilla/5.0"}, verify=False
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                return (
                    " ".join(
                        filter(
                            None,
                            [
                                soup.title.string if soup.title else "",
                                " ".join(
                                    m.get("content") or "" for m in soup.find_all("meta")
                                ),
                                " ".join(h.get_text() for h in soup.find_all("h1")),
                            ],
                        )
                    ),
                    "ok",
                )
            else:
                return "", f"http_{resp.status_code}"
        except (
            requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            logging.warning(f"[Attempt {attempt+1}] Error fetching {url}: {e}")
            time.sleep(backoff ** attempt)
        except Exception as e:
            logging.warning(f"[Attempt {attempt+1}] Unexpected error for {url}: {e}")
            break
    return "", "failed"

def enrich_company(name):
    """Enrich a single company by finding its website."""
    best_site, best_conf, verified = "", 0, "No"
    fetch_status = "not_fetched"
    
    with DDGS() as ddgs:
        for r in ddgs.text(f"{name} official site", max_results=SEARCH_RESULTS):
            url = r.get("href", "")
            title = r.get("title", "")
            snippet = r.get("body", "")
            
            # Skip unwanted domains
            if any(dom in url for dom in SKIP_DOMAINS):
                continue
                
            # Fetch and analyze page content
            page_text, status = fetch_homepage_text(url)
            fetch_status = status
            
            # Calculate confidence score
            score = fuzz.token_set_ratio(
                name.lower(), (title + " " + page_text + " " + snippet).lower()
            )
            
            if score > best_conf:
                best_conf = score
                best_site = url
    
    # Determine if verification is successful
    if best_conf >= CONFIDENCE_THRESHOLD:
        verified = "Yes"
    
    return {
        "representative": name,
        "website": best_site,
        "verified": verified,
        "confidence_score": best_conf,
        "fetch_status": fetch_status,
    }

def process_batch(batch_file, on_progress=None, progress_state=None):
    """Process a single batch file.

    on_progress: Optional callable accepting (current, total, message)
    progress_state: Optional dict with keys {"current": int, "total": int}
    """
    batch_name = os.path.basename(batch_file).replace(".csv", "")
    out_file = os.path.join(OUTPUT_DIR, f"{batch_name}_enriched.csv")

    # Resume if file exists
    processed = set()
    if os.path.exists(out_file):
        try:
            old = pd.read_csv(out_file)
            processed = set(old["representative"].tolist())
        except Exception:
            pass

    try:
        df = pd.read_csv(batch_file)
    except Exception as e:
        print(f"Error reading batch file {batch_file}: {e}")
        return

    with open(out_file, "a", encoding="utf-8", newline="") as f:
        for i, row in df.iterrows():
            name = (
                row["representative_name"]
                if "representative_name" in row
                else row.get("representative", "")
            )
            
            if not isinstance(name, str) or not name:
                continue
            if name in processed:
                continue
                
            print(f"[{batch_name}] Processing {i+1}/{len(df)}: {name}")
            record = enrich_company(name)
            pd.DataFrame([record]).to_csv(f, header=(f.tell() == 0), index=False)
            time.sleep(SLEEP_BETWEEN_QUERIES)
            if progress_state is not None:
                progress_state["current"] += 1
                if on_progress is not None:
                    try:
                        on_progress(
                            progress_state["current"],
                            progress_state.get("total", 0) or 0,
                            f"[{batch_name}] {progress_state['current']}/{progress_state.get('total','?')} processed"
                        )
                    except Exception:
                        pass

def _compute_total_tasks(batch_files):
    total = 0
    for batch_file in batch_files:
        try:
            batch_name = os.path.basename(batch_file).replace(".csv", "")
            out_file = os.path.join(OUTPUT_DIR, f"{batch_name}_enriched.csv")
            df = pd.read_csv(batch_file)
            processed = set()
            if os.path.exists(out_file):
                try:
                    old = pd.read_csv(out_file)
                    processed = set(old["representative"].tolist())
                except Exception:
                    processed = set()
            remaining = 0
            for _, row in df.iterrows():
                name = (
                    row.get("representative_name", row.get("representative", ""))
                )
                if isinstance(name, str) and name and name not in processed:
                    remaining += 1
            total += remaining
        except Exception:
            continue
    return total

def run_with_progress(on_progress):
    """Run enrichment reporting progress via callback on_progress(current, total, message)."""
    print("Starting website enrichment with progress...")
    if not os.path.exists(BATCH_DIR):
        msg = f"Error: Batch directory {BATCH_DIR} not found. Run normalization first."
        print(msg)
        if on_progress:
            on_progress(0, 0, msg)
        return

    batch_files = [
        os.path.join(BATCH_DIR, f) for f in os.listdir(BATCH_DIR) if f.endswith(".csv")
    ]
    batch_files.sort()

    if not batch_files:
        msg = "No batch files found. Run normalization first."
        print(msg)
        if on_progress:
            on_progress(0, 0, msg)
        return

    total = _compute_total_tasks(batch_files)
    state = {"current": 0, "total": total}

    for batch_file in batch_files:
        print(f"\nStarting batch: {batch_file}")
        process_batch(batch_file, on_progress=on_progress, progress_state=state)
        print(f"Finished batch: {batch_file}")

    if on_progress:
        on_progress(state["current"], total, "Website enrichment completed successfully!")

def main():
    """Main enrichment process."""
    print("Starting website enrichment...")
    
    if not os.path.exists(BATCH_DIR):
        print(f"Error: Batch directory {BATCH_DIR} not found. Run normalization first.")
        return

    batch_files = [
        os.path.join(BATCH_DIR, f) for f in os.listdir(BATCH_DIR) if f.endswith(".csv")
    ]
    batch_files.sort()
    
    if not batch_files:
        print("No batch files found. Run normalization first.")
        return

    for batch_file in batch_files:
        print(f"\nStarting batch: {batch_file}")
        process_batch(batch_file)
        print(f"Finished batch: {batch_file}")

    print("Website enrichment completed successfully!")

if __name__ == "__main__":
    main()