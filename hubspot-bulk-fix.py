#!/usr/bin/env python3
"""
HubSpot Bulk Contact Fix Script
Happen Ventures — Portal 861426

Fixes applied to all contacts:
  1. Whitespace strip        — firstname, lastname, jobtitle, company
  2. ALL-CAPS company names  — → Title Case
  3. ALL-CAPS job titles     — → Title Case
  4. Lowercase job titles    — → Title Case
  5. ALL-CAPS first names    — → Title Case
  6. ALL-CAPS last names     — → Title Case

Features:
  - Cursor-based pagination  (no 10K offset limit)
  - Batch updates            (100 contacts per API call)
  - Checkpoint / resume      (saves after every page to outputs folder)
  - Dry-run mode             (--dry-run: scan only, no writes)
  - Reset mode               (--reset: ignore checkpoint, start fresh)
  - Error logging            (failed IDs saved separately)

Usage:
  python3 hubspot-bulk-fix.py --dry-run    # preview changes only
  python3 hubspot-bulk-fix.py              # apply all fixes
  python3 hubspot-bulk-fix.py --reset      # restart from scratch
"""

import os
import sys
import json
import time
import argparse
import requests
from datetime import datetime
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
OUTPUTS_DIR   = Path(__file__).parent
ENV_FILE       = OUTPUTS_DIR / ".env"
CHECKPOINT_FILE = OUTPUTS_DIR / "hubspot-checkpoint.json"
FAILED_FILE    = OUTPUTS_DIR / "hubspot-failed-contacts.json"
SUMMARY_FILE   = OUTPUTS_DIR / "hubspot-run-summary.json"

# ── Config ─────────────────────────────────────────────────────────────────────
BATCH_SIZE      = 100          # HubSpot batch/update API max
PROPERTIES      = ["firstname", "lastname", "jobtitle", "company"]
RATE_LIMIT_DELAY = 0.11        # ~9 req/s — safely under the 10 req/s limit

# Words to keep in uppercase even inside title-cased strings
PRESERVE_UPPER  = {
    "LLC", "LLP", "INC", "CORP", "CO", "LTD", "PLC",
    "USA", "UK", "EU", "UN", "US",
    "IT", "HR", "PR", "AI", "ML", "BI", "SaaS",
    "CEO", "CFO", "CTO", "COO", "CMO", "CRO", "CPO",
    "VP", "SVP", "EVP", "AVP", "GM",
    "II", "III", "IV", "VI",
}

# ── Load .env ──────────────────────────────────────────────────────────────────
def load_env():
    env = {}
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    env[key.strip()] = val.strip()
    env.update(os.environ)   # real env vars take precedence
    return env

# ── Fix logic ──────────────────────────────────────────────────────────────────
def smart_title(text):
    """Title-case a string while preserving known acronyms."""
    words = text.split()
    result = []
    for word in words:
        # Strip punctuation for comparison
        core = word.strip(".,;:()&/")
        if core.upper() in PRESERVE_UPPER:
            result.append(word.replace(core, core.upper()))
        else:
            result.append(word.capitalize())
    return " ".join(result)

def is_all_caps(s):
    """True if string has letters and they're all uppercase."""
    letters = [c for c in s if c.isalpha()]
    return len(letters) > 1 and all(c.isupper() for c in letters)

def is_all_lower(s):
    """True if string has letters and they're all lowercase."""
    letters = [c for c in s if c.isalpha()]
    return len(letters) > 1 and all(c.islower() for c in letters)

def fix_field(field_name, value):
    """
    Apply all relevant fixes to a field value.
    Returns (fixed_value, list_of_fixes_applied).
    """
    if not value or not isinstance(value, str):
        return value, []

    original = value
    fixes = []

    # Fix 1: Strip whitespace
    stripped = value.strip()
    if stripped != value:
        value = stripped
        fixes.append("whitespace")

    # Skip very short or empty values after stripping
    if len(value) <= 1:
        return value, fixes

    # Fix 2: ALL-CAPS → Title Case (all fields)
    if is_all_caps(value):
        value = smart_title(value)
        fixes.append("caps→title")

    # Fix 3: All-lowercase job title → Title Case
    elif field_name == "jobtitle" and is_all_lower(value):
        value = smart_title(value)
        fixes.append("lower→title")

    return value, fixes

def compute_contact_fixes(contact):
    """
    Compute what needs fixing for one contact.
    Returns:
      updates      — dict of {property: new_value}
      fix_summary  — dict of {property: [list of fix types]}
    """
    props = contact.get("properties", {})
    updates = {}
    fix_summary = {}

    for field in PROPERTIES:
        val = props.get(field)
        if val:
            fixed, fixes = fix_field(field, val)
            if fixed != val and fixes:
                updates[field] = fixed
                fix_summary[field] = fixes

    return updates, fix_summary

# ── Checkpoint helpers ─────────────────────────────────────────────────────────
def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {
        "after":        None,
        "processed":    0,
        "updated":      0,
        "skipped":      0,
        "errors":       0,
        "started_at":   datetime.utcnow().isoformat(),
        "last_updated": None,
        "done":         False,
    }

def save_checkpoint(cp):
    cp["last_updated"] = datetime.utcnow().isoformat()
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(cp, f, indent=2)

# ── HubSpot API helpers ────────────────────────────────────────────────────────
def get_contacts_page(token, after=None):
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    params = {
        "limit":      BATCH_SIZE,
        "properties": ",".join(PROPERTIES),
    }
    if after:
        params["after"] = after
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

def batch_update_contacts(token, updates):
    """
    updates: list of {"id": str, "properties": {field: value}}
    HubSpot batch/update supports up to 100 records per call.
    """
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/update"
    resp = requests.post(
        url,
        headers={
            "Authorization":  f"Bearer {token}",
            "Content-Type":   "application/json",
        },
        json={"inputs": updates},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="HubSpot Bulk Contact Fix — Happen Ventures")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scan and report issues without making any changes")
    parser.add_argument("--reset",   action="store_true",
                        help="Ignore existing checkpoint and start from the beginning")
    args = parser.parse_args()

    # Load credentials
    env   = load_env()
    token = env.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        print("❌  HUBSPOT_ACCESS_TOKEN not found in .env file")
        sys.exit(1)

    mode_label = "DRY RUN (no writes)" if args.dry_run else "LIVE (writing to HubSpot)"
    print(f"\n{'═'*62}")
    print(f"  HubSpot Bulk Contact Fix")
    print(f"  Mode   : {mode_label}")
    print(f"  Portal : {env.get('HUBSPOT_PORTAL_ID', '861426')}")
    print(f"  Time   : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'═'*62}\n")

    # Handle reset
    if args.reset and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print("🔄  Checkpoint cleared — starting from scratch\n")

    # Load checkpoint
    cp = load_checkpoint()

    if cp.get("done") and not args.dry_run:
        print("✅  This run already completed successfully.")
        print(f"    Processed : {cp['processed']:,}")
        print(f"    Updated   : {cp['updated']:,}")
        print("    Use --reset to run again.\n")
        return

    if cp["processed"] > 0:
        print(f"📌  Resuming from checkpoint")
        print(f"    Processed so far : {cp['processed']:,}")
        print(f"    Updated so far   : {cp['updated']:,}")
        print(f"    Last saved       : {cp.get('last_updated', 'unknown')}\n")

    print(f"  {'Page':>5}  {'Fetched':>9}  {'To fix':>8}  {'Updated':>8}  {'Skipped':>8}  {'Errors':>7}")
    print(f"  {'-'*5}  {'-'*9}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*7}")

    failed_ids = []
    page_num   = 0

    try:
        while True:
            page_num += 1

            # ── Fetch one page ─────────────────────────────────────────────────
            try:
                data = get_contacts_page(token, after=cp["after"])
            except requests.HTTPError as e:
                print(f"\n❌  HTTP error on page {page_num}: {e}")
                print("    Progress saved — re-run to resume.")
                save_checkpoint(cp)
                break
            except requests.RequestException as e:
                print(f"\n❌  Network error on page {page_num}: {e}")
                save_checkpoint(cp)
                break

            contacts = data.get("results", [])
            if not contacts:
                break

            # ── Compute fixes ──────────────────────────────────────────────────
            batch_updates = []
            for contact in contacts:
                updates, fix_summary = compute_contact_fixes(contact)
                if updates:
                    batch_updates.append({
                        "id":           contact["id"],
                        "properties":   updates,
                        # kept for logging; stripped before API call
                        "_fix_summary": fix_summary,
                    })

            page_fixed   = len(batch_updates)
            page_skipped = len(contacts) - page_fixed

            cp["processed"] += len(contacts)
            cp["skipped"]   += page_skipped

            # ── Apply fixes (skipped in dry-run) ───────────────────────────────
            page_errors = 0
            if batch_updates:
                if not args.dry_run:
                    api_inputs = [
                        {"id": u["id"], "properties": u["properties"]}
                        for u in batch_updates
                    ]
                    try:
                        batch_update_contacts(token, api_inputs)
                        cp["updated"] += page_fixed
                    except requests.HTTPError as e:
                        page_errors = page_fixed
                        cp["errors"] += page_fixed
                        failed_ids.extend([u["id"] for u in batch_updates])
                        print(f"\n  ⚠️   Batch update failed on page {page_num}: {e}")
                else:
                    cp["updated"] += page_fixed   # count what *would* change

            # ── Progress line ──────────────────────────────────────────────────
            err_str = str(cp["errors"]) if cp["errors"] else "-"
            print(
                f"  {page_num:>5}  {cp['processed']:>9,}  "
                f"{page_fixed:>8,}  {cp['updated']:>8,}  "
                f"{cp['skipped']:>8,}  {err_str:>7}"
            )

            # ── Save checkpoint ────────────────────────────────────────────────
            next_cursor = data.get("paging", {}).get("next", {}).get("after")
            cp["after"] = next_cursor
            save_checkpoint(cp)

            # Save failed IDs incrementally
            if failed_ids:
                with open(FAILED_FILE, "w") as f:
                    json.dump(failed_ids, f, indent=2)

            # No more pages
            if not next_cursor:
                break

            time.sleep(RATE_LIMIT_DELAY)

    except KeyboardInterrupt:
        print("\n\n⚠️   Interrupted. Progress saved — re-run to resume.\n")
        save_checkpoint(cp)
        return

    # ── Mark done & write summary ──────────────────────────────────────────────
    # Only mark done if we naturally exhausted all pages (no timeout/error exit)
    completed_fully = cp["after"] is None  # None means no next cursor = end of contacts
    if not args.dry_run and completed_fully:
        cp["done"] = True
    save_checkpoint(cp)

    summary = {
        "mode":              mode_label,
        "portal_id":         env.get("HUBSPOT_PORTAL_ID", "861426"),
        "completed_at":      datetime.utcnow().isoformat(),
        "total_processed":   cp["processed"],
        "total_updated":     cp["updated"],
        "total_skipped":     cp["skipped"],
        "total_errors":      cp["errors"],
        "failed_contact_ids": failed_ids,
    }
    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)

    # ── Final report ───────────────────────────────────────────────────────────
    verb = "Would update" if args.dry_run else "Updated"
    print(f"\n{'═'*62}")
    print(f"  {'DRY RUN COMPLETE' if args.dry_run else 'RUN COMPLETE'}")
    print(f"  Total processed  : {cp['processed']:,}")
    print(f"  {verb:<16} : {cp['updated']:,}")
    print(f"  Skipped (clean)  : {cp['skipped']:,}")
    print(f"  Errors           : {cp['errors']:,}")
    print(f"{'═'*62}")

    if args.dry_run:
        print("\n  👆  Dry run only — nothing was written to HubSpot.")
        print("  Run without --dry-run to apply all fixes.\n")
    elif cp["errors"] > 0:
        print(f"\n  ⚠️   {cp['errors']} contacts failed — see hubspot-failed-contacts.json\n")
    else:
        print("\n  ✅  All fixes applied successfully.\n")

if __name__ == "__main__":
    main()
