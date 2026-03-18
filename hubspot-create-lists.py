#!/usr/bin/env python3
"""
HubSpot Create Audit Lists
Happen Ventures — Portal 861426

Creates 7 dynamic (active) contact lists for ongoing CRM optimization:
  1. No Owner Assigned
  2. Missing Company Association
  3. No Email Address
  4. No Job Title
  5. No Lifecycle Stage
  6. Enrichment Candidates (email exists but missing job title or company)
  7. Aircall Contacts (sourced via Aircall integration)

Lists are DYNAMIC — they update automatically as contacts change.
Uses HubSpot Contacts v1 Lists API (stable, widely supported).

Usage:
  python3 hubspot-create-lists.py            # create all lists
  python3 hubspot-create-lists.py --dry-run  # preview without creating
"""

import os
import sys
import json
import argparse
import requests
from datetime import datetime
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
OUTPUTS_DIR = Path(__file__).parent
ENV_FILE    = OUTPUTS_DIR / ".env"

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
    env.update(os.environ)
    return env

# ── List Definitions ───────────────────────────────────────────────────────────
# Uses v1 filter format:
#   filters = array of groups (outer = OR between groups)
#   each group = array of conditions (inner = AND within group)
#
# Operators: HAS_PROPERTY, NOT_HAS_PROPERTY, EQ, NEQ, CONTAINS, NOT_CONTAINS

LISTS = [
    {
        "name": "Audit — No Owner Assigned",
        "filters": [
            [
                {"operator": "NOT_HAS_PROPERTY", "property": "hubspot_owner_id"}
            ]
        ]
    },
    {
        "name": "Audit — Missing Company Association",
        "filters": [
            [
                {"operator": "NOT_HAS_PROPERTY", "property": "associatedcompanyid"}
            ]
        ]
    },
    {
        "name": "Audit — No Email Address",
        "filters": [
            [
                {"operator": "NOT_HAS_PROPERTY", "property": "email"}
            ]
        ]
    },
    {
        "name": "Audit — No Job Title",
        "filters": [
            [
                {"operator": "NOT_HAS_PROPERTY", "property": "jobtitle"}
            ]
        ]
    },
    {
        "name": "Audit — No Lifecycle Stage",
        "filters": [
            [
                {"operator": "NOT_HAS_PROPERTY", "property": "lifecyclestage"}
            ]
        ]
    },
    {
        # AND inside each group, OR between groups:
        # (has email AND no job title) OR (has email AND no company)
        "name": "Enrichment Candidates",
        "filters": [
            [
                {"operator": "HAS_PROPERTY",     "property": "email"},
                {"operator": "NOT_HAS_PROPERTY", "property": "jobtitle"}
            ],
            [
                {"operator": "HAS_PROPERTY",     "property": "email"},
                {"operator": "NOT_HAS_PROPERTY", "property": "company"}
            ]
        ]
    },
    {
        # OR between the two source data fields
        "name": "Audit — Aircall Contacts",
        "filters": [
            [
                {"operator": "EQ", "property": "hs_analytics_source_data_1", "value": "aircall"}
            ],
            [
                {"operator": "EQ", "property": "hs_analytics_source_data_2", "value": "aircall"}
            ]
        ]
    }
]

# ── API ────────────────────────────────────────────────────────────────────────
def create_list(token, name, filters, dry_run=False):
    if dry_run:
        return {"listId": "DRY_RUN", "name": name}

    payload = {
        "name":    name,
        "dynamic": True,
        "filters": filters
    }

    resp = requests.post(
        "https://api.hubapi.com/contacts/v1/lists",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        },
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Create HubSpot Audit Lists")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview list definitions without creating them")
    args = parser.parse_args()

    env   = load_env()
    token = env.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        print("❌  HUBSPOT_ACCESS_TOKEN not found in .env")
        sys.exit(1)

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"\n{'═'*62}")
    print(f"  HubSpot Create Audit Lists")
    print(f"  Mode   : {mode}")
    print(f"  Portal : {env.get('HUBSPOT_PORTAL_ID', '861426')}")
    print(f"  Lists  : {len(LISTS)}")
    print(f"{'═'*62}\n")

    results = []
    for i, lst in enumerate(LISTS, 1):
        print(f"  [{i}/{len(LISTS)}] {lst['name']} ... ", end="", flush=True)
        try:
            result  = create_list(token, lst["name"], lst["filters"], dry_run=args.dry_run)
            list_id = result.get("listId", "?")
            print(f"✅  (ID: {list_id})")
            results.append({"name": lst["name"], "id": list_id, "status": "created"})
        except requests.HTTPError as e:
            err_body = e.response.text[:200] if e.response else str(e)
            print(f"❌  Failed — {e.response.status_code}: {err_body}")
            results.append({"name": lst["name"], "id": None, "status": "failed", "error": err_body})
        except Exception as e:
            print(f"❌  Error — {e}")
            results.append({"name": lst["name"], "id": None, "status": "error", "error": str(e)})

    # Save results
    summary_file = OUTPUTS_DIR / "hubspot-lists-summary.json"
    with open(summary_file, "w") as f:
        json.dump({
            "created_at": datetime.utcnow().isoformat(),
            "mode":       mode,
            "portal_id":  env.get("HUBSPOT_PORTAL_ID", "861426"),
            "lists":      results
        }, f, indent=2)

    created = sum(1 for r in results if r["status"] == "created")
    failed  = sum(1 for r in results if r["status"] != "created")

    print(f"\n{'═'*62}")
    print(f"  {'DRY RUN COMPLETE' if args.dry_run else 'DONE'}")
    print(f"  Created : {created}")
    print(f"  Failed  : {failed}")
    print(f"{'═'*62}")

    if args.dry_run:
        print("\n  👆  Dry run — no lists were created.")
        print("  Run without --dry-run to create them in HubSpot.\n")
    elif created > 0:
        print(f"\n  ✅  Lists are live — HubSpot → Contacts → Lists")
        print(f"  Results saved to hubspot-lists-summary.json\n")

if __name__ == "__main__":
    main()
