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
Uses HubSpot ILS v3 API with correct OR root / AND branch structure.
Skips lists that already exist (matches by name).

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

OUTPUTS_DIR = Path(__file__).parent
ENV_FILE    = OUTPUTS_DIR / ".env"

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

# ── Helpers for building filter branches ──────────────────────────────────────
def prop_filter(property_name, operator, values=None):
    """Build a single PROPERTY filter for ILS v3."""
    op = {"operationType": "MULTISTRING", "operator": operator}
    if values:
        op["values"] = values
    return {
        "filterType": "PROPERTY",
        "property":   property_name,
        "operation":  op
    }

def and_branch(filters):
    """Wrap a list of filters in an AND branch."""
    return {
        "filterBranchType": "AND",
        "filterBranches":   [],
        "filters":          filters
    }

def or_root(and_branches):
    """
    ILS v3 requires the ROOT branch to be OR.
    Each condition group goes inside as an AND branch.
    """
    return {
        "filterBranchType": "OR",
        "filterBranches":   and_branches,
        "filters":          []
    }

# ── List Definitions ───────────────────────────────────────────────────────────
LISTS = [
    {
        "name": "Audit — No Owner Assigned",
        "filterBranch": or_root([
            and_branch([prop_filter("hubspot_owner_id", "IS_NOT_KNOWN")])
        ])
    },
    {
        "name": "Audit — Missing Company Association",
        "filterBranch": or_root([
            and_branch([prop_filter("num_associated_companies", "IS_EQUAL_TO", ["0"])])
        ])
    },
    {
        "name": "Audit — No Email Address",
        "filterBranch": or_root([
            and_branch([prop_filter("email", "IS_NOT_KNOWN")])
        ])
    },
    {
        "name": "Audit — No Job Title",
        "filterBranch": or_root([
            and_branch([prop_filter("jobtitle", "IS_NOT_KNOWN")])
        ])
    },
    {
        "name": "Audit — No Lifecycle Stage",
        "filterBranch": or_root([
            and_branch([prop_filter("lifecyclestage", "IS_NOT_KNOWN")])
        ])
    },
    {
        # (has email AND no jobtitle) OR (has email AND no company)
        "name": "Enrichment Candidates",
        "filterBranch": or_root([
            and_branch([
                prop_filter("email",    "IS_KNOWN"),
                prop_filter("jobtitle", "IS_NOT_KNOWN")
            ]),
            and_branch([
                prop_filter("email",   "IS_KNOWN"),
                prop_filter("company", "IS_NOT_KNOWN")
            ])
        ])
    },
    {
        # aircall in source data 1 OR source data 2
        "name": "Audit — Aircall Contacts",
        "filterBranch": or_root([
            and_branch([prop_filter("hs_analytics_source_data_1", "IS_EQUAL_TO", ["aircall"])]),
            and_branch([prop_filter("hs_analytics_source_data_2", "IS_EQUAL_TO", ["aircall"])])
        ])
    }
]

# ── API helpers ────────────────────────────────────────────────────────────────
BASE = "https://api.hubapi.com"

def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def get_existing_lists(token):
    """Return a dict of {name: listId} for all existing contact lists."""
    existing = {}
    offset = None
    while True:
        params = {"objectTypeId": "0-1", "count": 250}
        if offset:
            params["offset"] = offset
        resp = requests.get(f"{BASE}/crm/v3/lists", headers=headers(token),
                            params=params, timeout=30)
        if not resp.ok:
            break
        data = resp.json()
        for lst in data.get("lists", []):
            existing[lst.get("name")] = lst.get("listId")
        if not data.get("hasMore"):
            break
        offset = data.get("offset")
    return existing

def create_list(token, name, filter_branch):
    payload = {
        "objectTypeId":  "0-1",
        "processingType": "DYNAMIC",
        "name":          name,
        "filterBranch":  filter_branch
    }
    resp = requests.post(f"{BASE}/crm/v3/lists", headers=headers(token),
                         json=payload, timeout=30)
    if not resp.ok:
        # Print the full response body for diagnosis
        raise Exception(f"HTTP {resp.status_code}: {resp.text}")
    return resp.json()

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Create HubSpot Audit Lists")
    parser.add_argument("--dry-run", action="store_true")
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

    # Fetch existing lists so we can skip duplicates
    existing = {}
    if not args.dry_run:
        print("  Checking existing lists ... ", end="", flush=True)
        existing = get_existing_lists(token)
        print(f"found {len(existing)} existing\n")

    results = []
    for i, lst in enumerate(LISTS, 1):
        name = lst["name"]
        print(f"  [{i}/{len(LISTS)}] {name} ... ", end="", flush=True)

        if args.dry_run:
            print("✅  (dry run)")
            results.append({"name": name, "id": "DRY_RUN", "status": "would_create"})
            continue

        if name in existing:
            list_id = existing[name]
            print(f"⏭️   Already exists (ID: {list_id}) — skipped")
            results.append({"name": name, "id": list_id, "status": "skipped"})
            continue

        try:
            result  = create_list(token, name, lst["filterBranch"])
            list_id = result.get("listId", "?")
            print(f"✅  Created (ID: {list_id})")
            results.append({"name": name, "id": list_id, "status": "created"})
        except Exception as e:
            print(f"❌  Failed\n      {e}")
            results.append({"name": name, "id": None, "status": "failed", "error": str(e)})

    summary_file = OUTPUTS_DIR / "hubspot-lists-summary.json"
    with open(summary_file, "w") as f:
        json.dump({
            "created_at": datetime.utcnow().isoformat(),
            "mode": mode,
            "portal_id": env.get("HUBSPOT_PORTAL_ID", "861426"),
            "lists": results
        }, f, indent=2)

    created  = sum(1 for r in results if r["status"] == "created")
    skipped  = sum(1 for r in results if r["status"] == "skipped")
    failed   = sum(1 for r in results if r["status"] == "failed")

    print(f"\n{'═'*62}")
    print(f"  {'DRY RUN COMPLETE' if args.dry_run else 'DONE'}")
    print(f"  Created  : {created}")
    print(f"  Skipped  : {skipped}  (already existed)")
    print(f"  Failed   : {failed}")
    print(f"{'═'*62}")
    if not args.dry_run and (created + skipped) > 0:
        print(f"\n  ✅  View lists: HubSpot → Contacts → Lists\n")

if __name__ == "__main__":
    main()
