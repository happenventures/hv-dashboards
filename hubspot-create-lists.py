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
# Each list is a DYNAMIC list that auto-updates as contacts change.
# filterBranchType AND = all filters must match
# filterBranchType OR  = any filter must match

LISTS = [
    {
        "name": "🔴 Audit — No Owner Assigned",
        "description": "Contacts with no HubSpot owner. Assign owners to improve follow-up.",
        "filterBranch": {
            "filterBranchType": "AND",
            "filterBranches": [],
            "filters": [
                {
                    "filterType": "PROPERTY",
                    "property": "hubspot_owner_id",
                    "operation": {
                        "operationType": "MULTISTRING",
                        "operator": "IS_NOT_KNOWN"
                    }
                }
            ]
        }
    },
    {
        "name": "🔴 Audit — Missing Company Association",
        "description": "Contacts not linked to any company record in HubSpot.",
        "filterBranch": {
            "filterBranchType": "AND",
            "filterBranches": [],
            "filters": [
                {
                    "filterType": "PROPERTY",
                    "property": "associatedcompanyid",
                    "operation": {
                        "operationType": "MULTISTRING",
                        "operator": "IS_NOT_KNOWN"
                    }
                }
            ]
        }
    },
    {
        "name": "🔴 Audit — No Email Address",
        "description": "Contacts with no email. Cannot be emailed or enriched without one.",
        "filterBranch": {
            "filterBranchType": "AND",
            "filterBranches": [],
            "filters": [
                {
                    "filterType": "PROPERTY",
                    "property": "email",
                    "operation": {
                        "operationType": "MULTISTRING",
                        "operator": "IS_NOT_KNOWN"
                    }
                }
            ]
        }
    },
    {
        "name": "🟡 Audit — No Job Title",
        "description": "Contacts missing a job title. Useful for enrichment and segmentation.",
        "filterBranch": {
            "filterBranchType": "AND",
            "filterBranches": [],
            "filters": [
                {
                    "filterType": "PROPERTY",
                    "property": "jobtitle",
                    "operation": {
                        "operationType": "MULTISTRING",
                        "operator": "IS_NOT_KNOWN"
                    }
                }
            ]
        }
    },
    {
        "name": "🟡 Audit — No Lifecycle Stage",
        "description": "Contacts with no lifecycle stage set. Assign stages for pipeline clarity.",
        "filterBranch": {
            "filterBranchType": "AND",
            "filterBranches": [],
            "filters": [
                {
                    "filterType": "PROPERTY",
                    "property": "lifecyclestage",
                    "operation": {
                        "operationType": "MULTISTRING",
                        "operator": "IS_NOT_KNOWN"
                    }
                }
            ]
        }
    },
    {
        "name": "🟢 Enrichment Candidates",
        "description": "Contacts with an email but missing job title or company — good targets for enrichment via Clearbit/Apollo.",
        "filterBranch": {
            "filterBranchType": "AND",
            "filterBranches": [
                {
                    "filterBranchType": "OR",
                    "filterBranches": [],
                    "filters": [
                        {
                            "filterType": "PROPERTY",
                            "property": "jobtitle",
                            "operation": {
                                "operationType": "MULTISTRING",
                                "operator": "IS_NOT_KNOWN"
                            }
                        },
                        {
                            "filterType": "PROPERTY",
                            "property": "company",
                            "operation": {
                                "operationType": "MULTISTRING",
                                "operator": "IS_NOT_KNOWN"
                            }
                        }
                    ]
                }
            ],
            "filters": [
                {
                    "filterType": "PROPERTY",
                    "property": "email",
                    "operation": {
                        "operationType": "MULTISTRING",
                        "operator": "IS_KNOWN"
                    }
                }
            ]
        }
    },
    {
        "name": "⚫ Audit — Aircall Contacts",
        "description": "Contacts sourced from Aircall integration. Monitor for junk re-imports.",
        "filterBranch": {
            "filterBranchType": "OR",
            "filterBranches": [],
            "filters": [
                {
                    "filterType": "PROPERTY",
                    "property": "hs_analytics_source_data_1",
                    "operation": {
                        "operationType": "MULTISTRING",
                        "operator": "IS_EQUAL_TO",
                        "values": ["aircall"]
                    }
                },
                {
                    "filterType": "PROPERTY",
                    "property": "hs_analytics_source_data_2",
                    "operation": {
                        "operationType": "MULTISTRING",
                        "operator": "IS_EQUAL_TO",
                        "values": ["aircall"]
                    }
                }
            ]
        }
    }
]

# ── API ────────────────────────────────────────────────────────────────────────
def create_list(token, list_def, dry_run=False):
    payload = {
        "objectTypeId": "0-1",       # contacts
        "processingType": "DYNAMIC", # auto-updates
        "name": list_def["name"],
        "filterBranch": list_def["filterBranch"]
    }

    if dry_run:
        return {"id": "DRY_RUN", "name": list_def["name"]}

    resp = requests.post(
        "https://api.hubapi.com/crm/v3/lists",
        headers={
            "Authorization":  f"Bearer {token}",
            "Content-Type":   "application/json",
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
            result = create_list(token, lst, dry_run=args.dry_run)
            list_id = result.get("listId") or result.get("id", "?")
            print(f"✅  (ID: {list_id})")
            results.append({"name": lst["name"], "id": list_id, "status": "created"})
        except requests.HTTPError as e:
            print(f"❌  Failed — {e.response.status_code}: {e.response.text[:120]}")
            results.append({"name": lst["name"], "id": None, "status": "failed", "error": str(e)})
        except Exception as e:
            print(f"❌  Error — {e}")
            results.append({"name": lst["name"], "id": None, "status": "error", "error": str(e)})

    # Save results
    summary_file = OUTPUTS_DIR / "hubspot-lists-summary.json"
    with open(summary_file, "w") as f:
        json.dump({
            "created_at": datetime.utcnow().isoformat(),
            "mode": mode,
            "portal_id": env.get("HUBSPOT_PORTAL_ID", "861426"),
            "lists": results
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
        print(f"\n  ✅  Lists are live in HubSpot → Contacts → Lists")
        print(f"  Results saved to hubspot-lists-summary.json\n")

if __name__ == "__main__":
    main()
