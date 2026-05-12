#!/usr/bin/env python3
"""
Script to call the placement API with cursor 1-5 and find duplicate items across cursors.
"""

import requests
import json
from collections import defaultdict

BASE_URL = "http://atlas-serving.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/v2/placements/use-case-sfv-p5"
PARAMS_BASE = {
    "ssoId": "768",
    "userId": "1",
    "pseudoId": "1",
}
CURSORS = [1, 2, 3, 4, 5]


def extract_item_ids(response_json: dict) -> list[dict]:
    """
    Extract all item identifiers from the response.
    - Full items (with 'items' array): use item['Id'] or item['ActivityId']
    - Simple items (with just 'id'): use item['id']
    Returns list of dicts: {id, name, type}
    """
    results = []
    top_items = response_json.get("items", [])

    for entry in top_items:
        if "id" in entry and "items" not in entry:
            # Simple item node (e.g. {"id": "8kJnqDNwaXyk"})
            results.append({
                "id": entry["id"],
                "name": entry["id"],
                "type": "node_id",
            })
        elif "items" in entry:
            # Full item group
            content_type = entry.get("payload", {}).get("content_type", "unknown")
            for item in entry["items"]:
                item_id = str(item.get("Id") or item.get("ActivityId") or item.get("id") or "unknown")
                results.append({
                    "id": item_id,
                    "name": item.get("Name", item_id),
                    "type": content_type,
                })

    return results


def fetch_cursor(cursor: int) -> dict | None:
    params = {**PARAMS_BASE, "cursor": cursor}
    try:
        print(f"  Fetching cursor={cursor}...")
        resp = requests.get(BASE_URL, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError as e:
        print(f"  [ERROR] Cannot connect to API: {e}")
        return None
    except requests.exceptions.Timeout:
        print(f"  [ERROR] Request timed out for cursor={cursor}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"  [ERROR] HTTP error for cursor={cursor}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"  [ERROR] Failed to parse JSON for cursor={cursor}")
        return None


def main():
    cursor_items: dict[int, list[dict]] = {}

    print("=" * 60)
    print("Fetching API responses for cursor 1-5")
    print("=" * 60)

    for cursor in CURSORS:
        data = fetch_cursor(cursor)
        if data is None:
            cursor_items[cursor] = []
            continue

        items = extract_item_ids(data)
        cursor_items[cursor] = items

        returned_cursor = data.get("pages", {}).get("cursor", "N/A")
        print(f"  cursor={cursor} → got {len(items)} items (response cursor: {returned_cursor})")

    print()
    print("=" * 60)
    print("Items per cursor")
    print("=" * 60)
    for cursor, items in cursor_items.items():
        print(f"\nCursor {cursor} ({len(items)} items):")
        for item in items:
            print(f"  [{item['type']}] {item['id']} — {item['name']}")

    # Find duplicates: items whose ID appears in more than one cursor
    id_to_cursors: dict[str, list[int]] = defaultdict(list)
    id_to_info: dict[str, dict] = {}

    for cursor, items in cursor_items.items():
        for item in items:
            item_id = item["id"]
            id_to_cursors[item_id].append(cursor)
            id_to_info[item_id] = item  # keep latest info

    duplicates = {
        item_id: cursors
        for item_id, cursors in id_to_cursors.items()
        if len(cursors) > 1
    }

    print()
    print("=" * 60)
    print(f"Duplicate items across cursors: {len(duplicates)} found")
    print("=" * 60)

    if not duplicates:
        print("✅ No duplicate items found across cursor 1-5.")
    else:
        print(f"⚠️  Found {len(duplicates)} duplicate item(s):\n")
        for item_id, cursors in sorted(duplicates.items(), key=lambda x: x[1]):
            info = id_to_info[item_id]
            print(f"  ID: {item_id}")
            print(f"  Name: {info['name']}")
            print(f"  Type: {info['type']}")
            print(f"  Appears in cursors: {cursors}")
            print()

    # Cross-cursor matrix summary
    print("=" * 60)
    print("Cross-cursor overlap summary")
    print("=" * 60)
    cursor_sets = {
        cursor: set(item["id"] for item in items)
        for cursor, items in cursor_items.items()
    }

    for i in CURSORS:
        for j in CURSORS:
            if i >= j:
                continue
            overlap = cursor_sets[i] & cursor_sets[j]
            if overlap:
                print(f"  Cursor {i} ∩ Cursor {j}: {len(overlap)} shared item(s) → {list(overlap)}")
            else:
                print(f"  Cursor {i} ∩ Cursor {j}: (none)")


if __name__ == "__main__":
    main()
