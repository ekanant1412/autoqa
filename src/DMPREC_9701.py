import requests
import json
from datetime import datetime
import os

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9701"

BASE_URL = (
    "http://atlas-serving.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/v2/placements/711-sfv-moscow"
)

REQUESTS_CONFIG = [
    {
        "name": "ชุดที่ 1",
        "id": "",
    },
    {
        "name": "ชุดที่ 2",
        "id": "jq3obQbQL7Bq",
    },
    {
        "name": "ชุดที่ 3",
        "id": "g0dj858Per19",
    },
]

COMMON_PARAMS = {
    "ssoId": "22838335",
    "deviceId": "boi",
    "userId": "1",
    "pseudoId": "1",
    "limit": 10,
    "returnItemMetadata": "false",
    "ga_id": "868707658.1772007422",
}

TIMEOUT_SEC = 20

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

LOG_TXT     = f"{ART_DIR}/dedup_check.log"
RESULT_JSON = f"{ART_DIR}/dedup_check_result.json"


# =================================================
def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def call_rest(name: str, id: str) -> list:
    params = dict(COMMON_PARAMS)
    if id:
        params["id"] = id

    resp = requests.get(BASE_URL, params=params, timeout=TIMEOUT_SEC)

    with open(f"{ART_DIR}/response_{name}.json", "w", encoding="utf-8") as f:
        f.write(resp.text)

    tlog(f"  HTTP={resp.status_code}")
    tlog(f"  URL={resp.url}")
    resp.raise_for_status()

    data = resp.json()
    tlog(f"  response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("items") or data.get("data") or data.get("results") or []
    else:
        tlog(f"  [WARN] unexpected response type: {type(data)}")
        return []

    if not isinstance(items, list):
        tlog(f"  [WARN] items is not a list, got: {type(items)}")
        return []

    return [
        {
            "id": str(it.get("id") or it.get("id") or ""),
            "title": str(it.get("title") or ""),
        }
        for it in items
        if isinstance(it, dict)
    ]


def print_table(all_results: list):
    names = [r["name"] for r in all_results]
    max_items = max((len(r["items"]) for r in all_results), default=0)

    col_w = 20
    header = f"{'rank':<6}" + "".join(f"{n:<{col_w}}" for n in names)
    tlog("\n" + "=" * len(header))
    tlog(header)
    tlog("=" * len(header))

    for i in range(max_items):
        row = f"{i:<6}"
        for r in all_results:
            items = r["items"]
            cell = items[i]["id"] if i < len(items) else "-"
            row += f"{cell:<{col_w}}"
        tlog(row)

    tlog("=" * len(header))


def find_duplicates(all_results: list) -> list:
    pairs = []
    n = len(all_results)
    for i in range(n):
        for j in range(i + 1, n):
            a = all_results[i]
            b = all_results[j]

            # ✅ exclude id ที่ถูก seed ใน request นั้นๆ ออกก่อนเช็ค
            seed_ids = set()
            if a.get("id"):
                seed_ids.add(a["id"])
            if b.get("id"):
                seed_ids.add(b["id"])

            ids_a = {it["id"] for it in a["items"]} - seed_ids
            ids_b = {it["id"] for it in b["items"]} - seed_ids

            overlap = ids_a & ids_b
            pairs.append({
                "pair": f"{a['name']} vs {b['name']}",
                "excluded_seed_ids": list(seed_ids),
                "overlap_count": len(overlap),
                "overlap_ids": list(overlap),
            })
    return pairs


# =================================================
def run_check():
    open(LOG_TXT, "w", encoding="utf-8").close()
    tlog(f"TEST={TEST_KEY}")
    tlog(f"BASE_URL={BASE_URL}")

    all_results = []

    for cfg in REQUESTS_CONFIG:
        tlog(f"\n[{cfg['name']}] id={cfg['id'] or '(empty)'}")
        items = call_rest(cfg["name"], cfg["id"])
        tlog(f"  items returned: {len(items)}")
        for i, it in enumerate(items):
            tlog(f"  [{i}] {it['id']}  {it['title'][:40]}")
        all_results.append({
            "name": cfg["name"],
            "id": cfg["id"],
            "items": items,
        })

    tlog("\n=== COMPARISON TABLE ===")
    print_table(all_results)

    tlog("\n=== DUPLICATE CHECK ===")
    dup_pairs = find_duplicates(all_results)
    issues = []

    for pair in dup_pairs:
        if pair["overlap_count"] > 0:
            tlog(f"  ❌ {pair['pair']} → overlap={pair['overlap_count']} ids: {pair['overlap_ids']}")
            issues.append(pair)
        else:
            tlog(f"  ✅ {pair['pair']} → no overlap")

    status = "FAIL" if issues else "PASS"
    tlog(f"\nSTATUS: {'✅ PASS' if status == 'PASS' else '❌ FAIL'}")

    result = {
        "test_key": TEST_KEY,
        "base_url": BASE_URL,
        "requests": [
            {
                "name": r["name"],
                "id": r["id"],
                "item_count": len(r["items"]),
                "ids": [it["id"] for it in r["items"]],
            }
            for r in all_results
        ],
        "duplicate_pairs": dup_pairs,
        "status": status,
    }

    with open(RESULT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    tlog(f"\nSaved: {RESULT_JSON}")
    tlog(f"Saved: {LOG_TXT}")

    if status == "FAIL":
        fail_summary = "; ".join(
            f"{p['pair']} overlap={p['overlap_count']}" for p in issues
        )
        raise AssertionError(f"{TEST_KEY} FAIL: {fail_summary}")

    return result


# =================================================
# ✅ PYTEST ENTRY
# =================================================
def test_DMPREC_9701():
    result = run_check()
    print("RESULT:", result["status"])


if __name__ == "__main__":
    run_check()