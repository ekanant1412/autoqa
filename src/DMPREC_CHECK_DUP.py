import requests
import json
from datetime import datetime
import os

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-DEDUP"

GRAPHQL_URL = "https://content-public-api.trueid-preprod.net/content/v1/graphql"

HEADERS = {
    "Authorization": "Bearer 5cc953b97d5a1df61c25082c0b41031c998f4a308550c6c4a5d8bb09",
    "Content-Type": "application/json",
    "Cookie": (
        "visid_incap_2699781=Wxr5rq5bTGGhE43uVg7IHpF2CWkAAAAAQUIPAAAAAAD8GwI4XJNlBzzQWbQ9v/XG; "
        "visid_incap_2736461=3/1tTuIkQ0OM0m2DGlSixBEg+2gAAAAAQUIPAAAAAAAJEYs6M2tYVSmhrxO7VQQu"
    ),
}

REQUESTS_CONFIG = [
    {
        "name": "ชุดที่ 1",
        "content_id": "",
    },
    {
        "name": "ชุดที่ 2",
        "content_id": "jq3obQbQL7Bq",
    },
    {
        "name": "ชุดที่ 3",
        "content_id": "g0dj858Per19",
    },
]

COMMON_PARAMS = {
    "placement_id": "711-sfv-moscow",
    "country": "th",
    "lang": "th",
    "device_id": "Wgi432MFx99iGI9KeMwLcuQamrguVUyD",
    "limit": 10,
    "ga_id": "868707658.1772007453",
}

TIMEOUT_SEC = 20

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

LOG_TXT = f"{ART_DIR}/dedup_check.log"
RESULT_JSON = f"{ART_DIR}/dedup_check_result.json"


# =================================================
def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def build_query(content_id: str) -> str:
    return (
        f'query {{\n'
        f'\tshort (placement_id: "{COMMON_PARAMS["placement_id"]}", '
        f'country: "{COMMON_PARAMS["country"]}", '
        f'lang: "{COMMON_PARAMS["lang"]}", '
        f'device_id: "{COMMON_PARAMS["device_id"]}", '
        f'content_id: "{content_id}", '
        f'limit: {COMMON_PARAMS["limit"]}, '
        f'ga_id: "{COMMON_PARAMS["ga_id"]}"){{  \n'
        f'\t\tid\n'
        f'\t\ttitle\n'
        f'\t}}\n'
        f'}}'
    )


def call_graphql(name: str, content_id: str) -> list:
    query = build_query(content_id)
    payload = json.dumps({"query": query, "variables": None})

    resp = requests.post(
        GRAPHQL_URL,
        headers=HEADERS,
        data=payload,
        timeout=TIMEOUT_SEC
    )

    with open(f"{ART_DIR}/response_{name}.json", "w", encoding="utf-8") as f:
        f.write(resp.text)

    tlog(f"  HTTP={resp.status_code}")
    resp.raise_for_status()

    data = resp.json()

    # debug: ดู structure จริง
    tlog(f"  response keys: {list(data.keys())}")
    if "errors" in data:
        tlog(f"  GraphQL errors: {data['errors']}")

    items = data.get("data", {}).get("short", [])

    # ป้องกัน None
    if not isinstance(items, list):
        tlog(f"  [WARN] 'short' is not a list, got: {type(items)} value={items}")
        return []

    return [{"id": it.get("id", ""), "title": it.get("title", "")} for it in items]
    
    resp.raise_for_status()
    ...


def print_table(all_results: list):
    """แสดงตารางเปรียบเทียบ id แต่ละ request"""
    names = [r["name"] for r in all_results]
    max_items = max(len(r["items"]) for r in all_results)

    # header
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


def find_duplicates(all_results: list) -> dict:
    """เปรียบเทียบ ids ระหว่าง request คู่ต่างๆ"""
    pairs = []
    n = len(all_results)
    for i in range(n):
        for j in range(i + 1, n):
            a = all_results[i]
            b = all_results[j]
            ids_a = set(it["id"] for it in a["items"])
            ids_b = set(it["id"] for it in b["items"])
            overlap = ids_a & ids_b
            pairs.append({
                "pair": f"{a['name']} vs {b['name']}",
                "overlap_count": len(overlap),
                "overlap_ids": list(overlap),
            })
    return pairs


# =================================================
def run_check():
    open(LOG_TXT, "w", encoding="utf-8").close()
    tlog(f"TEST={TEST_KEY}")
    tlog(f"URL={GRAPHQL_URL}")

    all_results = []

    for cfg in REQUESTS_CONFIG:
        tlog(f"\n[{cfg['name']}] content_id={cfg['content_id'] or '(empty)'}")
        items = call_graphql(cfg["name"], cfg["content_id"])
        tlog(f"  items returned: {len(items)}")
        for i, it in enumerate(items):
            tlog(f"  [{i}] {it['id']}  {it['title'][:40]}")
        all_results.append({
            "name": cfg["name"],
            "content_id": cfg["content_id"],
            "items": items,
        })

    # =================================================
    # ตารางเปรียบเทียบ
    # =================================================
    tlog("\n=== COMPARISON TABLE ===")
    print_table(all_results)

    # =================================================
    # DUPLICATE CHECK
    # =================================================
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
        "url": GRAPHQL_URL,
        "requests": [
            {"name": r["name"], "content_id": r["content_id"], "item_count": len(r["items"]), "ids": [it["id"] for it in r["items"]]}
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
# def test_DMPREC_DEDUP():
#     result = run_check()
#     print("RESULT:", result["status"])


if __name__ == "__main__":
    run_check()


