import requests
import time

# ===================================
# CONFIG
# ===================================

URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sfv-p7"
    "?shelfId=BJq5rZqYzjgJ"
    "&total_candidates=300"
    "&pool_limit_category_items=50"
    "&language=th"
    "&limit=100"
    "&userId=null"
    "&pseudoId=null"
    "&cursor=1"
    "&ga_id=100118391.0851155978"
    "&ssoId=22092422"
    "&is_use_live=false"
    "&verbose=debug"
)

RUNS = 20
TIMEOUT = 20

BANNED_IDS = {
    "wj5GOky1mkYx","yVb5Mwjx55yK","09pGq8RbEv1K","5GMzjgY2VO8A",
    "xo4XD2gbaJpo","2OramNWW42mM","lk1xWMlyB3pq","rGRzA9EdZRLG",
    "QXzdvLe5y5yq","nY4NvWQvwyZ5","KRZ0EyX7X6bW","8GYgr8R94B5l",
    "npXJYPV0LPWb","G5pjqMJekWyP","YoQPY686MqbN","a13LzAMLLVNg",
    "YVdk7XjNLn5p","0XNNmQDmN6LX","jl1Oq5YMADw0","WkZlKxJoW3wq",
    "4OeVdaXapd5l","LVQ1o5Wd8QdV","6kMBqG4BMD3y"
}

# ===================================
# HELPERS
# ===================================

def deep_find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = deep_find(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for it in obj:
            r = deep_find(it, key)
            if r is not None:
                return r
    return None


def extract_merge_ids(json_data):
    merge_page = deep_find(json_data, "merge_page")
    if not merge_page:
        return []

    items = merge_page.get("result", {}).get("items", [])
    ids = []

    for it in items:
        if not isinstance(it, dict):
            continue

        # CMS item
        if isinstance(it.get("id"), str):
            ids.append(it["id"])

        # LIVE wrapper
        if isinstance(it.get("items"), list):
            for live in it["items"]:
                if isinstance(live, dict) and "Id" in live:
                    ids.append(str(live["Id"]))

    return ids


# ===================================
# MAIN LOOP
# ===================================

print("\n========== EXCLUDE VALIDATION ==========\n")

for run in range(1, RUNS + 1):

    print(f"\n--- RUN {run} ---")

    r = requests.get(URL, timeout=TIMEOUT)
    data = r.json()

    ids = extract_merge_ids(data)
    id_set = set(ids)

    # ===== Evidence =====
    print(f"Total IDs extracted from merge_page: {len(ids)}")

    # show all
    print("Sample extracted IDs:")
    for x in ids:
        print("  ", x)

    # ===== Check banned =====
    intersection = sorted(id_set.intersection(BANNED_IDS))

    print("\nCheck banned IDs existence:")
    if intersection:
        print("❌ FOUND (SHOULD NOT EXIST):")
        for x in intersection:
            print("   ", x)
    else:
        print("✅ No banned IDs found in merge_page")

    time.sleep(0.2)

print("\n========== DONE ==========\n")