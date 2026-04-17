"""
check_tophit_violations.py
รันแยกเพื่อดูว่า item ไหนใน bucketize_tophit_sfv ที่มี publish_date เกิน 28 วัน (sfv-p6)

usage:
    python src/Test_7-11_New_preprod/check_tophit_violations.py
"""

import requests
import json
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Optional

URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sfv-p6"
    "?shelfId=Kaw6MLVzPWmo"
    "&total_candidates=200"
    "&pool_limit_category_items=100"
    "&language=th&pool_tophit_date=28"
    "&userId=null&pseudoId=null"
    "&cursor=1&ga_id=999999999.999999999"
    "&is_use_live=true&verbose=debug&pool_latest_date=365"
    "&limit=3&limit_seen_item=1"
)

NODE_NAME   = "bucketize_tophit_sfv"
WINDOW_DAYS = 28
TIMEOUT     = 30


def deep_find_node(obj: Any, target: str) -> Optional[dict]:
    if isinstance(obj, dict):
        if target in obj and isinstance(obj[target], dict):
            return obj[target]
        if obj.get("name") == target:
            return obj
        for v in obj.values():
            found = deep_find_node(v, target)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = deep_find_node(v, target)
            if found:
                return found
    return None


def main():
    now_utc = datetime.now(ZoneInfo("Asia/Bangkok")).astimezone(timezone.utc)
    cutoff  = now_utc - timedelta(days=WINDOW_DAYS)

    print(f"URL     : {URL}")
    print(f"Now UTC : {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Cutoff  : {cutoff.strftime('%Y-%m-%d %H:%M:%S %Z')}  (เกิน {WINDOW_DAYS} วัน = fail)")
    print()

    r = requests.get(URL, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()

    node = deep_find_node(data, NODE_NAME)
    if not node:
        print(f"❌ ไม่พบ node '{NODE_NAME}' ใน response")
        return

    result_obj = node.get("result", {})
    if not isinstance(result_obj, dict):
        print("❌ result ไม่ใช่ dict")
        return

    total_items    = 0
    total_groups   = 0
    problem_groups = 0

    for group_name, items in result_obj.items():
        if not isinstance(items, list):
            continue

        total_groups += 1
        violations = []

        for idx, it in enumerate(items):
            total_items += 1
            item_id  = it.get("id", "?")
            pd_raw   = it.get("publish_date", "")

            try:
                pd_dt = datetime.fromisoformat(
                    pd_raw.replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except Exception as e:
                violations.append({
                    "idx": idx, "id": item_id,
                    "publish_date": pd_raw,
                    "reason": f"parse error: {e}",
                })
                continue

            age_days = (now_utc - pd_dt).days
            if pd_dt < cutoff or pd_dt > now_utc:
                violations.append({
                    "idx": idx,
                    "id": item_id,
                    "publish_date": pd_raw,
                    "age_days": age_days,
                    "reason": "out_of_28_day_window",
                })

        if violations:
            problem_groups += 1
            print(f"━━━ group: {group_name}  ({len(violations)} violation(s)) ━━━")
            for v in violations:
                age = v.get("age_days", "?")
                print(f"  idx={v['idx']}  id={v['id']}")
                print(f"         publish_date : {v['publish_date']}")
                print(f"         age          : {age} วัน  ← เกิน {WINDOW_DAYS} วัน")
                print(f"         reason       : {v['reason']}")
            print()

    print("─" * 60)
    print(f"Groups checked : {total_groups}")
    print(f"Items checked  : {total_items}")
    print(f"Problem groups : {problem_groups}")
    if problem_groups == 0:
        print("✅ ไม่พบ violation")
    else:
        print(f"❌ พบ {problem_groups} group(s) ที่มีปัญหา")


if __name__ == "__main__":
    main()
