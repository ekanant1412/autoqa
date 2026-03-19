"""
test_pp2_vs_pp5_no_duplicate.py
════════════════════════════════════════════════════════════════════════════════
เช็คว่า p-p2 และ p-p5 เมื่อยิงพร้อมกัน ต้องไม่มี item id ซ้ำกัน
ทดสอบ 2 กรณี x 1000 รอบ:
  - ssoId = "999"      (logged-in user)
  - ssoId = "nologin"  (guest user)

วิธีรัน:
  pip install pytest requests
  pytest test_pp2_vs_pp5_no_duplicate.py -v -s
════════════════════════════════════════════════════════════════════════════════
"""

import pytest
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
BASE_URL = "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"

ENDPOINTS = {
    "p-p2": f"{BASE_URL}/api/v1/universal/p-p2",
    "p-p5": f"{BASE_URL}/api/v1/universal/p-p5",
}

SSO_IDS    = ["999", "nologin"]
ROUNDS     = 1000
TIMEOUT    = 30

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════
SEP = "═" * 65

def log(msg):        print(msg)
def step(l, v=None): log(f"  ▶ {l}{(': ' + str(v)) if v is not None else ''}")
def ok(l):           log(f"  ✅ {l}")
def fail(l):         log(f"  ❌ {l}")


def call_api(name: str, url: str, sso_id: str) -> dict:
    params = {"ssoId": sso_id}
    resp   = requests.get(url, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    return {
        "name":        name,
        "status_code": resp.status_code,
        "body":        body,
        "items":       [item["id"] for item in body.get("items", []) if "id" in item],
        "request_id":  body.get("request_id"),
    }


def call_both_concurrent(sso_id: str) -> tuple[dict, dict]:
    results = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(call_api, name, url, sso_id): name
            for name, url in ENDPOINTS.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            results[name] = future.result()
    return results["p-p2"], results["p-p5"]


# ══════════════════════════════════════════════════════════════════════════════
# Parametrize: ssoId x round (1..ROUNDS)
# ══════════════════════════════════════════════════════════════════════════════
def pytest_generate_tests(metafunc):
    if "param" in metafunc.fixturenames:
        params = [
            pytest.param(
                {"sso_id": sso_id, "round": r},
                id=f"ssoId={sso_id}-round={r}"
            )
            for sso_id in SSO_IDS
            for r in range(1, ROUNDS + 1)
        ]
        metafunc.parametrize("param", params)


# ══════════════════════════════════════════════════════════════════════════════
# Test
# ══════════════════════════════════════════════════════════════════════════════
class TestPP2vsPP5NoDuplicate:

    def test_no_duplicate_between_dag(self, param):
        sso_id = param["sso_id"]
        round_ = param["round"]

        pp2, pp5 = call_both_concurrent(sso_id)

        log(f"\n[ssoId={sso_id}  round={round_}/{ROUNDS}]")
        step("p-p2 items", len(pp2["items"]))
        step("p-p5 items", len(pp5["items"]))

        set_pp2    = set(pp2["items"])
        set_pp5    = set(pp5["items"])
        duplicates = sorted(set_pp2 & set_pp5)

        # ── T1: HTTP 200 ───────────────────────────────────────────
        assert pp2["status_code"] == 200, \
            f"[ssoId={sso_id} round={round_}] p-p2 returned HTTP {pp2['status_code']}"
        assert pp5["status_code"] == 200, \
            f"[ssoId={sso_id} round={round_}] p-p5 returned HTTP {pp5['status_code']}"

        # ── T2: มี items ───────────────────────────────────────────
        assert pp2["items"], \
            f"[ssoId={sso_id} round={round_}] p-p2 returned no items"
        assert pp5["items"], \
            f"[ssoId={sso_id} round={round_}] p-p5 returned no items"

        # ── T3: ไม่มี cross-DAG duplicate ─────────────────────────
        if duplicates:
            fail(f"round={round_} cross-DAG duplicates: {duplicates}")
        else:
            ok(f"round={round_} no cross-DAG duplicates ✓")

        assert not duplicates, (
            f"[ssoId={sso_id} round={round_}] T3 FAIL — "
            f"{len(duplicates)} duplicate(s) between p-p2 and p-p5: {duplicates}\n"
            f"  p-p2 req_id: {pp2['request_id']}\n"
            f"  p-p5 req_id: {pp5['request_id']}"
        )

        # ── T4: ไม่มี duplicate ภายใน p-p2 ───────────────────────
        seen, dupes_pp2 = set(), []
        for i in pp2["items"]:
            if i in seen: dupes_pp2.append(i)
            else: seen.add(i)

        assert not dupes_pp2, (
            f"[ssoId={sso_id} round={round_}] T4 FAIL — "
            f"internal duplicates in p-p2: {dupes_pp2}"
        )

        # ── T5: ไม่มี duplicate ภายใน p-p5 ───────────────────────
        seen, dupes_pp5 = set(), []
        for i in pp5["items"]:
            if i in seen: dupes_pp5.append(i)
            else: seen.add(i)

        assert not dupes_pp5, (
            f"[ssoId={sso_id} round={round_}] T5 FAIL — "
            f"internal duplicates in p-p5: {dupes_pp5}"
        )


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s"]))