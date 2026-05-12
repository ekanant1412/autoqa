"""
test_pp2_vs_pp5_no_duplicate.py
════════════════════════════════════════════════════════════════════════════════
- รัน 1000 รอบ
- ssoId เริ่มจาก 1001 ไปเรื่อยๆ: round=1→ssoId=1001, round=2→ssoId=1002, ...
- ยิง p-p2 และ p-p5 พร้อมกัน (concurrent) แสดงเวลาที่ call แต่ละ request

วิธีรัน:
  pip install pytest requests
  pytest test_pp2_vs_pp5_no_duplicate.py -v -s
════════════════════════════════════════════════════════════════════════════════
"""

import pytest
import requests
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
BASE_URL = "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"

ENDPOINTS = {
    "p-p2": f"{BASE_URL}/api/v1/universal/p-p2",
    "p-p5": f"{BASE_URL}/api/v1/universal/p-p5",
}

ROUNDS    = 100
SSO_START = 1001   # round=1→1001, round=2→1002, ..., round=1000→2000
TIMEOUT   = 30


def get_sso_id(round_: int) -> str:
    return str(SSO_START + (round_ - 1))  # 1001, 1002, 1003, ...


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════
SEP = "═" * 70

def log(msg):        print(msg)
def step(l, v=None): log(f"  ▶ {l}{(': ' + str(v)) if v is not None else ''}")
def ok(l):           log(f"  ✅ {l}")
def fail(l):         log(f"  ❌ {l}")

def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]  # HH:MM:SS.mmm


def call_api(name: str, url: str, sso_id: str) -> dict:
    params    = {"ssoId": sso_id}
    call_time = ts()
    t_start   = time.perf_counter()
    resp      = requests.get(url, params=params, timeout=TIMEOUT)
    elapsed   = round((time.perf_counter() - t_start) * 1000, 1)

    resp.raise_for_status()
    body = resp.json()

    return {
        "name":        name,
        "status_code": resp.status_code,
        "items":       [item["id"] for item in body.get("items", []) if "id" in item],
        "request_id":  body.get("request_id"),
        "call_time":   call_time,
        "elapsed_ms":  elapsed,
    }


def call_both_concurrent(sso_id: str) -> tuple[dict, dict]:
    results = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(call_api, name, url, sso_id): name
            for name, url in ENDPOINTS.items()
        }
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return results["p-p2"], results["p-p5"]


# ══════════════════════════════════════════════════════════════════════════════
# Parametrize: round 1..ROUNDS
# ══════════════════════════════════════════════════════════════════════════════
def pytest_generate_tests(metafunc):
    if "round_" in metafunc.fixturenames:
        metafunc.parametrize(
            "round_",
            range(1, ROUNDS + 1),
            ids=[f"round={r}-ssoId={get_sso_id(r)}" for r in range(1, ROUNDS + 1)],
        )


# ══════════════════════════════════════════════════════════════════════════════
# Test
# ══════════════════════════════════════════════════════════════════════════════
class TestPP2vsPP5NoDuplicate:

    def test_no_duplicate_between_dag(self, round_: int):
        sso_id = get_sso_id(round_)

        pp2, pp5 = call_both_concurrent(sso_id)

        # ── Timing log ────────────────────────────────────────────
        fmt_dt   = "%H:%M:%S.%f"
        dt_pp2   = datetime.strptime(pp2["call_time"], fmt_dt)
        dt_pp5   = datetime.strptime(pp5["call_time"], fmt_dt)
        diff_ms  = abs((dt_pp2 - dt_pp5).total_seconds() * 1000)

        log(f"\n{'─'*70}")
        log(f"  round={round_}/{ROUNDS}  |  ssoId={sso_id}")
        log(f"{'─'*70}")
        log(f"  {'':6}  {'call_time':15}  {'elapsed':>10}  {'items':>6}  request_id")
        log(f"  {'p-p2':6}  {pp2['call_time']:15}  {str(pp2['elapsed_ms'])+'ms':>10}  {len(pp2['items']):>6}  {pp2['request_id']}")
        log(f"  {'p-p5':6}  {pp5['call_time']:15}  {str(pp5['elapsed_ms'])+'ms':>10}  {len(pp5['items']):>6}  {pp5['request_id']}")
        log(f"  call_time diff: {diff_ms:.1f} ms {'(concurrent ✓)' if diff_ms < 50 else '(not concurrent ⚠️)'}")

        set_pp2    = set(pp2["items"])
        set_pp5    = set(pp5["items"])
        duplicates = sorted(set_pp2 & set_pp5)

        # ── T1: HTTP 200 ───────────────────────────────────────────
        assert pp2["status_code"] == 200, \
            f"[round={round_} ssoId={sso_id}] p-p2 returned HTTP {pp2['status_code']}"
        assert pp5["status_code"] == 200, \
            f"[round={round_} ssoId={sso_id}] p-p5 returned HTTP {pp5['status_code']}"

        # ── T2: มี items ───────────────────────────────────────────
        assert pp2["items"], \
            f"[round={round_} ssoId={sso_id}] p-p2 returned no items"
        assert pp5["items"], \
            f"[round={round_} ssoId={sso_id}] p-p5 returned no items"

        # ── T3: ไม่มี cross-DAG duplicate ─────────────────────────
        if duplicates:
            fail(f"cross-DAG duplicates: {duplicates}")
        else:
            ok(f"no cross-DAG duplicates ✓")

        assert not duplicates, (
            f"[round={round_} ssoId={sso_id}] T3 FAIL — "
            f"{len(duplicates)} duplicate(s) between p-p2 and p-p5: {duplicates}\n"
            f"  p-p2 call_time={pp2['call_time']}  elapsed={pp2['elapsed_ms']}ms\n"
            f"  p-p5 call_time={pp5['call_time']}  elapsed={pp5['elapsed_ms']}ms"
        )

        # ── T4: ไม่มี duplicate ภายใน p-p2 ───────────────────────
        seen, dupes_pp2 = set(), []
        for i in pp2["items"]:
            if i in seen: dupes_pp2.append(i)
            else: seen.add(i)

        assert not dupes_pp2, (
            f"[round={round_} ssoId={sso_id}] T4 FAIL — "
            f"internal duplicates in p-p2: {dupes_pp2}"
        )

        # ── T5: ไม่มี duplicate ภายใน p-p5 ───────────────────────
        seen, dupes_pp5 = set(), []
        for i in pp5["items"]:
            if i in seen: dupes_pp5.append(i)
            else: seen.add(i)

        assert not dupes_pp5, (
            f"[round={round_} ssoId={sso_id}] T5 FAIL — "
            f"internal duplicates in p-p5: {dupes_pp5}"
        )

        ok(f"PASSED")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s"]))