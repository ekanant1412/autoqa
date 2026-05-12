"""
test_pp2_vs_pp5_no_duplicate.py
════════════════════════════════════════════════════════════════════════════════
- รัน 1000 รอบ
- ssoId เปลี่ยนทุกรอบ: "999", "nologin" สลับกัน (หรือเพิ่มใน SSO_IDS)
- ยิง p-p2 และ p-p5 พร้อมกัน (concurrent) และ log เวลา call แต่ละ request
- เช็คว่าไม่มี item id ซ้ำกันระหว่าง 2 DAG

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

# ssoId วนซ้ำทุกรอบตามลำดับนี้
SSO_IDS = ["999", "nologin"]

ROUNDS  = 100
TIMEOUT = 30

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════
SEP = "═" * 70

def log(msg):        print(msg)
def step(l, v=None): log(f"  ▶ {l}{(': ' + str(v)) if v is not None else ''}")
def ok(l):           log(f"  ✅ {l}")
def fail(l):         log(f"  ❌ {l}")


def call_api(name: str, url: str, sso_id: str) -> dict:
    params     = {"ssoId": sso_id}
    call_start = datetime.now()
    ts_start   = time.perf_counter()

    resp = requests.get(url, params=params, timeout=TIMEOUT)

    ts_end   = time.perf_counter()
    call_end = datetime.now()
    elapsed  = round((ts_end - ts_start) * 1000, 2)  # ms

    resp.raise_for_status()
    body = resp.json()

    return {
        "name":        name,
        "status_code": resp.status_code,
        "items":       [item["id"] for item in body.get("items", []) if "id" in item],
        "request_id":  body.get("request_id"),
        "call_start":  call_start.strftime("%H:%M:%S.%f")[:-3],   # HH:MM:SS.mmm
        "call_end":    call_end.strftime("%H:%M:%S.%f")[:-3],
        "elapsed_ms":  elapsed,
    }


def call_both_concurrent(sso_id: str) -> tuple[dict, dict]:
    """ยิง p-p2 และ p-p5 พร้อมกัน return (pp2, pp5)"""
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
# Parametrize: round=1..ROUNDS, ssoId วนตาม index
# ══════════════════════════════════════════════════════════════════════════════
def pytest_generate_tests(metafunc):
    if "param" in metafunc.fixturenames:
        params = [
            pytest.param(
                {
                    "round":  r,
                    "sso_id": SSO_IDS[(r - 1) % len(SSO_IDS)],
                },
                id=f"round={r}-ssoId={SSO_IDS[(r - 1) % len(SSO_IDS)]}"
            )
            for r in range(1, ROUNDS + 1)
        ]
        metafunc.parametrize("param", params)


# ══════════════════════════════════════════════════════════════════════════════
# Test
# ══════════════════════════════════════════════════════════════════════════════
class TestPP2vsPP5NoDuplicate:

    def test_no_duplicate_between_dag(self, param):
        round_  = param["round"]
        sso_id  = param["sso_id"]

        # ── Concurrent call ────────────────────────────────────────
        dispatch_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        pp2, pp5 = call_both_concurrent(sso_id)

        # ── Time log ───────────────────────────────────────────────
        log(f"\n{'─' * 70}")
        log(f"  Round {round_}/{ROUNDS}  |  ssoId={sso_id}")
        log(f"{'─' * 70}")
        log(f"  {'':20s}  {'start':>14}  {'end':>14}  {'elapsed':>10}")
        log(f"  {'dispatch (both)':20s}  {dispatch_time:>14}")
        log(f"  {'p-p2':20s}  {pp2['call_start']:>14}  {pp2['call_end']:>14}  {pp2['elapsed_ms']:>8.2f} ms")
        log(f"  {'p-p5':20s}  {pp5['call_start']:>14}  {pp5['call_end']:>14}  {pp5['elapsed_ms']:>8.2f} ms")

        # ตรวจว่า start time ต่างกันเท่าไหร่ (ms)
        fmt = "%H:%M:%S.%f"
        t_pp2 = datetime.strptime(pp2["call_start"] + "000", fmt)
        t_pp5 = datetime.strptime(pp5["call_start"] + "000", fmt)
        start_diff_ms = abs((t_pp2 - t_pp5).total_seconds() * 1000)
        log(f"  start time diff (pp2 vs pp5)         : {start_diff_ms:.2f} ms")
        if start_diff_ms < 50:
            ok(f"Called at nearly the same time ({start_diff_ms:.2f} ms apart) ✓")
        else:
            log(f"  ⚠️  Start time diff > 50ms ({start_diff_ms:.2f} ms) — not truly concurrent")

        # ── Item info ──────────────────────────────────────────────
        log(f"\n  p-p2  items={len(pp2['items'])}  req_id={pp2['request_id']}")
        log(f"  p-p2  ids: {pp2['items']}")
        log(f"\n  p-p5  items={len(pp5['items'])}  req_id={pp5['request_id']}")
        log(f"  p-p5  ids: {pp5['items']}")

        set_pp2    = set(pp2["items"])
        set_pp5    = set(pp5["items"])
        duplicates = sorted(set_pp2 & set_pp5)

        # ── T1: HTTP 200 ───────────────────────────────────────────
        assert pp2["status_code"] == 200, \
            f"[round={round_} ssoId={sso_id}] p-p2 HTTP {pp2['status_code']}"
        assert pp5["status_code"] == 200, \
            f"[round={round_} ssoId={sso_id}] p-p5 HTTP {pp5['status_code']}"

        # ── T2: มี items ───────────────────────────────────────────
        assert pp2["items"], \
            f"[round={round_} ssoId={sso_id}] p-p2 returned no items"
        assert pp5["items"], \
            f"[round={round_} ssoId={sso_id}] p-p5 returned no items"

        # ── T3: ไม่มี cross-DAG duplicate ─────────────────────────
        if not duplicates:
            ok(f"No cross-DAG duplicates ✓")
        else:
            fail(f"cross-DAG duplicates found: {duplicates}")

        assert not duplicates, (
            f"[round={round_} ssoId={sso_id}] T3 FAIL — "
            f"{len(duplicates)} duplicate(s): {duplicates}\n"
            f"  p-p2 req_id={pp2['request_id']}  start={pp2['call_start']}\n"
            f"  p-p5 req_id={pp5['request_id']}  start={pp5['call_start']}"
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

        ok(f"All checks passed ✓")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s"]))