"""test_all.py — รวม test ทุกไฟล์ให้รันด้วย pytest ทีเดียว
=============================================================

ไฟล์ที่ pytest เก็บ test โดยตรง (มี test_ function อยู่แล้ว):
  - Verify_*.py  (ทุกไฟล์ใน directory นี้)

ไฟล์ที่ชื่อมีอักขระพิเศษ/ช่องว่าง → ไฟล์นี้จัดการแทนด้วย importlib:
  - Verify_creator_logic_p7,p8.py
  - Verify_merge_page_random_p8.py
  - Verify_tophit_logic_p7,p8.py              (p7 only)
  - Verify_latest_logic_p7,p8.py              (p7 only)
  - Verify_seen_item_full_cursor_p8.py
  - Verify_duplicate_item_all_cursor_p8.py

รันคำสั่ง:
  pytest               # รันทุก test ทั้งหมด
  pytest test_all.py -v  # รันเฉพาะไฟล์ที่ชื่อมีอักขระพิเศษ
"""

import importlib.util
import sys
import pytest
from collections import deque
from pathlib import Path

HERE = Path(__file__).parent


# ─── Helper: โหลด module จาก path (รองรับชื่อไฟล์พิเศษ) ────────────────────
def _load(filename: str, alias: str):
    path = HERE / filename
    spec = importlib.util.spec_from_file_location(alias, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return mod


# ════════════════════════════════════════════════════════════════════════════
# 1) [7-11]-Check_creator_logic.py
#    ตรวจ create_by ไม่ควรมี consecutive เกิน MAX_CONSECUTIVE_ALLOWED (p7 + p8)
# ════════════════════════════════════════════════════════════════════════════
_creator = _load("Verify_creator_logic_p7,p8.py", "_tc_creator")


def test_7_11_creator_logic_sfv_p7():
    """[7-11] sfv-p7: create_by consecutive check"""
    _creator.test_7_11_creator_logic_sfv_p7()


def test_7_11_creator_logic_sfv_p8():
    """[7-11] sfv-p8: create_by consecutive check"""
    _creator.test_7_11_creator_logic_sfv_p8()


# ════════════════════════════════════════════════════════════════════════════
# 2) [7-11]-Check_merge_page_random.py
#    ตรวจว่า merge_page ส่งผลลัพธ์ random เพียงพอ (Kendall ≤ 0.9, sticky < 5)
# ════════════════════════════════════════════════════════════════════════════
_rand = _load("Verify_merge_page_random_p8.py", "_tc_merge_random")



def test_7_11_merge_page_random_sfv_p8():
    """[7-11] sfv-p8: merge_page randomness check"""
    _rand.test_7_11_merge_page_random_sfv_p8()


# ════════════════════════════════════════════════════════════════════════════
# 3) [7-11]-Check_tophit_latest_logic.py
#    ตรวจ bucketize_tophit_sfv: publish_date ต้องอยู่ใน window และเรียงลด
# ════════════════════════════════════════════════════════════════════════════
_tophit = _load("Verify_tophit_logic_p7,p8.py", "_tc_tophit")


def test_7_11_tophit_latest_logic_sfv_p7():
    """[7-11] sfv-p7: bucketize_tophit_sfv publish_date check"""
    _tophit.test_7_11_tophit_latest_logic_sfv_p7()


# ════════════════════════════════════════════════════════════════════════════
# 4) latest.py
#    ตรวจ bucketize_tophit_sfv publish_date (คล้าย #3 แต่ window ต่างกัน)
# ════════════════════════════════════════════════════════════════════════════
_latest = _load("Verify_latest_logic_p7,p8.py", "_tc_latest")


def test_latest_sfv_publish_date_sfv_p7():
    """latest sfv-p7: bucketize_tophit_sfv publish_date check"""
    _latest.test_latest_sfv_publish_date_sfv_p7()


# ════════════════════════════════════════════════════════════════════════════
# 5) Mark some SFV as viewed and verify they are handled as seen items per requirement.py
#    TC-B seen FIFO strict check
# ════════════════════════════════════════════════════════════════════════════
_seen_tc = _load("Verify_seen_item_full_cursor_p8.py", "_tc_seen")

_SEEN_MAX_CURSORS = 10  # จำกัด cursor สำหรับ test (ไม่ต้องรัน 50 ครั้ง)


def test_mark_sfv_as_viewed_seen_items():
    """TC-B: slice_pagination(req N) ids ต้องปรากฏใน get_seen_item_redis(req N+1)"""
    expected_seen = deque()
    prev_slice_ids = None
    cursor = _seen_tc.START_CURSOR
    failures = []

    for _ in range(_SEEN_MAX_CURSORS):
        status, j, _ = _seen_tc.fetch_json(cursor)
        if status != 200:
            break

        slice_ids = _seen_tc.extract_slice_pagination_ids(j)
        seen_ids = _seen_tc.extract_seen_ids(j)

        if not slice_ids:
            break

        if prev_slice_ids is not None:
            _seen_tc.fifo_push_strict(expected_seen, prev_slice_ids, _seen_tc.SEEN_LIMIT)
            expected_set = set(expected_seen)
            actual_set = set(seen_ids)
            missing = sorted(expected_set - actual_set)
            size_ok = len(seen_ids) <= _seen_tc.SEEN_LIMIT

            if not size_ok:
                failures.append(
                    f"cursor={cursor}: seen count={len(seen_ids)} > SEEN_LIMIT={_seen_tc.SEEN_LIMIT}"
                )
            if missing:
                failures.append(
                    f"cursor={cursor}: {len(missing)} expected ids missing from seen. "
                    f"Sample: {missing[:5]}"
                )

        prev_slice_ids = slice_ids
        cursor += _seen_tc.CURSOR_STEP

    assert not failures, "TC-B seen FIFO check failed:\n" + "\n".join(failures)


# ════════════════════════════════════════════════════════════════════════════
# 6) Verify no duplicate item IDs in the final response (and across pages if pagination is used)..py
#    ตรวจ merge_page: ไม่มี id ซ้ำทั้งภายในหน้าและข้ามหน้า (จำกัด 10 cursor)
# ════════════════════════════════════════════════════════════════════════════
_dedup = _load("Verify_duplicate_item_all_cursor_p8.py", "_tc_dedup")


@pytest.mark.skip(reason="ไม่เทสเคสนี้")
def test_verify_no_duplicate_item_ids_sfv_p8():
    """Verify sfv-p8: ไม่มี duplicate IDs ใน merge_page (intra + cross, 10 cursors)"""
    _dedup.test_verify_no_duplicate_item_ids_sfv_p8()


# ════════════════════════════════════════════════════════════════════════════


def test_verify_pagination_no_repeat_sfv_p8():
    """Verify sfv-p8: merge_page randomness + no-repeat check"""
    _rand.test_7_11_merge_page_random_sfv_p8()
