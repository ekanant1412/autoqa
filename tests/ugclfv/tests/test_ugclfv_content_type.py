"""
test_ugclfv_content_type.py
────────────────────────────
Pytest wrapper สำหรับ check_ugclfv_content_type.py
เพื่อให้ collect ได้ด้วย pytest และส่ง JUnit XML เข้า Xray ได้
"""

import sys
import os

# เพิ่ม path ให้ import check_ugclfv_content_type ได้
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import check_ugclfv_content_type as checker

# ── เลือก profile ที่ต้องการทดสอบ ──────────────────────────────────────────────
PROFILES = checker.PROFILES


def _run(profile_name: str):
    profile = next(p for p in PROFILES if p["name"] == profile_name)
    checker._current_profile = profile_name
    return checker.run_profile(profile)


# ── sfv-p4 ─────────────────────────────────────────────────────────────────────

class TestUgclfvSfvP4:
    _results = None

    @classmethod
    def setup_class(cls):
        cls._results = _run("sfv-p4")

    def test_content_type_is_ugclfv(self):
        """[sfv-p4] candidate_latest_ugc_lfv ทุก ID ต้องมี content_type == ugclfv"""
        assert self._results["content_type"], \
            "บาง ID มี content_type ไม่ใช่ ugclfv"

    def test_ugclfv_appears_in_merge_page(self):
        """[sfv-p4] ugclfv IDs ต้องปรากฏใน merge_page cursor=1"""
        assert self._results["merge_page"], \
            "ไม่พบ ugclfv ID ใน merge_page"

    def test_pin_reserved_positions_match_slice_globals(self):
        """[sfv-p4] reservedPositions ต้องมาจาก slice_pin_globals ทั้งหมด"""
        assert self._results["pin"], \
            "พบ reserved ID ที่ไม่อยู่ใน slice_pin_globals"

    def test_no_duplicate_ids_across_cursors(self):
        """[sfv-p4] ไม่มี duplicate ID ข้ามทุก cursor"""
        assert self._results["dup"], \
            "พบ duplicate ID ใน merge_page ระหว่าง cursor"

    def test_seen_items_accumulate_correctly(self):
        """[sfv-p4] get_seen_item_redis ต้องมี 5 IDs แรกของ logic_filter ทุก cursor"""
        assert self._results["seen"], \
            "get_seen_item_redis ไม่ครบตาม logic_filter"


# ── sfv-p5 ─────────────────────────────────────────────────────────────────────

class TestUgclfvSfvP5:
    _results = None

    @classmethod
    def setup_class(cls):
        cls._results = _run("sfv-p5")

    def test_content_type_is_ugclfv(self):
        """[sfv-p5] candidate_latest_ugc_lfv ทุก ID ต้องมี content_type == ugclfv"""
        assert self._results["content_type"], \
            "บาง ID มี content_type ไม่ใช่ ugclfv"

    def test_ugclfv_appears_in_merge_page(self):
        """[sfv-p5] ugclfv IDs ต้องปรากฏใน merge_page cursor=1"""
        assert self._results["merge_page"], \
            "ไม่พบ ugclfv ID ใน merge_page"

    def test_pin_reserved_positions_match_slice_globals(self):
        """[sfv-p5] reservedPositions ต้องมาจาก slice_pin_globals ทั้งหมด"""
        assert self._results["pin"], \
            "พบ reserved ID ที่ไม่อยู่ใน slice_pin_globals"

    def test_no_duplicate_ids_across_cursors(self):
        """[sfv-p5] ไม่มี duplicate ID ข้ามทุก cursor"""
        assert self._results["dup"], \
            "พบ duplicate ID ใน merge_page ระหว่าง cursor"

    def test_seen_items_accumulate_correctly(self):
        """[sfv-p5] get_seen_item_redis ต้องมี 5 IDs แรกของ logic_filter ทุก cursor"""
        assert self._results["seen"], \
            "get_seen_item_redis ไม่ครบตาม logic_filter"
