"""conftest.py — pytest bootstrap สำหรับ Test_7-11_New

- เพิ่ม directory นี้เข้า sys.path เพื่อให้ import DMPREC_*.py ได้
- สร้าง reports/ directory ล่วงหน้า
"""

import os
import sys
from pathlib import Path

HERE = Path(__file__).parent

# ให้ import module ใน directory นี้ได้โดยตรง
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

# สร้าง reports directory ล่วงหน้า (ป้องกัน FileNotFoundError จาก DMPREC files)
os.makedirs(HERE / "reports", exist_ok=True)
