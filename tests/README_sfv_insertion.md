# SFV-P4 Insertion Logic Test

## วิธีรัน

```bash
# จาก root ของ project
cd <project-root>

# รัน insertion tests พร้อม JSON report
pytest tests/test_sfv_insertion.py -v \
  --json-report --json-report-file=reports/sfv_insertion_report.json

# รันพร้อม HTML report (ถ้ามี pytest-html)
pytest tests/test_sfv_insertion.py -v \
  --html=reports/sfv_insertion_report.html --self-contained-html
```

---

## Test Cases ที่มี

| ID | สิ่งที่ทดสอบ |
|----|-------------|
| TC01_all_fix_distinct_positions | ทุก section เป็น fix, positions ต่างกัน, ตรวจ override ตามลำดับ |
| TC02_number_first_1 | เริ่ม insert ที่ block 1 (positions 0–5 ต้องไม่มี insert) |
| TC03_items_per_block_2 | history insert 2 items ต่อ block |
| TC04_position_exceeds_block_boundary | position=10 เกิน block_size=6 → ต้อง skip |
| TC05_all_random | ทุก section เป็น random → ตรวจแค่ว่ามี item จาก pool ใน result |

---

## เพิ่ม Test Case ใหม่

ที่ไฟล์ `test_sfv_insertion.py` เพิ่ม dict เข้า `TEST_CASES`:

```python
{
    "id": "TC06_my_new_case",
    "sso_id": "xxxxxxxxx",
    "sections": {
        "history":      dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[0]),
        "pin":          dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[1]),
        "live":         dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[2]),
        "google_trend": dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[3]),
        "viral":        dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[4]),
        "creator":      dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
    },
},
```

---

## Logic การ Validate

```
สำหรับแต่ละ section (ตามลำดับ: history→pin→live→google_trend→viral→creator):
  - ถ้า calculate_type=fix:
      สำหรับแต่ละ block ตั้งแต่ number_first:
        สำหรับแต่ละ position ใน positions[]:
          abs_pos = block_num * block_size + rel_pos
          ถ้า rel_pos < block_size AND abs_pos < total_item_size:
            → ตำแหน่งนี้ควรมี item จาก section นี้
            (section หลังทับ section ก่อน ถ้า position เดียวกัน)
  - ถ้า calculate_type=random:
      ตรวจแค่ว่ามี item จาก pool นี้ใน special_content_positions
```

---

## Output

- **JSON**: `reports/sfv_insertion_report.json`
- **CSV**: `reports/sfv_insertion_results_<timestamp>.csv`
- แต่ละ test case จะ save per-test JSON ใน pytest tmp_path ด้วย
