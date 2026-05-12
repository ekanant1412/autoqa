"""conftest.py — pytest bootstrap สำหรับ Test_7-11_New

- เพิ่ม directory นี้เข้า sys.path เพื่อให้ import DMPREC_*.py ได้
- สร้าง reports/ directory ล่วงหน้า
- สร้าง HTML + CSV report พร้อม evidence อัตโนมัติหลังรัน test เสร็จ
"""

import csv
import os
import re
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).parent

if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

os.makedirs(HERE / "reports", exist_ok=True)


# ─── Evidence Fixture ─────────────────────────────────────────────────────────

_evidence_store: dict[str, dict] = {}


@pytest.fixture
def evidence(request):
    store = {}
    yield store
    _evidence_store[request.node.nodeid] = store


# ─── Collect Results ──────────────────────────────────────────────────────────

_raw_reports = []  # เก็บ report ดิบก่อน แล้วค่อย merge evidence ตอน sessionfinish


def _extract_url_from_report(report) -> str:
    """ดึง URL จาก stdout / sections / longrepr ของ report"""
    # 1) ดึงจาก captured stdout (กรณีไม่ได้ใช้ -s)
    stdout = getattr(report, "capstdout", "") or ""
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("URL="):
            return line[4:].strip()

    # 2) ดึงจาก sections (pytest sections เช่น "Captured stdout call")
    for section_name, section_content in getattr(report, "sections", []):
        if "stdout" in section_name.lower():
            for line in section_content.splitlines():
                line = line.strip()
                if line.startswith("URL="):
                    return line[4:].strip()

    # 3) fallback — ดึง URL แรกที่เจอใน longrepr (error message)
    if report.longrepr:
        m = re.search(r"https?://\S+", str(report.longrepr))
        if m:
            url = m.group(0).rstrip("'\")")
            return url

    return ""


def pytest_runtest_logreport(report):
    if report.when != "call":
        return

    if report.passed:
        status = "PASSED"
        reason = ""
    elif report.failed:
        status = "FAILED"
        reason = str(report.longrepr).splitlines()[-1] if report.longrepr else ""
    else:
        status = "SKIPPED"
        reason = ""

    url_from_stdout = _extract_url_from_report(report)

    node_parts = report.nodeid.split("::")
    _raw_reports.append({
        "nodeid":         report.nodeid,
        "file":           node_parts[0].split("/")[-1] if node_parts else "",
        "class":          node_parts[1] if len(node_parts) > 2 else "",
        "test":           node_parts[-1],
        "status":         status,
        "duration_s":     f"{report.duration:.3f}",
        "reason":         reason,
        "url_from_stdout": url_from_stdout,
    })


# ─── HTML Report ──────────────────────────────────────────────────────────────

def _build_html(rows: list[dict], timestamp: str) -> str:
    total   = len(rows)
    passed  = sum(1 for r in rows if r["status"] == "PASSED")
    failed  = sum(1 for r in rows if r["status"] == "FAILED")
    skipped = total - passed - failed
    pass_pct = round(passed / total * 100) if total else 0

    # group by class
    groups: dict[str, list] = {}
    for r in rows:
        groups.setdefault(r["class"] or r["file"], []).append(r)

    def tc_number(test_name: str) -> str:
        """ดึง TC-XX จาก test name"""
        import re
        m = re.search(r"tc(\d+)", test_name)
        return f"TC-{m.group(1)}" if m else "-"

    def status_badge(status: str) -> str:
        colors = {"PASSED": "#22c55e", "FAILED": "#ef4444", "SKIPPED": "#f59e0b"}
        icons  = {"PASSED": "✓", "FAILED": "✗", "SKIPPED": "−"}
        c = colors.get(status, "#6b7280")
        i = icons.get(status, "?")
        return (
            f'<span style="background:{c};color:#fff;padding:2px 10px;'
            f'border-radius:12px;font-weight:600;font-size:12px">{i} {status}</span>'
        )

    def url_cell(url: str) -> str:
        if not url:
            return '<span style="color:#9ca3af">—</span>'
        # ถ้า URL ยาว ตัดให้สั้น แต่ tooltip แสดงเต็ม
        short = url if len(url) <= 80 else url[:77] + "..."
        return f'<span title="{url}" style="font-size:11px;color:#3b82f6;word-break:break-all">{short}</span>'

    rows_html = ""
    for group_name, group_rows in groups.items():
        rows_html += f"""
        <tr>
          <td colspan="7"
              style="background:#f1f5f9;padding:8px 16px;font-weight:700;
                     color:#475569;font-size:13px;border-top:2px solid #e2e8f0">
            📂 {group_name}
          </td>
        </tr>"""
        for r in group_rows:
            tc  = tc_number(r["test"])
            dur = f"{r['duration_s']}s"
            sample = r["data_sample"] or '<span style="color:#9ca3af">—</span>'
            count  = r["item_count"]  or '<span style="color:#9ca3af">—</span>'
            reason = r["reason"] or ""
            reason_cell = (
                f'<span style="color:#ef4444;font-size:11px">{reason[:120]}</span>'
                if reason else '<span style="color:#9ca3af">—</span>'
            )
            bg = "#fff" if r["status"] != "FAILED" else "#fff5f5"
            rows_html += f"""
        <tr style="background:{bg};border-bottom:1px solid #f1f5f9">
          <td style="padding:10px 12px;font-weight:600;color:#64748b;white-space:nowrap">{tc}</td>
          <td style="padding:10px 12px;font-size:12px;color:#1e293b;max-width:220px">{r['test']}</td>
          <td style="padding:10px 12px;text-align:center">{status_badge(r['status'])}</td>
          <td style="padding:10px 12px;font-size:11px;max-width:300px">{url_cell(r['url'])}</td>
          <td style="padding:10px 12px;text-align:center;font-weight:600;color:#0f172a">{count}</td>
          <td style="padding:10px 12px;font-size:11px;color:#334155;max-width:260px">{sample}</td>
          <td style="padding:10px 12px;font-size:11px;max-width:200px">{reason_cell}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Test Report — {timestamp}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #f8fafc; color: #1e293b; }}
  .header {{ background: linear-gradient(135deg,#1e293b,#334155); color:#fff; padding:32px 40px; }}
  .header h1 {{ font-size:22px; font-weight:700; margin-bottom:4px; }}
  .header p  {{ font-size:13px; opacity:.7; }}
  .summary {{ display:flex; gap:16px; padding:24px 40px; flex-wrap:wrap; }}
  .card {{ background:#fff; border-radius:12px; padding:20px 28px; flex:1; min-width:140px;
           box-shadow:0 1px 3px rgba(0,0,0,.08); border-top:4px solid #e2e8f0; }}
  .card.total  {{ border-color:#6366f1; }}
  .card.pass   {{ border-color:#22c55e; }}
  .card.fail   {{ border-color:#ef4444; }}
  .card.skip   {{ border-color:#f59e0b; }}
  .card .num   {{ font-size:36px; font-weight:800; line-height:1; }}
  .card .label {{ font-size:12px; color:#64748b; margin-top:4px; text-transform:uppercase; letter-spacing:.5px; }}
  .card.total .num {{ color:#6366f1; }}
  .card.pass  .num {{ color:#22c55e; }}
  .card.fail  .num {{ color:#ef4444; }}
  .card.skip  .num {{ color:#f59e0b; }}
  .progress-bar {{ margin:0 40px 24px; height:8px; background:#e2e8f0; border-radius:99px; overflow:hidden; }}
  .progress-fill {{ height:100%; background:linear-gradient(90deg,#22c55e,#4ade80);
                    width:{pass_pct}%; border-radius:99px; transition:width .6s; }}
  .table-wrap {{ margin:0 40px 40px; background:#fff; border-radius:12px;
                 box-shadow:0 1px 3px rgba(0,0,0,.08); overflow:auto; }}
  table {{ width:100%; border-collapse:collapse; }}
  thead th {{ background:#1e293b; color:#fff; padding:12px 12px; font-size:12px;
              text-transform:uppercase; letter-spacing:.5px; text-align:left; white-space:nowrap; }}
  tbody tr:hover {{ background:#f8fafc !important; }}
  .footer {{ text-align:center; padding:16px; font-size:12px; color:#94a3b8; }}
</style>
</head>
<body>

<div class="header">
  <h1>🧪 Test Report — sr-b3 Search Endpoint</h1>
  <p>Generated: {timestamp} &nbsp;|&nbsp; Service: ai-universal-service-711 (preprod-gcp-ai-bn)</p>
</div>

<div class="summary">
  <div class="card total"><div class="num">{total}</div><div class="label">Total</div></div>
  <div class="card pass"> <div class="num">{passed}</div><div class="label">Passed</div></div>
  <div class="card fail"> <div class="num">{failed}</div><div class="label">Failed</div></div>
  <div class="card skip"> <div class="num">{skipped}</div><div class="label">Skipped</div></div>
</div>

<div class="progress-bar"><div class="progress-fill"></div></div>

<div class="table-wrap">
  <table>
    <thead>
      <tr>
        <th>TC#</th>
        <th>Test Name</th>
        <th style="text-align:center">Status</th>
        <th>URL</th>
        <th style="text-align:center">Items</th>
        <th>Data / Evidence</th>
        <th>Fail Reason</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</div>

<div class="footer">Pass rate: {pass_pct}% &nbsp;|&nbsp; {passed}/{total} tests passed</div>
</body>
</html>"""


# ─── Session Finish ───────────────────────────────────────────────────────────

def pytest_sessionfinish(session, exitstatus):
    if not _raw_reports:
        return

    # merge evidence เข้ากับ report หลัง teardown ทุกตัวเสร็จแล้ว
    rows = []
    for r in _raw_reports:
        ev = _evidence_store.get(r["nodeid"], {})
        # ใช้ URL จาก evidence fixture ก่อน ถ้าไม่มีค่อย fallback ไป stdout
        url = ev.get("url", "") or r.get("url_from_stdout", "")
        rows.append({
            "file":        r["file"],
            "class":       r["class"],
            "test":        r["test"],
            "status":      r["status"],
            "duration_s":  r["duration_s"],
            "url":         url,
            "item_count":  ev.get("item_count", ""),
            "data_sample": ev.get("data_sample", ""),
            "reason":      r["reason"],
        })

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    report_dir = HERE / "reports"

    # CSV
    csv_path = report_dir / f"report_{timestamp}.csv"
    fieldnames = ["file", "class", "test", "status", "duration_s",
                  "url", "item_count", "data_sample", "reason"]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # HTML
    html_path = report_dir / f"report_{timestamp}.html"
    html_content = _build_html(rows, timestamp)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    total   = len(rows)
    passed  = sum(1 for r in rows if r["status"] == "PASSED")
    failed  = sum(1 for r in rows if r["status"] == "FAILED")
    skipped = sum(1 for r in rows if r["status"] == "SKIPPED")
    print(f"\n📊 Report → {html_path}")
    print(f"📄 CSV    → {csv_path}")
    print(f"   Total: {total}  |  Passed: {passed}  |  Failed: {failed}  |  Skipped: {skipped}")

    # ─── สรุป FAIL / SKIP พร้อม URL ──────────────────────────────────────────
    not_passed = [r for r in rows if r["status"] in ("FAILED", "SKIPPED")]
    if not_passed:
        print("\n" + "─" * 70)
        print("🔍  FAIL / SKIP — URL สำหรับตรวจสอบ")
        print("─" * 70)
        for r in not_passed:
            icon = "❌" if r["status"] == "FAILED" else "⏭"
            print(f"\n{icon}  [{r['status']}] {r['test']}")
            if r["url"]:
                print(f"   URL : {r['url']}")
            else:
                print(f"   URL : (ไม่พบ — ตรวจสอบ stdout หรือ longrepr)")
            if r["reason"]:
                # ตัดให้ไม่ยาวเกิน 200 ตัวอักษร
                short = r["reason"] if len(r["reason"]) <= 200 else r["reason"][:197] + "..."
                print(f"   WHY : {short}")
        print("─" * 70)
