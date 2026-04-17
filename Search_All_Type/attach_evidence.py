#!/usr/bin/env python3
"""
attach_evidence.py
Bundle all evidence JSON files + HTML report into a zip,
then attach the zip to the Jira issue (DMPREC-15999) via Jira REST API.

Required env vars:
  JIRA_BASE_URL      e.g. https://yourcompany.atlassian.net
  JIRA_USER_EMAIL    e.g. your.email@company.com
  JIRA_API_TOKEN     Jira API token
  XRAY_TEST_EXEC_KEY e.g. DMPREC-15999
"""

import os
import sys
import glob
import zipfile
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

JIRA_BASE_URL  = os.environ["JIRA_BASE_URL"].strip().rstrip("/")
JIRA_EMAIL     = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN = os.environ["JIRA_API_TOKEN"]
ISSUE_KEY      = os.environ.get("XRAY_TEST_EXEC_KEY", "DMPREC-15999")

EVIDENCE_DIR   = "reports/evidence"
HTML_REPORT    = "reports/report.html"
ZIP_OUTPUT     = "reports/evidence_bundle.zip"


def build_zip():
    """Pack all evidence JSON + HTML report into one zip file."""
    ev_files = glob.glob(f"{EVIDENCE_DIR}/*.json")
    if not ev_files:
        print("No evidence files found — skipping")
        sys.exit(0)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    zip_path  = f"reports/evidence_bundle_{timestamp}.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fp in sorted(ev_files):
            zf.write(fp, arcname=os.path.join("evidence", os.path.basename(fp)))
            print(f"  + {os.path.basename(fp)}")
        if os.path.exists(HTML_REPORT):
            zf.write(HTML_REPORT, arcname="report.html")
            print(f"  + report.html")

    print(f"Bundle created : {zip_path}  ({len(ev_files)} evidence files)")
    return zip_path


def attach_to_jira(zip_path: str):
    """POST zip file as attachment to the Jira issue."""
    url  = f"{JIRA_BASE_URL}/rest/api/3/issue/{ISSUE_KEY}/attachments"
    auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
    hdrs = {
        "X-Atlassian-Token": "no-check",
        "Accept": "application/json",
    }

    print(f"Attaching to {ISSUE_KEY} ...")
    with open(zip_path, "rb") as f:
        resp = requests.post(
            url,
            headers=hdrs,
            auth=auth,
            files={"file": (os.path.basename(zip_path), f, "application/zip")},
            timeout=60,
        )

    print(f"HTTP Status : {resp.status_code}")
    if resp.status_code in (200, 201):
        attachments = resp.json()
        for att in attachments:
            print(f"  ✓ Attached : {att.get('filename')} (id={att.get('id')})")
        print("Done")
    else:
        print(f"ERROR: {resp.text[:500]}")
        sys.exit(1)


def main():
    zip_path = build_zip()
    attach_to_jira(zip_path)


if __name__ == "__main__":
    main()
