import requests
import json
import os
import random
from datetime import datetime
from collections import Counter

# ===================== CONFIG =====================
URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/sfv-p7"
    "?shelfId=Kaw6MLVzPWmo"
    "&limit=100"
    "&cursor=1"
    "&language=th"
    "&ssoId=22092422"
    "&verbose=debug"
)

RUNS = 10
TOP_K = 50
TIMEOUT = 20

OUT_TXT = "tc_merge_random.log"
OUT_ALL_IDS = "tc_merge_random_all_ids.txt"
OUT_CSV = "tc_merge_random_runs.csv"
OUT_HTML = "tc_merge_random_report.html"
OUT_JSON = "tc_merge_random.json"

# =================================================


def log(msg):
    line = f"{datetime.now()} | {msg}"
    print(line)
    with open(OUT_TXT, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def extract_ids(resp_json):
    try:
        return [
            x["id"]
            for x in resp_json["data"]["results"]["merge_page"]["result"]["items"]
            if isinstance(x, dict) and "id" in x
        ]
    except Exception:
        return []


# =========================
# Similarity Metrics
# =========================

def jaccard(a, b):
    sa, sb = set(a), set(b)
    return len(sa & sb) / max(1, len(sa | sb))


def kendall_similarity(a, b):
    pos_b = {x: i for i, x in enumerate(b)}
    common = [x for x in a if x in pos_b]
    n = len(common)
    if n < 5:
        return None

    seq = [pos_b[x] for x in common]

    inv = 0
    for i in range(n):
        for j in range(i + 1, n):
            if seq[i] > seq[j]:
                inv += 1

    total = n * (n - 1) // 2
    return 1.0 - inv / total if total else None


# =========================
# OUTPUT HELPERS
# =========================

def write_all_ids(run_lists):
    with open(OUT_ALL_IDS, "w", encoding="utf-8") as f:
        for r, ids in enumerate(run_lists, 1):
            f.write(f"\n===== RUN {r} =====\n")
            for i, _id in enumerate(ids, 1):
                f.write(f"{i:03d} {_id}\n")


def write_csv(run_lists):
    max_rank = min(len(x) for x in run_lists)

    headers = ["rank"] + [f"run_{i}" for i in range(1, len(run_lists) + 1)]
    rows = [",".join(headers)]

    for r in range(max_rank):
        row = [str(r + 1)]
        for run in run_lists:
            row.append(run[r])
        rows.append(",".join(row))

    with open(OUT_CSV, "w", encoding="utf-8") as f:
        f.write("\n".join(rows))


def write_html(summary, pairwise, run_lists):
    html = []
    html.append("<html><head><meta charset='utf-8'>")
    html.append("<style>table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:4px;font-size:12px}</style>")
    html.append("</head><body>")

    html.append("<h2>Randomness Proof Report</h2>")

    html.append("<h3>Summary</h3><table>")
    for k, v in summary.items():
        html.append(f"<tr><th>{k}</th><td>{v}</td></tr>")
    html.append("</table>")

    html.append("<h3>Pairwise Similarity</h3>")
    html.append("<table>")
    html.append("<tr><th>A</th><th>B</th><th>Jaccard</th><th>Kendall</th></tr>")

    for p in pairwise:
        kend = p["kendall"]
        kend_str = "" if kend is None else f"{kend:.3f}"

        html.append(
            "<tr>"
            f"<td>{p['a']}</td>"
            f"<td>{p['b']}</td>"
            f"<td>{p['jaccard']:.3f}</td>"
            f"<td>{kend_str}</td>"
            "</tr>"
        )

    html.append("</table>")

    html.append("<h3>All IDs (Top view)</h3>")
    html.append("<table>")
    html.append("<tr><th>Rank</th>")
    for i in range(len(run_lists)):
        html.append(f"<th>Run {i+1}</th>")
    html.append("</tr>")

    max_rank = min(len(x) for x in run_lists)

    for r in range(max_rank):
        html.append("<tr>")
        html.append(f"<td>{r+1}</td>")
        for run in run_lists:
            html.append(f"<td>{run[r]}</td>")
        html.append("</tr>")

    html.append("</table></body></html>")

    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write("".join(html))


# =========================
# MAIN
# =========================

def main():
    open(OUT_TXT, "w").close()

    run_lists = []

    log("START RANDOM PROOF TEST")

    for i in range(1, RUNS + 1):
        log(f"RUN {i}")

        r = requests.get(URL, timeout=TIMEOUT)
        data = r.json()

        ids = extract_ids(data)
        log(f"items={len(ids)}")

        for idx, _id in enumerate(ids, 1):
            log(f"{idx:03d} {_id}")

        if ids:
            run_lists.append(ids[:TOP_K])

    if len(run_lists) < 2:
        log("NOT ENOUGH RUNS")
        return

    # =====================
    # similarity compare
    # =====================
    pairwise = []
    j_scores = []
    k_scores = []

    for i in range(len(run_lists)):
        for j in range(i + 1, len(run_lists)):
            a = run_lists[i]
            b = run_lists[j]

            jac = jaccard(a, b)
            kend = kendall_similarity(a, b)

            pairwise.append({
                "a": i + 1,
                "b": j + 1,
                "jaccard": jac,
                "kendall": kend,
            })

            j_scores.append(jac)
            if kend is not None:
                k_scores.append(kend)

    sticky = 0
    for pos in range(TOP_K):
        c = Counter(lst[pos] for lst in run_lists)
        if c.most_common(1)[0][1] / len(run_lists) >= 0.8:
            sticky += 1

    verdict = "PASS_RANDOM_ENOUGH"
    if (sum(k_scores)/len(k_scores)) > 0.9 or sticky >= 5:
        verdict = "FAIL_NOT_RANDOM"

    summary = {
        "runs": len(run_lists),
        "avg_jaccard": round(sum(j_scores)/len(j_scores), 3),
        "avg_kendall": round(sum(k_scores)/len(k_scores), 3),
        "sticky_positions": sticky,
        "VERDICT": verdict,
    }

    write_all_ids(run_lists)
    write_csv(run_lists)
    write_html(summary, pairwise, run_lists)

    with open(OUT_JSON, "w") as f:
        json.dump(summary, f, indent=2)

    log("DONE")
    log(f"HTML REPORT -> {OUT_HTML}")


if __name__ == "__main__":
    main()
