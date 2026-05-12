import requests
import json

SFV_URL = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sfv-b4"
    "?candidate_selection=mix"
    "&shelfId=8rMb9ZqnV5oA"
    "&ugc_sfv_ratio=0"
    "&verbose=debug"
    "&ssoId=68538853"
)

resp = requests.get(SFV_URL, timeout=30)
data = resp.json()

def print_keys(obj, prefix="", depth=3):
    if depth == 0:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            print(f"{prefix}{k}  →  {type(v).__name__}", end="")
            if isinstance(v, list):
                print(f"  (len={len(v)})", end="")
            print()
            print_keys(v, prefix + "  ", depth - 1)
    elif isinstance(obj, list) and len(obj) > 0:
        print(f"{prefix}[0]:")
        print_keys(obj[0], prefix + "  ", depth - 1)

print("=== TOP-LEVEL KEYS ===")
print_keys(data, depth=4)
