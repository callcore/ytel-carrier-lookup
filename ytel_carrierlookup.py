import csv
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote
from tqdm import tqdm
import sys
import threading
from collections import Counter
from datetime import datetime
import argparse

# =========================
# Argument parsing
# =========================
parser = argparse.ArgumentParser(description="Ytel Carrier Lookup")
parser.add_argument(
    "--resume",
    help="Resume a specific run folder (timestamp name)",
    required=False
)
args = parser.parse_args()

# =========================
# Base paths
# =========================
BASE_DIR = Path(__file__).resolve().parent
RESULTS_BASE_DIR = BASE_DIR / "Results_NetworkLookup"
RESULTS_BASE_DIR.mkdir(exist_ok=True)

# =========================
# Run directory selection
# =========================
if args.resume:
    RUN_DIR = RESULTS_BASE_DIR / args.resume
    if not RUN_DIR.exists():
        print(f"❌ Run folder not found: {RUN_DIR}")
        sys.exit(1)
    RESUME_MODE = True
else:
    RUN_TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    RUN_DIR = RESULTS_BASE_DIR / RUN_TS
    RUN_DIR.mkdir(parents=True)
    RESUME_MODE = False

print(f"\n📂 Using run directory:\n{RUN_DIR}\n")

# =========================
# Input
# =========================
INPUT_CSV = BASE_DIR / "listid_phone_live.csv"

# =========================
# Output files
# =========================
RESULTS_CSV = RUN_DIR / "listid_phone_live_results.csv"
VERIZON_RESULTS_CSV = RUN_DIR / "listid_phone_live_results_verizon.csv"
ERRORS_CSV = RUN_DIR / "listid_phone_live_errors.csv"

# =========================
# Configuration
# =========================
MAX_WORKERS = 40
BATCH_SIZE = 5000
MAX_RETRIES = 3
BACKOFF_BASE = 0.75
MAX_CONSECUTIVE_UNKNOWNS = 100

API_BASE_URL = "https://api.ytel.com/api/v4/carrier/lookup"
TOKEN_URL = "https://api.ytel.com/auth/v3/token"

# =========================
# Thread-local session
# =========================
thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=MAX_WORKERS,
            pool_maxsize=MAX_WORKERS,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        thread_local.session = session
    return thread_local.session


# =========================
# Token management (.env.ytel)
# =========================
_token_lock = threading.Lock()
_access_token = None
_credentials = None


def load_credentials():
    global _credentials
    if _credentials is not None:
        return _credentials

    creds = {}
    with open(BASE_DIR / ".env.ytel", "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                creds[k.strip()] = v.strip()

    if "username" not in creds or "password" not in creds:
        raise RuntimeError(".env.ytel must contain username and password")

    _credentials = creds
    return _credentials


def get_new_token():
    global _access_token
    creds = load_credentials()

    payload = {
        "captcha": "",
        "grantType": "resource_owner_credentials",
        "username": creds["username"],
        "password": creds["password"],
        "refreshToken": ""
    }

    session = get_session()
    response = session.post(TOKEN_URL, json=payload, timeout=15)
    response.raise_for_status()

    data = response.json()
    if not data.get("status") or not data.get("payload"):
        raise RuntimeError("Failed to obtain access token")

    _access_token = data["payload"][0]["accessToken"]
    return _access_token


def get_token():
    global _access_token
    with _token_lock:
        if _access_token is None:
            return get_new_token()
        return _access_token


# =========================
# Resume support
# =========================
def load_processed_numbers():
    processed = set()

    for path in (RESULTS_CSV, VERIZON_RESULTS_CSV, ERRORS_CSV):
        if path.exists():
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    processed.add(row["phone_number"])

    return processed


# =========================
# Phone normalization
# =========================
def normalize_phone(phone_number):
    digits = "".join(filter(str.isdigit, phone_number))
    if len(digits) == 10:
        return f"+1{digits}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


# =========================
# API lookup
# =========================
def lookup_number(row):
    phone_number = row.get("phone_number", "")
    normalized = normalize_phone(phone_number)

    if not normalized:
        return ("ERROR", row, None, None, "INVALID_NUMBER")

    phone_encoded = quote(normalized, safe="")
    url = f"{API_BASE_URL}/{phone_encoded}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {get_token()}",
            }

            session = get_session()
            response = session.get(url, headers=headers, timeout=15)

            if response.status_code in (401, 403):
                with _token_lock:
                    get_new_token()
                headers["Authorization"] = f"Bearer {get_token()}"
                response = session.get(url, headers=headers, timeout=15)

                if response.status_code in (401, 403):
                    return ("ERROR", row, None, None, "AUTH_ERROR")

            if response.status_code == 429:
                raise requests.exceptions.HTTPError("RATE_LIMIT")

            response.raise_for_status()
            data = response.json()

            if data.get("status") and data.get("payload"):
                payload = data["payload"][0]
                network = payload.get("network", "UNKNOWN")
                line_type = "wireless" if payload.get("wireless") else "landline_or_unknown"
                return ("OK", row, network, line_type, None)

            return ("OK", row, "UNKNOWN", "landline_or_unknown", None)

        except requests.exceptions.Timeout:
            error = "TIMEOUT"
        except requests.exceptions.HTTPError as e:
            error = str(e)
        except Exception as e:
            error = f"EXCEPTION: {e}"

        if attempt < MAX_RETRIES:
            time.sleep(BACKOFF_BASE * attempt)
        else:
            return ("ERROR", row, None, None, error)


# =========================
# Main
# =========================
def main():
    processed = load_processed_numbers()
    print(f"🔁 Resume mode: {RESUME_MODE}")
    print(f"✔ Already processed: {len(processed)}\n")

    rows = []
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        input_fields = reader.fieldnames

        if "phone_number" not in input_fields:
            raise RuntimeError("Input CSV must contain 'phone_number'")

        for row in reader:
            if row["phone_number"] not in processed:
                rows.append(row)

    print(f"▶ Remaining to process: {len(rows)}\n")

    result_fields = input_fields + ["network", "line_type"]
    error_fields = input_fields + ["error"]

    files = {
        "results": (RESULTS_CSV, result_fields),
        "verizon": (VERIZON_RESULTS_CSV, result_fields),
        "errors": (ERRORS_CSV, error_fields),
    }

    writers = {}
    handles = {}

    for key, (path, fields) in files.items():
        exists = path.exists()
        f = open(path, "a", newline="", encoding="utf-8")
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        writers[key] = w
        handles[key] = f

    stats = Counter({
        "OK": 0,
        "VERIZON": 0,
        "UNKNOWN": 0,
        "RATE_LIMIT": 0,
        "ERROR": 0,
    })

    consecutive_unknowns = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            with tqdm(total=len(rows), desc="Carrier lookup", unit="number") as pbar:

                for i in range(0, len(rows), BATCH_SIZE):
                    batch = rows[i:i + BATCH_SIZE]
                    futures = [executor.submit(lookup_number, row) for row in batch]

                    for future in as_completed(futures):
                        status, row, network, line_type, error = future.result()
                        pbar.update(1)

                        if status == "OK":
                            out = row.copy()
                            out["network"] = network
                            out["line_type"] = line_type

                            # ALWAYS write to main results
                            writers["results"].writerow(out)

                            # ALSO write Verizon rows to Verizon file
                            if network and "verizon" in network.lower():
                                writers["verizon"].writerow(out)
                                stats["VERIZON"] += 1

                            stats["OK"] += 1

                            if network == "UNKNOWN":
                                stats["UNKNOWN"] += 1
                                consecutive_unknowns += 1
                            else:
                                consecutive_unknowns = 0

                        else:
                            out = row.copy()
                            out["error"] = error
                            writers["errors"].writerow(out)
                            stats["ERROR"] += 1
                            consecutive_unknowns = 0

                            if error == "RATE_LIMIT":
                                stats["RATE_LIMIT"] += 1

                        pbar.set_postfix({
                            "OK": stats["OK"],
                            "VZ": stats["VERIZON"],
                            "UNKNOWN": stats["UNKNOWN"],
                            "429": stats["RATE_LIMIT"],
                            "ERR": stats["ERROR"],
                        })

                        if consecutive_unknowns >= MAX_CONSECUTIVE_UNKNOWNS:
                            raise RuntimeError("Too many consecutive UNKNOWN results")

    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user (Ctrl+C)")
    except RuntimeError as e:
        print("\n🚨 FATAL:", e)
    finally:
        for f in handles.values():
            f.close()

        print("\n===== Run Summary =====")
        for k, v in stats.items():
            print(f"{k}: {v}")
        print("\n✅ Safe to restart.")
        sys.exit(0)


if __name__ == "__main__":
    main()
