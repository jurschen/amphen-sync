import gzip
import json
import os
import re
import sys
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

PB_URL = "https://api.amphen.net"
ADMIN_EMAIL = os.environ["ADMIN_EMAIL"]
ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]
HEADERS_REQ = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
LOCK_FILE = "/tmp/sync.lock"

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=10))

NUTRIENT_MAP = {
    "calories": ("energy-kcal", "energy-kcal"),
    "fat": ("fat", "fat"),
    "saturated_fat": ("saturated-fat", "saturated-fat"),
    "carbs": ("carbohydrates", "carbohydrates"),
    "sugar": ("sugars", "sugars"),
    "fiber": ("fiber", "fiber"),
    "protein": ("proteins", "proteins"),
    "salt": ("salt", "salt"),
    "sodium": ("sodium", "sodium"),
    "alcohol": ("alcohol", "alcohol"),
}


def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

MAX_LOCK_AGE = 3 * 60 * 60  # 6 Stunden

def acquire_lock():
    if os.path.exists(LOCK_FILE):
        age = time.time() - os.path.getmtime(LOCK_FILE)
        if age > MAX_LOCK_AGE:
            log(f"Alte Lock-Datei gefunden (Alter: {round(age/3600, 1)}h) — wird als verwaist behandelt und entfernt.")
            os.remove(LOCK_FILE)
        else:
            log("Ein anderer Sync läuft bereits (Lock-Datei vorhanden) — breche ab.")
            sys.exit(0)
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))


def release_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)


def get_auth_token():
    r = session.post(
        f"{PB_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    return r.json()["token"]


def extract_nutrients(product):
    result = {k: 0 for k in NUTRIENT_MAP}
    basis_unit = "g"
    nutriments = product.get("nutriments")
    if nutriments:
        for field, (old_key, _) in NUTRIENT_MAP.items():
            result[field] = nutriments.get(f"{old_key}_100g", 0)
        return result, basis_unit
    nutrition = product.get("nutrition", {})
    aggregated = nutrition.get("aggregated_set", {})
    nutrients = aggregated.get("nutrients", {})
    if nutrients:
        per = aggregated.get("per", "100g")
        basis_unit = per.replace("100", "").strip() or "g"
        for field, (_, new_key) in NUTRIENT_MAP.items():
            entry = nutrients.get(new_key)
            if entry and isinstance(entry, dict):
                result[field] = entry.get("value", 0)
    return result, basis_unit


def extract_serving(product):
    quantity = product.get("serving_quantity")
    unit = product.get("serving_quantity_unit", "g")
    raw = product.get("serving_size", "")
    if quantity:
        return quantity, unit
    match = re.search(r"\d+(\.\d+)?", raw or "")
    unit_match = re.search(r"[a-zA-Z]+", raw or "")
    amount = float(match.group()) if match else 0
    parsed_unit = unit_match.group() if unit_match else unit
    return amount, parsed_unit


def parse_products(raw_stream):
    with gzip.GzipFile(fileobj=raw_stream) as f:
        for line in f:
            try:
                product = json.loads(line)
            except json.JSONDecodeError:
                continue
            code = product.get("code")
            name = product.get("product_name")
            if not code or not name:
                continue
            nutrients, basis_unit_type = extract_nutrients(product)
            serving_amount, serving_unit = extract_serving(product)
            yield {
                "code": code,
                "name": name,
                "brand": product.get("brands", ""),
                "serving_amount": serving_amount,
                "serving_unit": serving_unit,
                "basis_amount": 100,
                "basis_unit_type": basis_unit_type,
                **nutrients,
            }


def save_product(product, headers):
    record_id = product["code"]
    payload = {**product, "id": record_id}
    for attempt in range(3):
        try:
            resp = session.post(
                f"{PB_URL}/api/collections/off_products/records",
                json=payload,
                headers=headers,
                timeout=15,
            )
            if resp.status_code == 400:
                resp2 = session.patch(
                    f"{PB_URL}/api/collections/off_products/records/{record_id}",
                    json=product,
                    headers=headers,
                    timeout=15,
                )
                if resp2.status_code >= 400:
                    log(f"POST-Fehler: {resp.text[:200]} | PATCH-Fehler: {resp2.text[:200]}")
                return resp2
            return resp
        except requests.exceptions.RequestException as e:
            log(f"Fehler bei {product.get('code')}, Versuch {attempt + 1}: {e}")
            time.sleep(2)
    return None


def run_sync(headers):
    count = 0
    errors = 0
    url = "https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz"
    with session.get(url, stream=True, headers=HEADERS_REQ, timeout=60) as r:
        r.raise_for_status()
        for product in parse_products(r.raw):
            resp = save_product(product, headers)
            if resp is None or resp.status_code >= 400:
                errors += 1
            count += 1
            if count % 500 == 0:
                log(f"{count} verarbeitet, {errors} Fehler")
    return count, errors


def sync():
    acquire_lock()
    try:
        token = get_auth_token()
        headers = {"Authorization": token}
        log("Sync gestartet")

        for download_attempt in range(3):
            try:
                count, errors = run_sync(headers)
                log(f"Fertig, insgesamt {count} verarbeitet, {errors} Fehler")
                break
            except (requests.exceptions.RequestException, OSError) as e:
                log(f"Download-Verbindung abgebrochen (Versuch {download_attempt + 1}/3): {e}")
                if download_attempt == 2:
                    log("Alle Download-Versuche fehlgeschlagen, breche endgültig ab.")
                time.sleep(10)
    finally:
        release_lock()


if __name__ == "__main__":
    sync()
