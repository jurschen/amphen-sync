import gzip
import json
import os
import re
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PB_URL = "https://api.amphen.net"
ADMIN_EMAIL = os.environ["ADMIN_EMAIL"]
ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]
HEADERS_REQ = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=10))

# unser Feldname -> (alter Schlüssel im "nutriments"-Objekt, neuer Schlüssel im "nutrients"-Objekt)
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


def get_auth_token():
    r = session.post(
        f"{PB_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    return r.json()["token"]


def extract_nutrients(product):
    result = {k: 0 for k in NUTRIENT_MAP}
    basis_unit = "100g"

    nutriments = product.get("nutriments")
    if nutriments:
        for field, (old_key, _) in NUTRIENT_MAP.items():
            result[field] = nutriments.get(f"{old_key}_100g", 0)
        return result, basis_unit

    nutrition = product.get("nutrition", {})
    aggregated = nutrition.get("aggregated_set", {})
    nutrients = aggregated.get("nutrients", {})
    if nutrients:
        basis_unit = aggregated.get("per", "100g")
        for field, (_, new_key) in NUTRIENT_MAP.items():
            entry = nutrients.get(new_key)
            if entry and isinstance(entry, dict):
                result[field] = entry.get("value", 0)

    return result, basis_unit


def extract_serving(product):
    quantity = product.get("serving_quantity")
    unit = product.get("serving_quantity_unit", "")
    raw = product.get("serving_size", "")
    if quantity:
        return f"{quantity}{unit}"
    match = re.search(r"[\d.]+", raw or "")
    if match:
        return f"{match.group()}{unit}"
    return raw or ""


def stream_off_products():
    url = "https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz"
    with session.get(url, stream=True, headers=HEADERS_REQ) as r:
        r.raise_for_status()
        with gzip.GzipFile(fileobj=r.raw) as f:
            for line in f:
                try:
                    product = json.loads(line)
                except json.JSONDecodeError:
                    continue
                code = product.get("code")
                name = product.get("product_name")
                if not code or not name:
                    continue
                nutrients, basis_unit = extract_nutrients(product)
                yield {
                    "code": code,
                    "name": name,
                    "brand": product.get("brands", ""),
                    "serving_size": extract_serving(product),
                    "basis_unit": basis_unit,
                    **nutrients,
                }


def save_product(product, headers):
    for attempt in range(3):
        try:
            resp = session.post(
                f"{PB_URL}/api/collections/off_products/records",
                json=product,
                headers=headers,
                timeout=15,
            )
            return resp
        except requests.exceptions.RequestException as e:
            print(f"Fehler bei {product.get('code')}, Versuch {attempt + 1}: {e}")
            time.sleep(2)
    return None


def sync():
    token = get_auth_token()
    headers = {"Authorization": token}
    count = 0
    errors = 0
    for product in stream_off_products():
        resp = save_product(product, headers)
        if resp is None or resp.status_code >= 400:
            errors += 1
        count += 1
        if count % 500 == 0:
            print(f"{count} verarbeitet, {errors} Fehler")
    print(f"Fertig, insgesamt {count} verarbeitet, {errors} Fehler")


if __name__ == "__main__":
    sync()
