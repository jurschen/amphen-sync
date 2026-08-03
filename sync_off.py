import gzip
import json
import os
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


def get_auth_token():
    r = session.post(
        f"{PB_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    return r.json()["token"]


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
                nutriments = product.get("nutriments", {})
                yield {
                    "code": code,
                    "name": name,
                    "brand": product.get("brands", ""),
                    "protein": nutriments.get("proteins_100g", 0),
                    "carbs": nutriments.get("carbohydrates_100g", 0),
                    "fat": nutriments.get("fat_100g", 0),
                    "calories": nutriments.get("energy-kcal_100g", 0),
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
