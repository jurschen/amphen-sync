import gzip
import json
import os
import requests

PB_URL = "https://api.amphen.net"
ADMIN_EMAIL = os.environ["ADMIN_EMAIL"]
ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]

HEADERS_REQ = {"User-Agent": "Amphen - Fitness App - Contact: info@amphen.net"}


def get_auth_token():
    r = requests.post(
        f"{PB_URL}/api/collections/_superusers/auth-with-password",
        json={"identity": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    return r.json()["token"]


def stream_off_products():
    url = "https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz"
    with requests.get(url, stream=True, headers=HEADERS_REQ) as r:
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


def sync():
    token = get_auth_token()
    headers = {"Authorization": token}
    count = 0
    for product in stream_off_products():
        requests.post(
            f"{PB_URL}/api/collections/off_products/records",
            json=product,
            headers=headers,
        )
        count += 1
        if count % 1000 == 0:
            print(f"{count} Produkte importiert")
    print(f"Fertig, insgesamt {count} Produkte importiert")


if __name__ == "__main__":
    sync()
