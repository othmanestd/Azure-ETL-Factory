"""
Generates mock retail sales data for the ETL pipeline.
Produces CSV files simulating daily sales exports from a POS system.
"""
import random
import uuid
import csv
from datetime import datetime, timedelta


STORES = [
    {"id": "ST001", "name": "Paris Centre", "region": "Europe", "country": "FR"},
    {"id": "ST002", "name": "Casablanca Mall", "region": "Africa", "country": "MA"},
    {"id": "ST003", "name": "London Oxford", "region": "Europe", "country": "UK"},
    {"id": "ST004", "name": "Berlin Mitte", "region": "Europe", "country": "DE"},
    {"id": "ST005", "name": "New York SoHo", "region": "Americas", "country": "US"},
]

PRODUCTS = [
    {"id": "SKU001", "name": "Running Shoes", "category": "Footwear", "base_price": 129.99},
    {"id": "SKU002", "name": "Sport Jacket", "category": "Apparel", "base_price": 89.99},
    {"id": "SKU003", "name": "Yoga Mat", "category": "Accessories", "base_price": 39.99},
    {"id": "SKU004", "name": "Water Bottle", "category": "Accessories", "base_price": 24.99},
    {"id": "SKU005", "name": "Training Shorts", "category": "Apparel", "base_price": 49.99},
    {"id": "SKU006", "name": "Gym Bag", "category": "Accessories", "base_price": 59.99},
    {"id": "SKU007", "name": "Tennis Racket", "category": "Equipment", "base_price": 199.99},
    {"id": "SKU008", "name": "Hiking Boots", "category": "Footwear", "base_price": 159.99},
    {"id": "SKU009", "name": "Swim Goggles", "category": "Accessories", "base_price": 19.99},
    {"id": "SKU010", "name": "Cycling Jersey", "category": "Apparel", "base_price": 74.99},
]

CHANNELS = ["in_store", "online", "mobile_app"]
PAYMENT_METHODS = ["credit_card", "debit_card", "cash", "mobile_pay"]


def generate_daily_sales(date: datetime, num_transactions: int = 500) -> list:
    """Generate sales transactions for a given day."""
    transactions = []
    for _ in range(num_transactions):
        store = random.choice(STORES)
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 4)
        discount = random.choice([0, 0, 0, 0.1, 0.15, 0.2, 0.25])
        unit_price = round(product["base_price"] * (1 - discount), 2)

        transactions.append({
            "sale_id": str(uuid.uuid4()),
            "sale_date": date.strftime("%Y-%m-%d"),
            "sale_time": f"{random.randint(8, 21):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
            "store_id": store["id"],
            "store_name": store["name"],
            "region": store["region"],
            "country": store["country"],
            "product_id": product["id"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": qty,
            "unit_price": unit_price,
            "discount_pct": discount,
            "total_amount": round(unit_price * qty, 2),
            "channel": random.choice(CHANNELS),
            "payment_method": random.choice(PAYMENT_METHODS),
            "customer_id": f"CUST{random.randint(1, 2000):06d}",
        })
    return transactions


if __name__ == "__main__":
    random.seed(42)
    base = datetime(2024, 1, 1)
    all_sales = []
    for day_offset in range(90):
        date = base + timedelta(days=day_offset)
        volume = random.randint(300, 800)
        daily = generate_daily_sales(date, volume)
        all_sales.extend(daily)

    with open("data/sample/sales_sample.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_sales[0].keys())
        writer.writeheader()
        writer.writerows(all_sales[:2000])
    print(f"Generated {len(all_sales)} sales over 90 days, sample (2000 rows) saved.")
