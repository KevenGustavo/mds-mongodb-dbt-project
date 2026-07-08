"""
===============================================================================
Backend Simulator: MongoDB Data Seeder (Advanced Version)
===============================================================================
Script Purpose:
    Generates complex, deeply nested JSON documents representing e-commerce 
    transactions. Designed to test ELT pipelines by intentionally introducing 
    data anomalies (duplicates, negative financials, missing fields, inconsistent 
    casing) and reading credentials securely from environment variables.
===============================================================================
"""

import os
import random
import uuid
import copy 
from datetime import datetime, timedelta, timezone 
from pymongo import MongoClient
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. Load Secrets & Configuration
# ---------------------------------------------------------

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
DB_NAME = "transactional"
COLLECTION_NAME = "ecommerce_orders"

# ---------------------------------------------------------
# 2. Data Generation Logic
# ---------------------------------------------------------

def generate_complex_orders(num_records=1000):
    cities = [
        "São Luís", "sao luis", "SAO LUIS", 
        "São Paulo", "SP", "Rio de Janeiro", "Curitiba"
    ]
    
    product_catalog = [
        {"id": "SKU-1001", "name": "Mechanical Keyboard v2", "base_price": 150.00},
        {"id": "SKU-1002", "name": "Wireless Ergonomic Mouse", "base_price": 85.50},
        {"id": "SKU-1003", "name": "Ultra-Wide Monitor 34", "base_price": 450.00},
        {"id": "SKU-1004", "name": "Desk Mat XXL", "base_price": 29.90},
        {"id": "SKU-1005", "name": "Noise Cancelling Headphones", "base_price": 199.00}
    ]
    
    orders = []
    base_date = datetime.now(timezone.utc) - timedelta(days=60)
    
    print(f"[*] Generating {num_records} complex JSON documents...")

    for i in range(num_records):
        cart_items = []
        for _ in range(random.randint(1, 5)):
            product = random.choice(product_catalog)
            
            qty = random.randint(1, 4) if random.random() > 0.02 else random.randint(-2, 0)
            price = product["base_price"] if random.random() > 0.02 else -product["base_price"]
            
            cart_items.append({
                "sku": product["id"],
                "product_name": product["name"],
                "financials": {
                    "unit_price": price,
                    "quantity": qty,
                    "discount_applied": round(random.uniform(0.0, 0.2), 2)
                }
            })

        pay_method = random.choice(["Credit Card", "PIX", "PayPal", "Debit"])
        if random.random() < 0.05:
            pay_method = None

        order_doc = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
            "processed_at": base_date + timedelta(days=random.randint(0, 60), minutes=random.randint(0, 1440)),
            "customer": {
                "customer_id": f"CUST-{random.randint(1000, 9999)}",
                "profile": {
                    "age": random.randint(18, 70),
                    "is_premium": random.choice([True, False])
                },
                "shipping_address": {
                    "city": random.choice(cities),
                    "state": "MA" if "luis" in random.choice(cities).lower() else "Outro",
                    "zip_code": f"{random.randint(10000, 99999)}-{random.randint(100, 999)}"
                }
            },
            "cart": cart_items,
            "payment": {
                "method": pay_method,
                "status": random.choice(["COMPLETED", "COMPLETED", "FAILED", "REFUNDED"])
            }
        }
        orders.append(order_doc)

    # ---------------------------------------------------------
    # 3. Inject Intentional Duplicates
    # ---------------------------------------------------------

    num_duplicates = int(num_records * 0.05)
    print(f"[*] Injecting {num_duplicates} identical duplicates to test deduplication...")
    
    duplicates = [copy.deepcopy(doc) for doc in random.sample(orders, num_duplicates)]
    orders.extend(duplicates)
    
    random.shuffle(orders)

    return orders

def seed_database():
    print("======================================================")
    print("Starting Database Seeder")
    print("======================================================")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("[+] Successfully connected to MongoDB.")
        
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        collection.drop()
        print(f"[-] Collection '{COLLECTION_NAME}' wiped clean.")

        data = generate_complex_orders(1000)
        collection.insert_many(data)
        
        print(f"[+] SUCCESS: {len(data)} documents inserted.")
        print("======================================================")
        
    except Exception as e:
        print(f"[!] CRITICAL ERROR: Could not connect or write to MongoDB.\nDetails: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    seed_database()