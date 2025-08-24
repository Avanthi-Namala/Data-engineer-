```python
import os, json, random
from datetime import datetime, timedelta

base = os.getenv("RAW_BASE", "/dbfs/mnt/raw")  # or abfss path if mounted
os.makedirs(os.path.join(base, "orders"), exist_ok=True)
os.makedirs(os.path.join(base, "customers"), exist_ok=True)
os.makedirs(os.path.join(base, "products"), exist_ok=True)
os.makedirs(os.path.join(base, "campaign_touches"), exist_ok=True)

# Customers CSV
customers = []
for i in range(1, 101):
    customers.append({
        "customer_id": f"C{i:04d}",
        "full_name": f"User {i}",
        "email": f"user{i}@example.com",
        "segment": random.choice(["A", "B", "VIP"]),
        "city": random.choice(["Hyderabad", "Bengaluru", "Mumbai", "Delhi"]),
        "country": "India",
        "signup_date": (datetime.today() - timedelta(days=random.randint(30, 400))).date().isoformat()
    })

with open(os.path.join(base, "customers", f"customers_1.csv"), "w") as f:
    f.write("customer_id,full_name,email,segment,city,country,signup_date\n")
    for c in customers:
        f.write(",".join([
            c["customer_id"], c["full_name"], c["email"], c["segment"],
            c["city"], c["country"], c["signup_date"]
        ]) + "\n")

# Products CSV
products = []
for i in range(1, 51):
    products.append({
        "product_id": f"P{i:04d}",
        "product_name": f"Product {i}",
        "category": random.choice(["Electronics", "Clothing", "Home"]),
        "subcategory": random.choice(["A", "B", "C"]),
        "list_price": round(random.uniform(10, 500), 2)
    })

with open(os.path.join(base, "products", f"products_1.csv"), "w") as f:
    f.write("product_id,product_name,category,subcategory,list_price\n")
    for p in products:
        f.write(",".join([
            p["product_id"], p["product_name"], p["category"],
            p["subcategory"], str(p["list_price"])]) + "\n")

# Campaign touches CSV
with open(os.path.join(base, "campaign_touches", f"touches_1.csv"), "w") as f:
    f.write("touch_id,touch_ts,customer_id,campaign_id,channel,medium,source\n")
    for i in range(1, 201):
        cust = random.choice(customers)
        ts = datetime.now() - timedelta(days=random.randint(0, 14), hours=random.randint(0, 23))
        f.write(f"T{i:05d},{ts.isoformat(timespec='seconds')},{cust['customer_id']},CMP{random.randint(1,5):03d},\n"
                .replace(",\n", f",{random.choice(['Search','Social','Email'])},{random.choice(['CPC','Organic','Referral'])},{random.choice(['Google','Meta','Newsletter'])}\n"))

# Orders JSON
for i in range(1, 301):
    cust = random.choice(customers)
    prod = random.choice(products)
    order = {
        "order_id": f"O{i:05d}",
        "order_ts": (datetime.now() - timedelta(days=random.randint(0, 7), hours=random.randint(0, 23))).isoformat(timespec='seconds'),
        "customer_id": cust["customer_id"],
        "product_id": prod["product_id"],
        "quantity": random.randint(1, 5),
        "unit_price": prod["list_price"],
        "payment_method": random.choice(["UPI", "CreditCard", "COD"]),
        "city": cust["city"],
        "country": cust["country"]
    }
    with open(os.path.join(base, "orders", f"order_{i:05d}.json"), "w") as f:
        f.write(json.dumps(order))

print("Sample data generated in", base)
```

