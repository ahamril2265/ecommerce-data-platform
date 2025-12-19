from pathlib import Path

from data_generator.generate_users import generate_users
from data_generator.generate_products import generate_products
from data_generator.generate_events import generate_events
from data_generator.generate_orders import generate_orders
from data_generator.generate_inventory import generate_inventory


OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    users = generate_users()
    products = generate_products()
    inventory = generate_inventory(products)
    events = generate_events(users, products)
    orders = generate_orders(users, products)

    users.to_parquet(OUTPUT_DIR / "users.parquet")
    products.to_parquet(OUTPUT_DIR / "products.parquet")
    inventory.to_parquet(OUTPUT_DIR / "inventory.parquet")
    events.to_parquet(OUTPUT_DIR / "events.parquet")
    orders.to_parquet(OUTPUT_DIR / "orders.parquet")

    print("✅ Data generation completed successfully")

if __name__ == "__main__":
    main()
