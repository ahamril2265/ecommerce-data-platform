import pandas as pd
import random
from faker import Faker
from .utils import generate_uuid, utc_now

fake = Faker()

CATEGORIES = {
    "Electronics": (20000, 80000),
    "Fashion": (500, 5000),
    "Home": (1000, 15000)
}

def generate_products(n_products: int = 500):
    products = []

    for _ in range(n_products):
        category = fake.random_element(list(CATEGORIES.keys()))
        price_range = CATEGORIES[category]

        products.append({
            "product_id": generate_uuid(),
            "product_name": fake.word().title(),
            "category": category,
            "price": round(random.uniform(*price_range), 2),
            "effective_from": utc_now(),
            "effective_to": None,
            "is_current": True
        })

    return pd.DataFrame(products)
