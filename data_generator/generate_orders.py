import pandas as pd
import random
from .utils import generate_uuid, utc_now

def generate_orders(users_df, products_df, n_orders: int = 5000):
    orders = []

    for _ in range(n_orders):
        user = users_df.sample(1).iloc[0]
        order_id = generate_uuid()

        n_items = random.randint(1, 4)

        for _ in range(n_items):
            product = products_df.sample(1).iloc[0]

            orders.append({
                "order_id": order_id,
                "order_item_id": generate_uuid(),
                "user_id": user["user_id"],
                "product_id": product["product_id"],
                "quantity": random.randint(1, 3),
                "unit_price": product["price"],
                "order_timestamp": utc_now(),
                "ingestion_timestamp": utc_now()
            })

    return pd.DataFrame(orders)
