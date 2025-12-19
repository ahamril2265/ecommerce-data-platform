import pandas as pd
from .utils import utc_now

def generate_inventory(products_df):
    inventory = []

    for _, row in products_df.iterrows():
        inventory.append({
            "product_id": row["product_id"],
            "stock_quantity": int(abs(hash(row["product_id"])) % 500 + 50),
            "last_updated": utc_now()
        })

    return pd.DataFrame(inventory)
