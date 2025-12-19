from pipelines.common.io import read_parquet, write_parquet
from pipelines.common.logger import get_logger

from pipelines.transform.silver_to_gold.build_fact_orders import build_fact_orders
from pipelines.transform.silver_to_gold.build_dim_users import build_dim_users
from pipelines.transform.silver_to_gold.build_dim_products import build_dim_products

logger = get_logger("silver_to_gold.runner")

SILVER_BASE = "data/processed"
GOLD_BASE = "data/curated"

def main():
    logger.info("🚀 Starting Silver → Gold pipeline")

    orders_df = read_parquet(f"{SILVER_BASE}/orders")
    users_df = read_parquet(f"{SILVER_BASE}/users")
    products_df = read_parquet(f"{SILVER_BASE}/products")

    fact_orders = build_fact_orders(orders_df)
    dim_users = build_dim_users(users_df)
    dim_products = build_dim_products(products_df)

    write_parquet(fact_orders, f"{GOLD_BASE}/fact_orders", partition_col="order_date")
    write_parquet(dim_users, f"{GOLD_BASE}/dim_users")
    write_parquet(dim_products, f"{GOLD_BASE}/dim_products")

    logger.info("🎉 Silver → Gold pipeline completed successfully")

if __name__ == "__main__":
    main()
