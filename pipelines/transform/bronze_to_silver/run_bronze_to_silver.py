from pipelines.common.io import write_parquet
from pipelines.common.logger import get_logger

from pipelines.transform.bronze_to_silver.clean_orders import clean_orders
from pipelines.transform.bronze_to_silver.clean_events import clean_events
from pipelines.transform.bronze_to_silver.clean_users import clean_users
from pipelines.transform.bronze_to_silver.clean_products import clean_products


logger = get_logger("bronze_to_silver.runner")

# -------- Paths --------
RAW_BASE = "data/raw"
SILVER_BASE = "data/processed"

RAW_ORDERS = f"{RAW_BASE}/orders.parquet"
RAW_EVENTS = f"{RAW_BASE}/events.parquet"
RAW_USERS = f"{RAW_BASE}/users.parquet"
RAW_PRODUCTS = f"{RAW_BASE}/products.parquet"

SILVER_ORDERS = f"{SILVER_BASE}/orders"
SILVER_EVENTS = f"{SILVER_BASE}/events"
SILVER_USERS = f"{SILVER_BASE}/users"
SILVER_PRODUCTS = f"{SILVER_BASE}/products"


def main():
    logger.info("🚀 Starting Bronze → Silver pipeline")

    # -------- Orders --------
    logger.info("▶ Cleaning orders")
    orders_df = clean_orders(RAW_ORDERS)
    write_parquet(
        orders_df,
        SILVER_ORDERS,
        partition_col="order_date"
    )
    logger.info("✅ Orders written to Silver")

    # -------- Events --------
    logger.info("▶ Cleaning events")
    events_df = clean_events(RAW_EVENTS)
    write_parquet(
        events_df,
        SILVER_EVENTS,
        partition_col="event_date"
    )
    logger.info("✅ Events written to Silver")

    # -------- Users --------
    logger.info("▶ Cleaning users")
    users_df = clean_users(RAW_USERS)
    write_parquet(
        users_df,
        SILVER_USERS
    )
    logger.info("✅ Users written to Silver")

    # -------- Products --------
    logger.info("▶ Cleaning products")
    products_df = clean_products(RAW_PRODUCTS)
    write_parquet(
        products_df,
        SILVER_PRODUCTS
    )
    logger.info("✅ Products written to Silver")

    logger.info("🎉 Bronze → Silver pipeline completed successfully")


if __name__ == "__main__":
    main()
