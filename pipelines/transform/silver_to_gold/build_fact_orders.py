from pipelines.common.logger import get_logger

logger = get_logger("silver_to_gold.fact_orders")

def build_fact_orders(silver_orders_df):
    if silver_orders_df.empty:
        raise ValueError("Silver → Gold (fact_orders): input is empty")

    logger.info(f"Input rows: {len(silver_orders_df)}")

    # Explicit copy to avoid SettingWithCopyWarning
    fact = silver_orders_df[[
        "order_id",
        "order_item_id",
        "user_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_timestamp"
    ]].copy()

    fact.loc[:, "order_date"] = fact["order_timestamp"].dt.date

    logger.info(f"Rows written: {len(fact)}")

    return fact
