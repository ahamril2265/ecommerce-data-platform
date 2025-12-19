from pipelines.common.logger import get_logger

logger = get_logger("analytics.daily_orders")

def build_daily_orders(fact_orders_df):
    if fact_orders_df.empty:
        raise ValueError("Analytics (daily_orders): input is empty")

    daily = (
        fact_orders_df
        .groupby("order_date", as_index=False)
        .agg(
            orders=("order_id", "nunique"),
            items_sold=("quantity", "sum")
        )
    )

    logger.info(f"Daily orders rows: {len(daily)}")
    return daily
