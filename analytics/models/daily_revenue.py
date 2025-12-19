from pipelines.common.logger import get_logger

logger = get_logger("analytics.daily_revenue")

def build_daily_revenue(fact_orders_df):
    if fact_orders_df.empty:
        raise ValueError("Analytics (daily_revenue): input is empty")

    df = fact_orders_df.copy()
    df["revenue"] = df["quantity"] * df["unit_price"]

    daily = (
        df.groupby("order_date", as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_items=("quantity", "sum"),
            total_revenue=("revenue", "sum")
        )
    )

    daily["average_order_value"] = (
        daily["total_revenue"] / daily["total_orders"]
    )

    logger.info(f"Daily revenue rows: {len(daily)}")
    return daily
