from pipelines.common.logger import get_logger

logger = get_logger("analytics.category_revenue")

def build_category_revenue(fact_orders_df, dim_products_df):
    if fact_orders_df.empty or dim_products_df.empty:
        raise ValueError("Analytics (category_revenue): input is empty")

    df = fact_orders_df.merge(
        dim_products_df[["product_id", "category"]],
        on="product_id",
        how="left"
    )

    df["revenue"] = df["quantity"] * df["unit_price"]

    category_daily = (
        df.groupby(["order_date", "category"], as_index=False)
        .agg(
            total_revenue=("revenue", "sum"),
            items_sold=("quantity", "sum")
        )
    )

    logger.info(f"Category revenue rows: {len(category_daily)}")
    return category_daily
