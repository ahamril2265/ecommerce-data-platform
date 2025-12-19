def build_daily_revenue(fact_orders_df):
    daily = (
        fact_orders_df
        .assign(revenue=lambda x: x.quantity * x.unit_price)
        .groupby("order_date", as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("revenue", "sum")
        )
    )
    return daily
