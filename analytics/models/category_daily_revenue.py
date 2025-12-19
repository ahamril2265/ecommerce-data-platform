def build_category_daily_revenue(fact_orders, dim_products):
    df = fact_orders.merge(
        dim_products[["product_id", "category"]],
        on="product_id",
        how="left"
    )

    df["revenue"] = df["quantity"] * df["unit_price"]

    return (
        df.groupby(
            ["order_date", "category"],
            as_index=False
        )
        .agg(
            total_revenue=("revenue", "sum"),
            total_items=("quantity", "sum")
        )
    )
