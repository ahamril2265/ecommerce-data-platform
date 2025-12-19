def build_top_products_daily(fact_orders, dim_products, top_n=10):
    df = fact_orders.merge(
        dim_products[["product_id", "product_name"]],
        on="product_id",
        how="left"
    )

    df["revenue"] = df["quantity"] * df["unit_price"]

    ranked = (
        df.groupby(
            ["order_date", "product_id", "product_name"],
            as_index=False
        )
        .agg(total_revenue=("revenue", "sum"))
        .sort_values(
            ["order_date", "total_revenue"],
            ascending=[True, False]
        )
    )

    ranked["rank"] = ranked.groupby("order_date").cumcount() + 1

    return ranked[ranked["rank"] <= top_n]
