def build_conversion_funnel_daily(fact_events):
    funnel = (
        fact_events
        .groupby(["event_date", "event_type"], as_index=False)
        .agg(users=("user_id", "nunique"))
    )

    pivot = funnel.pivot(
        index="event_date",
        columns="event_type",
        values="users"
    ).fillna(0)

    pivot["view_to_cart_rate"] = (
        pivot.get("add_to_cart", 0) / pivot.get("view", 1)
    )

    pivot["cart_to_purchase_rate"] = (
        pivot.get("purchase_intent", 0) / pivot.get("add_to_cart", 1)
    )

    return pivot.reset_index()
