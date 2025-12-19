import pandas as pd
import random
from datetime import timedelta
from .utils import generate_uuid, utc_now

EVENT_WEIGHTS = {
    "view": 0.9,
    "add_to_cart": 0.07,
    "purchase_intent": 0.03
}

def generate_events(users_df, products_df, n_events: int = 100_000):
    events = []

    start_time = utc_now() - timedelta(days=7)

    for _ in range(n_events):
        event_time = start_time + timedelta(
            seconds=random.randint(0, 7 * 24 * 3600)
        )

        events.append({
            "event_id": generate_uuid(),
            "user_id": users_df.sample(1).iloc[0]["user_id"],
            "product_id": products_df.sample(1).iloc[0]["product_id"],
            "event_type": random.choices(
                list(EVENT_WEIGHTS.keys()),
                list(EVENT_WEIGHTS.values())
            )[0],
            "event_timestamp": event_time,
            "ingestion_timestamp": utc_now()
        })

    return pd.DataFrame(events)
