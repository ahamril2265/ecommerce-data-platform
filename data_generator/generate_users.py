import pandas as pd
from faker import Faker
from datetime import timedelta
from .utils import generate_uuid, utc_now

fake = Faker()

def generate_users(n_users: int = 1000):
    users = []

    base_time = utc_now() - timedelta(days=180)

    for _ in range(n_users):
        created_at = base_time + timedelta(days=fake.random_int(0, 180))

        users.append({
            "user_id": generate_uuid(),
            "name": fake.name(),
            "email": fake.email(),
            "country": fake.random_element(
                elements=("India", "India", "India", "US", "UK")
            ),
            "effective_from": created_at,
            "effective_to": None,
            "is_current": True
        })

    return pd.DataFrame(users)
