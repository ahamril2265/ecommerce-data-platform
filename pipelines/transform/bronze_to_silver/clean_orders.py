import pandas as pd
from pipelines.common.io import read_parquet
from pipelines.common.logger import get_logger
from pipelines.common.metrics import record_execution_time
from pipelines.common.data_quality import hard_checks

logger = get_logger("bronze_to_silver.clean_orders")

def clean_orders(raw_path):
    with record_execution_time(logger, "Clean Orders"):
        df = read_parquet(raw_path)

        logger.info(f"Raw rows read: {len(df)}")

        # Deduplication
        df = df.drop_duplicates(subset=["order_item_id"])

        # Basic filtering
        df = df[df["quantity"] > 0]
        df = df[df["unit_price"] > 0]

        # 🔴 CRITICAL PRODUCTION CHECK
        if df.empty:
            raise ValueError(
                "Bronze → Silver (orders): cleaned dataset is empty"
            )

        # Hard data quality checks
        hard_checks(
            df,
            {
                "order_id": "not_null",
                "order_item_id": "not_null",
                "user_id": "not_null",
                "product_id": "not_null",
                "quantity": "positive",
                "unit_price": "positive"
            }
        )

        df["order_date"] = df["order_timestamp"].dt.date

        logger.info(f"Rows after cleaning: {len(df)}")

        return df
