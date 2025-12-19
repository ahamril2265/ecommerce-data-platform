from pipelines.common.io import read_parquet
from pipelines.common.logger import get_logger
from pipelines.common.metrics import record_execution_time
from pipelines.common.data_quality import hard_checks

logger = get_logger("bronze_to_silver.clean_products")

def clean_products(raw_path):
    with record_execution_time(logger, "Clean Products"):
        df = read_parquet(raw_path)

        logger.info(f"Raw products read: {len(df)}")

        # Deduplicate
        df = df.drop_duplicates(subset=["product_id"])

        # Filter invalid prices
        df = df[df["price"] > 0]

        # 🔴 Critical check
        if df.empty:
            raise ValueError(
                "Bronze → Silver (products): cleaned dataset is empty"
            )

        # Hard checks
        hard_checks(
            df,
            {
                "product_id": "not_null",
                "price": "positive"
            }
        )

        logger.info(f"Products after cleaning: {len(df)}")

        return df
