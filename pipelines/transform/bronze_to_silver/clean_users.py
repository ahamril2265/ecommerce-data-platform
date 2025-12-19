from pipelines.common.io import read_parquet
from pipelines.common.logger import get_logger
from pipelines.common.metrics import record_execution_time
from pipelines.common.data_quality import hard_checks

logger = get_logger("bronze_to_silver.clean_users")

def clean_users(raw_path):
    with record_execution_time(logger, "Clean Users"):
        df = read_parquet(raw_path)

        logger.info(f"Raw users read: {len(df)}")

        # Deduplicate by business key
        df = df.drop_duplicates(subset=["user_id"])

        # 🔴 Critical check
        if df.empty:
            raise ValueError(
                "Bronze → Silver (users): cleaned dataset is empty"
            )

        # Hard checks
        hard_checks(
            df,
            {
                "user_id": "not_null",
                "email": "not_null"
            }
        )

        logger.info(f"Users after cleaning: {len(df)}")

        return df
