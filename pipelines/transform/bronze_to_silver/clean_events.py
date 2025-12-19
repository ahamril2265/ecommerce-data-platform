from pipelines.common.io import read_parquet
from pipelines.common.logger import get_logger
from pipelines.common.metrics import record_execution_time
from pipelines.common.data_quality import hard_checks

logger = get_logger("bronze_to_silver.clean_events")

VALID_EVENT_TYPES = {"view", "add_to_cart", "purchase_intent"}

def clean_events(raw_path):
    with record_execution_time(logger, "Clean Events"):
        df = read_parquet(raw_path)

        logger.info(f"Raw events read: {len(df)}")

        # Drop duplicates
        df = df.drop_duplicates(subset=["event_id"])

        # Filter valid event types
        df = df[df["event_type"].isin(VALID_EVENT_TYPES)]

        # 🔴 Critical check
        if df.empty:
            raise ValueError(
                "Bronze → Silver (events): cleaned dataset is empty"
            )

        # Hard checks
        hard_checks(
            df,
            {
                "event_id": "not_null",
                "event_type": "not_null",
                "event_timestamp": "not_null"
            }
        )

        # Partition column
        df["event_date"] = df["event_timestamp"].dt.date

        logger.info(f"Events after cleaning: {len(df)}")

        return df
