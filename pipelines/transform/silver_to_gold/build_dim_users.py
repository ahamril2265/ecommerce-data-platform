from pipelines.common.logger import get_logger

logger = get_logger("silver_to_gold.dim_users")

def build_dim_users(users_df):
    """
    Build dim_users table from Silver users data.
    Currently SCD Type 1 (latest state only).
    """

    if users_df.empty:
        raise ValueError("Silver → Gold (dim_users): input is empty")

    logger.info(f"Input rows: {len(users_df)}")

    dim_users = users_df[[
        "user_id",
        "name",
        "email",
        "country",
        "effective_from",
        "effective_to",
        "is_current"
    ]].copy()

    logger.info(f"Rows written: {len(dim_users)}")

    return dim_users
