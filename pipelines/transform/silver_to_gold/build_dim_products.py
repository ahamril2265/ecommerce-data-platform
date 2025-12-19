from pipelines.common.logger import get_logger

logger = get_logger("silver_to_gold.dim_products")

def build_dim_products(products_df):
    """
    Build dim_products table from Silver products data.
    Currently SCD Type 1 (latest state only).
    """

    if products_df.empty:
        raise ValueError("Silver → Gold (dim_products): input is empty")

    logger.info(f"Input rows: {len(products_df)}")

    dim_products = products_df[[
        "product_id",
        "product_name",
        "category",
        "price",
        "effective_from",
        "effective_to",
        "is_current"
    ]].copy()

    logger.info(f"Rows written: {len(dim_products)}")

    return dim_products
