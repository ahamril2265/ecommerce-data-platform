from pipelines.common.io import read_parquet, write_parquet
from pipelines.common.logger import get_logger

from analytics.models.daily_revenue import build_daily_revenue
from analytics.models.daily_orders import build_daily_orders
from analytics.models.category_revenue import build_category_revenue

logger = get_logger("analytics.runner")

GOLD_BASE = "data/curated"
ANALYTICS_BASE = "data/analytics"

def main():
    logger.info("🚀 Starting Analytics pipeline")

    fact_orders = read_parquet(f"{GOLD_BASE}/fact_orders")
    dim_products = read_parquet(f"{GOLD_BASE}/dim_products")

    daily_revenue = build_daily_revenue(fact_orders)
    daily_orders = build_daily_orders(fact_orders)
    category_revenue = build_category_revenue(fact_orders, dim_products)

    write_parquet(daily_revenue, f"{ANALYTICS_BASE}/daily_revenue")
    write_parquet(daily_orders, f"{ANALYTICS_BASE}/daily_orders")
    write_parquet(category_revenue, f"{ANALYTICS_BASE}/category_revenue")

    logger.info("🎉 Analytics pipeline completed successfully")

if __name__ == "__main__":
    main()
