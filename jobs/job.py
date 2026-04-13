from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, expr, col, when
import logging

ORDERS_FILE = "/opt/data/olist_orders_dataset.csv"
ORDER_ITEM_FILE = "/opt/data/olist_order_items_dataset.csv"
CUSTOMERS_FILE = "/opt/data/olist_customers_dataset.csv"
PAYMENTS_FILE = "/opt/data/olist_order_payments_dataset.csv"
REVIEWS_FILE = "/opt/data/olist_order_reviews_dataset.csv"
PRODUCTS_FILE = "/opt/data/olist_products_dataset.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    filename="/opt/spark/logs/etl.log",   # 👈 file path
    filemode="a"  # append mode
)

logger = logging.getLogger("ECOM_ETL")


step_name = "Building Spark Session"
try:

    logger.info(f"Starting: {step_name}")
    spark = (
        SparkSession
        .builder
        .appName("ecomairflowspark")
        .getOrCreate()
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Reading {ORDERS_FILE} File"
try:

    logger.info(f"Starting: {step_name}")
    stg_orders_df = (
        spark
        .read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(ORDERS_FILE)
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Reading {ORDER_ITEM_FILE} File"
try:
    logger.info(f"Starting: {step_name}")
    stg_order_items_df = (spark.read.format("csv")
                          .option("header", "true")
                          .option("inferSchema", "true")
                          .load(ORDER_ITEM_FILE)
                          )
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Reading {CUSTOMERS_FILE} File"
try:

    logger.info(f"Starting: {step_name}")
    stg_customers_df = (spark.read.format("csv")
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .load("/opt/data/olist_customers_dataset.csv")
                        )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Reading {PAYMENTS_FILE} File"
try:

    logger.info(f"Starting: {step_name}")
    stg_payments_df = (spark.read.format("csv")
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .load("/opt/data/olist_order_payments_dataset.csv")
                       )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Reading {REVIEWS_FILE} File"
try:

    logger.info(f"Starting: {step_name}")
    stg_reviews_df = (spark.read.format("csv")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load("/opt/data/olist_order_reviews_dataset.csv")
                      )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Reading {PRODUCTS_FILE} File"
try:

    logger.info(f"Starting: {step_name}")
    stg_products_df = (spark.read.format("csv")
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .load("/opt/data/olist_products_dataset.csv")
                       )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Selecting required columns from dataframes"
try:

    logger.info(f"Starting: {step_name}")
    req_orders_df = stg_orders_df.select("customer_id","order_id","order_status","order_purchase_timestamp","order_delivered_customer_date","order_approved_at","order_delivered_carrier_date")
    req_order_items_df = stg_order_items_df.select("order_id","order_item_id","product_id","price","seller_id","freight_value","shipping_limit_date")
    req_customers_df = stg_customers_df.select("customer_id")
    req_payments_df = stg_payments_df.select("order_id","payment_type","payment_value")
    req_products_df = stg_products_df.select("product_id","product_category_name")
    req_reviews_df = stg_reviews_df.select("order_id","review_id","review_score")
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise

step_name = f"Data Cleaning , Droping Nulls and Duplicates"
try:
    logger.info(f"Starting: {step_name}")
# cleaning data
    cleaned_orders_df = req_orders_df.dropna().dropDuplicates().alias("o")
    cleaned_order_items_df = req_order_items_df.dropna().dropDuplicates().alias("oi")
    cleaned_customers_df = req_customers_df.dropna().dropDuplicates().alias("c")
    cleaned_payments_df = req_payments_df.dropna().dropDuplicates().alias("p")
    cleaned_products_df = req_products_df.dropna().dropDuplicates().alias("pd")
    cleaned_reviews_df = req_reviews_df.dropna().dropDuplicates().alias("r")
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise

step_name = f"Data Enrichment (JOINS)"
try:
    logger.info(f"Starting: {step_name}")

    de_joined_df = (
        cleaned_customers_df
        .join(cleaned_orders_df, on=["customer_id"], how="inner")
        .join(cleaned_order_items_df, on=["order_id"], how="inner")
        .join(cleaned_products_df, on=["product_id"], how="inner")
        .join(cleaned_payments_df, on=["order_id"], how="inner")
        .join(cleaned_reviews_df, on=["order_id"], how="inner")
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Feature Engineering"
try:
    logger.info(f"Starting: {step_name}")

    fe_joined_df = (
        de_joined_df.withColumns(
            {"order_date": expr("to_date(order_purchase_timestamp, 'YYYY-MON-DD')"),
             "shipping_time": unix_timestamp("order_delivered_carrier_date") - unix_timestamp("order_approved_at"),
             "delivery_time": unix_timestamp("order_delivered_customer_date") - unix_timestamp(
                 "order_purchase_timestamp"),
             "approval_time": unix_timestamp("order_approved_at") - unix_timestamp("order_purchase_timestamp")
             }
        )
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise

step_name = f"Order-level aggregation "
try:
    logger.info(f"Starting: {step_name}")

    fe_order_df = (
        fe_joined_df
        .where(expr("order_status='delivered'"))
        .groupBy(
            expr("order_id"),
            expr("customer_id"),
            expr("order_status"),
            expr("order_purchase_timestamp"),
            expr("order_delivered_customer_date"),
            expr("order_approved_at"),
            expr("order_delivered_carrier_date"),
            expr("order_date"),
            expr("shipping_time"),
            expr("delivery_time"),
            expr("approval_time"),
            expr("review_score"),
            expr("payment_type"),
            expr("payment_value")
        )
        .agg(expr("round(sum(price), 3) as total_order_price"),
             expr("round(sum(freight_value), 3) as total_fre_value"),
             expr("max(shipping_limit_date) as shipping_limit_date"))
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise

step_name = f"Calculating Fact table"
try:
    logger.info(f"Starting: {step_name}")
    fact_sales_df = (
        fe_order_df
        .groupBy("order_date")
        .agg(expr("count(*) as total_orders"),
             expr("round(sum(total_order_price),3) as total_revenue"),
             expr("round(avg(total_order_price), 3) as average_order_value"))
        .orderBy("order_date", desc=False)
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Customer analytics"
try:
    logger.info(f"Starting: {step_name}")
    customer_analytics_df = (
        fe_order_df
        .groupBy(expr("customer_id"))
        .agg(
            expr("count(*) as total_orders"),
            expr("avg(review_score) as average_review_score")
        )
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Product performance"
try:
    logger.info(f"Starting: {step_name}")
    product_performance_df = (
        fe_joined_df
        .where(expr("order_status = 'delivered'"))
        .groupBy(expr("product_id"))
        .agg(
            expr("count(*) as total_sales"),
            expr("round(avg(price), 3) as average_price"),
            expr("round(avg(review_score), 3) as average_reviews")
        )
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Delivery performance"
try:
    logger.info(f"Starting: {step_name}")
    delivery_performance_df = (
        fe_order_df
        .withColumn("delay_flag",
                    when(col("order_delivered_carrier_date") > col("shipping_limit_date"), "delayed").otherwise(
                        "on time"))
        .select(
            col("order_id"), (col("delivery_time") / 60), col("delay_flag"))
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Review insights"
try:
    logger.info(f"Starting: {step_name}")
    review_insights_df = (
        fe_order_df.where(expr("order_status = 'delivered'"))
        .groupBy(expr('review_score'))
        .agg(
            expr("count(*) as total_orders"),
            expr("(avg(delivery_time)/60) as average_delivery_time")
        )
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Payment analytics"
try:
    logger.info(f"Starting: {step_name}")
    payment_analytics_df = (
        fe_order_df
        .groupBy(expr("payment_type"))
        .agg(
            expr("count(*) as total_transactions"),
            expr("round(sum(total_order_price + total_fre_value), 3) as total_billed"),
            expr("round(sum(payment_value), 3) as total_payment"),
            expr("round(avg(payment_value), 3) as avg_payment_value")
        )
    )

    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Order payment breakdown"
try:
    logger.info(f"Starting: {step_name}")
    order_payment_breakdown_df = (
        fe_order_df.select(col("order_id"), col("payment_type"), col("payment_value"))
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Product category performance"
try:
    logger.info(f"Starting: {step_name}")
    product_category_performance_df = (
        fe_joined_df
        .where(expr("order_status = 'delivered'"))
        .groupBy(expr("product_category_name"))
        .agg(
            expr("round(sum(price), 3) as total_sales"),
            expr("count(*) as total_orders"),
            expr("round(avg(price), 3) as average_price")
        )
    )

    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Seller performance"
try:
    logger.info(f"Starting: {step_name}")
    seller_performance_df = (
        fe_joined_df
        .where(expr("order_status = 'delivered'"))
        .groupBy(expr("seller_id"))
        .agg(
            expr("count(*) as total_orders"),
            expr("round(sum(price), 3) as total_revenue"),
            expr("(avg(delivery_time)/60) as avg_delivery_time"),
            expr("round(avg(review_score), 2) as avg_rating")
        )
    )

    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Shipping cost analysis"
try:
    logger.info(f"Starting: {step_name}")
    shipping_cost_analysis_df = (
        fe_joined_df
        .where(expr("order_status = 'delivered'"))
        .groupBy(expr("product_category_name"))
        .agg(
            expr("round(avg(freight_value), 3) as avg_freight"),
            expr("round(avg(price), 3) as avg_price"),
            expr("round(((sum(freight_value)/sum(price))*100), 2) as freight_percentage")
        )
    )
    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


step_name = f"Calculating Order fulfillment analysis"
try:
    logger.info(f"Starting: {step_name}")
    order_fulfillment_analysis_df = (
        fe_order_df.select(col("order_id"),
                           (col("approval_time") / 60),
                           (col("shipping_time") / 60),
                           (col("delivery_time") / 60))
    )

    logger.info(f"Completed: {step_name}")

except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise
jdbc_url = "jdbc:mysql://mysql:3306/ecommerce"

properties = {
    "user": "spark",
    "password": "spark123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# 1. fact_sales
step_name = "Writing fact_sales_daily to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    fact_sales_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="fact_sales_daily", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 2. customer_analytics
step_name = "Writing customer_analytics to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    customer_analytics_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="customer_analytics", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 3. product_performance
step_name = "Writing product_performance to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    product_performance_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="product_performance", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 4. delivery_performance
step_name = "Writing delivery_performance to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    delivery_performance_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="delivery_performance", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 5. review_insights
step_name = "Writing review_insights to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    review_insights_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="review_insights", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 6. payment_analytics
step_name = "Writing payment_analytics to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    payment_analytics_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="payment_analytics", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 7. order_payment_breakdown
step_name = "Writing order_payment_breakdown to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    order_payment_breakdown_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="order_payment_breakdown", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 8. product_category_performance
step_name = "Writing product_category_performance to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    product_category_performance_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="product_category_performance", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 9. seller_performance
step_name = "Writing seller_performance to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    seller_performance_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="seller_performance", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 10. shipping_cost_analysis
step_name = "Writing shipping_cost_analysis to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    shipping_cost_analysis_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="shipping_cost_analysis", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# 11. order_fulfillment_analysis
step_name = "Writing order_fulfillment_analysis to MySQL"
try:
    logger.info(f"Starting: {step_name}")
    order_fulfillment_analysis_df.write.mode("overwrite") \
        .jdbc(url=jdbc_url, table="order_fulfillment_analysis", properties=properties)
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise


# Stop Spark
step_name = "Stopping Spark Session"
try:
    logger.info(f"Starting: {step_name}")
    spark.stop()
    logger.info(f"Completed: {step_name}")
except Exception as e:
    logger.error(f"Failed: {step_name} | Error: {str(e)}")
    raise

#
# jdbc_url = "jdbc:mysql://mysql:3306/ecommerce"
#
# properties = {
#     "user": "spark",
#     "password": "spark123",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }
#
# fact_sales_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="fact_sales_daily", properties=properties)
#
# customer_analytics_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="customer_analytics", properties=properties)
#
# product_performance_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="product_performance", properties=properties)
#
# delivery_performance_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="delivery_performance", properties=properties)
#
# review_insights_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="review_insights", properties=properties)
#
# payment_analytics_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="payment_analytics", properties=properties)
#
# order_payment_breakdown_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="order_payment_breakdown", properties=properties)
#
# product_category_performance_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="product_category_performance", properties=properties)
#
# seller_performance_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="seller_performance", properties=properties)
#
# shipping_cost_analysis_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="shipping_cost_analysis", properties=properties)
#
# order_fulfillment_analysis_df.write.mode("overwrite") \
#     .jdbc(url=jdbc_url, table="order_fulfillment_analysis", properties=properties)
#
#
# spark.stop()
