from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
import sys
import logging
import psycopg2

def suppress_spark_logs():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.ERROR)

    if hasattr(sys.stdout, 'fileno'):
        pass


suppress_spark_logs()

spark = SparkSession.builder \
    .appName("ETL to Star Schema") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

postgres_url = "jdbc:postgresql://postgres:5432/bigdata"
postgres_properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

try:

    conn = psycopg2.connect(
        host="postgres",
        database="bigdata",
        user="admin",
        password="admin123"
    )
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS star_schema.fact_sales CASCADE")
    cursor.execute("DROP TABLE IF EXISTS star_schema.dim_customer CASCADE")
    cursor.execute("DROP TABLE IF EXISTS star_schema.dim_product CASCADE")
    cursor.execute("DROP TABLE IF EXISTS star_schema.dim_seller CASCADE")
    cursor.execute("DROP TABLE IF EXISTS star_schema.dim_store CASCADE")
    cursor.execute("DROP TABLE IF EXISTS star_schema.dim_supplier CASCADE")
    cursor.execute("DROP TABLE IF EXISTS star_schema.dim_time CASCADE")

    cursor.close()
    conn.close()
except Exception as e:
    print(f"Cleanup warning: {e}")

data_dir = "/opt/spark/data"
all_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]

df = None
for file in all_files:
    temp_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file)

    if df is None:
        df = temp_df
    else:
        df = df.union(temp_df)

print(f"Total records loaded: {df.count()}")

df = df.dropDuplicates(["id"]) \
    .filter(col("customer_age").isNotNull()) \
    .filter(col("product_price") > 0)

df = df.withColumn("sale_date_parsed", to_date(col("sale_date"), "M/d/yyyy"))

dim_customer = df.select(
    col("id").alias("original_id"),
    col("customer_first_name").alias("first_name"),
    col("customer_last_name").alias("last_name"),
    col("customer_age").alias("age"),
    col("customer_email").alias("email"),
    col("customer_country").alias("country"),
    col("customer_postal_code").alias("postal_code")
).distinct()

window = Window.orderBy("original_id")
dim_customer = dim_customer.withColumn("customer_id", row_number().over(window))

dim_product = df.select(
    col("product_name"),
    col("product_category").alias("category"),
    col("product_price").alias("price"),
    col("product_brand").alias("brand"),
    col("product_rating").alias("rating"),
    col("product_reviews").alias("reviews"),
    col("product_weight").alias("weight"),
    col("product_color").alias("color"),
    col("product_size").alias("size"),
    col("product_material").alias("material")
).distinct()

window_product = Window.orderBy("product_name")
dim_product = dim_product.withColumn("product_id", row_number().over(window_product))

dim_seller = df.select(
    col("seller_first_name").alias("first_name"),
    col("seller_last_name").alias("last_name"),
    col("seller_email").alias("email"),
    col("seller_country").alias("country"),
    col("seller_postal_code").alias("postal_code")
).distinct()

window_seller = Window.orderBy("email")
dim_seller = dim_seller.withColumn("seller_id", row_number().over(window_seller))

dim_store = df.select(
    col("store_name"),
    col("store_city").alias("city"),
    col("store_state").alias("state"),
    col("store_country").alias("country"),
    col("store_phone").alias("phone"),
    col("store_email").alias("email")
).distinct()

window_store = Window.orderBy("store_name")
dim_store = dim_store.withColumn("store_id", row_number().over(window_store))

dim_supplier = df.select(
    col("supplier_name"),
    col("supplier_contact").alias("contact"),
    col("supplier_email").alias("email"),
    col("supplier_phone").alias("phone"),
    col("supplier_address").alias("address"),
    col("supplier_city").alias("city"),
    col("supplier_country").alias("country")
).distinct()

window_supplier = Window.orderBy("supplier_name")
dim_supplier = dim_supplier.withColumn("supplier_id", row_number().over(window_supplier))

dim_time = df.select("sale_date_parsed").distinct() \
    .withColumnRenamed("sale_date_parsed", "sale_date") \
    .withColumn("year", year("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .withColumn("day", dayofmonth("sale_date")) \
    .withColumn("quarter", quarter("sale_date")) \
    .withColumn("week_day", dayofweek("sale_date"))

window_time = Window.orderBy("sale_date")
dim_time = dim_time.withColumn("time_id", row_number().over(window_time))

customer_mapping = dim_customer.select("original_id", "customer_id")
product_mapping = dim_product.select("product_name", "product_id")
seller_mapping = dim_seller.select("email", "seller_id")
store_mapping = dim_store.select("store_name", "store_id")
supplier_mapping = dim_supplier.select("supplier_name", "supplier_id")
time_mapping = dim_time.select("sale_date", "time_id")

fact_sales = df \
    .join(customer_mapping, df.id == customer_mapping.original_id, "inner") \
    .join(product_mapping, df.product_name == product_mapping.product_name, "inner") \
    .join(seller_mapping, df.seller_email == seller_mapping.email, "inner") \
    .join(store_mapping, df.store_name == store_mapping.store_name, "inner") \
    .join(supplier_mapping, df.supplier_name == supplier_mapping.supplier_name, "inner") \
    .join(time_mapping, df.sale_date_parsed == time_mapping.sale_date, "inner") \
    .select(
    col("customer_id"),
    col("product_id"),
    col("seller_id"),
    col("store_id"),
    col("supplier_id"),
    col("time_id"),
    col("sale_quantity").alias("quantity"),
    col("sale_total_price").alias("total_price"),
    col("sale_date_parsed").alias("sale_date")
)

print(f"Fact table records: {fact_sales.count()}")

dim_customer.select("customer_id", "first_name", "last_name", "age", "email", "country", "postal_code") \
    .write.jdbc(url=postgres_url, table="star_schema.dim_customer", mode="overwrite", properties=postgres_properties)
print("Written dim_customer")

dim_product.select("product_id", "product_name", "category", "price", "brand", "rating", "reviews", "weight", "color",
                   "size", "material") \
    .write.jdbc(url=postgres_url, table="star_schema.dim_product", mode="overwrite", properties=postgres_properties)
print("Written dim_product")

dim_seller.select("seller_id", "first_name", "last_name", "email", "country", "postal_code") \
    .write.jdbc(url=postgres_url, table="star_schema.dim_seller", mode="overwrite", properties=postgres_properties)
print("Written dim_seller")

dim_store.select("store_id", "store_name", "city", "state", "country", "phone", "email") \
    .write.jdbc(url=postgres_url, table="star_schema.dim_store", mode="overwrite", properties=postgres_properties)
print("Written dim_store")

dim_supplier.select("supplier_id", "supplier_name", "contact", "email", "phone", "address", "city", "country") \
    .write.jdbc(url=postgres_url, table="star_schema.dim_supplier", mode="overwrite", properties=postgres_properties)
print("Written dim_supplier")

dim_time.select("time_id", "sale_date", "year", "month", "day", "quarter", "week_day") \
    .write.jdbc(url=postgres_url, table="star_schema.dim_time", mode="overwrite", properties=postgres_properties)
print("Written dim_time")

# Запись таблицы фактов
print("Writing fact table to PostgreSQL")
fact_sales.select("customer_id", "product_id", "seller_id", "store_id", "supplier_id", "time_id", "quantity",
                  "total_price", "sale_date") \
    .write.jdbc(url=postgres_url, table="star_schema.fact_sales", mode="overwrite", properties=postgres_properties)
print("Written fact_sales")

print("ETL completed")

spark.stop()