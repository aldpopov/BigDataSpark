from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import logging
def suppress_spark_logs():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.ERROR)

    if hasattr(sys.stdout, 'fileno'):
        pass

suppress_spark_logs()

spark = SparkSession.builder \
    .appName("Reports to ClickHouse") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.1.jar,/opt/spark/jars/clickhouse-jdbc-0.4.6-all.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

postgres_url = "jdbc:postgresql://postgres:5432/bigdata"
postgres_properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
clickhouse_properties = {
    "user": "default",
    "password": "",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "createTableOptions": "ENGINE = MergeTree() ORDER BY tuple()"
}

dim_product = spark.read.jdbc(url=postgres_url, table="star_schema.dim_product", properties=postgres_properties)
dim_customer = spark.read.jdbc(url=postgres_url, table="star_schema.dim_customer", properties=postgres_properties)
dim_time = spark.read.jdbc(url=postgres_url, table="star_schema.dim_time", properties=postgres_properties)
dim_store = spark.read.jdbc(url=postgres_url, table="star_schema.dim_store", properties=postgres_properties)
dim_supplier = spark.read.jdbc(url=postgres_url, table="star_schema.dim_supplier", properties=postgres_properties)
fact_sales = spark.read.jdbc(url=postgres_url, table="star_schema.fact_sales", properties=postgres_properties)

fact = fact_sales.alias("fact")
prod = dim_product.alias("prod")
cust = dim_customer.alias("cust")
time = dim_time.alias("time")
store = dim_store.alias("store")
supplier = dim_supplier.alias("supplier")

sales_enriched = fact \
    .join(prod, col("fact.product_id") == col("prod.product_id"), "left") \
    .join(cust, col("fact.customer_id") == col("cust.customer_id"), "left") \
    .join(time, col("fact.time_id") == col("time.time_id"), "left") \
    .join(store, col("fact.store_id") == col("store.store_id"), "left") \
    .join(supplier, col("fact.supplier_id") == col("supplier.supplier_id"), "left") \
    .select(
        col("fact.customer_id"),
        col("fact.product_id"),
        col("fact.seller_id"),
        col("fact.store_id"),
        col("fact.supplier_id"),
        col("fact.time_id"),
        col("fact.quantity"),
        col("fact.total_price"),
        col("fact.sale_date"),

        col("prod.product_name"),
        col("prod.category"),
        col("prod.price"),
        col("prod.brand"),
        col("prod.rating"),
        col("prod.reviews"),
        col("prod.weight"),
        col("prod.color"),
        col("prod.size"),
        col("prod.material"),

        col("cust.first_name").alias("customer_first_name"),
        col("cust.last_name").alias("customer_last_name"),
        col("cust.email").alias("customer_email"),
        col("cust.country").alias("customer_country"),
        col("cust.age").alias("customer_age"),

        col("time.year"),
        col("time.month"),
        col("time.day"),
        col("time.quarter"),
        col("time.week_day"),

        col("store.store_name"),
        col("store.city").alias("store_city"),
        col("store.state").alias("store_state"),
        col("store.country").alias("store_country"),
        col("store.phone").alias("store_phone"),
        col("store.email").alias("store_email"),

        col("supplier.supplier_name"),
        col("supplier.contact").alias("supplier_contact"),
        col("supplier.email").alias("supplier_email"),
        col("supplier.phone").alias("supplier_phone"),
        col("supplier.city").alias("supplier_city"),
        col("supplier.country").alias("supplier_country")
    )

#Топ-10 самых продаваемых продуктов
report1 = sales_enriched.groupBy("product_name") \
    .agg(sum("quantity").alias("total_quantity_sold")) \
    .orderBy(col("total_quantity_sold").desc()) \
    .limit(10)

#Общая выручка по категориям продуктов
report2 = sales_enriched.groupBy("category") \
    .agg(sum("total_price").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc())

#Средний рейтинг и количество отзывов для каждого продукта
report3 = dim_product \
    .filter(col("rating").isNotNull()) \
    .select("product_name", "rating", "reviews", "category") \
    .orderBy(col("rating").desc())

#Топ-10 клиентов с наибольшей суммой покупок
report4 = sales_enriched.groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_email") \
    .agg(sum("total_price").alias("total_spent")) \
    .orderBy(col("total_spent").desc()) \
    .limit(10)

#Распределение клиентов по странам
report5 = dim_customer.groupBy("country") \
    .agg(count("*").alias("customer_count")) \
    .orderBy(col("customer_count").desc())

#Средний чек для каждого клиента
report6 = sales_enriched.groupBy("customer_id", "customer_first_name", "customer_last_name") \
    .agg(
        avg("total_price").alias("avg_order_value"),
        count("*").alias("order_count"),
        sum("total_price").alias("total_spent")
    ) \
    .orderBy(col("avg_order_value").desc())

#Месячные тренды продаж
report7 = sales_enriched.groupBy("year", "month") \
    .agg(
        sum("total_price").alias("monthly_revenue"),
        count("*").alias("order_count"),
        sum("quantity").alias("total_quantity")
    ) \
    .orderBy("year", "month")

#Годовая выручка
report8 = sales_enriched.groupBy("year") \
    .agg(
        sum("total_price").alias("yearly_revenue"),
        count("*").alias("order_count")
    ) \
    .orderBy("year")

#Средний размер заказа по месяцам
report9 = sales_enriched.groupBy("year", "month") \
    .agg(
        avg("quantity").alias("avg_order_size"),
        avg("total_price").alias("avg_order_value")
    ) \
    .orderBy("year", "month")

#Топ-5 магазинов с наибольшей выручкой
report10 = sales_enriched.groupBy("store_name", "store_city", "store_country") \
    .agg(
        sum("total_price").alias("total_revenue"),
        count("*").alias("order_count")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5)

#Распределение продаж по городам и странам
report11 = sales_enriched.groupBy("store_country", "store_city") \
    .agg(
        sum("total_price").alias("total_revenue"),
        count("*").alias("order_count")
    ) \
    .orderBy(col("total_revenue").desc())

#Средний чек для каждого магазина
report12 = sales_enriched.groupBy("store_name", "store_city") \
    .agg(
        avg("total_price").alias("avg_check"),
        count("*").alias("order_count"),
        sum("total_price").alias("total_revenue")
    ) \
    .orderBy(col("avg_check").desc())

#Топ-5 поставщиков с наибольшей выручкой
report13 = sales_enriched.groupBy("supplier_name", "supplier_country") \
    .agg(
        sum("total_price").alias("total_revenue"),
        count("*").alias("order_count")
    ) \
    .orderBy(col("total_revenue").desc()) \
    .limit(5)

#Средняя цена товаров от каждого поставщика
report14 = sales_enriched.groupBy("supplier_name") \
    .agg(
        avg("price").alias("avg_price"),
        count("product_name").alias("product_count")
    ) \
    .orderBy(col("avg_price").desc())

#Распределение продаж по странам поставщиков
report15 = sales_enriched.groupBy("supplier_country") \
    .agg(
        sum("total_price").alias("total_revenue"),
        count("*").alias("order_count")
    ) \
    .orderBy(col("total_revenue").desc())

#Продукты с наивысшим рейтингом
report16 = dim_product \
    .filter(col("rating").isNotNull()) \
    .orderBy(col("rating").desc(), col("reviews").desc()) \
    .limit(10) \
    .select("product_name", "rating", "reviews", "category", "brand")

#Продукты с наименьшим рейтингом
report17 = dim_product \
    .filter(col("rating").isNotNull()) \
    .orderBy(col("rating").asc(), col("reviews").desc()) \
    .limit(10) \
    .select("product_name", "rating", "reviews", "category", "brand")

#Корреляция рейтинга и продаж
report18 = sales_enriched.groupBy("product_name", "rating", "category") \
    .agg(
        sum("quantity").alias("total_sales"),
        sum("total_price").alias("total_revenue"),
        count("*").alias("order_count")
    ) \
    .filter(col("rating").isNotNull()) \
    .orderBy(col("total_sales").desc())

#Продукты с наибольшим количеством отзывов
report19 = dim_product \
    .filter(col("reviews").isNotNull()) \
    .orderBy(col("reviews").desc()) \
    .limit(10) \
    .select("product_name", "reviews", "rating", "category", "brand")

def write_to_clickhouse(df, table_name):
    try:
        df.write.jdbc(
            url=clickhouse_url,
            table=table_name,
            mode="overwrite",
            properties=clickhouse_properties
        )
        print(f"Written to {table_name} ({df.count()} records)")
    except Exception as e:
        print(f"Error writing to {table_name}: {str(e)}")

print("\nWriting reports to ClickHouse...")

print("\n[1/6] Product Sales Showcase:")
write_to_clickhouse(report1, "top_products")
write_to_clickhouse(report2, "revenue_by_category")
write_to_clickhouse(report3, "product_ratings")

print("\n[2/6] Customer Sales Showcase:")
write_to_clickhouse(report4, "top_customers")
write_to_clickhouse(report5, "customers_by_country")
write_to_clickhouse(report6, "customer_avg_order")

print("\n[3/6] Time-based sales showcase:")
write_to_clickhouse(report7, "monthly_trends")
write_to_clickhouse(report8, "yearly_comparison")
write_to_clickhouse(report9, "monthly_avg_order_size")

print("\n[4/6] Shop Sales Showcase:")
write_to_clickhouse(report10, "top_stores")
write_to_clickhouse(report11, "sales_by_location")
write_to_clickhouse(report12, "store_avg_check")

print("\n[5/6] Vendor Sales Showcase:")
write_to_clickhouse(report13, "top_suppliers")
write_to_clickhouse(report14, "supplier_avg_price")
write_to_clickhouse(report15, "sales_by_supplier_country")

print("\n[6/6] Showcase of product quality:")
write_to_clickhouse(report16, "products_highest_rating")
write_to_clickhouse(report17, "products_lowest_rating")
write_to_clickhouse(report18, "rating_sales_correlation")
write_to_clickhouse(report19, "products_most_reviews")

print("ClickHouse reports completed.")
spark.stop()
