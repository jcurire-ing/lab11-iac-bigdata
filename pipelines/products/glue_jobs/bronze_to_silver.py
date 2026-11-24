from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

FLOCI_ENDPOINT = "http://floci:4566"
BUCKET = "ucv-data-platform"
DATABASE = "products_domain"
TABLE = "products_silver"
WAREHOUSE = f"s3://{BUCKET}/"
TABLE_LOCATION = f"s3://{BUCKET}/products/silver/{TABLE}"


def create_spark_session():
    return (
        SparkSession.builder.appName("products-bronze-to-silver-iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            "spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.glue_catalog.s3.endpoint", FLOCI_ENDPOINT)
        .config("spark.sql.catalog.glue_catalog.s3.path-style-access", "true")
        .config("spark.sql.catalog.glue_catalog.glue.endpoint", FLOCI_ENDPOINT)
        .config("spark.sql.catalog.glue_catalog.client.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", FLOCI_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Leer datos crudos desde S3 Bronze
    bronze_path = f"s3a://{BUCKET}/products/bronze/"
    raw_df = spark.read.option("multiline", "true").json(bronze_path)

    exploded_df = raw_df.select(
        F.explode("products").alias("p"),
        F.col("ingestion_date").cast("string").alias("ingestion_date"),
    )

    # 2. Limpieza, tipado y estandarización de columnas
    cleaned_df = exploded_df.select(
        F.col("p.id").cast("int").alias("product_id"),
        F.trim(F.col("p.title")).alias("title"),
        F.lower(F.trim(F.col("p.category"))).alias("category"),
        F.coalesce(F.trim(F.col("p.brand")), F.lit("Generic")).alias("brand"),
        F.trim(F.col("p.sku")).alias("sku"),
        F.round(F.col("p.price").cast("double"), 2).alias("price"),
        F.round(F.col("p.discountPercentage").cast("double"), 2).alias("discount_percentage"),
        F.round(
            F.col("p.price").cast("double")
            * (1 - F.col("p.discountPercentage").cast("double") / 100.0),
            2,
        ).alias("final_price"),
        F.round(F.col("p.rating").cast("double"), 2).alias("rating"),
        F.col("p.stock").cast("int").alias("stock"),
        F.trim(F.col("p.availabilityStatus")).alias("availability_status"),
        F.col("p.minimumOrderQuantity").cast("int").alias("minimum_order_quantity"),
        F.col("ingestion_date"),
        F.current_timestamp().alias("processed_at"),
    ).filter(F.col("product_id").isNotNull() & (F.col("price") >= 0))

    # 3. Deduplicación por product_id conservando la ingesta más reciente
    window_spec = Window.partitionBy("product_id").orderBy(F.col("ingestion_date").desc())
    silver_df = (
        cleaned_df.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # 4. Crear tabla Apache Iceberg en Glue Catalog y ejecutar MERGE INTO (ACID)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.{TABLE} (
            product_id INT,
            title STRING,
            category STRING,
            brand STRING,
            sku STRING,
            price DOUBLE,
            discount_percentage DOUBLE,
            final_price DOUBLE,
            rating DOUBLE,
            stock INT,
            availability_status STRING,
            minimum_order_quantity INT,
            ingestion_date STRING,
            processed_at TIMESTAMP
        )
        USING iceberg
        LOCATION '{TABLE_LOCATION}'
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    silver_df.createOrReplaceTempView("staged_products")

    spark.sql(
        f"""
        MERGE INTO glue_catalog.{DATABASE}.{TABLE} AS target
        USING staged_products AS source
        ON target.product_id = source.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    result_df = spark.table(f"glue_catalog.{DATABASE}.{TABLE}")
    print(f"Total records in glue_catalog.{DATABASE}.{TABLE}: {result_df.count()}")
    result_df.select(
        "product_id", "title", "category", "brand", "price", "final_price", "stock"
    ).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
