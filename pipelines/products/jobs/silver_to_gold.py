from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FLOCI_ENDPOINT = "http://floci:4566"
BUCKET = "ucv-data-platform"
DATABASE = "products_domain"
SILVER_TABLE = "products_silver"
GOLD_TABLE = "products_gold"
WAREHOUSE = f"s3://{BUCKET}/"
GOLD_LOCATION = f"s3://{BUCKET}/products/gold/{GOLD_TABLE}"


def create_spark_session():
    return (
        SparkSession.builder.appName("products-silver-to-gold-iceberg")
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

    # 1. Leer desde la tabla Iceberg Silver
    silver_df = spark.table(f"glue_catalog.{DATABASE}.{SILVER_TABLE}")

    # 2. Calcular KPIs y agregaciones de negocio por categoría
    gold_df = (
        silver_df.groupBy("category")
        .agg(
            F.count("product_id").alias("total_products"),
            F.sum("stock").alias("total_stock"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.avg("final_price"), 2).alias("avg_final_price"),
            F.round(F.avg("discount_percentage"), 2).alias("avg_discount_pct"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.round(F.sum(F.col("final_price") * F.col("stock")), 2).alias(
                "total_inventory_value"
            ),
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    # 3. Crear tabla Apache Iceberg Gold en Glue Data Catalog
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{DATABASE}.{GOLD_TABLE} (
            category STRING,
            total_products BIGINT,
            total_stock BIGINT,
            avg_price DOUBLE,
            avg_final_price DOUBLE,
            avg_discount_pct DOUBLE,
            avg_rating DOUBLE,
            total_inventory_value DOUBLE,
            updated_at TIMESTAMP
        )
        USING iceberg
        LOCATION '{GOLD_LOCATION}'
        TBLPROPERTIES ('format-version' = '2')
        """
    )

    gold_df.createOrReplaceTempView("staged_gold")

    spark.sql(
        f"""
        MERGE INTO glue_catalog.{DATABASE}.{GOLD_TABLE} AS target
        USING staged_gold AS source
        ON target.category = source.category
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    # 4. Mostrar resultados analíticos y snapshots de Iceberg
    result_df = spark.table(f"glue_catalog.{DATABASE}.{GOLD_TABLE}").orderBy(
        F.col("total_inventory_value").desc()
    )
    print(f"Total categories in glue_catalog.{DATABASE}.{GOLD_TABLE}: {result_df.count()}")
    result_df.show(30, truncate=False)

    print("Iceberg Snapshots (products_gold):")
    spark.sql(
        f"""
        SELECT snapshot_id, committed_at, operation
        FROM glue_catalog.{DATABASE}.{GOLD_TABLE}.snapshots
        """
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
