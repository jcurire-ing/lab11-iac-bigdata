# Laboratorio 12: Batch Data Pipeline con AWS Glue 5 y Apache Iceberg

Pipeline de datos batch bajo arquitectura **Medallion (Bronze, Silver, Gold)** desplegado localmente con **Terraform + Floci (AWS S3 & Glue Data Catalog)** y procesado con **Apache Spark (AWS Glue 5) + Apache Iceberg**.

## Componentes del Laboratorio 12
- **Infraestructura como Código (`terraform/`):** Bucket S3 `ucv-data-platform` y catálogos por dominio (`products_domain`) en Glue Data Catalog gestionados con Terraform sobre Floci.
- **Ingesta API -> Bronze (`pipelines/products/ingest/ingest_products.py`):** Extracción desde DummyJSON API y almacenamiento raw en `products/bronze/` particionado por `ingestion_date`.
- **Job Silver Iceberg (`pipelines/products/jobs/bronze_to_silver.py`):** Limpieza, tipado, deduplicación y escritura transaccional ACID (`MERGE INTO`) en la tabla Apache Iceberg `products_domain.products_silver`.
- **Job Gold Iceberg (`pipelines/products/jobs/silver_to_gold.py`):** Agregaciones analíticas por categoría en la tabla Apache Iceberg `products_domain.products_gold` con auditoría de *snapshots*.
