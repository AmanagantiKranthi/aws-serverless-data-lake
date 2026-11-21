# AWS Serverless Data Lake — Orders ETL

An original portfolio implementation demonstrating batch ETL using Amazon S3, AWS Glue (PySpark), Parquet and Athena.

## Architecture

`S3 raw CSV → Glue PySpark validation → deduplication → curated Parquet / rejected rows → Glue Catalog → Athena`

## Repository layout

- `src/glue_orders_etl.py` — AWS Glue Spark job.
- `data/sample_orders.csv` — synthetic sample input.
- `sql/data_quality_checks.sql` — Athena quality and analytics queries.
- `infra/README.md` — deployment, security and cost notes.

## Input data contract

| Field | Meaning | Rule |
|---|---|---|
| order_id | Order identifier | Required; deduplicated |
| order_ts | Order timestamp | Required; parseable |
| customer_id | Synthetic customer key | Required |
| product | Product label | Required |
| quantity | Units | Positive integer |
| unit_price | Unit price | Non-negative decimal |
| country | Country label | Required |

## Deployment outline

1. Upload `data/sample_orders.csv` to an S3 raw prefix.
2. Create a Glue database and an IAM execution role restricted to the required S3 prefixes and Glue/CloudWatch actions.
3. Create a Glue Spark job using `src/glue_orders_etl.py`.
4. Supply job arguments:
   - `--S3_INPUT_PATH s3://YOUR_BUCKET/raw/orders/`
   - `--S3_OUTPUT_PATH s3://YOUR_BUCKET/curated/orders/`
   - `--S3_REJECT_PATH s3://YOUR_BUCKET/rejected/orders/`
   - `--JOB_NAME orders-etl`
5. Run the job and inspect CloudWatch logs and output prefixes.
6. Create or update an Athena table over the curated Parquet data and run `sql/data_quality_checks.sql`.

## Quality controls

- Required-field and type validation.
- Rejected-row output with reason codes.
- Duplicate order ID handling (latest order timestamp wins).
- Parquet output partitioned by order date.
- Athena checks for duplicates, invalid values, daily order volume and revenue.

## Security and cost

Use IAM roles rather than access keys, block public S3 access, enable encryption, restrict permissions to required prefixes, and monitor Glue/Athena costs. Do not commit credentials, account IDs or private data.

## Project status

The source code and documentation are published in this repository. A live AWS execution, CloudWatch run evidence, Athena results and cost measurements have not yet been supplied. Add those only after running the project in your own AWS account.

## Author

Amanaganti Kranthi Kumar — original portfolio implementation.
