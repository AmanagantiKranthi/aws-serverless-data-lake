# AWS deployment notes

This folder documents the infrastructure setup. The portfolio version deliberately avoids hard-coding account IDs, bucket names, credentials or region-specific resource identifiers.

## Resources to create

1. S3 bucket with separate `raw/`, `curated/` and `rejected/` prefixes.
2. AWS Glue database and Spark job.
3. IAM execution role restricted to the bucket prefixes and Glue/CloudWatch operations needed by the job.
4. Glue Data Catalog table for the curated Parquet output.
5. Athena workgroup and query results location.

## Security checklist

- Use IAM roles; never commit access keys or secret values.
- Enable S3 Block Public Access.
- Prefer SSE-S3 or SSE-KMS encryption according to environment requirements.
- Restrict IAM permissions to the minimum required bucket prefixes and actions.
- Set lifecycle rules for raw, rejected and query-result data.
- Avoid logging personal or confidential source records.

## Cost checklist

- Choose a Glue worker type and worker count appropriate for the sample workload.
- Stop or remove resources after testing if they are not needed.
- Set Athena workgroup limits and monitor query scans.
- Review S3 storage and request costs.

## Validation before calling it deployed

Record the AWS region, Glue job run ID, CloudWatch log link, input/valid/rejected counts, curated S3 object path and Athena query result. Never publish credentials or sensitive account details.
