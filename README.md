# DG Wind Effects – PDGA Bronze Ingest

Bronze-layer ingestion for PDGA event pages:
- Fetch event page HTML
- Parse discovery fields (divisions + rounds, dates, status)
- Store raw HTML in S3 and metadata in DynamoDB
- Idempotent writes using a stable hash