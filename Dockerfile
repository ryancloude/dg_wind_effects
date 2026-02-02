FROM python:3.11-slim

# Prevents Python from writing .pyc files and buffers less (better logs)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (optional but sometimes needed for builds)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Copy only packaging files first (better docker layer caching)
COPY pyproject.toml ./

# If you have a README referenced in pyproject, copy it too
# COPY README.md ./

# Install your package dependencies
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir .

# Now copy the actual source
COPY ingest_pdga_event_pages/ ingest_pdga_event_pages/

# Re-install in case editable structure matters (not strictly needed, but safe)
RUN pip install --no-cache-dir .

# Default command (ECS can override args)
ENTRYPOINT ["ingest-pdga-event-pages"]