# LocalStack S3 Integration Guide

## Is LocalStack Worth Implementing?

### **YES** - When you should use LocalStack:

1. **Cloud-first architecture**: You're deploying to AWS and want to test S3 integration locally
2. **CI/CD testing**: You want to run integration tests that verify S3 operations without AWS credentials
3. **Multiple storage backends**: You plan to support both local and cloud storage
4. **Team development**: Multiple developers need consistent cloud-like environments
5. **Cost optimization**: You want to avoid AWS costs during development

### **NO** - When you DON'T need LocalStack:

1. **Simple local development**: File-based storage is sufficient
2. **No AWS deployment**: You're not using AWS in production
3. **Added complexity**: Your team prefers simpler local development
4. **Quick prototyping**: You want minimal setup overhead

## Implementation Approach

For **honey-duck**, I recommend a **hybrid approach**:

### Option A: UPath with Conditional Backend (Recommended)

Use Dagster's UPath support to handle both local and S3 paths transparently:

```python
from upath import UPath
import dagster as dg
from dagster._core.storage.upath_io_manager import UPathIOManager

class UniversalJSONIOManager(UPathIOManager):
    """IO Manager that works with local paths AND S3 via UPath.

    Supports:
    - Local: data/output/json/sales.json
    - S3: s3://my-bucket/output/sales.json
    - S3 (LocalStack): s3://test-bucket/output/sales.json (with endpoint_url)
    """

    extension: str = ".json"

    def __init__(self, base_path: str, s3_endpoint_url: str | None = None):
        """Initialize IO manager.

        Args:
            base_path: Local path or S3 URI (e.g., "s3://bucket/path")
            s3_endpoint_url: Optional LocalStack endpoint (e.g., "http://localhost:4566")
        """
        super().__init__(base_path=base_path)
        self.s3_endpoint_url = s3_endpoint_url

    def dump_to_path(self, context, obj, path: UPath):
        """Write DataFrame to JSON (works for both local and S3)."""
        import duckdb

        # UPath handles S3 vs local automatically
        path.parent.mkdir(parents=True, exist_ok=True)

        # If using S3, configure endpoint for LocalStack
        if str(path).startswith("s3://") and self.s3_endpoint_url:
            import os
            os.environ["AWS_ENDPOINT_URL_S3"] = self.s3_endpoint_url

        # DuckDB can write directly to S3 with proper credentials
        conn = duckdb.connect(":memory:")
        conn.register("_df", obj)
        conn.execute(f"COPY (SELECT * FROM _df) TO '{path}' (FORMAT JSON, ARRAY true)")
        conn.close()

        context.log.info(f"Wrote JSON to {path}")
```

**Usage:**

```python
# definitions.py
import os

# Use environment variable to switch between local and S3
BASE_PATH = os.getenv("DAGSTER_JSON_OUTPUT_PATH", "data/output/json")
S3_ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT")  # Set to "http://localhost:4566" for LocalStack

defs = dg.Definitions(
    assets=[...],
    resources={
        "json_io_manager": UniversalJSONIOManager(
            base_path=BASE_PATH,
            s3_endpoint_url=S3_ENDPOINT,
        ),
    },
)
```

### Environment Configuration

```bash
# .env.local - Local development (default)
DAGSTER_JSON_OUTPUT_PATH=data/output/json

# .env.localstack - LocalStack testing
DAGSTER_JSON_OUTPUT_PATH=s3://test-bucket/output
LOCALSTACK_ENDPOINT=http://localhost:4566
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1

# .env.production - Real AWS
DAGSTER_JSON_OUTPUT_PATH=s3://prod-bucket/dagster-output
AWS_ACCESS_KEY_ID=<real-key>
AWS_SECRET_ACCESS_KEY=<real-secret>
AWS_DEFAULT_REGION=eu-west-1
```

## LocalStack Setup

### Docker Compose

```yaml
# docker-compose.localstack.yml
version: '3.8'

services:
  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"  # LocalStack gateway
    environment:
      - SERVICES=s3
      - DEBUG=1
      - DATA_DIR=/tmp/localstack/data
      - AWS_DEFAULT_REGION=us-east-1
    volumes:
      - ./localstack-data:/tmp/localstack
      - /var/run/docker.sock:/var/run/docker.sock
```

### Initialize S3 Buckets

```bash
# scripts/setup-localstack.sh
#!/bin/bash
set -e

echo "Waiting for LocalStack to be ready..."
until aws --endpoint-url=http://localhost:4566 s3 ls >/dev/null 2>&1; do
    sleep 1
done

echo "Creating S3 buckets..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket-parquet

echo "LocalStack S3 buckets created"
```

### Start LocalStack

```bash
# Start LocalStack
docker-compose -f docker-compose.localstack.yml up -d

# Initialize buckets
chmod +x scripts/setup-localstack.sh
./scripts/setup-localstack.sh

# Run Dagster with LocalStack
export $(cat .env.localstack | xargs)
uv run dg dev
```

## Testing Strategy

### Unit Tests (No LocalStack)

```python
# tests/test_io_managers.py
import pytest
from pathlib import Path
from cogapp_deps.dagster.io_managers import UniversalJSONIOManager

def test_json_io_manager_local(tmp_path):
    """Test IO manager with local filesystem."""
    io_manager = UniversalJSONIOManager(base_path=str(tmp_path))
    # ... test local operations
```

### Integration Tests (With LocalStack)

```python
# tests/integration/test_s3_io.py
import pytest
import boto3
from moto import mock_s3  # Alternative to LocalStack for testing

@mock_s3
def test_json_io_manager_s3():
    """Test IO manager with mocked S3."""
    # Create mock S3 bucket
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket='test-bucket')

    # Test IO manager with S3 path
    io_manager = UniversalJSONIOManager(base_path="s3://test-bucket/output")
    # ... test S3 operations
```

### CI/CD (GitHub Actions Example)

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      localstack:
        image: localstack/localstack:latest
        ports:
          - 4566:4566
        env:
          SERVICES: s3

    steps:
      - uses: actions/checkout@v3

      - name: Install dependencies
        run: uv sync

      - name: Setup LocalStack buckets
        run: |
          aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket
        env:
          AWS_ACCESS_KEY_ID: test
          AWS_SECRET_ACCESS_KEY: test

      - name: Run tests
        run: uv run pytest
        env:
          LOCALSTACK_ENDPOINT: http://localhost:4566
          AWS_ACCESS_KEY_ID: test
          AWS_SECRET_ACCESS_KEY: test
```

## DuckDB S3 Support

DuckDB has **native S3 support** which works with LocalStack:

```python
import duckdb

# Configure DuckDB for S3 (works with LocalStack)
conn = duckdb.connect(":memory:")
conn.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='localhost:4566';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_access_key_id='test';
    SET s3_secret_access_key='test';
""")

# Write directly to S3/LocalStack
conn.execute("""
    COPY (SELECT * FROM my_table)
    TO 's3://test-bucket/output/data.json'
    (FORMAT JSON, ARRAY true)
""")

# Read from S3/LocalStack
df = conn.sql("""
    SELECT * FROM read_json('s3://test-bucket/output/data.json')
""").df()
```

## Recommendations for honey-duck

### Implement Now:
1. **UPath support in IO managers** - Minimal overhead, future-proof
2. **Environment-based configuration** - Easy to switch between local/S3
3. **DuckDB S3 configuration** - Already using DuckDB

### ⏳ Implement Later (when needed):
1. **LocalStack in docker-compose** - Only if deploying to AWS
2. **S3 integration tests** - Use `moto` (lightweight) instead initially
3. **Multi-region support** - Only if going production on AWS

### 🚫 Skip:
1. **Complex LocalStack setup** - If staying local-only
2. **S3 versioning/lifecycle** - Premature optimization
3. **CloudFormation integration** - Overkill for this project

## Quick Start Implementation

Add this to `cogapp_deps/dagster/io_managers.py`:

```python
class UniversalJSONIOManager(JSONIOManager):
    """Enhanced JSONIOManager with optional S3 support via UPath.

    Works with:
    - Local paths: data/output/json
    - S3 paths: s3://bucket/path
    - LocalStack S3: s3://bucket/path (with endpoint_url configured)

    Args:
        base_path: Local directory or S3 URI
        s3_endpoint_url: Optional S3 endpoint (for LocalStack: http://localhost:4566)
    """

    def __init__(self, base_path: str, s3_endpoint_url: str | None = None, **kwargs):
        super().__init__(base_path=base_path, **kwargs)
        self.s3_endpoint_url = s3_endpoint_url

        # Configure S3 endpoint if provided (for LocalStack)
        if s3_endpoint_url and base_path.startswith("s3://"):
            import os
            os.environ["AWS_ENDPOINT_URL_S3"] = s3_endpoint_url
```

Then in `definitions.py`:

```python
import os

# Auto-detect environment
JSON_BASE_PATH = os.getenv("DAGSTER_JSON_OUTPUT_PATH", "data/output/json")
S3_ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT")

resources = {
    "json_io_manager": UniversalJSONIOManager(
        base_path=JSON_BASE_PATH,
        s3_endpoint_url=S3_ENDPOINT,
    ),
}
```

## Conclusion

**For honey-duck, I recommend:**

1. Add UPath support to existing IO managers (low effort, high value)
2. Add environment variable configuration (already using this pattern)
3. ⏳ Skip LocalStack for now unless you're deploying to AWS
4. Use `moto` for S3 unit tests if needed (lighter than LocalStack)

This gives you **S3-readiness without the complexity** of running LocalStack unless you actually need it.
