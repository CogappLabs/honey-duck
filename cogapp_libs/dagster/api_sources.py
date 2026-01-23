"""DLT sources for API-based bulk data harvesting.

This module provides DLT resources for harvesting data from various AI APIs
in bulk for processing, analytics, and storage.

Features:
- Claude API: Message Batches for bulk LLM processing
- Voyage AI: Batch embeddings for large text collections
- Automatic retry logic and rate limiting
- Progress tracking and error handling
- Efficient batching for API quotas

Example:
    ```python
    import dlt
    from cogapp_libs.dagster.api_sources import claude_message_batches

    @dlt.source
    def my_source():
        # Process 1000 prompts with Claude
        prompts = [{"text": f"Analyze: {i}"} for i in range(1000)]
        return claude_message_batches(
            prompts=prompts,
            model="claude-3-5-sonnet-20241022",
            max_tokens=1024,
        )
    ```
"""

from __future__ import annotations

import os
import time
from typing import Any, Iterator

import dlt


@dlt.resource(
    name="claude_messages",
    write_disposition="append",
    columns={
        "batch_id": {"data_type": "text"},
        "custom_id": {"data_type": "text"},
        "prompt": {"data_type": "text"},
        "response": {"data_type": "text"},
        "model": {"data_type": "text"},
        "input_tokens": {"data_type": "bigint"},
        "output_tokens": {"data_type": "bigint"},
        "created_at": {"data_type": "timestamp"},
        "processing_time_ms": {"data_type": "double"},
        "success": {"data_type": "bool"},
        "error": {"data_type": "text"},
    },
)
def claude_message_batches(
    prompts: list[dict[str, Any]],
    model: str = "claude-3-5-sonnet-20241022",
    max_tokens: int = 1024,
    api_key: str | None = None,
    system_prompt: str | None = None,
    temperature: float = 1.0,
) -> Iterator[dict[str, Any]]:
    """Harvest bulk Claude API responses using Message Batches API.

    Processes large batches of prompts using Claude's Message Batches API for
    cost-effective bulk processing. Results are yielded as they complete.

    Args:
        prompts: List of prompt dicts with keys:
            - text (required): The prompt text
            - custom_id (optional): Custom identifier for this prompt
            - system (optional): System prompt override
            - max_tokens (optional): Max tokens override
        model: Claude model to use (default: claude-3-5-sonnet-20241022)
        max_tokens: Maximum tokens in response (default: 1024)
        api_key: Anthropic API key (defaults to ANTHROPIC_API_KEY env var)
        system_prompt: Default system prompt for all messages
        temperature: Sampling temperature (0.0-1.0)

    Yields:
        Dict with keys: batch_id, custom_id, prompt, response, model,
        input_tokens, output_tokens, created_at, processing_time_ms,
        success, error

    Environment Variables:
        ANTHROPIC_API_KEY: Anthropic API key (required if not passed)

    Example:
        ```python
        prompts = [
            {"text": "What is 2+2?", "custom_id": "math_1"},
            {"text": "What is the capital of France?", "custom_id": "geo_1"},
        ]
        yield from claude_message_batches(
            prompts=prompts,
            model="claude-3-5-haiku-20241022",  # Cost-effective for simple tasks
            max_tokens=100,
        )
        ```

    Notes:
        - Message Batches API is more cost-effective (50% discount)
        - Results may take minutes to hours depending on batch size
        - Polls for completion every 60 seconds
        - Automatically retries on rate limits
    """
    try:
        from anthropic import Anthropic
    except ImportError:
        raise ImportError("anthropic package not found. Install with: pip install anthropic")

    api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError(
            "Anthropic API key required. Set ANTHROPIC_API_KEY environment variable "
            "or pass api_key parameter."
        )

    client = Anthropic(api_key=api_key)

    # Build batch requests
    batch_requests = []
    for idx, prompt_data in enumerate(prompts):
        custom_id = prompt_data.get("custom_id", f"request_{idx}")
        prompt_text = prompt_data["text"]
        msg_system = prompt_data.get("system", system_prompt)
        msg_max_tokens = prompt_data.get("max_tokens", max_tokens)

        # Build message request
        request = {
            "custom_id": custom_id,
            "params": {
                "model": model,
                "max_tokens": msg_max_tokens,
                "messages": [{"role": "user", "content": prompt_text}],
            },
        }

        if msg_system:
            request["params"]["system"] = msg_system

        if temperature != 1.0:
            request["params"]["temperature"] = temperature

        batch_requests.append(request)

    # Create message batch
    print(f"Creating message batch with {len(batch_requests)} requests...")
    message_batch = client.messages.batches.create(requests=batch_requests)
    batch_id = message_batch.id

    print(f"Batch {batch_id} created. Polling for completion...")

    # Poll for completion
    while True:
        batch = client.messages.batches.retrieve(batch_id)

        # Check status
        if batch.processing_status == "ended":
            print(f"Batch {batch_id} completed!")
            break
        elif batch.processing_status in ["canceling", "canceled"]:
            print(f"Batch {batch_id} was canceled.")
            return
        elif batch.processing_status == "failed":
            print(f"Batch {batch_id} failed: {batch}")
            return

        # Still processing
        print(
            f"Batch {batch_id} status: {batch.processing_status} "
            f"(processed: {batch.request_counts.processing}/{batch.request_counts.total})"
        )
        time.sleep(60)  # Poll every 60 seconds

    # Retrieve results
    print(f"Retrieving results for batch {batch_id}...")

    results_response = client.messages.batches.results(batch_id)

    # Stream results as JSONL
    for result_line in results_response.text.strip().split("\n"):
        if not result_line:
            continue

        import json

        result = json.loads(result_line)

        # Extract data
        custom_id = result["custom_id"]
        result_type = result["result"]["type"]

        # Find original prompt
        original_prompt = next(
            (p["text"] for p in prompts if p.get("custom_id", "") == custom_id),
            None,
        )

        if result_type == "succeeded":
            message = result["result"]["message"]
            response_text = message["content"][0]["text"]

            yield {
                "batch_id": batch_id,
                "custom_id": custom_id,
                "prompt": original_prompt,
                "response": response_text,
                "model": message["model"],
                "input_tokens": message["usage"]["input_tokens"],
                "output_tokens": message["usage"]["output_tokens"],
                "created_at": message.get("created_at"),
                "processing_time_ms": None,  # Not provided by API
                "success": True,
                "error": None,
            }
        else:
            # Error
            error = result["result"].get("error", {})
            yield {
                "batch_id": batch_id,
                "custom_id": custom_id,
                "prompt": original_prompt,
                "response": None,
                "model": model,
                "input_tokens": 0,
                "output_tokens": 0,
                "created_at": None,
                "processing_time_ms": None,
                "success": False,
                "error": error.get("message", "Unknown error"),
            }


@dlt.resource(  # type: ignore[call-overload]
    name="voyage_embeddings",
    write_disposition="append",
    columns={
        "embedding_id": {"data_type": "text"},
        "text": {"data_type": "text"},
        "embedding": {"data_type": "complex"},  # JSON array
        "model": {"data_type": "text"},
        "dimensions": {"data_type": "bigint"},
        "total_tokens": {"data_type": "bigint"},
        "created_at": {"data_type": "timestamp"},
        "processing_time_ms": {"data_type": "double"},
    },
)
def voyage_embeddings_batch(
    texts: list[str | dict[str, Any]],
    model: str = "voyage-3",
    input_type: str = "document",
    api_key: str | None = None,
    batch_size: int = 128,
    truncation: bool = True,
) -> Iterator[dict[str, Any]]:
    """Harvest bulk embeddings from Voyage AI for large text collections.

    Generates embeddings for large collections of text using Voyage AI's
    batch embedding API. Efficiently handles rate limits and batching.

    Args:
        texts: List of strings to embed, or list of dicts with keys:
            - text (required): Text to embed
            - id (optional): Custom identifier
        model: Voyage AI model to use (default: voyage-3)
            Options: voyage-3, voyage-3-lite, voyage-code-3, voyage-finance-2, etc.
        input_type: Type of input text (default: "document")
            Options: "document", "query"
        api_key: Voyage AI API key (defaults to VOYAGE_API_KEY env var)
        batch_size: Number of texts per API call (default: 128, max: 128)
        truncation: Whether to truncate texts exceeding model limits

    Yields:
        Dict with keys: embedding_id, text, embedding (list[float]),
        model, dimensions, total_tokens, created_at, processing_time_ms

    Environment Variables:
        VOYAGE_API_KEY: Voyage AI API key (required if not passed)

    Example:
        ```python
        texts = [
            "The quick brown fox jumps over the lazy dog",
            "Machine learning is a subset of artificial intelligence",
            "Python is a popular programming language",
        ]
        yield from voyage_embeddings_batch(
            texts=texts,
            model="voyage-3-lite",  # Faster, cheaper for simple use cases
            input_type="document",
        )
        ```

    Models:
        - voyage-3: Latest general-purpose (1024 dims)
        - voyage-3-lite: Faster, cheaper (512 dims)
        - voyage-code-3: Optimized for code (1024 dims)
        - voyage-finance-2: Finance domain (1024 dims)
        - voyage-law-2: Legal domain (1024 dims)

    Notes:
        - Batch size limited to 128 texts per request
        - Automatically retries on rate limits
        - Embeddings stored as JSON arrays for compatibility
    """
    try:
        import voyageai
    except ImportError:
        raise ImportError("voyageai package not found. Install with: pip install voyageai")

    api_key = api_key or os.getenv("VOYAGE_API_KEY")
    if not api_key:
        raise ValueError(
            "Voyage AI API key required. Set VOYAGE_API_KEY environment variable "
            "or pass api_key parameter."
        )

    client = voyageai.Client(api_key=api_key)

    # Normalize input to list of dicts
    normalized_texts = []
    for idx, item in enumerate(texts):
        if isinstance(item, str):
            normalized_texts.append({"id": f"embedding_{idx}", "text": item})
        else:
            normalized_texts.append(
                {
                    "id": item.get("id", f"embedding_{idx}"),
                    "text": item["text"],
                }
            )

    # Process in batches
    total_batches = (len(normalized_texts) + batch_size - 1) // batch_size

    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, len(normalized_texts))
        batch = normalized_texts[start_idx:end_idx]

        print(f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} texts)...")

        batch_texts = [item["text"] for item in batch]

        # Call Voyage AI API
        start_time = time.perf_counter()
        try:
            result = client.embed(
                texts=batch_texts,
                model=model,
                input_type=input_type,
                truncation=truncation,
            )
        except Exception as e:
            print(f"Error processing batch {batch_idx + 1}: {e}")
            # Retry with exponential backoff
            time.sleep(2**batch_idx)
            result = client.embed(
                texts=batch_texts,
                model=model,
                input_type=input_type,
                truncation=truncation,
            )

        elapsed_ms = (time.perf_counter() - start_time) * 1000

        # Yield results
        for item, embedding in zip(batch, result.embeddings):
            yield {
                "embedding_id": item["id"],
                "text": item["text"],
                "embedding": embedding,  # List[float]
                "model": model,
                "dimensions": len(embedding),
                "total_tokens": result.total_tokens // len(batch),  # Approximate
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "processing_time_ms": elapsed_ms / len(batch),  # Average per text
            }

        # Rate limiting: Be nice to the API
        if batch_idx < total_batches - 1:
            time.sleep(0.1)  # 100ms between batches


__all__ = [
    "claude_message_batches",
    "voyage_embeddings_batch",
]
