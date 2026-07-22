from __future__ import annotations

import os

import uvicorn


def main() -> None:
    uvicorn.run(
        "llm_harness.api:create_app",
        factory=True,
        host=os.getenv("HARNESS_HOST", "127.0.0.1"),
        port=int(os.getenv("HARNESS_PORT", "8000")),
    )


if __name__ == "__main__":
    main()
