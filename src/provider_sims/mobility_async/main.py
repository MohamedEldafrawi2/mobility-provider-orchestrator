from __future__ import annotations

import os

import uvicorn


def run() -> None:
    uvicorn.run(
        "provider_sims.mobility_async.app:create_app",
        factory=True,
        host=os.environ.get("SIM_HOST", "0.0.0.0"),  # noqa: S104 - container-facing bind
        port=int(os.environ.get("SIM_PORT", "9003")),
        log_config=None,
    )


if __name__ == "__main__":
    run()
