from __future__ import annotations

import os

import uvicorn


def run() -> None:
    uvicorn.run(
        "provider_sims.rail_osdm.app:create_app",
        factory=True,
        host=os.environ.get("SIM_HOST", "0.0.0.0"),  # noqa: S104 - container-facing bind
        port=int(os.environ.get("SIM_PORT", "9001")),
        log_config=None,
    )


if __name__ == "__main__":
    run()
