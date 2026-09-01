"""Coordinator process entry point."""

import uvicorn

from distributed_sql.common.config import get_coordinator_settings


def main() -> None:
    settings = get_coordinator_settings()
    uvicorn.run(
        "distributed_sql.coordinator.app:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
    )


if __name__ == "__main__":
    main()
