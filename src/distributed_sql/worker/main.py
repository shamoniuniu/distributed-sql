"""Worker process entry point."""

import uvicorn

from distributed_sql.common.config import get_worker_settings


def main() -> None:
    settings = get_worker_settings()
    uvicorn.run(
        "distributed_sql.worker.app:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
    )


if __name__ == "__main__":
    main()
