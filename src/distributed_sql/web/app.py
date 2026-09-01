"""Static WebUI routes served by the Coordinator."""

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

_STATIC = Path(__file__).with_name("static")


def create_web_router() -> APIRouter:
    router = APIRouter(include_in_schema=False)

    @router.get("/", response_class=FileResponse)
    async def index() -> FileResponse:
        return FileResponse(_STATIC / "index.html")

    @router.get("/web/app.css", response_class=FileResponse)
    async def stylesheet() -> FileResponse:
        return FileResponse(_STATIC / "app.css", media_type="text/css")

    @router.get("/web/app.js", response_class=FileResponse)
    async def javascript() -> FileResponse:
        return FileResponse(_STATIC / "app.js", media_type="text/javascript")

    return router
