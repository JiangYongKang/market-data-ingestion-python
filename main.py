"""Local entry point: ``uv run uvicorn main:app`` / ``python main.py``."""
from app.api import app  # noqa: F401

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.api:app", host="127.0.0.1", port=8000)
