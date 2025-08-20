from fastapi import FastAPI, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
from pydantic import BaseModel
import os

from .logic import run_pipeline, compute_overlay_from_event

app = FastAPI(title="Quake PGA Web", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class CustomEvent(BaseModel):
    lat: float
    lon: float
    mag: float
    depth: float


@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.post("/api/run")
def api_run():
    try:
        data = run_pipeline()
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/run_custom")
def api_run_custom(ev: CustomEvent):
    try:
        if not (-90.0 <= ev.lat <= 90.0):
            return JSONResponse({"error": "lat ต้องอยู่ในช่วง -90 ถึง 90"}, status_code=400)
        if not (-180.0 <= ev.lon <= 180.0):
            return JSONResponse({"error": "lon ต้องอยู่ในช่วง -180 ถึง 180"}, status_code=400)
        if not (0.0 <= ev.mag <= 10.0):
            return JSONResponse({"error": "mag ต้องอยู่ในช่วง 0 ถึง 10"}, status_code=400)
        if not (0.0 <= ev.depth <= 700.0):
            return JSONResponse({"error": "depth ต้องอยู่ในช่วง 0 ถึง 700 กม."}, status_code=400)

        data = compute_overlay_from_event(ev.dict())
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=True
    )
