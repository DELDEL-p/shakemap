# project/app/main.py
from __future__ import annotations

from typing import Annotated, Optional
from pathlib import Path
import os
import sys

from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# --------- Import logic ให้ทำงานได้ทั้ง 2 โหมด ---------
try:
    # กรณีรันเป็นแพ็กเกจ: uvicorn project.app.main:app
    from .logic import run_pipeline, simulate_event
except Exception:
    # กรณีรันเป็นโมดูลเดี่ยว: uvicorn main:app (ไม่มี parent package)
    # เติม sys.path ให้มองเห็นไฟล์ logic.py ที่อยู่โฟลเดอร์เดียวกัน
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))
    from logic import run_pipeline, simulate_event  # type: ignore

app = FastAPI(title="Quake PGA Web", version="1.0.0")

# --------- CORS ---------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------- Static mounting (หา static อย่างฉลาด) ---------
BASE_DIR = Path(__file__).resolve().parent

def resolve_static_dir() -> Path:
    # ลำดับการค้นหา:
    candidates: list[Path | str | None] = [
        os.getenv("STATIC_DIR"),           # ให้ override ได้ด้วย env var
        BASE_DIR / "static",               # ./static ข้างไฟล์ main.py
        BASE_DIR.parent / "static",        # ../static
        Path.cwd() / "static",             # ./static จาก working dir
    ]
    for p in candidates:
        if not p:
            continue
        pth = Path(p)
        if pth.exists() and pth.is_dir():
            return pth
    # fallback สุดท้าย (อาจมี/ไม่มี)
    return BASE_DIR / "static"

STATIC_DIR = resolve_static_dir()
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --------- Routes ---------
@app.get("/")
def index():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    # ถ้าไม่มี index.html จะไม่ให้แครช และบอกตำแหน่งที่หาไฟล์อยู่
    hint = f"<code>{STATIC_DIR}</code>"
    return HTMLResponse(
        f"""
        <html>
          <head><title>Quake PGA Web</title></head>
          <body style="font-family:ui-sans-serif,system-ui,Segoe UI,Roboto">
            <h2>index.html not found</h2>
            <p>ระบบกำลังมองหาไฟล์ที่ {hint}</p>
            <p>ตรวจสอบว่าได้วาง <b>index.html</b> ถูกโฟลเดอร์แล้วหรือยัง
               หรือกำหนดตัวแปรแวดล้อม <b>STATIC_DIR</b> ให้ถูกทาง</p>
            <p>API สุขภาพ: <a href="/api/health">/api/health</a></p>
            <p>เอกสาร: <a href="/docs">/docs</a></p>
          </body>
        </html>
        """,
        status_code=200,
    )

# --- /api/run รองรับ simulate ผ่าน body.mode === "simulate" ---
@app.post("/api/run")
def api_run(body: Optional[dict] = Body(default=None)):
    try:
        if body and body.get("mode") == "simulate":
            lat   = float(body["lat"])
            lon   = float(body["lon"])
            depth = float(body["depth"])
            mag   = float(body["mag"])
            data = simulate_event(lat=lat, lon=lon, depth_km=depth, mag=mag)
            return JSONResponse(data)

        # ปกติ: ดึงเหตุการณ์ล่าสุดจาก TMD
        data = run_pipeline()
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- /api/simulate ไว้เรียกตรงจาก frontend ได้โดยไม่ผ่าน /api/run ---
class SimRequest(BaseModel):
    lat:   Annotated[float, Field(ge=-90,  le=90)]
    lon:   Annotated[float, Field(ge=-180, le=180)]
    depth: Annotated[float, Field(ge=0,    le=700)]
    mag:   Annotated[float, Field(ge=1,    le=10)]

@app.post("/api/simulate")
def api_simulate(req: SimRequest):
    try:
        data = simulate_event(
            lat=float(req.lat),
            lon=float(req.lon),
            depth_km=float(req.depth),
            mag=float(req.mag),
        )
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/health")
def health():
    return {"status": "ok"}

