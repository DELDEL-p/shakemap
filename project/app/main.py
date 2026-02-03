from typing import Optional
from pathlib import Path
import time, threading
import asyncio
from collections import deque
import json

from fastapi import FastAPI, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ==== ดึงฟังก์ชันจาก logic.py ของโปรเจกคุณ ====
from .logic import (
    fetch_latest_event_in_thailand,
    compute_overlay_from_event,
    simulate_event,
    get_soil_info,
)

app = FastAPI(title="SHAKEMAP API", version="1.3.0")

from .logic import debug_vs30_paths

@app.get("/api/soil_debug")
def soil_debug():
    return debug_vs30_paths()


# CORS (เปิดกว้างสำหรับ dev)
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


def _json(data, status_code: int = 200):
    """
    คืน JSON แบบ UTF-8 ชัดเจน + ไม่ escape ภาษาไทย
    """
    return JSONResponse(
        content=json.loads(json.dumps(data, ensure_ascii=False)),
        status_code=status_code,
        media_type="application/json; charset=utf-8",
    )


# ================== In-memory Cache (คำนวณครั้งแรก) ==================
_CACHE_LOCK = threading.Lock()
_CACHE = {
    "data": None,        # JSON ผลลัพธ์เต็ม (รวม data URL/HTML/เมตา)
    "event_key": None,   # คีย์อ้างอิงเหตุการณ์ล่าสุดที่คำนวณแล้ว
    "ts": 0.0,           # เวลาที่คำนวณ (epoch)
}

# ตั้ง TTL ถ้าอยากให้รีเฟรชอัตโนมัติเมื่อพ้นเวลา; None = ไม่หมดอายุเอง
CACHE_TTL_SEC: Optional[int] = None  # เช่น 600 = 10 นาที


def _make_event_key(meta: dict) -> str:
    return f"{meta.get('time_utc') or meta.get('time_th')}|{meta.get('lat')}|{meta.get('lon')}|{meta.get('mag')}|{meta.get('depth_km')}"


def _get_cached_ok() -> bool:
    if _CACHE["data"] is None:
        return False
    if CACHE_TTL_SEC is None:
        return True
    return (time.time() - (_CACHE["ts"] or 0)) < CACHE_TTL_SEC


def _compute_and_store() -> dict:
    """คำนวณผลจากเหตุการณ์ล่าสุด แล้วเก็บลงแคช (ต้องเรียกภายใต้ LOCK)"""
    ev = fetch_latest_event_in_thailand()
    data = compute_overlay_from_event(ev)
    meta = data.get("meta", {})
    _CACHE["data"] = data
    _CACHE["event_key"] = _make_event_key(meta)
    _CACHE["ts"] = time.time()
    return data


def _get_or_compute(force: bool = False) -> dict:
    # 1) บังคับคำนวณใหม่
    if force:
        with _CACHE_LOCK:
            return _compute_and_store()

    # 2) ถ้ายังไม่มีแคช → คำนวณใหม่
    if _CACHE["data"] is None:
        with _CACHE_LOCK:
            if _CACHE["data"] is None:
                return _compute_and_store()
            return _CACHE["data"]

    # 3) มีแคชแล้ว → เช็กว่ามี "เหตุการณ์ใหม่" ไหม (เปรียบเทียบ event_key)
    try:
        ev = fetch_latest_event_in_thailand()  # ดึง meta ล่าสุด (ไม่เรนเดอร์ภาพ)
        if ev:
            new_key = _make_event_key({
                "time_utc":  ev.get("time_utc"),
                "time_th":   ev.get("time_th"),
                "lat":       ev.get("lat"),
                "lon":       ev.get("lon"),
                "mag":       ev.get("mag"),
                "depth_km":  ev.get("depth"),
            })
            if new_key and new_key != _CACHE["event_key"]:
                with _CACHE_LOCK:
                    data = compute_overlay_from_event(ev)
                    _CACHE["data"] = data
                    _CACHE["event_key"] = new_key
                    _CACHE["ts"] = time.time()
                    return data
    except Exception:
        pass

    return _CACHE["data"]


# ================== Queue: จำกัดผู้ใช้พร้อมกัน 10 คน ==================
MAX_ACTIVE = 10
HEARTBEAT_TIMEOUT = 45.0
PROMOTE_BATCH = 5

_q_lock = asyncio.Lock()
_active = {}         # key=(client_id, tab_id) -> last_seen_ts
_queue = deque()     # item=(client_id, tab_id, enq_ts)


def _now() -> float:
    return time.time()


def _queue_position(client_id: str, tab_id: str):
    pos = 1
    for (c, t, _) in _queue:
        if c == client_id and t == tab_id:
            return pos
        pos += 1
    return None


async def _maintain_and_promote():
    """ลบ active ที่หมดอายุ และโปรโมตจากคิวตามช่องว่าง"""
    now = _now()
    expired = [k for k, ts in list(_active.items()) if (now - ts) > HEARTBEAT_TIMEOUT]
    for k in expired:
        _active.pop(k, None)

    slots = max(0, MAX_ACTIVE - len(_active))
    moved = 0
    while slots > 0 and _queue and moved < PROMOTE_BATCH:
        c_id, t_id, _ = _queue[0]
        key = (c_id, t_id)
        if key in _active:
            _queue.popleft()
            continue
        _active[key] = _now()
        _queue.popleft()
        slots -= 1
        moved += 1


# ================== Routes ==================
@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


# --------- Queue APIs ----------
@app.post("/api/queue/enter")
async def queue_enter(req: Request):
    body = await req.json()
    client_id = body.get("client_id")
    tab_id = body.get("tab_id")
    if not client_id or not tab_id:
        return _json({"error": "missing client_id/tab_id"}, status_code=400)

    async with _q_lock:
        await _maintain_and_promote()
        key = (client_id, tab_id)

        if key in _active:
            _active[key] = _now()
            return _json({"state": "active", "active": len(_active), "limit": MAX_ACTIVE})

        if len(_active) < MAX_ACTIVE:
            _active[key] = _now()
            return _json({"state": "active", "active": len(_active), "limit": MAX_ACTIVE})

        if _queue_position(client_id, tab_id) is None:
            _queue.append((client_id, tab_id, _now()))
        pos = _queue_position(client_id, tab_id)
        return _json({"state": "queued", "position": pos, "active": len(_active), "limit": MAX_ACTIVE})


@app.get("/api/queue/status")
async def queue_status(client_id: str, tab_id: str):
    async with _q_lock:
        await _maintain_and_promote()
        key = (client_id, tab_id)
        if key in _active:
            return _json({"state": "active", "active": len(_active), "limit": MAX_ACTIVE})
        pos = _queue_position(client_id, tab_id)
        if pos is not None:
            return _json({"state": "queued", "position": pos, "active": len(_active), "limit": MAX_ACTIVE})
        return _json({"state": "none", "active": len(_active), "limit": MAX_ACTIVE})


@app.post("/api/queue/heartbeat")
async def queue_heartbeat(req: Request):
    body = await req.json()
    client_id = body.get("client_id")
    tab_id = body.get("tab_id")
    if not client_id or not tab_id:
        return _json({"error": "missing client_id/tab_id"}, status_code=400)

    async with _q_lock:
        await _maintain_and_promote()
        key = (client_id, tab_id)
        if key in _active:
            _active[key] = _now()
            return _json({"state": "active", "active": len(_active), "limit": MAX_ACTIVE})
        pos = _queue_position(client_id, tab_id)
        if pos is not None:
            return _json({"state": "queued", "position": pos, "active": len(_active), "limit": MAX_ACTIVE})
        return _json({"state": "none", "active": len(_active), "limit": MAX_ACTIVE})


@app.post("/api/queue/leave")
async def queue_leave(req: Request):
    body = await req.json()
    client_id = body.get("client_id")
    tab_id = body.get("tab_id")
    if not client_id or not tab_id:
        return _json({"error": "missing client_id/tab_id"}, status_code=400)

    async with _q_lock:
        key = (client_id, tab_id)
        _active.pop(key, None)

        for i, (c, t, ts) in enumerate(list(_queue)):
            if c == client_id and t == tab_id:
                try:
                    _queue.remove((c, t, ts))
                except Exception:
                    pass
                break

        await _maintain_and_promote()
        return _json({"ok": True, "active": len(_active), "limit": MAX_ACTIVE})


# --------- Data APIs ----------
@app.get("/api/run")
def api_run_get():
    try:
        data = _get_or_compute(force=False)
        return _json(data)
    except Exception as e:
        return _json({"error": str(e)}, status_code=500)


@app.post("/api/run")
def api_run(body: dict = Body(default_factory=dict)):
    """
    โหมดปกติ: POST /api/run  (body ว่างก็ได้)
    โหมดจำลอง: POST /api/run { "mode":"simulate", "lat":..., "lon":..., "depth":..., "mag":... }
    บังคับรีเฟรช: POST /api/run { "force": true }
    """
    try:
        if body.get("mode") == "simulate":
            lat = float(body["lat"])
            lon = float(body["lon"])
            depth = float(body["depth"])
            mag = float(body["mag"])
            data = simulate_event(lat=lat, lon=lon, depth_km=depth, mag=mag)
            return _json(data)

        force = bool(body.get("force"))
        data = _get_or_compute(force=force)
        return _json(data)
    except Exception as e:
        return _json({"error": str(e)}, status_code=500)


@app.post("/api/refresh")
def api_refresh():
    try:
        data = _get_or_compute(force=True)
        return _json({"ok": True, "meta": data.get("meta", {}), "event_key": _CACHE["event_key"]})
    except Exception as e:
        return _json({"error": str(e)}, status_code=500)


@app.get("/api/cache_state")
def api_cache_state():
    # อันนี้เป็น dict ตรง ๆ ก็ได้ แต่ให้เหมือนกันไปเลย
    return _json({
        "has_cache": _CACHE["data"] is not None,
        "event_key": _CACHE["event_key"],
        "ts": _CACHE["ts"],
        "ttl_sec": CACHE_TTL_SEC,
    })


# ชั้นดิน/ประเภทชั้นดิน (Site Class จาก Vs30)
@app.get("/api/soil")
def api_soil(lat: float, lon: float):
    try:
        return _json(get_soil_info(lat=lat, lon=lon))
    except Exception as e:
        return _json({"status": "error", "error": str(e), "lat": lat, "lon": lon}, status_code=500)


@app.post("/api/simulate")
def api_simulate(body: dict = Body(...)):
    try:
        lat = float(body["lat"])
        lon = float(body["lon"])
        depth = float(body["depth"])
        mag = float(body["mag"])
        data = simulate_event(lat=lat, lon=lon, depth_km=depth, mag=mag)
        return _json(data)
    except Exception as e:
        return _json({"error": str(e)}, status_code=500)
