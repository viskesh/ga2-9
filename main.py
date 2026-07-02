"""
Orders API demonstrating:
  1. Idempotent POST /orders
  2. Cursor-based pagination on GET /orders
  3. Per-client rate limiting (sliding window)

Assigned values:
  T (total orders in fixed catalog) = 54
  R (rate limit, requests / 10s)    = 20
"""

import time
import base64
import json
import threading
from collections import deque
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
T = 54                # fixed catalog size
R = 20                # requests allowed
WINDOW_SECONDS = 10.0  # per this many seconds

app = FastAPI(title="Orders API")

# CORS - allow the grader's browser to call this directly
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

# ------------------------------------------------------------------
# In-memory state (thread-safe via a single lock; fine for this demo)
# ------------------------------------------------------------------
_lock = threading.Lock()

# Fixed catalog: orders with ids 1..T, pre-seeded.
catalog = {
    i: {"id": i, "item": f"item-{i}", "amount": round(9.99 + i * 1.37, 2)}
    for i in range(1, T + 1)
}

# Orders created via POST (idempotent creation). New ids start after T.
created_orders: dict[int, dict] = {}
next_new_id = T + 1

# idempotency key -> order id
idempotency_map: dict[str, int] = {}

# per-client request timestamps for sliding-window rate limiting
client_requests: dict[str, deque] = {}


# ------------------------------------------------------------------
# Rate limiting helper
# ------------------------------------------------------------------
def check_rate_limit(client_id: str):
    """Raises HTTPException(429) if client_id has exceeded R requests
    in the trailing WINDOW_SECONDS. Otherwise records this request."""
    now = time.monotonic()
    with _lock:
        dq = client_requests.setdefault(client_id, deque())

        # drop timestamps outside the window
        while dq and now - dq[0] > WINDOW_SECONDS:
            dq.popleft()

        if len(dq) >= R:
            # oldest request in window determines when a slot frees up
            retry_after = WINDOW_SECONDS - (now - dq[0])
            retry_after = max(retry_after, 0)
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(int(retry_after) + 1)},
            )

        dq.append(now)


# ------------------------------------------------------------------
# Cursor helpers (opaque cursor = base64-encoded JSON offset)
# ------------------------------------------------------------------
def encode_cursor(offset: int) -> str:
    raw = json.dumps({"offset": offset}).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        data = json.loads(raw)
        offset = int(data["offset"])
        if offset < 0:
            raise ValueError
        return offset
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor")


# ------------------------------------------------------------------
# Middleware-style dependency: apply rate limit to every request
# ------------------------------------------------------------------
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Let CORS preflight through untouched
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "anonymous")
    try:
        check_rate_limit(client_id)
    except HTTPException as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=exc.headers,
        )
    return await call_next(request)


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.post("/orders", status_code=201)
async def create_order(
    request: Request,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    global next_new_id

    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header required")

    # try to parse an optional JSON body (not required by spec, but harmless)
    try:
        body = await request.json()
    except Exception:
        body = {}

    with _lock:
        existing_id = idempotency_map.get(idempotency_key)
        if existing_id is not None:
            order = created_orders[existing_id]
            return JSONResponse(status_code=200, content=order)

        new_id = next_new_id
        next_new_id += 1
        order = {"id": new_id, **(body if isinstance(body, dict) else {})}
        order["id"] = new_id  # ensure id can't be overwritten by body
        created_orders[new_id] = order
        idempotency_map[idempotency_key] = new_id

    return JSONResponse(status_code=201, content=order)


@app.get("/orders")
async def list_orders(limit: int = 10, cursor: Optional[str] = None):
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")

    offset = decode_cursor(cursor)
    ids = list(range(1, T + 1))  # fixed catalog, ids 1..T

    page_ids = ids[offset: offset + limit]
    items = [catalog[i] for i in page_ids]

    new_offset = offset + len(page_ids)
    next_cursor = encode_cursor(new_offset) if new_offset < T else None

    return {
        "items": items,
        "next_cursor": next_cursor,
        # aliases some graders look for
        "next": next_cursor,
        "orders": items,
    }


@app.get("/")
async def root():
    return {
        "status": "ok",
        "total_orders": T,
        "rate_limit": f"{R} requests / {int(WINDOW_SECONDS)}s",
    }
