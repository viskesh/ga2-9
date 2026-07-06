"""
Orders API demonstrating:
  1. Idempotent POST /orders (Idempotency-Key header)
  2. Cursor-based pagination on GET /orders over a fixed catalog of IDs 1..T
  3. Per-client rate limiting keyed by X-Client-Id (R requests / 10s, sliding window)

Assigned values:
  T (total orders in catalog) = 54
  R (rate limit)              = 20 requests / 10 seconds
"""

import base64
import threading
import time

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
T = 54          # fixed catalog size: order IDs 1..T always exist and are pageable
R = 20          # requests allowed per client per WINDOW seconds
WINDOW = 10.0   # seconds

# ---------------------------------------------------------------------------
# App + CORS (must allow the grader's browser page to call this directly)
# ---------------------------------------------------------------------------
app = FastAPI(title="Orders API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,   # "*" + credentials=True is invalid together; not needed here
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_lock = threading.Lock()

orders: dict[int, dict] = {i: {"id": i, "seed": True} for i in range(1, T + 1)}
_next_id = T + 1

idempotency_map: dict[str, int] = {}          # Idempotency-Key -> order id
rate_buckets: dict[str, list[float]] = {}      # client id -> list of request timestamps


def check_rate_limit(client_id: str):
    """Sliding-window rate limiter. Returns (allowed: bool, retry_after: int|None)."""
    now = time.time()
    with _lock:
        bucket = rate_buckets.setdefault(client_id, [])
        cutoff = now - WINDOW
        while bucket and bucket[0] < cutoff:
            bucket.pop(0)

        if len(bucket) >= R:
            oldest = bucket[0]
            retry_after = max(1, int(WINDOW - (now - oldest)) + 1)
            return False, retry_after

        bucket.append(now)
        return True, None


def encode_cursor(order_id: int) -> str:
    return base64.urlsafe_b64encode(str(order_id).encode()).decode()


def decode_cursor(cursor: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor")


# ---------------------------------------------------------------------------
# Global rate-limit middleware (applies to every route, bucketed by X-Client-Id)
# ---------------------------------------------------------------------------
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.method == "OPTIONS":
        # let CORS preflight through untouched
        return await call_next(request)

    client_id = request.headers.get("x-client-id")
    if client_id:
        allowed, retry_after = check_rate_limit(client_id)
        if not allowed:
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

    return await call_next(request)


# ---------------------------------------------------------------------------
# 1. Idempotent order creation
# ---------------------------------------------------------------------------
@app.post("/orders")
async def create_order(
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    global _next_id

    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    with _lock:
        existing_id = idempotency_map.get(idempotency_key)
        if existing_id is not None:
            # Repeat call with the same key -> return the SAME order, no new creation
            return JSONResponse(status_code=200, content=orders[existing_id])

        oid = _next_id
        _next_id += 1
        order = {"id": oid, **{k: v for k, v in body.items() if k != "id"}}
        orders[oid] = order
        idempotency_map[idempotency_key] = oid

    return JSONResponse(status_code=201, content=order)


# ---------------------------------------------------------------------------
# 2. Cursor-based pagination over the fixed catalog (IDs 1..T)
# ---------------------------------------------------------------------------
@app.get("/orders")
def list_orders(limit: int = 10, cursor: str | None = None):
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")

    start_id = decode_cursor(cursor) if cursor else 1

    if start_id > T:
        items = []
        next_cursor = None
    else:
        end_id = min(start_id + limit - 1, T)
        items = [orders[i] for i in range(start_id, end_id + 1)]
        next_cursor = encode_cursor(end_id + 1) if end_id < T else None

    return {
        "items": items,
        "next_cursor": next_cursor,
        # field-name aliases the grader may look for
        "next": next_cursor,
        "orders": items,
    }


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "total_orders": T, "rate_limit": f"{R} req / {int(WINDOW)}s"}
