"""
Orders API — demonstrates:
  1. Idempotent POST /orders
  2. Cursor-based pagination on GET /orders
  3. Per-client rate limiting (X-Client-Id header)

Assigned values:
  Total orders (T) = 54
  Rate limit (R)    = 20 requests / 10 seconds
"""

import time
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Orders API")

# --- CORS: allow the grader's page (any origin) to call this API directly ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Assigned values ---
TOTAL_ORDERS = 54
RATE_LIMIT = 20        # R requests
WINDOW_SECONDS = 10    # per this many seconds

# --- Fixed catalog of orders 1..T, used for pagination ---
CATALOG: List[dict] = [
    {"id": i, "item": f"Product {i}", "amount": round(9.99 + i * 1.5, 2)}
    for i in range(1, TOTAL_ORDERS + 1)
]

# --- Idempotency store: maps Idempotency-Key -> the order that was created ---
idempotency_store: Dict[str, dict] = {}
_next_new_order_id = TOTAL_ORDERS + 1  # new POSTed orders get IDs after the catalog

# --- Rate limiter state: client_id -> deque of request timestamps (sliding window) ---
request_log: Dict[str, Deque[float]] = defaultdict(deque)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Runs on every request. Buckets by X-Client-Id, allows R requests / 10s."""
    client_id = request.headers.get("X-Client-Id", "anonymous")
    now = time.time()
    log = request_log[client_id]

    # Drop timestamps that have aged out of the 10-second window
    while log and now - log[0] > WINDOW_SECONDS:
        log.popleft()

    if len(log) >= RATE_LIMIT:
        retry_after = WINDOW_SECONDS - (now - log[0])
        return Response(
            content='{"detail":"Rate limit exceeded"}',
            status_code=429,
            media_type="application/json",
            headers={"Retry-After": str(max(1, int(retry_after) + 1))},
        )

    log.append(now)
    return await call_next(request)


@app.post("/orders", status_code=201)
def create_order(idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key")):
    """Create an order. Same Idempotency-Key twice => same order id, no duplicate."""
    global _next_new_order_id

    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    if idempotency_key in idempotency_store:
        # Seen this key before — return the SAME order, don't make a new one
        return idempotency_store[idempotency_key]

    new_order = {
        "id": _next_new_order_id,
        "item": "New Order",
        "amount": 0.0,
    }
    _next_new_order_id += 1
    idempotency_store[idempotency_key] = new_order
    return new_order


@app.get("/orders")
def list_orders(limit: int = Query(10, gt=0), cursor: Optional[str] = None):
    """Cursor-paginated read over the fixed catalog of orders 1..T."""
    start = int(cursor) if cursor else 0
    end = start + limit
    items = CATALOG[start:end]
    next_cursor = str(end) if end < TOTAL_ORDERS else None
    return {
        "items": items,
        "next_cursor": next_cursor,
        "next": next_cursor,   # alias, per spec
        "orders": items,       # alias, per spec
    }


@app.get("/")
def root():
    return {"status": "ok", "total_orders": TOTAL_ORDERS, "rate_limit": RATE_LIMIT}
