# ThetaData v3 URLs & Sync-Only Behavior

## 0. Use ThetaTerminal v3 (fixes 404 "No context found")

The v3 HTTP endpoints (`/v3/option/list/expirations`, etc.) are **only** available when running **ThetaTerminal v3**. Older versions (e.g. v1.8.6) return 404.

- **JAR:** Use `ThetaTerminalv3.jar` from your Downloads folder (or set `THETA_JAR_PATH` to its path).
- **Start manually:** Quit any existing ThetaTerminal, then:
  ```bash
  cd ~/Downloads
  java -jar ThetaTerminalv3.jar YOUR_EMAIL YOUR_PASSWORD
  ```
  Leave it running; the options server and prefetch script expect it on `http://127.0.0.1:25503`.
- **Config:** The project sets `THETA_JAR_PATH=/Users/idanamar/Downloads/ThetaTerminalv3.jar` in `.env` and in `prefetch_options_datasp.py` default so scripts use v3 when they launch the terminal.

## 1. ThetaData v3 URL structure (404 fix)

**Official mapping** (from [ThetaData v2→v3 Migration Guide](https://docs.thetadata.us/Articles/Getting-Started/v2-migration-guide.html)):

| v2 Endpoint           | v3 Endpoint                     |
|-----------------------|----------------------------------|
| `/v2/list/expirations`| **`/v3/option/list/expirations`** |
| `/v2/list/strikes`    | `/v3/option/list/strikes`       |

- **Wrong (404):** `/v3/market/option/expirations` or `/v3/list/option/expirations`  
- **Correct:** `http://127.0.0.1:25503/v3/option/list/expirations?symbol=AAPL`

Parameter: use **`symbol`** (v3), not `root` (v2).

### Where it’s used in this repo

- **`options/prefetch_options_datasp.py`**  
  - `_theta_http_ready`: `GET /v3/option/list/expirations` with `params={"symbol": "AAPL"}`  
  - `_get_expirations`: `_theta_get("/v3/option/list/expirations", {"symbol": root.upper()}, ...)`

- **`options/opsp.py`**  
  - Readiness probe and `fetch_chain_for_ticker`: `GET /v3/option/list/expirations` with `params={"symbol": ..., "format": "json"}`

Other v3 option endpoints used:

- Greeks snapshot: `/v3/option/greeks/bulk_snapshot`
- Greeks history: `/v3/option/greeks/bulk_hist`
- Quote snapshot: `/v3/option/snapshot/quote`

---

## 2. Sync Data = sync only (no ghost recommendations)

### Backend

- **`POST /options/trigger/prefetch`** (Sync Data):
  - Adds a **single** background task: `_bg_sync_data(years)`.
  - `_bg_sync_data` **only** calls `svc.run_sync_data(years=years)`.
- **`run_sync_data()`** in `app/services/options_service.py`:
  - Prefetch today for missing symbols (if any).
  - Optionally prefetch for given years.
  - **Does not** call `run_optsp()` or any recommendation generation.

So Sync Data never triggers recommendations on the server.

### Frontend (Flutter)

- **Sync Data button** → `ctrl.triggerPrefetch()` in `options_dashboard_controller.dart`.
- `triggerPrefetch()`:
  - Calls `_service.triggerPrefetch(...)` (POST to `/options/trigger/prefetch`).
  - Starts `_monitorPrefetchJob(...)` to poll until prefetch job finishes.
  - When prefetch finishes it only updates `prefetchState` / `prefetchMessage`; it does **not** call `generateRecommendations()` or `triggerRunOptsp()`.

- **Generate Recommendations** is a separate button and calls `generateRecommendations()` → `triggerRunOptsp()`.

So there is no frontend chain that runs recommendations after Sync.

### If you still see optsp after Sync

1. Restart the options server so it loads the current `run_sync_data` (no `run_optsp`).
2. Confirm there is only one background task for Sync: in `app/routers/options.py`, `trigger_prefetch` must only call `background_tasks.add_task(_bg_sync_data, years)` (no other `add_task` for optsp).
3. Do a full rebuild/restart of the Flutter app so it’s not using an old build that might have different behavior.

---

## 3. Reference: call chain for Sync vs Generate

**Sync Data (sync only):**

1. User taps “Sync Data” → Flutter `triggerPrefetch()`.
2. POST `/options/trigger/prefetch` → `trigger_prefetch()` → `add_task(_bg_sync_data)`.
3. `_bg_sync_data()` → `svc.run_sync_data(years)` → prefetch only (no `run_optsp`).

**Generate Recommendations (explicit only):**

1. User taps “Generate Recommendations” → Flutter `generateRecommendations()`.
2. POST `/options/trigger/run-optsp` → `trigger_run_optsp()` → `add_task(_bg_run_optsp)`.
3. `_bg_run_optsp()` → `svc.run_optsp(run_date=...)`.

These two flows are independent; Sync never enqueues or calls optsp.
