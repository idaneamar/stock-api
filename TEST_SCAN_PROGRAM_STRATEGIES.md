# Test: Scan uses program strategies when program_id is set

## Rule (enforced in code)

- **If `program_id` is provided** → use ONLY that program's strategies (ignore global enabled/disabled).
- **If `program_id` is absent/empty** → use ONLY globally enabled strategies.

So: disabling a strategy globally must NOT affect scans that send a `program_id` whose program includes that strategy.

## Manual test scenario

1. **Setup**
   - Ensure program "STR9" (e.g. `str_code_9`) exists and has strategy X in its `enabled_strategy_names` (e.g. "Trend", "Momentum", "VWAP").
   - In the app, open Strategies and **disable** strategy X (e.g. "Trend") globally.

2. **Run scan with program**
   - On Home, open the scan dialog and select program "Baseline – STR CODE 9" (or the STR9 program).
   - Run the scan.

3. **Verify backend logs**
   - In POST `/scans` logs you should see:
     - `POST /scans received program_id: str_code_9` (or the actual program id).
   - When analysis runs for that scan you should see:
     - `SCAN_STRATEGIES scan_id=<id> received program_id=str_code_9 source=program strategy_ids=[...] strategy_names=[...]`
     - `strategy_names` must include the strategy X (e.g. "Trend") even though X is disabled globally.

4. **Conclusion**
   - If strategy X appears in `strategy_names_used` for the scan, the backend correctly used the program's strategies and ignored the global disabled state for that scan.

## Log reference

- **Frontend (browser console):** `POST /scans payload (exact JSON): { ..., "program_id": "str_code_9" }` when a program is selected; no `program_id` key when "No Program" is selected.
- **Backend (POST /scans):** `POST /scans received program_id: ...` or `(absent/empty - backend will use only globally enabled strategies)`.
- **Backend (analysis):** `SCAN_STRATEGIES scan_id=... received program_id=... source=program|globally_enabled strategy_ids=[...] strategy_names=[...]`.
