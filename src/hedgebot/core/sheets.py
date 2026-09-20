"""Per-session stats to the user's OWN Google Sheet.

Credentials are per user, added through the bot and stored (encrypted) in hb_credentials under the
venue "gsheets": `secret` is the service-account JSON, `meta.spreadsheet_id` is the target sheet.
Auth is a Google service account — the user shares their sheet with the service-account email, so the
bot never touches any other sheet and needs no OAuth dance.

One worksheet (tab) per session, titled "Сесія <id>": a header row, one row per hedge as it closes,
and a totals row when the session finishes. Every call is best-effort and guarded — a broken or
missing sheet must never disturb a live trading session, so failures are logged and swallowed.

gspread is synchronous, so each call hops to a thread. It's only invoked on hedge open/close (minutes
to hours apart), so re-authenticating per call is cheap enough and keeps the code stateless.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import datetime, timezone

LOGGER = logging.getLogger(__name__)

HEADERS = [
    "Відкрито (UTC)", "Закрито (UTC)", "Монета", "Напрям (Entropy)",
    "Статус відкриття", "Статус позиції", "PnL $ (після комісій)", "Комісії $",
    "Обсяг Lighter $", "Обсяг Entropy $",
]


def _fmt_ts(v) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return str(v)


class SheetsLogger:
    def __init__(self, store, notify=None) -> None:
        self._store = store
        self._notify = notify                       # async (owner_id, text) -> None, for surfaced errors
        self._warned: set[int] = set()              # owners already told about a write failure

    async def enabled(self, owner_id: int) -> bool:
        return (await self._store.get_credentials(owner_id, "gsheets")) is not None

    def _title(self, sid: int) -> str:
        return f"Сесія {sid}"

    async def test_connection(self, owner_id: int) -> tuple[bool, str]:
        """Open the sheet once so setup mistakes (not shared with the service account, Sheets API not
        enabled, wrong id) surface immediately when the user connects it, not silently at trade time."""
        creds = await self._store.get_credentials(owner_id, "gsheets")
        if not creds:
            return False, "ключ не збережено"
        spreadsheet_id = creds.meta.get("spreadsheet_id")
        if not spreadsheet_id:
            return False, "немає ID таблиці"
        try:
            info = json.loads(creds.secret)
        except (ValueError, TypeError):
            return False, "JSON ключа зіпсовано"
        try:
            title = await asyncio.to_thread(self._title_sync, info, spreadsheet_id)
            return True, title
        except Exception as err:  # noqa: BLE001
            return False, str(err)[:200]

    @staticmethod
    def _title_sync(info: dict, spreadsheet_id: str) -> str:
        import gspread
        gc = gspread.service_account_from_dict(info)
        return gc.open_by_key(spreadsheet_id).title

    # ── public API (all best-effort) ────────────────────────────────────────────────────────────────
    async def ensure_sheet(self, owner_id: int, sid: int) -> None:
        await self._run(owner_id, self._ensure_sync, sid)

    async def append_hedge(self, owner_id: int, sid: int, row: dict) -> None:
        await self._run(owner_id, self._append_sync, sid, row)

    async def append_totals(self, owner_id: int, sid: int, rows: list[dict]) -> None:
        await self._run(owner_id, self._totals_sync, sid, rows)

    # ── plumbing ────────────────────────────────────────────────────────────────────────────────────
    async def _run(self, owner_id: int, fn, *args) -> None:
        creds = await self._store.get_credentials(owner_id, "gsheets")
        if not creds:
            return
        spreadsheet_id = creds.meta.get("spreadsheet_id")
        if not spreadsheet_id:
            return
        try:
            info = json.loads(creds.secret)
        except (ValueError, TypeError):
            LOGGER.warning("gsheets creds for %s are not valid JSON", owner_id)
            return
        try:
            await asyncio.to_thread(self._threaded, info, spreadsheet_id, fn, *args)
            self._warned.discard(owner_id)          # a write got through — allow future warnings again
        except Exception as err:  # noqa: BLE001 — never let a sheet error escape into the session loop
            LOGGER.exception("google sheets write failed")
            await self._warn(owner_id, err)

    async def _warn(self, owner_id: int, err: Exception) -> None:
        if not self._notify or owner_id in self._warned:
            return
        self._warned.add(owner_id)                  # once per owner until a write succeeds again
        with contextlib.suppress(Exception):
            await self._notify(
                owner_id,
                "⚠️ Не вдалося писати в Google-таблицю: "
                f"<code>{str(err)[:180]}</code>\n\nПеревір, що таблицею <b>поділено з сервіс-акаунтом</b> "
                "(Editor) і що ввімкнено <b>Google Sheets API</b>. Статистика поки не пишеться.")

    def _threaded(self, info: dict, spreadsheet_id: str, fn, *args) -> None:
        import gspread
        gc = gspread.service_account_from_dict(info)
        ss = gc.open_by_key(spreadsheet_id)
        fn(ss, *args)

    def _worksheet(self, ss, sid: int, create: bool):
        import gspread
        title = self._title(sid)
        try:
            return ss.worksheet(title)
        except gspread.WorksheetNotFound:
            if not create:
                return None
            ws = ss.add_worksheet(title=title, rows=200, cols=len(HEADERS))
            ws.append_row(HEADERS, value_input_option="USER_ENTERED")
            return ws

    def _ensure_sync(self, ss, sid: int) -> None:
        self._worksheet(ss, sid, create=True)

    @staticmethod
    def _row_values(row: dict) -> list:
        return [
            _fmt_ts(row.get("opened_at")), _fmt_ts(row.get("closed_at")),
            row.get("coin", ""), row.get("side", ""),
            row.get("open_status", ""), row.get("status", ""),
            round(float(row.get("pnl") or 0), 4), round(float(row.get("fees") or 0), 4),
            round(float(row.get("lighter_vol") or 0), 2), round(float(row.get("entropy_vol") or 0), 2),
        ]

    def _append_sync(self, ss, sid: int, row: dict) -> None:
        ws = self._worksheet(ss, sid, create=True)
        ws.append_row(self._row_values(row), value_input_option="USER_ENTERED")

    def _totals_sync(self, ss, sid: int, rows: list[dict]) -> None:
        ws = self._worksheet(ss, sid, create=True)
        pnl = sum(float(r.get("pnl") or 0) for r in rows)
        fees = sum(float(r.get("fees") or 0) for r in rows)
        lvol = sum(float(r.get("lighter_vol") or 0) for r in rows)
        evol = sum(float(r.get("entropy_vol") or 0) for r in rows)
        ws.append_row(
            ["", "", f"РАЗОМ ({len(rows)} хедж.)", "", "", "",
             round(pnl, 4), round(fees, 4), round(lvol, 2), round(evol, 2)],
            value_input_option="USER_ENTERED")
