"""Textual-based browser for the email database."""

from __future__ import annotations

import argparse
import base64
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from textual.app import App, ComposeResult
from textual.widgets import DataTable, Footer, Header, TextLog
from textual.screen import Screen


class EmailBrowserApp(App):
    """Interactive browser for exploring messages."""

    CSS_PATH = None

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("f", "filter", "Filter"),
        ("s", "sort", "Sort"),
        ("enter", "view", "View"),
    ]

    def __init__(self, db_path: Path) -> None:
        super().__init__()
        self.conn = sqlite3.connect(db_path)
        # Some databases may contain invalid UTF-8 in the generated
        # ``subject`` column which causes ``fetchmany`` to raise an
        # ``OperationalError`` when Textual loads rows.  Using a
        # ``text_factory`` that replaces undecodable characters prevents
        # the crash while still showing the rest of the row.
        self.conn.text_factory = lambda b: b.decode("utf-8", "replace")
        self.filter_clause = ""
        self.sort_clause = "received_at"
        self.table: DataTable | None = None
        self._cursor: sqlite3.Cursor | None = None

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------
    def _query_messages(self) -> sqlite3.Cursor:
        query = "SELECT id, received_at, subject FROM emails"
        if self.filter_clause:
            query += f" WHERE {self.filter_clause}"
        if self.sort_clause:
            query += f" ORDER BY {self.sort_clause}"
        return self.conn.execute(query)

    def _load_message(self, msg_id: int) -> Dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT as_json FROM emails WHERE id=?",
            (msg_id,),
        ).fetchone()
        if not row:
            return None
        return json.loads(row[0])

    # ------------------------------------------------------------------
    # Textual lifecycle
    # ------------------------------------------------------------------
    def compose(self) -> ComposeResult:
        yield Header()
        self.table = DataTable(zebra_stripes=True)
        self.table.cursor_type = "row"
        self.table.focus()
        yield self.table
        yield Footer()

    def on_mount(self) -> None:
        assert self.table is not None
        self.table.add_columns("ID", "Received", "Subject")
        self.refresh_table()
        self.table.focus()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------
    def action_view(self) -> None:
        assert self.table is not None
        if not self.table.row_count:
            return
        row_key = self.table.cursor_row_key
        if row_key is None:
            return
        key_value = row_key.value if hasattr(row_key, "value") else row_key
        msg_id = int(key_value)
        data = self._load_message(msg_id)
        if not data:
            return
        self.push_screen(MessageScreen(msg_id, data))

    async def action_filter(self) -> None:
        clause = await self.ask("Filter SQL:")
        if clause is not None:
            self.filter_clause = clause
            self.refresh_table()

    async def action_sort(self) -> None:
        column = await self.ask("Sort column:")
        if column is not None:
            self.sort_clause = column
            self.refresh_table()

    def refresh_table(self) -> None:
        assert self.table is not None
        self.table.clear()
        self._cursor = self._query_messages()
        self._load_next_batch()

    def _load_next_batch(self, batch_size: int = 1000) -> None:
        assert self.table is not None
        if self._cursor is None:
            return
        rows = self._cursor.fetchmany(batch_size)
        for row in rows:
            self.table.add_row(str(row[0]), row[1], row[2], key=row[0])
        if rows:
            self.call_later(self._load_next_batch)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        key_value = event.row_key.value if hasattr(event.row_key, "value") else event.row_key
        msg_id = int(key_value)
        data = self._load_message(msg_id)
        if data:
            self.push_screen(MessageScreen(msg_id, data))

    # ------------------------------------------------------------------
    # Attachment helpers
    # ------------------------------------------------------------------
    def save_attachments(self, msg_id: int) -> None:
        path_str = self.console.input("Save to directory: ")
        if not path_str:
            return
        out_dir = Path(path_str)
        out_dir.mkdir(parents=True, exist_ok=True)
        data = self._load_message(msg_id)
        if not data:
            return
        attachments = self._collect_attachments(data.get("payload"))
        for idx, att in enumerate(attachments, 1):
            filename = att.get("filename") or f"attachment_{idx}"
            with open(out_dir / filename, "wb") as fp:
                fp.write(base64.b64decode(att.get("data_b64", "")))
        self.console.print(f"Saved {len(attachments)} attachments to {out_dir}")

    def _collect_attachments(self, payload: Any) -> List[Dict[str, Any]]:
        attachments: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            for part in payload:
                attachments.extend(self._collect_attachments(part))
        elif isinstance(payload, dict):
            if payload.get("type") == "binary":
                headers = payload.get("headers", {})
                disp = headers.get("Content-Disposition", [""])[0]
                filename = None
                if "filename=" in disp:
                    filename = disp.split("filename=")[-1].strip().strip('"')
                attachments.append(
                    {
                        "data_b64": payload.get("data_b64", ""),
                        "content_type": payload.get("content_type", ""),
                        "filename": filename,
                    }
                )
            else:
                attachments.extend(self._collect_attachments(payload.get("payload")))
        return attachments


class MessageScreen(Screen):
    """Screen for viewing a single message."""

    BINDINGS = [
        ("b", "app.pop_screen", "Back"),
        ("a", "save_attachments", "Save attachments"),
    ]

    def __init__(self, msg_id: int, data: Dict[str, Any]) -> None:
        super().__init__()
        self.msg_id = msg_id
        self.data = data
        self.log: TextLog | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        self.log = TextLog(highlight=False, markup=False)
        yield self.log
        yield Footer()

    def on_mount(self) -> None:
        assert self.log is not None
        lines: List[str] = []
        headers = self.data.get("headers", {})
        for k, values in headers.items():
            if values:
                lines.append(f"{k}: {values[0]}")
        lines.append("")
        self._payload_to_lines(self.data.get("payload"), lines)
        for line in lines:
            self.log.write(line)

    def action_save_attachments(self) -> None:
        self.app.save_attachments(self.msg_id)

    def _payload_to_lines(self, payload: Any, lines: List[str], indent: int = 0) -> None:
        if isinstance(payload, list):
            for part in payload:
                self._payload_to_lines(part, lines, indent)
        elif isinstance(payload, dict):
            if payload.get("type") == "text":
                text = payload.get("text", "")
                for line in text.splitlines():
                    lines.append(" " * indent + line)
            else:
                lines.append(" " * indent + f"[binary data {payload.get('content_type')}]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Browse email database")
    parser.add_argument("sqlite_db", type=Path, help="Path to SQLite database")
    args = parser.parse_args()
    EmailBrowserApp(args.sqlite_db).run()


if __name__ == "__main__":
    main()

