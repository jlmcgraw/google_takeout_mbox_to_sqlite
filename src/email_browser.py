import argparse
import base64
import curses
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Sequence


class EmailBrowser:
    """Simple curses-based email browser."""

    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(db_path)
        self.filter_clause = ""
        self.sort_clause = "received_at"
        self.messages: Sequence[tuple] = []
        self.selected = 0

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------
    def _query_messages(self) -> None:
        query = "SELECT id, received_at, subject FROM emails"
        if self.filter_clause:
            query += f" WHERE {self.filter_clause}"
        if self.sort_clause:
            query += f" ORDER BY {self.sort_clause}"
        query += " LIMIT 1000"
        self.messages = self.conn.execute(query).fetchall()
        self.selected = 0

    def _get_message(self, msg_id: int) -> Dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT as_json FROM emails WHERE id=?", (msg_id,)
        ).fetchone()
        if not row:
            return None
        return json.loads(row[0])

    # ------------------------------------------------------------------
    # UI helpers
    # ------------------------------------------------------------------
    def run(self) -> None:
        curses.wrapper(self._main)

    def _main(self, stdscr: curses.window) -> None:
        curses.curs_set(0)
        stdscr.nodelay(False)
        stdscr.keypad(True)
        self._query_messages()

        while True:
            self._draw_list(stdscr)
            key = stdscr.getch()
            if key in (ord("q"), 27):
                break
            if key in (curses.KEY_DOWN, ord("j")):
                if self.selected < len(self.messages) - 1:
                    self.selected += 1
            elif key in (curses.KEY_UP, ord("k")):
                if self.selected > 0:
                    self.selected -= 1
            elif key in (curses.KEY_ENTER, ord("\n"), curses.KEY_RIGHT):
                if self.messages:
                    msg_id = self.messages[self.selected][0]
                    self._view_message(stdscr, msg_id)
            elif key == ord("f"):
                clause = self._prompt(stdscr, "Filter SQL: ")
                if clause is not None:
                    self.filter_clause = clause
                    self._query_messages()
            elif key == ord("s"):
                clause = self._prompt(stdscr, "Sort column: ")
                if clause is not None:
                    self.sort_clause = clause
                    self._query_messages()

    def _draw_list(self, stdscr: curses.window) -> None:
        stdscr.clear()
        h, w = stdscr.getmaxyx()
        for idx, row in enumerate(self.messages[: h - 2]):
            marker = ">" if idx == self.selected else " "
            line = f"{marker} {row[0]:5} {row[1]} {row[2]}"
            stdscr.addnstr(idx, 0, line, w - 1)
        stdscr.addnstr(h - 1, 0, "q quit  ENTER view  f filter  s sort", w - 1)
        stdscr.refresh()

    def _view_message(self, stdscr: curses.window, msg_id: int) -> None:
        data = self._get_message(msg_id)
        if not data:
            return
        lines = []
        headers = data.get("headers", {})
        for k, values in headers.items():
            if values:
                lines.append(f"{k}: {values[0]}")
        lines.append("")
        self._payload_to_lines(data.get("payload"), lines)

        offset = 0
        while True:
            stdscr.clear()
            h, w = stdscr.getmaxyx()
            for i in range(h - 2):
                if offset + i < len(lines):
                    stdscr.addnstr(i, 0, lines[offset + i], w - 1)
            stdscr.addnstr(
                h - 1,
                0,
                "UP/DOWN scroll  a save attachments  b back",
                w - 1,
            )
            stdscr.refresh()
            key = stdscr.getch()
            if key in (ord("b"), curses.KEY_LEFT, ord("q")):
                break
            elif key in (curses.KEY_DOWN, ord("j")) and offset < len(lines) - h + 2:
                offset += 1
            elif key in (curses.KEY_UP, ord("k")) and offset > 0:
                offset -= 1
            elif key == ord("a"):
                self._save_attachments(stdscr, msg_id)

    def _prompt(self, stdscr: curses.window, text: str) -> str | None:
        h, w = stdscr.getmaxyx()
        stdscr.addnstr(h - 1, 0, text, w - 1)
        stdscr.clrtoeol()
        curses.echo()
        curses.curs_set(1)
        try:
            inp = stdscr.getstr(h - 1, len(text)).decode().strip()
        except Exception:
            inp = ""
        curses.noecho()
        curses.curs_set(0)
        return inp

    # ------------------------------------------------------------------
    # Attachment helpers
    # ------------------------------------------------------------------
    def _save_attachments(self, stdscr: curses.window, msg_id: int) -> None:
        path_str = self._prompt(stdscr, "Save to directory: ")
        if not path_str:
            return
        out_dir = Path(path_str)
        out_dir.mkdir(parents=True, exist_ok=True)
        data = self._get_message(msg_id)
        if not data:
            return
        attachments = self._collect_attachments(data.get("payload"))
        for idx, att in enumerate(attachments, 1):
            filename = att.get("filename") or f"attachment_{idx}"
            with open(out_dir / filename, "wb") as fp:
                fp.write(base64.b64decode(att.get("data_b64", "")))
        stdscr.addnstr(
            0,
            0,
            f"Saved {len(attachments)} attachments to {out_dir}",
            stdscr.getmaxyx()[1] - 1,
        )
        stdscr.getch()

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
                lines.append(
                    " " * indent + f"[binary data {payload.get('content_type')}]"
                )


def main() -> None:
    parser = argparse.ArgumentParser(description="Browse email database")
    parser.add_argument("sqlite_db", type=Path, help="Path to SQLite database")
    args = parser.parse_args()
    EmailBrowser(args.sqlite_db).run()


if __name__ == "__main__":
    main()
