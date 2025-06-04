import logging
import sqlite3
from dataclasses import dataclass, asdict
from textwrap import dedent
from typing import List


@dataclass
class MboxDatabase:
    table: str
    columns: List[str]
    conn: sqlite3.Connection

    def create_table(self) -> None:
        create_table_sql = dedent(
            """
                                  CREATE TABLE IF NOT EXISTS emails
                                  (
                                      id
                                                  INTEGER
                                          PRIMARY KEY
                                          AUTOINCREMENT,
                                      message_id
                                                  TEXT
                                          UNIQUE,
                                      as_json
                                                  JSON   NOT NULL,
                                      received_at TEXT,
                                      subject
                                                  TEXT
                                          GENERATED
                                              ALWAYS AS (
                                              json_extract
                                              (
                                                      "as_json",
                                                      '$.headers.Subject[0]'
                                              )) VIRTUAL NOT NULL
                                  );
                                  """
        ).strip()

        self.conn.execute(create_table_sql)

        create_view_sql = dedent(
            """
                                 CREATE VIEW IF NOT EXISTS email_overview AS
                                 SELECT id,
                                        message_id,
                                        json_extract(as_json, '$.headers.Date[0]') AS Date
                                 FROM emails;"""
        ).strip()
        self.conn.execute(create_view_sql)
        self.conn.commit()

    def construct_insert_statement(self) -> str:
        sql = f"INSERT OR IGNORE INTO {self.table} (message_id, as_json, received_at) VALUES (?, ?, ?)"
        return sql

    def insert_email(self, record) -> None:
        fields = asdict(record)
        columns = ", ".join(fields.keys())
        placeholders = ", ".join("?" for _ in fields)
        values = tuple(fields.values())

        try:
            self.conn.execute(
                f"INSERT OR IGNORE INTO emails ({columns}) VALUES ({placeholders})",
                values,
            )
            logging.debug(f"Inserted {record.message_id}")
        except sqlite3.IntegrityError as e:
            logging.error(f"Failed to insert email {record.message_id!r}: {e}")
