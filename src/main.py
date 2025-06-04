import argparse
import logging
import mailbox
import sqlite3
from pathlib import Path
from textwrap import dedent

from tqdm import tqdm

from mbox_database import MboxDatabase
from mbox_message import MboxMessage


def process_mbox(mbox_path: Path, db_path: Path) -> None:
    logging.info(f"Opening mbox file: '{mbox_path}'")
    mbox = mailbox.mbox(mbox_path)
    total = len(mbox)

    with sqlite3.connect(db_path) as conn:
        # conn.execute("PRAGMA journal_mode = WAL")
        # conn.execute("PRAGMA synchronous = NORMAL")
        # conn.execute("PRAGMA temp_store = MEMORY")
        # conn.execute("PRAGMA cache_size = 10000")

        table = MboxDatabase("emails", ["message_id", "as_json", "received_at"], conn=conn)
        email_insert_statement = table.construct_insert_statement()

        # Create the table
        table.create_table()

        batch = []
        batch_size = 5_000

        cursor = conn.cursor()
        cursor.execute("BEGIN")

        # Load messages from mbox -> sqlite in batches
        for msg in tqdm(mbox, desc="Importing emails", unit="email", total=total):
            record = MboxMessage(msg)
            batch.append(record.as_tuple())

            if len(batch) >= batch_size:
                tqdm.write("Processing batch")
                cursor.executemany(email_insert_statement, batch)
                batch.clear()

        # Load what's left
        if batch:
            cursor.executemany(email_insert_statement, batch)

        create_indexes(conn)

    logging.info("Processing complete")


def create_indexes(conn: sqlite3.Connection) -> None:
    index_name = "index_received"
    existing = conn.execute("PRAGMA index_list(emails)").fetchall()
    if any(row[1] == index_name for row in existing):
        return
    create_index_sql = dedent("""
        CREATE INDEX index_received
        ON emails (json_extract(as_json, '$.headers.Date[0]'))
    """).strip()
    conn.execute(create_index_sql)
    logging.info(f"Created index: {index_name}")

def main() -> None:
    args = parse_arguments()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")

    process_mbox(args.mbox_file, args.sqlite_db)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load emails from mbox into SQLite")
    parser.add_argument("mbox_file", type=Path, help="Path to .mbox file")
    parser.add_argument("sqlite_db", type=Path, help="Path to SQLite database")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    parsed = parser.parse_args()
    return parsed


if __name__ == "__main__":
    main()
