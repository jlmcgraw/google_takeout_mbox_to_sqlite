# google_takeout_mbox_to_sqlite

Load emails and chats from a Google Takeout `mbox` archive into a SQLite database.

## Requirements

* Python 3.12+
* [`tqdm`](https://pypi.org/project/tqdm/) for progress bars

## Installation

Install the project in editable mode or simply install the dependencies:

```bash
pip install -e .
```

## Usage

Run the importer with the path to your exported `.mbox` file and the
SQLite database you want to create or update:

```bash
python -m src.main <mbox-file> <database-file> [--verbose]
```

A progress bar shows the number of messages processed. When finished an index
on the message `Date` header is created automatically.

## Database layout

The importer creates a single table named `emails` containing:

- `id` – auto incrementing primary key
- `message_id` – the message `Message-ID`, generated if missing
- `as_json` – the entire message encoded as JSON
- `received_at` – timestamp in UTC
- `subject` – generated column extracted from the headers

A view called `email_overview` exposes the `Date` header for convenience.

## License

This project is distributed under the terms of the GNU General Public License
version 3.
