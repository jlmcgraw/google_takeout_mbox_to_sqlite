import base64
import hashlib
import json
import logging
from dataclasses import dataclass
from email.header import decode_header, make_header
from email.message import Message
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
import xml.etree.ElementTree as ET

SQLITE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

@dataclass
class MboxMessage:
    msg: Message
    message_id: str | None = None
    as_json: str | None = None
    received_at: datetime | None = None

    def __post_init__(self):
        msg_dict = self._msg_to_dict(self.msg)
        self.as_json = json.dumps(msg_dict, sort_keys=True, ensure_ascii=True, indent=2)

        self.message_id = self._get_or_generate_message_id(msg_dict)
        self.received_at = self._extract_received_at(msg_dict)

    def as_tuple(self) -> tuple:
        return self.message_id, self.as_json, self.received_at.strftime(SQLITE_DATE_FORMAT)

    def write_json(self, path: str = "out.json"):
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(self.as_json, fp, indent=2, ensure_ascii=True)

    def _get_or_generate_message_id(self, msg_dict: dict) -> str:
        msg_id = str(self.msg.get("Message-ID", "")).strip()

        if not msg_id:
            digest = self._hash_dict(msg_dict)
            logging.warning(f"Missing Message-ID; using SHA256 digest")
            return digest

        return msg_id

    def _extract_received_at(self, msg_dict: dict) -> datetime:
        headers = msg_dict.get("headers", {})
        payloads = msg_dict.get("payload", [])

        if self._is_google_chat(headers, payloads):
            logging.debug(f"Received a Google Chat message: {self.message_id}")
            return self._extract_chat_timestamp(payloads)
        try:
            raw_date = self.msg.get("Date")
            if not raw_date:
                print(msg_dict)
                # input("Missing date header, Enter to continue...")
                raise ValueError("Missing 'Date' header")
            dt = parsedate_to_datetime(raw_date)
            return dt.astimezone(timezone.utc)
        except Exception as e:
            # "01/31/2007 01:49AM"
            # 08 December 2005"
            logging.error("Failed to parse message date: %s", e)
            return datetime.now(timezone.utc)

    def _msg_to_dict(self, msg: Message) -> dict:
        headers = {}
        for name, raw_value in msg.raw_items():
            decoded_value = str(make_header(decode_header(raw_value)))
            headers.setdefault(name, []).append(decoded_value)

        if msg.is_multipart():
            parts = msg.get_payload()
            payload = [self._msg_to_dict(part) for part in parts]
        else:
            content_type = msg.get_content_type()
            data = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset(failobj="utf-8")

            if content_type.startswith("text/"):
                try:
                    text = data.decode(charset, errors="replace")
                except LookupError:
                    logging.warning("Unknown charset '%s'; using utf-8 fallback", charset)
                    text = data.decode("utf-8", errors="replace")
                payload = {
                    "type": "text",
                    "content_type": content_type,
                    "charset": charset,
                    "text": text,
                }
            else:
                payload = {
                    "type": "binary",
                    "content_type": content_type,
                    "data_b64": base64.b64encode(data).decode("ascii"),
                }

        return {"headers": headers, "payload": payload}

    @staticmethod
    def _hash_dict(data: dict) -> str:
        canonical = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _is_google_chat(headers: dict, payloads: list) -> bool:
        raw_labels = headers.get("X-Gmail-Labels", [])

        # Normalize to a flat list of labels
        if isinstance(raw_labels, list) and len(raw_labels) == 1 and isinstance(raw_labels[0], str):
            labels = [label.strip() for label in raw_labels[0].split(",")]
        elif isinstance(raw_labels, list):
            labels = raw_labels
        else:
            labels = []

        return "Chat" in labels

    @staticmethod
    def _extract_chat_timestamp(payloads: list) -> datetime:
        ns = {
            "cli": "jabber:client",
            "con": "google:archive:conversation",
            "x": "jabber:x:delay",
            "time": "google:timestamp",
        }

        for part in payloads:
            if not isinstance(part, dict):
                continue
            ct_list = part.get("headers", {}).get("Content-Type", [])
            if not ct_list or not ct_list[0].startswith("text/xml"):
                continue
            xml_text = part.get("payload", {}).get("text", "")
            try:
                root = ET.fromstring(xml_text)
            except ET.ParseError:
                continue

            for msg in root.findall(".//cli:message", ns):
                ts_epoch = msg.attrib.get("{google:internal}time-stamp")
                ts_delay = msg.find(".//x:x", ns)
                ts_google = msg.find(".//time:time", ns)

                if ts_epoch:
                    return datetime.fromtimestamp(int(ts_epoch) / 1000, tz=timezone.utc)
                elif ts_delay is not None:
                    jabber_dt = datetime.strptime(ts_delay.attrib["stamp"], "%Y%m%dT%H:%M:%S")
                    return jabber_dt.replace(tzinfo=timezone.utc)
                elif ts_google is not None:
                    return datetime.fromtimestamp(int(ts_google.attrib["ms"]) / 1000, tz=timezone.utc)

        logging.warning("No valid timestamp found in Google Chat payload; using current time")
        return datetime.now(timezone.utc)
