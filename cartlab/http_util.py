from __future__ import annotations

import json
import http.client
import re
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ID = re.compile(r"^[a-zA-Z0-9_.:@-]{1,140}$")


def valid_id(value):
    if not isinstance(value, str) or not ID.fullmatch(value):
        raise ValueError("invalid identifier")
    return value


def request(url, data=None, timeout=2):
    payload = json.dumps(data, ensure_ascii=False).encode() if data is not None else None
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    # Local simulation must not inherit an unrelated machine HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)
    except http.client.HTTPException as exc:
        raise OSError("HTTP transport interrupted") from exc


class LabHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64


class JsonHandler(BaseHTTPRequestHandler):
    server_version = "CartLab/1.0"

    def log_message(self, *_):
        pass

    def body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= 16 * 1024 * 1024:
            raise ValueError("invalid request size")
        value = json.loads(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValueError("body must be an object")
        return value

    def send_json(self, value, status=200):
        self.send_bytes(json.dumps(value, ensure_ascii=False).encode(), "application/json; charset=utf-8", status)

    def send_bytes(self, value, content_type, status=200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(value)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        try:
            self.wfile.write(value)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
