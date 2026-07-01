#!/usr/bin/env python3
"""LSports INPLAY Predictions Server — Python 3.7+"""
import json, os, socket, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATABASE_URL = os.environ.get("DATABASE_URL")

# In-memory store
BETS = []
RESULTS = {}
SSE_CLIENTS = []
LOCK = threading.Lock()

# ── database (PostgreSQL on Render, JSON files locally) ──────────────────────

def _db_conn():
    import pg8000.native, urllib.parse
    u = urllib.parse.urlparse(DATABASE_URL)
    return pg8000.native.Connection(
        host=u.hostname, port=u.port or 5432,
        database=u.path.lstrip("/"),
        user=u.username, password=u.password,
        ssl_context=True
    )

def _db_init():
    con = _db_conn()
    con.run("""CREATE TABLE IF NOT EXISTS store (
        key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
    con.close()

def save_data():
    if DATABASE_URL:
        con = _db_conn()
        con.run("INSERT INTO store(key,value) VALUES('bets',:v) ON CONFLICT(key) DO UPDATE SET value=:v",
                v=json.dumps(BETS))
        con.run("INSERT INTO store(key,value) VALUES('results',:v) ON CONFLICT(key) DO UPDATE SET value=:v",
                v=json.dumps(RESULTS))
        con.close()
    else:
        DATA_DIR.mkdir(exist_ok=True)
        (DATA_DIR / "bets.json").write_text(json.dumps(BETS, indent=2))
        (DATA_DIR / "results.json").write_text(json.dumps(RESULTS, indent=2))

def load_data():
    global BETS, RESULTS
    if DATABASE_URL:
        try:
            _db_init()
            con = _db_conn()
            rows = con.run("SELECT key, value FROM store WHERE key IN ('bets','results')")
            con.close()
            for key, val in rows:
                if key == "bets":    BETS = json.loads(val)
                if key == "results": RESULTS = json.loads(val)
        except Exception as e:
            print(f"DB load error: {e}")
    else:
        if (DATA_DIR / "bets.json").exists():
            BETS = json.loads((DATA_DIR / "bets.json").read_text())
        if (DATA_DIR / "results.json").exists():
            RESULTS = json.loads((DATA_DIR / "results.json").read_text())

# ── helpers ───────────────────────────────────────────────────────────────────

def local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"

def broadcast(data):
    msg = ("data: " + json.dumps(data) + "\n\n").encode()
    with LOCK:
        dead = []
        for cli in SSE_CLIENTS:
            try:
                cli.wfile.write(msg)
                cli.wfile.flush()
            except Exception:
                dead.append(cli)
        for d in dead:
            SSE_CLIENTS.remove(d)

# ── request handler ───────────────────────────────────────────────────────────

MIME = {
    ".html": "text/html; charset=utf-8",
    ".css":  "text/css",
    ".js":   "application/javascript",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".ico":  "image/x-icon",
    ".json": "application/json",
}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self.cors()
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/events":
            self._sse()
        elif path == "/api/bets":
            self._json({"bets": BETS, "results": RESULTS})
        elif path == "/api/info":
            ip = local_ip()
            self._json({"ip": ip, "betUrl": f"http://{ip}:3000/bet.html"})
        elif path == "/ping":
            self._json({"ok": True})
        else:
            self._static(path)

    def do_POST(self):
        global RESULTS
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")

        if path == "/api/bet":
            body["id"] = str(uuid.uuid4())[:8]
            if not body.get("ts"):
                body["ts"] = int(time.time() * 1000)
            BETS.append(body)
            save_data()
            broadcast({"type": "bet", "bets": BETS, "results": RESULTS})
            self._json({"ok": True, "total": len(BETS)})
        elif path == "/api/results":
            RESULTS = body
            save_data()
            broadcast({"type": "results", "bets": BETS, "results": RESULTS})
            self._json({"ok": True})
        else:
            self.send_response(404)
            self.end_headers()

    def _json(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.cors()
        self.end_headers()
        self.wfile.write(body)

    def _static(self, path):
        if path in ("/", ""):
            path = "/bet.html"
        fp = BASE_DIR / path.lstrip("/")
        if fp.exists() and fp.is_file():
            data = fp.read_bytes()
            ct = MIME.get(fp.suffix.lower(), "application/octet-stream")
            self.send_response(200)
            self.send_header("Content-Type", ct)
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")

    def _sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.cors()
        self.end_headers()
        init_msg = ("data: " + json.dumps({"type": "init", "bets": BETS, "results": RESULTS}) + "\n\n").encode()
        try:
            self.wfile.write(init_msg)
            self.wfile.flush()
        except Exception:
            return
        with LOCK:
            SSE_CLIENTS.append(self)
        while True:
            try:
                time.sleep(20)
                self.wfile.write(b": heartbeat\n\n")
                self.wfile.flush()
            except Exception:
                break
        with LOCK:
            if self in SSE_CLIENTS:
                SSE_CLIENTS.remove(self)


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    load_data()
    PORT = int(os.environ.get("PORT", 3000))
    ip = local_ip()

    server = ThreadingHTTPServer(("", PORT), Handler)

    sep = "=" * 52
    print(f"\n{sep}")
    print(f"  LSports INPLAY Predictions — Running!")
    print(sep)
    print(f"\n  Dashboard:  http://{ip}:{PORT}/dashboard.html")
    print(f"  Bet form:   http://{ip}:{PORT}/bet.html")
    print(f"  Back office: http://{ip}:{PORT}/admin.html")
    print(f"\n  Bets loaded: {len(BETS)}")
    print(f"  Press Ctrl+C to stop")
    print(f"{sep}\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
