#!/usr/bin/env python3
"""Ultra-fast async embed resolver - extracts video_id/server_id/quality"""
import argparse, asyncio, html, json, os, re, sys, urllib.parse
from dataclasses import dataclass, field, asdict
from typing import List, Optional

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False
    import urllib.request, urllib.error

TIMEOUT    = 8
UA         = "Mozilla/5.0"
CONCUR     = 32  # max parallel resolves

TOKEN_RE = re.compile(r'[?&]play=([^&"\'<>]+)', re.I)
LOAD_RE  = re.compile(r"""load_sources\(['"]([^'"]+)['"]\)""")
LI_RE    = re.compile(r'<li\b([^>]*\bdata-id=[^>]*)>', re.I | re.S)
QUAL_RE  = re.compile(r"""<span\b[^>]*class=['"][^'"]*\bquality\b[^'"]*['"][^>]*>(.*?)</span>""", re.I | re.S)
TAG_RE   = re.compile(r'<(?:script|style)\b.*?</(?:script|style)>|<[^>]+>', re.I | re.S)
DID_RE   = re.compile(r"""data-id\s*=\s*['"](.*?)['"]""", re.I)
SID_RE   = re.compile(r"""data-server\s*=\s*['"](.*?)['"]""", re.I)

# ── data ──────────────────────────────────────────────────────────────────────

@dataclass
class Src:
    video_id:  str
    server_id: str
    quality:   str = ""

@dataclass
class R:
    input_url: str
    ok:        bool       = False
    status:    str        = ""
    sources:   List[Src]  = field(default_factory=list)
    errors:    List[str]  = field(default_factory=list)

    def j(self):
        return {
            "input_url": self.input_url,
            "ok":        self.ok,
            "status":    self.status,
            "sources":   [asdict(s) for s in self.sources],
            "errors":    self.errors,
        }

# ── parsing ────────────────────────────────────────────────────────────────────

def _token(s: str) -> Optional[str]:
    m = TOKEN_RE.search(s)
    if m: return urllib.parse.unquote(m.group(1))
    m = LOAD_RE.search(s)
    if m: return m.group(1)
    return None

def _sources(body: str) -> List[Src]:
    out  = []
    ms   = list(LI_RE.finditer(body))
    ends = [ms[i+1].start() for i in range(len(ms)-1)] + [
        max(body.find("</ul>", ms[-1].end()) if ms else -1, 0) or len(body)
    ]
    for m, end in zip(ms, ends):
        a  = m.group(1)
        vi = DID_RE.search(a)
        si = SID_RE.search(a)
        if not vi or not si: continue
        frag = body[m.end():end]
        qm   = QUAL_RE.search(frag)
        q    = TAG_RE.sub(" ", qm.group(1)).strip() if qm else ""
        out.append(Src(vi.group(1), si.group(1), q))
    return out

# ── async HTTP (aiohttp path) ──────────────────────────────────────────────────

if HAS_AIOHTTP:
    _HDRS_GET  = {"User-Agent": UA, "Accept": "text/html"}
    _HDRS_POST = {
        "User-Agent":       UA,
        "Content-Type":     "application/x-www-form-urlencoded",
        "X-Requested-With": "XMLHttpRequest",
    }

    async def _get(sess: "aiohttp.ClientSession", url: str, ref: str = None):
        hdrs = dict(_HDRS_GET)
        if ref: hdrs["Referer"] = ref
        async with sess.get(url, headers=hdrs, allow_redirects=True) as resp:
            return resp.status, str(resp.url), await resp.text(errors="replace")

    async def _post(sess: "aiohttp.ClientSession", url: str, data: dict, ref: str):
        p   = urllib.parse.urlsplit(url)
        hdrs = dict(_HDRS_POST)
        hdrs["Origin"]  = f"{p.scheme}://{p.netloc}"
        hdrs["Referer"] = ref
        async with sess.post(url, headers=hdrs, data=data) as resp:
            return await resp.text(errors="replace")

    async def _resolve_one(sess: "aiohttp.ClientSession", url: str) -> R:
        r = R(url)
        try:
            status, final_url, body = await _get(sess, url)
            if status >= 400:
                r.errors.append(f"HTTP{status}"); r.status = "http_error"; return r

            # token hunt: URL first, then body, then a second GET if needed
            tk = _token(final_url) or _token(body)
            if not tk:
                _, _, pg = await _get(sess, final_url, url)
                tk = _token(pg)
            if not tk:
                r.errors.append("no token"); r.status = "no_token"; return r

            resp_url = urllib.parse.urljoin(final_url, "/response.php")
            rh       = await _post(sess, resp_url, {"token": tk}, final_url)
            r.sources = _sources(rh)
            r.ok      = bool(r.sources)
            r.status  = "ok" if r.ok else "no_sources"
        except Exception as e:
            r.status = "error"; r.errors.append(f"{type(e).__name__}:{e}")
        return r

    async def resolve_many(urls: List[str]) -> List[R]:
        connector = aiohttp.TCPConnector(
            limit=CONCUR,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)
        sem     = asyncio.Semaphore(CONCUR)

        async def bounded(u):
            async with sem:
                return await _resolve_one(sess, u)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as sess:
            return await asyncio.gather(*[bounded(u) for u in urls])

    def resolve(url: str) -> R:
        return asyncio.run(resolve_many([url]))[0]

# ── sync fallback (stdlib urllib) ──────────────────────────────────────────────
else:
    def _g(u, ref=None):
        hdrs = {"User-Agent": UA, "Accept": "text/html"}
        if ref: hdrs["Referer"] = ref
        req = urllib.request.Request(u, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return resp.status, resp.geturl(), resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, u, e.read().decode("utf-8", "replace")

    def _p(u, data, ref):
        p = urllib.parse.urlsplit(u)
        hdrs = {
            "User-Agent": UA,
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Requested-With": "XMLHttpRequest",
            "Origin":  f"{p.scheme}://{p.netloc}",
            "Referer": ref,
        }
        req = urllib.request.Request(
            u, data=urllib.parse.urlencode(data).encode(),
            headers=hdrs, method="POST"
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return resp.read().decode("utf-8", "replace")

    def resolve(url: str) -> R:
        r = R(url)
        try:
            status, final_url, body = _g(url)
            if status >= 400:
                r.errors.append(f"HTTP{status}"); r.status = "http_error"; return r
            tk = _token(final_url) or _token(body)
            if not tk:
                _, _, pg = _g(final_url, url)
                tk = _token(pg)
            if not tk:
                r.errors.append("no token"); r.status = "no_token"; return r
            resp_url = urllib.parse.urljoin(final_url, "/response.php")
            rh = _p(resp_url, {"token": tk}, final_url)
            r.sources = _sources(rh)
            r.ok      = bool(r.sources)
            r.status  = "ok" if r.ok else "no_sources"
        except Exception as e:
            r.status = "error"; r.errors.append(f"{type(e).__name__}:{e}")
        return r

    def resolve_many(urls: List[str]) -> List[R]:
        return [resolve(u) for u in urls]

# ── HTTP server ────────────────────────────────────────────────────────────────

def _make_server(port: int):
    import json
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class H(BaseHTTPRequestHandler):
        server_version = "FR/2"

        def do_GET(self):
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            try:
                if self.path.startswith("/health"):
                    self._j({"ok": 1}); return
                if self.path.startswith("/resolve"):
                    urls = q.get("url") or []
                    if not urls:
                        self._j({"ok": 0, "error": "missing url"}, 400); return
                    if len(urls) == 1:
                        self._j(resolve(urls[0]).j()); return
                    # batch
                    if HAS_AIOHTTP:
                        results = asyncio.run(resolve_many(urls))
                    else:
                        results = resolve_many(urls)
                    self._j([r.j() for r in results]); return
                self._j({"ok": 0}, 404)
            except Exception as e:
                self._j({"ok": 0, "error": str(e)}, 500)

        def log_message(self, *a): pass

        def _j(self, d, s=200):
            b = json.dumps(d, separators=(',', ':')).encode()
            self.send_response(s)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b)

    return ThreadingHTTPServer(("0.0.0.0", port), H)

# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Fast embed resolver")
    ap.add_argument("urls", nargs="*",
                    default=["https://multiembed.mov/?video_id=280&tmdb=1"])
    ap.add_argument("--serve",  action="store_true")
    ap.add_argument("--port",   type=int, default=int(os.environ.get("PORT", "8787")))
    ap.add_argument("--output", help="Write JSON results to file")
    a = ap.parse_args()

    if a.serve:
        srv = _make_server(a.port)
        print(f"Listening on {a.port}", flush=True)
        srv.serve_forever()
        return 0

    if HAS_AIOHTTP:
        results = asyncio.run(resolve_many(a.urls))
    else:
        results = resolve_many(a.urls)

    out = [r.j() for r in results] if len(results) > 1 else results[0].j()
    text = json.dumps(out, indent=2, ensure_ascii=False)
    print(text)

    if a.output:
        with open(a.output, "w") as f:
            f.write(text)

    return 0 if all(r.ok for r in results) else 1

if __name__ == "__main__":
    raise SystemExit(main())
