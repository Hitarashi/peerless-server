#!/usr/bin/env python3
"""
extract_backend.py — automate LastWave backend/secret extraction.

Pipeline:
  1. Download the latest LastWave release APK from GitHub (or use a local APK).
  2. Decompile with apktool.
  3. Parse com.lastwave.app.BuildConfig <clinit> for fill-array-data byte arrays.
  4. XOR-decode them cyclically with SECRET_MASK_BYTES (Gradle obfuscateSecret scheme).
  5. Print backend URLs / API keys. Optionally health-test the backends (--test).

Requirements: python3, apktool (on PATH), curl not required.

Usage:
  python3 extract_backend.py                    # download latest release APK
  python3 extract_backend.py path/to/app.apk    # use local APK
  python3 extract_backend.py --test             # after extraction, GET each backend root
  python3 extract_backend.py --smali dir/       # skip apktool, use existing decompile
  python3 extract_backend.py --keep             # keep temp working dir
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request

GITHUB_API = "https://api.github.com/repos/Clash-Projects/LastWave-native/releases/latest"
MASK_FIELD = "SECRET_MASK_BYTES"
# Known meaningful field name fragments (informational only; all [B fields are decoded)
PAIRS = [
    ("LOSSLESS_BACKEND_URL_BYTES", "LOSSLESS_API_KEY_BYTES"),
    ("BACKEND_B_URL_BYTES", "BACKEND_B_KEY_BYTES"),
]


def fail(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def http_get(url: str, dest: str | None = None, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "extract-backend/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
    if dest:
        with open(dest, "wb") as f:
            f.write(data)
    return data


def download_latest_apk(workdir: str) -> str:
    print(f"[1/4] fetching latest release info\n      {GITHUB_API}")
    rel = json.loads(http_get(GITHUB_API))
    assets = rel.get("assets", [])
    # Prefer the plain "release" variant; skip android7 / raw builds.
    apks = [a for a in assets if a["name"].lower().endswith(".apk")]
    if not apks:
        fail("no .apk assets in latest release")
    chosen = (
        next((a for a in apks if "release" in a["name"].lower()), None)
        or apks[0]
    )
    dest = os.path.join(workdir, chosen["name"])
    print(f"      downloading {chosen['name']} ({chosen['size'] // 1_000_000} MB)")
    http_get(chosen["browser_download_url"], dest, timeout=300)
    return dest


def decompile(apk: str, workdir: str) -> str:
    if not shutil.which("apktool"):
        fail("apktool not found on PATH — install it (e.g. `sudo apt install apktool` "
             "or see https://apktool.org) or pass --smali <dir>")
    out = os.path.join(workdir, "smali_out")
    print(f"[2/4] decompiling with apktool -> {out}")
    r = subprocess.run(
        ["apktool", "d", "-f", "-o", out, apk],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    if r.returncode != 0:
        fail(f"apktool failed:\n{r.stderr[-2000:]}")
    return out


def find_buildconfig(smali_dir: str) -> str:
    # Locate the app's BuildConfig.smali that carries the secret mask.
    for root, _dirs, files in os.walk(smali_dir):
        for fn in files:
            if fn != "BuildConfig.smali":
                continue
            path = os.path.join(root, fn)
            try:
                with open(path, encoding="utf-8", errors="replace") as f:
                    head = f.read(65536)
            except OSError:
                continue
            if MASK_FIELD in head:
                return path
    fail(f"BuildConfig containing {MASK_FIELD} not found under {smali_dir}")
    return ""  # unreachable


def parse_array_bytes(smali: str, label: str) -> list[int]:
    m = re.search(
        rf":{re.escape(label)}\s*\n\s*\.array-data\s+\d+\s*\n(.*?)\n\s*\.end array-data",
        smali, re.S,
    )
    if not m:
        fail(f"array data for :{label} not found")
    vals = []
    for tok in m.group(1).split():
        tok = tok.rstrip(",").rstrip("t")  # tolerate trailing commas / 't' byte suffix
        try:
            vals.append(int(tok, 16) if "x" in tok.lower() else int(tok))
        except ValueError:
            continue
    return [v & 0xFF for v in vals]


def parse_buildconfig(path: str) -> dict[str, object]:
    print(f"[3/4] parsing {path}")
    with open(path, encoding="utf-8", errors="replace") as f:
        smali = f.read()

    fields: dict[str, object] = {}

    # Constant field initializers: .field ... NAME:TYPE = "value"
    for m in re.finditer(r'^\.field\s+[^=]*?(\w+):[^=]*=\s*"([^"]*)"', smali, re.M):
        fields[m.group(1)] = m.group(2)

    # <clinit>: track register -> array label, bind on sput-object
    clinit = re.search(r"\.method[^}]*<clinit>.*?\.end method", smali, re.S)
    if not clinit:
        fail("no <clinit> in BuildConfig")
    body = clinit.group(0)

    reg_to_label: dict[str, str] = {}
    field_to_label: dict[str, str] = {}
    for line in body.splitlines():
        line = line.strip()
        m = re.match(r"new-array\s+(v\d+),", line)
        if m:
            reg_to_label.pop(m.group(1), None)
            continue
        m = re.match(r"fill-array-data\s+(v\d+),\s*:(\w+)", line)
        if m:
            reg_to_label[m.group(1)] = m.group(2)
            continue
        m = re.match(r"sput-object\s+(v\d+),\s*L\S+->(\w+):", line)
        if m:
            label = reg_to_label.get(m.group(1))
            if label:
                field_to_label[m.group(2)] = label

    for fname, label in field_to_label.items():
        fields[fname] = parse_array_bytes(smali, label)

    return fields


def xor_decode(arr: list[int], mask: list[int]) -> bytes:
    return bytes((b ^ mask[i % len(mask)]) & 0xFF for i, b in enumerate(arr))


def display(value: object) -> str:
    if isinstance(value, bytes):
        try:
            s = value.decode("utf-8")
            if s and all(c.isprintable() or c in "\r\n\t" for c in s):
                return s
        except UnicodeDecodeError:
            pass
        return value.hex()
    return str(value)


def extract(workdir_fields: dict[str, object]) -> dict[str, bytes]:
    mask_raw = workdir_fields.get(MASK_FIELD)
    mask = bytes(mask_raw) if isinstance(mask_raw, list) else None
    if mask is None:
        fail(f"{MASK_FIELD} not found — this build may use the native "
             "(liblastwave_audio.so) secret vault instead of BuildConfig")

    decoded: dict[str, bytes] = {}
    print("\n=== BuildConfig fields (decoded) ===")
    for name, val in fields_sorted(workdir_fields):
        if isinstance(val, list):
            if name == MASK_FIELD:
                print(f"  {name:32} {bytes(val).hex()}  (xor mask, {len(val)}B)")
                continue
            dec = xor_decode(val, mask)
            decoded[name] = dec
            print(f"  {name:32} {display(dec)}")
        else:
            print(f"  {name:32} {val}")
    return decoded


def fields_sorted(fields: dict[str, object]):
    byte_fields = [k for k in fields if isinstance(fields[k], list)]
    other = [k for k in fields if not isinstance(fields[k], list)]
    for k in sorted(other) + sorted(byte_fields):
        yield k, fields[k]


def test_backends(decoded: dict[str, bytes]) -> None:
    print("\n=== endpoint tests ===")
    for url_f, key_f in PAIRS:
        url = decoded.get(url_f)
        key = decoded.get(key_f)
        if not url:
            continue
        base = url.decode("utf-8", "replace").rstrip("/")
        label = base.split("//", 1)[-1]
        status = get_status(base + "/")
        print(f"  {label:32} root            -> {status}")
        if key:
            k = key.decode("utf-8", "replace")
            status = get_status(f"{base}/search/?s=test", headers={"X-API-Key": k})
            print(f"  {label:32} /search (key)   -> {status}")


def get_status(url: str, headers: dict | None = None, timeout: int = 15) -> str:
    hdr = {"User-Agent": "extract-backend/1.0"}
    hdr.update(headers or {})
    req = urllib.request.Request(url, headers=hdr)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return f"HTTP {r.status}"
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code}"
    except Exception as e:  # timeout, DNS, TLS...
        return f"FAIL ({type(e).__name__})"


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract LastWave backend URLs/keys from an APK")
    ap.add_argument("apk", nargs="?", help="local APK path (default: download latest release)")
    ap.add_argument("--smali", help="existing apktool output dir — skip download+decompile")
    ap.add_argument("--test", action="store_true", help="HTTP-test extracted backends")
    ap.add_argument("--keep", action="store_true", help="keep temporary working directory")
    args = ap.parse_args()

    workdir = tempfile.mkdtemp(prefix="lastwave-extract-")
    try:
        if args.smali:
            smali_dir = args.smali
            print(f"[1-2/4] using existing smali dir {smali_dir}")
        else:
            if args.apk:
                apk = args.apk
                if not os.path.isfile(apk):
                    fail(f"APK not found: {apk}")
                print(f"[1/4] using local APK {apk}")
            else:
                apk = download_latest_apk(workdir)
            smali_dir = decompile(apk, workdir)

        print("[3/4] locating BuildConfig")
        bc_path = find_buildconfig(smali_dir)
        fields = parse_buildconfig(bc_path)

        print("[4/4] decoding")
        decoded = extract(fields)

        if args.test:
            test_backends(decoded)
    finally:
        if args.keep:
            print(f"\nworking dir kept: {workdir}")
        else:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
