import asyncio
import json
import logging
import os
import re

import aiohttp
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bitly-checker")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
# Optional: comma/space separated Telegram user IDs. Empty = everyone can use the bot.
ADMINS = {int(x) for x in re.split(r"[,\s]+", os.environ.get("ADMINS", "")) if x.lstrip("-").isdigit()}
CONCURRENCY = int(os.environ.get("CONCURRENCY", "5"))
DEBOUNCE = 6  # seconds to wait for more messages before starting the check

KEYS = tuple(k.strip().lower() for k in os.environ.get("LIMIT_KEY", "short,encode,link").split(",") if k.strip())

BASE = "https://api-ssl.bitly.com/v4"
TOKEN_RE = re.compile(r"[A-Za-z0-9_\-\.]{20,}")

app = Client("bitly-checker", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
buffers = {}  # user_id -> {"tokens": [...], "task": asyncio.Task, "msg": Message}


# ---------------------------------------------------------------- Bitly API
def usage_exceeded(data):
    return "API_USAGE_LIMIT_EXCEEDED" in json.dumps(data or {})


async def api_get(session, token, path):
    """Returns (status, json, headers). Retries on rate-limit style responses."""
    st, data, headers = None, {}, {}
    for attempt in range(4):
        try:
            async with session.get(
                BASE + path,
                headers={"Authorization": f"Bearer {token}"},
                timeout=aiohttp.ClientTimeout(total=25),
            ) as r:
                st, headers = r.status, r.headers
                try:
                    data = await r.json(content_type=None)
                except Exception:
                    data = {}
                if st == 429 and usage_exceeded(data):
                    return st, data, headers  # token is valid, monthly API quota is over
                if st == 429 or (st == 403 and "X-Ratelimit-Reason" in headers):
                    await asyncio.sleep(2 * (attempt + 1))
                    continue
                return st, data, headers
        except (aiohttp.ClientError, asyncio.TimeoutError):
            st = None
            await asyncio.sleep(1 + attempt)
    return st, data, headers


async def get_org_guid(session, token, user):
    st, data, _ = await api_get(session, token, "/organizations")
    if st == 200:
        orgs = data.get("organizations") or []
        if orgs and orgs[0].get("guid"):
            return orgs[0]["guid"]
    group = user.get("default_group_guid")
    if group:
        st, data, _ = await api_get(session, token, f"/groups/{group}")
        if st == 200 and data.get("organization_guid"):
            return data["organization_guid"]
    return None


def pick_main(limits):
    """Pick the short-link creation limit. Falls back to None if nothing matches."""
    for key in KEYS:
        for item in limits:
            text = f"{item.get('name', '')} {item.get('description', '')}".lower()
            if (
                key in text
                and isinstance(item.get("limit"), (int, float))
                and isinstance(item.get("count"), (int, float))
            ):
                return item
    return None


async def check_token(session, sem, token):
    async with sem:
        res = {"token": token, "status": "error", "login": "", "limit": None,
               "count": None, "remaining": None, "all": [], "note": ""}

        st, user, _ = await api_get(session, token, "/user")
        if st in (401, 403):
            await asyncio.sleep(2)  # confirm once before calling it invalid
            st, user, _ = await api_get(session, token, "/user")
            if st in (401, 403):
                res["status"] = "invalid"
                return res
        if st == 429 and usage_exceeded(user):
            res.update(status="valid", remaining=0, note="monthly API requests exhausted")
            return res
        if st != 200:
            res["note"] = f"HTTP {st}" if st else "network error"
            return res

        res["status"] = "valid"
        res["login"] = user.get("login") or user.get("name") or ""

        org = await get_org_guid(session, token, user)
        if not org:
            res["note"] = "org not found, limit unknown"
            return res

        st, pl, _ = await api_get(session, token, f"/organizations/{org}/plan_limits")
        if st == 429 and usage_exceeded(pl):
            res.update(remaining=0, note="monthly API requests exhausted")
            return res
        if st != 200:
            res["note"] = f"plan_limits HTTP {st}, limit unknown"
            return res

        limits = pl.get("plan_limits") or []
        res["all"] = limits
        main = pick_main(limits)
        if main:
            res["limit"] = int(main["limit"])
            res["count"] = int(main["count"])
            res["remaining"] = max(res["limit"] - res["count"], 0)
        else:
            res["note"] = "limit unknown (see all limits)"
        return res


# ---------------------------------------------------------------- helpers
def mask(t):
    return f"{t[:4]}...{t[-4:]}"


def extract_tokens(text):
    return TOKEN_RE.findall(text or "")


def chunk_lines(lines, limit=3500):
    """Split lines into groups that fit in one Telegram message."""
    chunks, cur, size = [], [], 0
    for line in lines:
        if cur and size + len(line) + 1 > limit:
            chunks.append(cur)
            cur, size = [], 0
        cur.append(line)
        size += len(line) + 1
    if cur:
        chunks.append(cur)
    return chunks


def short_num(n):
    """1000 -> 1k, 80000 -> 80k, 10 -> 10"""
    if n >= 1000 and n % 100 == 0:
        return f"{n / 1000:g}k"
    return str(n)


async def send_block(message, tokens, header=None, footer=None):
    """Tokens one per line in a code block (tap = copy all). Optional header above, footer below."""
    if not tokens:
        return
    parts = chunk_lines(tokens)
    for n, part in enumerate(parts, 1):
        text = f"{header}\n" if header else ""
        text += f"<pre>{chr(10).join(part)}</pre>"
        if footer and n == len(parts):
            text += f"\n{footer}"
        await message.reply_text(text, parse_mode=ParseMode.HTML)
        await asyncio.sleep(0.5)


async def safe_edit(msg, text):
    try:
        await msg.edit_text(text)
    except Exception:
        pass


def allowed(message):
    return message.from_user and (not ADMINS or message.from_user.id in ADMINS)


# ---------------------------------------------------------------- main flow
async def run_check(message, tokens):
    tokens = list(dict.fromkeys(tokens))  # dedupe, keep order
    status = await message.reply_text(f"🔍 {len(tokens)} tokens check ho rahe hain...")
    sem = asyncio.Semaphore(CONCURRENCY)
    done = 0

    async with aiohttp.ClientSession() as session:
        async def worker(t):
            nonlocal done
            r = await check_token(session, sem, t)
            done += 1
            if done % 10 == 0 and done != len(tokens):
                await safe_edit(status, f"🔍 Checking... {done}/{len(tokens)}")
            return r

        results = await asyncio.gather(*(worker(t) for t in tokens))

    valid = [r for r in results if r["status"] == "valid"]
    invalid = [r for r in results if r["status"] == "invalid"]
    errors = [r for r in results if r["status"] == "error"]
    with_limit = [r for r in valid if r["remaining"] and r["remaining"] > 0]
    exhausted = [r for r in valid if r["remaining"] == 0]
    unknown = [r for r in valid if r["remaining"] is None]
    total_left = sum(r["remaining"] or 0 for r in valid)

    exhausted_tokens = [r["token"] for r in exhausted]
    unknown_tokens = [r["token"] for r in unknown]
    error_tokens = [r["token"] for r in errors]
    invalid_tokens = [r["token"] for r in invalid]
    total_limit = sum(r["limit"] or 0 for r in valid)

    # tokens that still have limit, grouped by their total limit (1000, 10, 5 ...)
    groups = {}
    for r in valid:
        if r["remaining"] and r["remaining"] > 0:
            groups.setdefault(r["limit"], []).append(r)

    # short overview goes into the progress message
    summary = (
        f"✅ Check complete — {len(results)} tokens\n"
        f"🟢 Valid: {len(valid)} | 🔴 Invalid: {len(invalid)}\n"
        f"📊 Total limit: {short_num(total_limit)} | Bachi: {total_left}\n"
        f"⛔ Limit khatam: {len(exhausted)}"
    )
    if unknown:
        summary += f"\n❔ Limit unknown: {len(unknown)}"
    if errors:
        summary += f"\n🟡 Error (dobara check karo): {len(errors)}"
    await safe_edit(status, summary)

    # one message per limit group: tokens, then total limit below
    for limit in sorted(groups, reverse=True):
        grp = sorted(groups[limit], key=lambda r: r["remaining"], reverse=True)
        left = sum(r["remaining"] for r in grp)
        footer = (
            f"Total limit: {short_num(limit * len(grp))} — each token {short_num(limit)} limit\n"
            f"Bachi: {left}"
        )
        await send_block(message, [r["token"] for r in grp], footer=footer)

    await send_block(message, exhausted_tokens, header="⛔ Limit khatam 👇🏻")
    await send_block(message, unknown_tokens, header="❔ Valid, limit unknown 👇🏻")
    await send_block(message, error_tokens, header="🟡 Error, dobara check karo 👇🏻")
    await send_block(message, invalid_tokens, header="❌ Invalid token 👇🏻")


async def delayed_run(user_id):
    await asyncio.sleep(DEBOUNCE)
    buf = buffers.pop(user_id, None)
    if not buf or not buf["tokens"]:
        return
    try:
        await run_check(buf["msg"], buf["tokens"])
    except Exception as e:
        log.exception("check failed")
        await buf["msg"].reply_text(f"❌ Error: {e}")


def queue_tokens(message, tokens):
    uid = message.from_user.id
    buf = buffers.setdefault(uid, {"tokens": [], "task": None, "msg": message})
    buf["tokens"] += tokens
    buf["msg"] = message
    if buf["task"]:
        buf["task"].cancel()
    buf["task"] = asyncio.create_task(delayed_run(uid))


# ---------------------------------------------------------------- handlers
@app.on_message(filters.command("start") & filters.private)
async def start(_, message: Message):
    if not allowed(message):
        return
    await message.reply_text(
        "👋 Bitly Limit Checker\n\n"
        "Bitly tokens bhejo (har line me ek token) — text ya .txt file, 150-200 ek saath chalega.\n"
        "Reply text me milega: har limit group ka alag message (tokens line se, niche total limit + each token limit), last me invalid tokens.\n\n"
        "Debug: /raw <token> — us token ki saari plan limits dikhata hai."
    )


@app.on_message(filters.command("raw") & filters.private)
async def raw(_, message: Message):
    if not allowed(message):
        return
    toks = extract_tokens(" ".join(message.command[1:]))
    if not toks:
        return await message.reply_text("Usage: /raw <token>")
    async with aiohttp.ClientSession() as session:
        r = await check_token(session, asyncio.Semaphore(1), toks[0])
    out = json.dumps({k: v for k, v in r.items() if k != "token"}, indent=2)
    await message.reply_text(f"<pre>{out[:3800]}</pre>", parse_mode=ParseMode.HTML)


@app.on_message(filters.private & (filters.text | filters.document) & ~filters.command(["start", "raw"]))
async def on_tokens(_, message: Message):
    if not allowed(message):
        return
    text = message.text or ""
    if message.document:
        name = (message.document.file_name or "").lower()
        if not name.endswith((".txt", ".csv", ".log")):
            return await message.reply_text("Sirf .txt file bhejo.")
        bio = await message.download(in_memory=True)
        text = bio.getvalue().decode("utf-8", "ignore")
    tokens = extract_tokens(text)
    if not tokens:
        return await message.reply_text("Koi token nahi mila. Har line me ek token bhejo.")
    queue_tokens(message, tokens)


if __name__ == "__main__":
    log.info("Bitly limit checker started")
    app.run()
