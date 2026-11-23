import asyncio
import io
import json
import logging
import os
import re

import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bitly-checker")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
# Optional: comma/space separated Telegram user IDs. Empty = everyone can use the bot.
ADMINS = {int(x) for x in re.split(r"[,\s]+", os.environ.get("ADMINS", "")) if x.lstrip("-").isdigit()}
CONCURRENCY = int(os.environ.get("CONCURRENCY", "5"))
DEBOUNCE = 4  # seconds to wait for more messages before starting the check

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
    for key in ("short", "encode", "link"):
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


def as_file(text, name):
    bio = io.BytesIO(text.encode("utf-8"))
    bio.name = name
    return bio


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

    lines = []
    for i, r in enumerate(results, 1):
        if r["status"] == "valid":
            if r["remaining"] is not None and r["limit"] is not None:
                info = f"used {r['count']}/{r['limit']} | left {r['remaining']}"
            elif r["remaining"] == 0:
                info = "left 0"
            else:
                info = "left ?"
            extra = " | all: " + ", ".join(
                f"{x.get('name')} {x.get('count')}/{x.get('limit')}" for x in r["all"]
            ) if r["all"] else ""
            lines.append(f"{i}. {mask(r['token'])} | VALID | {r['login']} | {info} | {r['note']}{extra}")
        elif r["status"] == "invalid":
            lines.append(f"{i}. {mask(r['token'])} | INVALID (removed)")
        else:
            lines.append(f"{i}. {mask(r['token'])} | ERROR (kept, retry) | {r['note']}")

    summary = (
        f"✅ Check complete — {len(results)} tokens\n\n"
        f"🟢 Valid: {len(valid)}\n"
        f"🔴 Invalid (removed): {len(invalid)}\n"
        f"🟡 Error (kept, dobara check karo): {len(errors)}\n\n"
        f"📊 Limit bacha hai: {len(with_limit)}\n"
        f"⛔ Limit khatam: {len(exhausted)}\n"
        f"❔ Limit unknown: {len(unknown)}\n"
        f"➕ Total remaining: {total_left}"
    )
    await safe_edit(status, summary)

    cleaned = [r["token"] for r in results if r["status"] != "invalid"]
    await message.reply_document(as_file("\n".join(cleaned), "cleaned_tokens.txt"),
                                 caption="Invalid hata diye (error wale rakhe hain)")
    if with_limit:
        with_limit.sort(key=lambda r: r["remaining"], reverse=True)
        await message.reply_document(as_file("\n".join(r["token"] for r in with_limit), "with_limit_tokens.txt"),
                                     caption="Sirf wo jinme limit bachi hai (zyada se kam)")
    await message.reply_document(as_file("\n".join(lines), "report.txt"), caption="Full report")


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
        "Main har token check karke bataunga kitni limit bachi hai, invalid hata dunga aur cleaned list file me dunga.\n\n"
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
    await message.reply_text(f"<pre>{out[:3800]}</pre>", parse_mode=__import__("pyrogram").enums.ParseMode.HTML)


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
