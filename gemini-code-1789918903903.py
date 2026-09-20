import os
import sys
import re
import html
import time
import logging
from datetime import datetime, timezone, timedelta
from functools import partial
from pathlib import Path

import aiosqlite
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.functions.users import GetFullUserRequest
from telethon.errors import AuthKeyDuplicatedError

from telegram import Update, ChatPermissions, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)
from telegram.enums import ChatMemberStatus, ChatType, ParseMode
from telegram.error import TelegramError

# Logging Configuration
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("bot")

load_dotenv()

def get_env_int(key: str, default: int | None = None, required: bool = False) -> int:
    val = os.getenv(key)
    if val is None or val.strip() == "":
        if required:
            logger.critical(f"Missing required environment variable: {key}")
            sys.exit(1)
        return default  # type: ignore
    try:
        return int(val)
    except ValueError:
        logger.critical(f"Environment variable {key} must be an integer.")
        sys.exit(1)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.critical("Missing required environment variable: BOT_TOKEN")
    sys.exit(1)

API_ID = get_env_int("API_ID", required=True)

API_HASH = os.getenv("API_HASH")
if not API_HASH:
    logger.critical("Missing required environment variable: API_HASH")
    sys.exit(1)

BOT_OWNER_ID = get_env_int("BOT_OWNER_ID", required=True)

DATABASE_PATH = os.getenv("DATABASE_PATH", "data/moderation.sqlite3")
MTPROTO_SESSION_PATH = os.getenv("MTPROTO_SESSION_PATH", "data/telegram_bot_mtproto")
USER_FULL_CACHE_TTL_SECONDS = get_env_int("USER_FULL_CACHE_TTL_SECONDS", 300)
PERMISSION_NOTICE_COOLDOWN_SECONDS = get_env_int("PERMISSION_NOTICE_COOLDOWN_SECONDS", 3600)

Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)
Path(MTPROTO_SESSION_PATH).parent.mkdir(parents=True, exist_ok=True)

# Helper Functions
DURATION_REGEX = re.compile(r"^(\d+)\s*([sSmMhHdD])$")
URL_REGEX = re.compile(r"(https?://|www\.|t\.me/|telegram\.me/)", re.IGNORECASE)
PERMISSION_NOTICE_COOLDOWN: dict[int, float] = {}

def parse_duration(text: str) -> int | None:
    match = DURATION_REGEX.match(text.strip())
    if not match:
        return None
    amount, unit = int(match.group(1)), match.group(2).lower()
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    seconds = amount * mult.get(unit, 0)
    return seconds if 30 <= seconds <= 2_592_000 else None

def format_duration(seconds: int) -> str:
    if seconds % 86400 == 0: return f"{seconds // 86400}d"
    if seconds % 3600 == 0: return f"{seconds // 3600}h"
    if seconds % 60 == 0: return f"{seconds // 60}m"
    return f"{seconds}s"

def make_html_mention(user_id: int, name: str) -> str:
    return f'<a href="tg://user?id={user_id}">{html.escape(name)}</a>'

async def check_bot_permissions(bot: Bot, chat_id: int) -> tuple[bool, bool]:
    try:
        member = await bot.get_chat_member(chat_id, bot.id)
        if member.status != ChatMemberStatus.ADMINISTRATOR:
            return False, False
        return getattr(member, "can_delete_messages", False), getattr(member, "can_restrict_members", False)
    except Exception:
        return False, False

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

# Database Handler
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init_db(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA foreign_keys = ON;")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS groups (
                    chat_id INTEGER PRIMARY KEY, title TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                    punishment TEXT NOT NULL DEFAULT 'ban', timeout_seconds INTEGER NOT NULL DEFAULT 86400,
                    log_chat_id INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS whitelist (
                    chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY(chat_id, user_id)
                );
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_warnings (
                    chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL, warning_count INTEGER NOT NULL DEFAULT 0,
                    last_reason TEXT, updated_at TEXT NOT NULL, PRIMARY KEY(chat_id, user_id)
                );
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS moderation_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                    action TEXT NOT NULL, created_at TEXT NOT NULL
                );
            """)
            await db.commit()

    async def upsert_group(self, chat_id: int, title: str) -> None:
        now = utc_now()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO groups (chat_id, title, enabled, punishment, timeout_seconds, created_at, updated_at)
                VALUES (?, ?, 1, 'ban', 86400, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title, updated_at = excluded.updated_at
            """, (chat_id, title, now, now))
            await db.commit()

    async def get_group(self, chat_id: int) -> dict | None:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM groups WHERE chat_id = ?", (chat_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_all_groups(self) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM groups") as cursor:
                return [dict(r) for r in await cursor.fetchall()]

    async def update_group_enabled(self, chat_id: int, enabled: bool) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE groups SET enabled = ?, updated_at = ? WHERE chat_id = ?", (1 if enabled else 0, utc_now(), chat_id))
            await db.commit()

    async def update_group_mode(self, chat_id: int, mode: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE groups SET punishment = ?, updated_at = ? WHERE chat_id = ?", (mode, utc_now(), chat_id))
            await db.commit()

    async def update_group_timeout(self, chat_id: int, timeout_seconds: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE groups SET timeout_seconds = ?, updated_at = ? WHERE chat_id = ?", (timeout_seconds, utc_now(), chat_id))
            await db.commit()

    async def update_group_log_chat(self, chat_id: int, log_chat_id: int | None) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE groups SET log_chat_id = ?, updated_at = ? WHERE chat_id = ?", (log_chat_id, utc_now(), chat_id))
            await db.commit()

    async def add_whitelist(self, chat_id: int, user_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT OR IGNORE INTO whitelist (chat_id, user_id, created_at) VALUES (?, ?, ?)", (chat_id, user_id, utc_now()))
            await db.commit()

    async def remove_whitelist(self, chat_id: int, user_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM whitelist WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
            await db.commit()

    async def is_whitelisted(self, chat_id: int, user_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT 1 FROM whitelist WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)) as cursor:
                return (await cursor.fetchone()) is not None

    async def get_whitelist(self, chat_id: int) -> list[int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM whitelist WHERE chat_id = ?", (chat_id,)) as cursor:
                return [r[0] for r in await cursor.fetchall()]

    async def get_warnings(self, chat_id: int, user_id: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT warning_count FROM user_warnings WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def increment_warnings(self, chat_id: int, user_id: int, reason: str) -> int:
        now = utc_now()
        async with aiosqlite.connect(self.db_path) as db:
            current = await self.get_warnings(chat_id, user_id)
            new_count = current + 1
            await db.execute("""
                INSERT INTO user_warnings (chat_id, user_id, warning_count, last_reason, updated_at)
                VALUES (?, ?, ?, ?, ?) ON CONFLICT(chat_id, user_id) DO UPDATE SET
                warning_count = excluded.warning_count, last_reason = excluded.last_reason, updated_at = excluded.updated_at
            """, (chat_id, user_id, new_count, reason, now))
            await db.commit()
            return new_count

    async def clear_warnings(self, chat_id: int, user_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM user_warnings WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
            await db.commit()

    async def record_moderation_log(self, chat_id: int, user_id: int, action: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO moderation_logs (chat_id, user_id, action, created_at) VALUES (?, ?, ?, ?)", (chat_id, user_id, action, utc_now()))
            await db.commit()

# Telethon Inspection Service
class TelethonService:
    def __init__(self, session_path: str, api_id: int, api_hash: str, bot_token: str, ttl_seconds: int = 300):
        self.session_path = session_path
        self.api_id = api_id
        self.api_hash = api_hash
        self.bot_token = bot_token
        self.ttl = ttl_seconds
        self.client: TelegramClient | None = None
        self._cache: dict[int, tuple[float, tuple[bool, str | None]]] = {}

    async def start(self) -> None:
        self.client = TelegramClient(self.session_path, self.api_id, self.api_hash)
        try:
            await self.client.start(bot_token=self.bot_token)
        except AuthKeyDuplicatedError:
            sess_file = f"{self.session_path}.session"
            if os.path.exists(sess_file):
                os.rename(sess_file, f"{self.session_path}_{int(time.time())}.session.bak")
            self.client = TelegramClient(self.session_path, self.api_id, self.api_hash)
            await self.client.start(bot_token=self.bot_token)

    async def stop(self) -> None:
        if self.client and self.client.is_connected():
            await self.client.disconnect()

    async def inspect_user(self, user_id: int) -> tuple[bool, str | None]:
        now = time.time()
        if user_id in self._cache:
            cached_time, res = self._cache[user_id]
            if now - cached_time < self.ttl:
                return res

        if not self.client or not self.client.is_connected():
            return False, None

        try:
            full = (await self.client(GetFullUserRequest(user_id))).full_user
            has_channel = getattr(full, "personal_channel_id", None) is not None
            has_bio_link = bool(URL_REGEX.search(getattr(full, "about", "") or ""))

            if has_channel and has_bio_link: result = (True, "both")
            elif has_channel: result = (True, "personal_channel")
            elif has_bio_link: result = (True, "bio_link")
            else: result = (False, None)

            self._cache[user_id] = (now, result)
            return result
        except Exception:
            return False, None

# Command Handlers
async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat, user = update.effective_chat, update.effective_user
    if not chat or not user: return False
    if chat.type == ChatType.PRIVATE: return True
    try:
        m = await chat.get_member(user.id)
        return m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception: return False

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database, owner_id: int):
    chat, user = update.effective_chat, update.effective_user
    if chat.type == ChatType.PRIVATE:
        if user.id == owner_id:
            await update.message.reply_text("👑 <b>Owner Commands:</b>\n/groups - Active Groups\n/broadcast <text> - Send announcement", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text("👋 Add me to your group as an admin to activate moderation.")
    else:
        await update.message.reply_text("🛡️ Moderation Bot Active. Use /status to view settings.", parse_mode=ParseMode.HTML)

async def cmd_enable(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database):
    if not await is_admin(update, context): return
    await db.upsert_group(update.effective_chat.id, update.effective_chat.title or "Group")
    await db.update_group_enabled(update.effective_chat.id, True)
    await update.message.reply_text("✅ Moderation protection enabled.")

async def cmd_disable(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database):
    if not await is_admin(update, context): return
    await db.upsert_group(update.effective_chat.id, update.effective_chat.title or "Group")
    await db.update_group_enabled(update.effective_chat.id, False)
    await update.message.reply_text("⚠️ Moderation protection disabled.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database):
    if not await is_admin(update, context): return
    chat = update.effective_chat
    group = await db.get_group(chat.id) or {}
    can_del, can_res = await check_bot_permissions(context.bot, chat.id)
    wl = await db.get_whitelist(chat.id)

    msg = (
        f"🛡️ <b>Status:</b> {chat.title}\n\n"
        f"• Protection: {'Enabled' if group.get('enabled') else 'Disabled'}\n"
        f"• Punishment: {group.get('punishment', 'ban').upper()}\n"
        f"• Timeout: {format_duration(group.get('timeout_seconds', 86400))}\n"
        f"• Delete Permission: {'✅' if can_del else '❌'}\n"
        f"• Ban/Mute Permission: {'✅' if can_res else '❌'}\n"
        f"• Whitelisted Users: {len(wl)}\n"
        f"• Log Chat: <code>{group.get('log_chat_id') or 'Not Set'}</code>"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database):
    if not await is_admin(update, context): return
    if not context.args or context.args[0].lower() not in ("ban", "mute"):
        await update.message.reply_text("Usage: /mode ban OR /mode mute")
        return
    mode = context.args[0].lower()
    await db.upsert_group(update.effective_chat.id, update.effective_chat.title or "Group")
    await db.update_group_mode(update.effective_chat.id, mode)
    await update.message.reply_text(f"⚙️ Mode set to <b>{mode.upper()}</b>.", parse_mode=ParseMode.HTML)

async def cmd_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database):
    if not await is_admin(update, context): return
    if not context.args or (sec := parse_duration(context.args[0])) is None:
        await update.message.reply_text("Usage: /timeout <30m|1h|24h|7d>")
        return
    await db.upsert_group(update.effective_chat.id, update.effective_chat.title or "Group")
    await db.update_group_timeout(update.effective_chat.id, sec)
    await update.message.reply_text(f"⏱️ Mute duration set to <b>{format_duration(sec)}</b>.", parse_mode=ParseMode.HTML)

async def cmd_whitelist(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database):
    if not await is_admin(update, context): return
    chat = update.effective_chat
    target = update.message.reply_to_message.from_user.id if update.message.reply_to_message else (int(context.args[0]) if context.args and context.args[0].isdigit() else None)
    if target:
        await db.add_whitelist(chat.id, target)
        await update.message.reply_text(f"✅ Whitelisted <code>{target}</code>.", parse_mode=ParseMode.HTML)
    else:
        wl = await db.get_whitelist(chat.id)
        await update.message.reply_text("📄 <b>Whitelist:</b>\n" + "\n".join([f"• <code>{u}</code>" for u in wl]) if wl else "No whitelisted users.", parse_mode=ParseMode.HTML)

async def cmd_unwhitelist(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database):
    if not await is_admin(update, context): return
    target = update.message.reply_to_message.from_user.id if update.message.reply_to_message else (int(context.args[0]) if context.args and context.args[0].isdigit() else None)
    if target:
        await db.remove_whitelist(update.effective_chat.id, target)
        await update.message.reply_text(f"🗑️ Removed <code>{target}</code> from whitelist.", parse_mode=ParseMode.HTML)

async def cmd_logchat(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database):
    if not await is_admin(update, context): return
    cid = int(context.args[0]) if context.args and context.args[0].lstrip("-").isdigit() else update.effective_chat.id
    await db.upsert_group(update.effective_chat.id, update.effective_chat.title or "Group")
    await db.update_group_log_chat(update.effective_chat.id, cid)
    await update.message.reply_text(f"📋 Log chat set to <code>{cid}</code>.", parse_mode=ParseMode.HTML)

# Owner Commands
async def cmd_groups(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database, owner_id: int):
    if update.effective_user.id != owner_id: return
    groups, active = await db.get_all_groups(), []
    for g in groups:
        try:
            m = await context.bot.get_chat_member(g["chat_id"], context.bot.id)
            if m.status == ChatMemberStatus.ADMINISTRATOR: active.append(g)
        except Exception: continue
    msg = f"📊 <b>Active Groups ({len(active)}):</b>\n\n" + "\n".join([f"• <b>{g['title']}</b> (<code>{g['chat_id']}</code>)" for g in active])
    await update.message.reply_text(msg if active else "No active groups.", parse_mode=ParseMode.HTML)

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database, owner_id: int):
    if update.effective_user.id != owner_id: return
    text = (update.message.reply_to_message.text or update.message.reply_to_message.caption) if update.message.reply_to_message else " ".join(context.args)
    if not text: return
    groups, s, f = await db.get_all_groups(), 0, 0
    for g in groups:
        try:
            await context.bot.send_message(g["chat_id"], text)
            s += 1
        except Exception: f += 1
    await update.message.reply_text(f"📢 <b>Broadcast Complete</b>\nSuccess: {s} | Failed: {f}", parse_mode=ParseMode.HTML)

# Chat Member & Moderation Processing
async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database):
    if update.my_chat_member and update.my_chat_member.new_chat_member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER):
        await db.upsert_group(update.my_chat_member.chat.id, update.my_chat_member.chat.title or "Group")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE, db: Database, telethon: TelethonService, cooldown_sec: int):
    msg, chat, user = update.effective_message, update.effective_chat, update.effective_user
    if not msg or not chat or not user or chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP) or user.is_bot:
        return

    group = await db.get_group(chat.id)
    if not group:
        await db.upsert_group(chat.id, chat.title or "Group")
        group = await db.get_group(chat.id)

    if not group["enabled"]: return

    try:
        m = await chat.get_member(user.id)
        if m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER): return
    except Exception: return

    if await db.is_whitelisted(chat.id, user.id): return

    is_flagged, reason_code = await telethon.inspect_user(user.id)
    if not is_flagged or not reason_code: return

    can_del, can_res = await check_bot_permissions(context.bot, chat.id)
    if not can_del or not can_res:
        now = time.time()
        if now - PERMISSION_NOTICE_COOLDOWN.get(chat.id, 0.0) > cooldown_sec:
            PERMISSION_NOTICE_COOLDOWN[chat.id] = now
            await chat.send_message("⚠️ <b>Admin Permissions Missing!</b> Need Delete & Ban/Mute permissions.", parse_mode=ParseMode.HTML)
        return

    user_mention = make_html_mention(user.id, user.first_name)
    current_warns = await db.get_warnings(chat.id, user.id)
    new_warns = current_warns + 1

    reason_text = "Please remove your personal channel from your profile." if reason_code == "personal_channel" else \
                  ("Please remove the link from your bio." if reason_code == "bio_link" else "Please remove the personal channel and link from your profile.")

    if new_warns < 3:
        try: await msg.delete()
        except Exception: return
        await db.increment_warnings(chat.id, user.id, reason_code)
        await chat.send_message(f"⚠️ Warning {new_warns}/3\n👤 {user_mention}\n{reason_text}", parse_mode=ParseMode.HTML)
    else:
        try: await msg.delete()
        except Exception: pass

        punishment = group["punishment"]
        success, action_label, notif = False, "", ""

        if punishment == "ban":
            try:
                await chat.ban_member(user.id)
                success, action_label, notif = True, "ban", f"🚫 {user_mention} was banned."
            except Exception: await db.record_moderation_log(chat.id, user.id, "ban_failed")
        elif punishment == "mute":
            sec = group["timeout_seconds"]
            try:
                await chat.restrict_member(user.id, permissions=ChatPermissions(can_send_messages=False), until_date=datetime.now(timezone.utc)+timedelta(seconds=sec))
                success, action_label, notif = True, f"mute_{sec}s", f"🔇 {user_mention} was muted."
            except Exception: await db.record_moderation_log(chat.id, user.id, "mute_failed")

        if success:
            await db.clear_warnings(chat.id, user.id)
            await db.record_moderation_log(chat.id, user.id, action_label)
            await chat.send_message(notif, parse_mode=ParseMode.HTML)
            if group["log_chat_id"]:
                try:
                    await context.bot.send_message(group["log_chat_id"], f"🛡️ <b>Action:</b> {action_label.upper()}\nUser: {user_mention}\nGroup: {chat.title}", parse_mode=ParseMode.HTML)
                except Exception: pass

# App Lifecycle
async def post_init(app: Application) -> None:
    await app.bot_data["db"].init_db()
    await app.bot_data["telethon"].start()

async def post_shutdown(app: Application) -> None:
    await app.bot_data["telethon"].stop()

def main() -> None:
    db = Database(DATABASE_PATH)
    telethon = TelethonService(MTPROTO_SESSION_PATH, API_ID, API_HASH, BOT_TOKEN, USER_FULL_CACHE_TTL_SECONDS)

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    app.bot_data["db"], app.bot_data["telethon"] = db, telethon

    # Register Command Handlers
    app.add_handler(CommandHandler("start", partial(cmd_start, db=db, owner_id=BOT_OWNER_ID)))
    app.add_handler(CommandHandler("enable", partial(cmd_enable, db=db)))
    app.add_handler(CommandHandler("disable", partial(cmd_disable, db=db)))
    app.add_handler(CommandHandler("status", partial(cmd_status, db=db)))
    app.add_handler(CommandHandler("mode", partial(cmd_mode, db=db)))
    app.add_handler(CommandHandler("timeout", partial(cmd_timeout, db=db)))
    app.add_handler(CommandHandler("whitelist", partial(cmd_whitelist, db=db)))
    app.add_handler(CommandHandler("unwhitelist", partial(cmd_unwhitelist, db=db)))
    app.add_handler(CommandHandler("logchat", partial(cmd_logchat, db=db)))

    # Owner Command Handlers
    app.add_handler(CommandHandler("groups", partial(cmd_groups, db=db, owner_id=BOT_OWNER_ID)))
    app.add_handler(CommandHandler("broadcast", partial(cmd_broadcast, db=db, owner_id=BOT_OWNER_ID)))

    # Chat Member & Message Handlers
    app.add_handler(ChatMemberHandler(partial(handle_my_chat_member, db=db), ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, partial(handle_message, db=db, telethon=telethon, cooldown_sec=PERMISSION_NOTICE_COOLDOWN_SECONDS)))

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()