"""
Nafezly Telegram Bot — v3.0
Single-port architecture: Starlette + uvicorn + python-telegram-bot webhook
Author: refactored by Claude for 24/7 stability on Render
"""

import asyncio
import logging
import os
import platform
import secrets
import sys
import time
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime, timezone
from typing import Generator

import httpx
import psycopg2
import psycopg2.pool
import uvicorn
from bs4 import BeautifulSoup
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from telegram import Update
from telegram.error import TelegramError, TimedOut, NetworkError, RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ==========================
# Logging
# ==========================

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# ==========================
# Configuration
# ==========================

BOT_TOKEN: str = os.environ["BOT_TOKEN"]
DATABASE_URL: str = os.environ["DATABASE_URL"]
OWNER_ID: int = 2121957939
WEBHOOK_URL: str = os.environ.get(
    "WEBHOOK_URL", "https://nafezly-bot-5vdr.onrender.com"
).rstrip("/")
PORT: int = int(os.environ.get("PORT", 10000))
SECRET_TOKEN: str = os.environ.get("SECRET_TOKEN", secrets.token_hex(32))

# Webhook path — a secret token makes it unguessable
WEBHOOK_PATH: str = f"/{BOT_TOKEN}"

NAFEZLY_URL: str = "https://nafezly.com/projects"
CHECK_INTERVAL: int = 30
RETRY_DELAY: int = 120
MAX_SEND_CONCURRENCY: int = 10
KEEP_ALIVE_INTERVAL: int = 300   # 5 minutes
BOT_VERSION: str = "3.0.0"

# ==========================
# Runtime state
# ==========================

_start_time: datetime = datetime.now(timezone.utc)
_monitor_task: asyncio.Task | None = None
_keepalive_task: asyncio.Task | None = None
_task_manager_task: asyncio.Task | None = None

# Global PTB Application — set during startup
_ptb_app: Application | None = None

# ==========================
# Database — thread-safe pool
# ==========================

_db_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _db_pool
    if _db_pool is None:
        _db_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DATABASE_URL,
        )
        logger.info("PostgreSQL connection pool created.")
    return _db_pool


@contextmanager
def db_cursor() -> Generator[psycopg2.extensions.cursor, None, None]:
    pool = get_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            pool.putconn(conn)
        except Exception:
            pass


def _db_operation_with_retry(fn, *args, max_retries: int = 3, **kwargs):
    """Retry a DB callable up to max_retries times on transient errors."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            logger.warning(
                "DB transient error (attempt %d/%d): %s", attempt, max_retries, exc
            )
            time.sleep(2 ** attempt)
        except Exception as exc:
            raise exc
    raise last_exc


def init_db() -> None:
    with db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id    BIGINT PRIMARY KEY,
                username   TEXT,
                first_name TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sent_projects (
                link TEXT PRIMARY KEY
            )
        """)
    logger.info("Database tables verified/created.")


# ==========================
# Subscriber helpers
# ==========================

def db_load_subscribers() -> list[dict]:
    with db_cursor() as cur:
        cur.execute("SELECT chat_id, username, first_name FROM subscribers")
        return [
            {"chat_id": r[0], "username": r[1], "first_name": r[2]}
            for r in cur.fetchall()
        ]


def db_add_subscriber(
    chat_id: int, username: str | None, first_name: str | None
) -> bool:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO subscribers (chat_id, username, first_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (chat_id) DO NOTHING
            """,
            (chat_id, username, first_name),
        )
        return cur.rowcount == 1


def db_remove_subscriber(chat_id: int) -> None:
    with db_cursor() as cur:
        cur.execute("DELETE FROM subscribers WHERE chat_id = %s", (chat_id,))


def db_load_sent_projects() -> set[str]:
    with db_cursor() as cur:
        cur.execute("SELECT link FROM sent_projects")
        return {row[0] for row in cur.fetchall()}


def db_add_sent_project(link: str) -> None:
    with db_cursor() as cur:
        cur.execute(
            "INSERT INTO sent_projects (link) VALUES (%s) ON CONFLICT (link) DO NOTHING",
            (link,),
        )


def db_count_sent_projects() -> int:
    with db_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sent_projects")
        return cur.fetchone()[0]


def db_check_connection() -> bool:
    try:
        with db_cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False


# ==========================
# HTTP client
# ==========================

_http_client: httpx.AsyncClient | None = None


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=5.0),
            follow_redirects=True,
        )
        logger.info("HTTP client created.")
    return _http_client


async def close_http_client() -> None:
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        logger.info("HTTP client closed.")


async def http_get_with_retry(
    url: str,
    max_retries: int = 3,
    backoff: float = 5.0,
) -> httpx.Response:
    """GET with exponential back-off. Raises on final failure."""
    client = get_http_client()
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response
        except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError) as exc:
            last_exc = exc
            wait = backoff * (2 ** (attempt - 1))
            logger.warning(
                "HTTP attempt %d/%d for %s failed: %s — retrying in %.0fs",
                attempt, max_retries, url, exc, wait,
            )
            if attempt < max_retries:
                await asyncio.sleep(wait)
    raise last_exc  # type: ignore[misc]


# ==========================
# Project scraper
# ==========================

def _scrape_project_links(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for tag in soup.select("a[href*='/project/']"):
        href = tag["href"]
        title = tag.get_text(strip=True)
        results.append((title, href))
    return results


async def _seed_sent_projects(client: httpx.AsyncClient) -> set[str]:
    logger.info("sent_projects is empty — seeding from current listings.")
    try:
        response = await http_get_with_retry(NAFEZLY_URL)
        projects = _scrape_project_links(response.text)
        links = {link for _, link in projects}
        loop = asyncio.get_running_loop()
        for link in links:
            await loop.run_in_executor(None, db_add_sent_project, link)
        logger.info("Seeded %d existing project links.", len(links))
        return links
    except Exception:
        logger.exception("Failed to seed sent_projects; will retry next cycle.")
        return set()


# ==========================
# Telegram — safe send
# ==========================

async def safe_send_message(
    app: Application,
    chat_id: int,
    text: str,
    max_retries: int = 3,
) -> bool:
    """Send with timeout protection and retry. Returns True on success."""
    for attempt in range(1, max_retries + 1):
        try:
            await asyncio.wait_for(
                app.bot.send_message(chat_id=chat_id, text=text),
                timeout=15.0,
            )
            return True
        except RetryAfter as exc:
            logger.warning(
                "Rate-limited for chat_id=%s; waiting %.1fs", chat_id, exc.retry_after
            )
            await asyncio.sleep(exc.retry_after)
        except (TimedOut, NetworkError) as exc:
            wait = 5.0 * attempt
            logger.warning(
                "Network/timeout error sending to %s (attempt %d): %s — retrying in %.0fs",
                chat_id, attempt, exc, wait,
            )
            if attempt < max_retries:
                await asyncio.sleep(wait)
        except TelegramError as exc:
            logger.warning(
                "TelegramError for chat_id=%s: %s — not retrying.", chat_id, exc
            )
            return False
        except asyncio.TimeoutError:
            logger.warning(
                "asyncio timeout sending to chat_id=%s (attempt %d).", chat_id, attempt
            )
        except Exception:
            logger.exception("Unexpected error sending to chat_id=%s.", chat_id)
            return False
    return False


async def _send_to_subscribers(
    app: Application,
    message: str,
    subscribers: list[dict],
) -> None:
    semaphore = asyncio.Semaphore(MAX_SEND_CONCURRENCY)

    async def _send(user: dict) -> None:
        async with semaphore:
            success = await safe_send_message(app, user["chat_id"], message)
            if not success:
                logger.warning("Gave up sending to chat_id=%s.", user["chat_id"])

    await asyncio.gather(*(_send(u) for u in subscribers))


# ==========================
# Project monitor task
# ==========================

async def check_projects(app: Application) -> None:
    logger.info("Project monitor started.")
    loop = asyncio.get_running_loop()

    try:
        sent_projects: set[str] = await loop.run_in_executor(
            None, _db_operation_with_retry, db_load_sent_projects
        )
    except Exception:
        logger.exception("Could not load sent_projects from DB. Starting fresh.")
        sent_projects = set()

    if not sent_projects:
        sent_projects = await _seed_sent_projects(get_http_client())

    while True:
        try:
            response = await http_get_with_retry(NAFEZLY_URL)
            projects = _scrape_project_links(response.text)
            new_projects = [(t, l) for t, l in projects if l not in sent_projects]

            if new_projects:
                try:
                    subscribers: list[dict] = await loop.run_in_executor(
                        None, _db_operation_with_retry, db_load_subscribers
                    )
                except Exception:
                    logger.exception("Failed to load subscribers; skipping notification.")
                    subscribers = []

                for title, link in new_projects:
                    sent_projects.add(link)
                    try:
                        await loop.run_in_executor(None, db_add_sent_project, link)
                    except Exception:
                        logger.exception(
                            "Failed to persist sent project link: %s", link
                        )

                    message = (
                        "🚨 مشروع جديد على نفذلي\n\n"
                        f"📌 العنوان:\n{title}\n\n"
                        f"🔗 رابط المشروع:\n{link}"
                    )
                    logger.info("New project detected: %s", link)
                    await _send_to_subscribers(app, message, subscribers)

            await asyncio.sleep(CHECK_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Project monitor task cancelled — shutting down.")
            raise

        except (httpx.TimeoutException, httpx.RequestError) as exc:
            logger.warning(
                "HTTP error in monitor: %s — retrying in %ds.", exc, RETRY_DELAY
            )
            await asyncio.sleep(RETRY_DELAY)

        except Exception:
            logger.exception(
                "Unexpected error in project monitor — retrying in %ds.", RETRY_DELAY
            )
            await asyncio.sleep(RETRY_DELAY)


# ==========================
# Keep-alive task
# ==========================

async def keep_alive_loop() -> None:
    logger.info("Keep-alive task started.")
    client = get_http_client()
    while True:
        try:
            await asyncio.sleep(KEEP_ALIVE_INTERVAL)
            response = await asyncio.wait_for(
                client.get(f"{WEBHOOK_URL}/health"), timeout=15.0
            )
            logger.info(
                "Keep-alive ping → %s %s", response.status_code, WEBHOOK_URL
            )
        except asyncio.CancelledError:
            logger.info("Keep-alive task cancelled.")
            raise
        except Exception as exc:
            logger.warning("Keep-alive ping failed: %s", exc)


# ==========================
# Task manager — restart crashed tasks
# ==========================

async def task_manager(app: Application) -> None:
    """Watchdog: restarts monitor and keep-alive if they crash."""
    global _monitor_task, _keepalive_task
    logger.info("Task manager started.")

    _monitor_task = asyncio.create_task(
        check_projects(app), name="project_monitor"
    )
    _keepalive_task = asyncio.create_task(keep_alive_loop(), name="keep_alive")

    while True:
        try:
            await asyncio.sleep(10)

            if _monitor_task.done() and not _monitor_task.cancelled():
                exc = _monitor_task.exception() if not _monitor_task.cancelled() else None
                if exc:
                    logger.critical(
                        "project_monitor crashed: %s — restarting.", exc
                    )
                else:
                    logger.warning("project_monitor exited cleanly — restarting.")
                _monitor_task = asyncio.create_task(
                    check_projects(app), name="project_monitor"
                )
                logger.info("project_monitor restarted.")

            if _keepalive_task.done() and not _keepalive_task.cancelled():
                exc = (
                    _keepalive_task.exception()
                    if not _keepalive_task.cancelled()
                    else None
                )
                if exc:
                    logger.critical("keep_alive crashed: %s — restarting.", exc)
                else:
                    logger.warning("keep_alive exited cleanly — restarting.")
                _keepalive_task = asyncio.create_task(
                    keep_alive_loop(), name="keep_alive"
                )
                logger.info("keep_alive restarted.")

        except asyncio.CancelledError:
            logger.info("Task manager cancelled — shutting down.")
            raise
        except Exception:
            logger.exception("Unexpected error in task manager.")
            await asyncio.sleep(10)


# ==========================
# Helpers
# ==========================

def _uptime_str() -> str:
    delta = datetime.now(timezone.utc) - _start_time
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"


def _task_status(task: asyncio.Task | None) -> str:
    if task is None:
        return "❌ Not started"
    if task.done():
        return "🔴 Stopped"
    return "🟢 Running"


# ==========================
# Command handlers
# ==========================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name

    loop = asyncio.get_running_loop()
    try:
        inserted = await loop.run_in_executor(
            None,
            _db_operation_with_retry,
            db_add_subscriber,
            chat_id,
            username,
            first_name,
        )
    except Exception:
        logger.exception("DB error in /start for chat_id=%s", chat_id)
        await update.message.reply_text("⚠️ حدث خطأ. يرجى المحاولة لاحقاً.")
        return

    if inserted:
        await update.message.reply_text(
            "✅ تم الاشتراك بنجاح.\nستصلك إشعارات المشاريع الجديدة من نفذلي."
        )
    else:
        await update.message.reply_text("✅ أنت مشترك بالفعل.")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, db_remove_subscriber, chat_id)
    except Exception:
        logger.exception("DB error in /stop for chat_id=%s", chat_id)
    await update.message.reply_text("❌ تم إلغاء الاشتراك.")


async def cmd_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    loop = asyncio.get_running_loop()
    try:
        users = await loop.run_in_executor(None, db_load_subscribers)
        await update.message.reply_text(f"👥 عدد المشتركين الحالي: {len(users)}")
    except Exception:
        logger.exception("DB error in /count")
        await update.message.reply_text("⚠️ تعذّر جلب العدد حالياً.")


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != OWNER_ID:
        return

    loop = asyncio.get_running_loop()
    try:
        users = await loop.run_in_executor(None, db_load_subscribers)
    except Exception:
        logger.exception("DB error in /users")
        await update.message.reply_text("⚠️ تعذّر جلب القائمة.")
        return

    if not users:
        await update.message.reply_text("لا يوجد مشتركون.")
        return

    lines = [f"👥 عدد المشتركين: {len(users)}\n"]
    for user in users:
        name = user.get("first_name") or "Unknown"
        uname = user.get("username")
        lines.append(f"• {name} (@{uname})" if uname else f"• {name}")

    await update.message.reply_text("\n".join(lines))


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "🤖 *أوامر البوت*\n\n"
        "/start — الاشتراك في الإشعارات\n"
        "/stop — إلغاء الاشتراك\n"
        "/count — عدد المشتركين\n"
        "/ping — التحقق من حالة البوت\n"
        "/status — تفاصيل حالة النظام\n"
        "/stats — إحصائيات البوت\n"
        "/health — التحقق من صحة البوت\n"
        "/version — معلومات الإصدار\n"
        "/help — عرض هذه القائمة"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    text = f"Pong 🟢\n\n⏱ Uptime: {_uptime_str()}\n🕐 Time: {now}"
    await update.message.reply_text(text)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    loop = asyncio.get_running_loop()
    try:
        sub_count = len(await loop.run_in_executor(None, db_load_subscribers))
        proj_count = await loop.run_in_executor(None, db_count_sent_projects)
    except Exception:
        sub_count = proj_count = -1

    text = (
        "📊 *Bot Status*\n\n"
        f"🤖 Bot: 🟢 Online\n"
        f"📡 Monitor: {_task_status(_monitor_task)}\n"
        f"💓 Keep-alive: {_task_status(_keepalive_task)}\n"
        f"👥 Subscribers: {sub_count}\n"
        f"📁 Known Projects: {proj_count}\n"
        f"⏱ Uptime: {_uptime_str()}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    loop = asyncio.get_running_loop()
    try:
        sub_count = len(await loop.run_in_executor(None, db_load_subscribers))
        proj_count = await loop.run_in_executor(None, db_count_sent_projects)
        db_ok = await loop.run_in_executor(None, db_check_connection)
    except Exception:
        sub_count = proj_count = -1
        db_ok = False

    text = (
        "📈 *Bot Statistics*\n\n"
        f"👥 Total Subscribers: {sub_count}\n"
        f"📁 Total Known Projects: {proj_count}\n"
        f"🗄 Database: {'🟢 Connected' if db_ok else '🔴 Error'}\n"
        f"📡 Monitor: {_task_status(_monitor_task)}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Bot is alive 🟢")


async def cmd_version(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    py_version = platform.python_version()
    render_env = os.environ.get("RENDER", "local")
    text = (
        f"🔖 *Version Info*\n\n"
        f"🤖 Bot Version: {BOT_VERSION}\n"
        f"🐍 Python: {py_version}\n"
        f"☁️ Environment: {render_env}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


# ==========================
# Starlette route handlers
# ==========================

async def handle_root(request: Request) -> PlainTextResponse:
    """GET / — UptimeRobot health check."""
    return PlainTextResponse("Bot is alive", status_code=200)


async def handle_health(request: Request) -> PlainTextResponse:
    """GET /health — UptimeRobot health check."""
    return PlainTextResponse("Bot is alive", status_code=200)


async def handle_webhook(request: Request) -> PlainTextResponse:
    """POST /<BOT_TOKEN> — Telegram webhook endpoint."""
    # Validate the secret token Telegram sends
    if SECRET_TOKEN:
        incoming = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if incoming != SECRET_TOKEN:
            logger.warning("Webhook received with invalid secret token.")
            return PlainTextResponse("Forbidden", status_code=403)

    try:
        data = await request.json()
    except Exception:
        logger.warning("Webhook received non-JSON payload.")
        return PlainTextResponse("Bad Request", status_code=400)

    if _ptb_app is None:
        logger.error("PTB application not initialised yet.")
        return PlainTextResponse("Service Unavailable", status_code=503)

    try:
        update = Update.de_json(data, _ptb_app.bot)
        await _ptb_app.process_update(update)
    except Exception:
        logger.exception("Error processing Telegram update.")

    # Always return 200 so Telegram doesn't retry
    return PlainTextResponse("OK", status_code=200)


# ==========================
# ASGI lifespan
# ==========================

async def lifespan_startup() -> None:
    """Called by uvicorn before serving requests."""
    global _ptb_app, _task_manager_task

    logger.info("=" * 60)
    logger.info("Nafezly Bot v%s starting up.", BOT_VERSION)
    logger.info(
        "Python %s | Render=%s",
        platform.python_version(),
        os.environ.get("RENDER", "local"),
    )
    logger.info("=" * 60)

    # Database
    try:
        init_db()
    except Exception:
        logger.exception("FATAL: Could not initialise database. Exiting.")
        sys.exit(1)

    # Build the PTB Application (no run_webhook — we drive it manually)
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("stop",    cmd_stop))
    app.add_handler(CommandHandler("count",   cmd_count))
    app.add_handler(CommandHandler("users",   cmd_users))
    app.add_handler(CommandHandler("help",    cmd_help))
    app.add_handler(CommandHandler("ping",    cmd_ping))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("stats",   cmd_stats))
    app.add_handler(CommandHandler("health",  cmd_health))
    app.add_handler(CommandHandler("version", cmd_version))

    # Initialise PTB internals (bot session, etc.)
    await app.initialize()
    await app.start()
    _ptb_app = app

    # Register webhook with Telegram
    webhook_full_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
    try:
        await app.bot.set_webhook(
            url=webhook_full_url,
            secret_token=SECRET_TOKEN,
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )
        logger.info("Webhook registered: %s", webhook_full_url)
    except Exception:
        logger.exception("Failed to register webhook — bot may not receive updates.")

    # Start background task manager (monitor + keep-alive)
    _task_manager_task = asyncio.create_task(
        task_manager(app), name="task_manager"
    )
    logger.info("Task manager started. Bot is fully initialised.")
    logger.info("Listening on port %d", PORT)


async def lifespan_shutdown() -> None:
    """Called by uvicorn on shutdown."""
    global _task_manager_task, _monitor_task, _keepalive_task, _ptb_app

    logger.info("Shutdown initiated.")

    # Cancel background tasks
    for task, name in [
        (_task_manager_task, "task_manager"),
        (_monitor_task, "project_monitor"),
        (_keepalive_task, "keep_alive"),
    ]:
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            logger.info("%s stopped.", name)

    # Stop PTB
    if _ptb_app is not None:
        try:
            await _ptb_app.stop()
            await _ptb_app.shutdown()
            logger.info("PTB application stopped.")
        except Exception:
            logger.exception("Error stopping PTB application.")

    await close_http_client()

    global _db_pool
    if _db_pool:
        _db_pool.closeall()
        logger.info("Database pool closed.")

    logger.info("Shutdown complete.")


# ==========================
# Starlette ASGI app
# ==========================

# We use a custom lifespan context manager for Starlette


@asynccontextmanager
async def lifespan(app):
    await lifespan_startup()
    yield
    await lifespan_shutdown()


starlette_app = Starlette(
    routes=[
        Route("/",              handle_root,    methods=["GET"]),
        Route("/health",        handle_health,  methods=["GET"]),
        Route(WEBHOOK_PATH,     handle_webhook, methods=["POST"]),
    ],
    lifespan=lifespan,
)


# ==========================
# Entry point
# ==========================

def main() -> None:
    uvicorn.run(
        starlette_app,
        host="0.0.0.0",
        port=PORT,
        log_level="warning",       # uvicorn access logs handled by our logger
        access_log=False,
        lifespan="on",
    )


if __name__ == "__main__":
    main()
