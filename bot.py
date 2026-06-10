"""
Nafezly Telegram Bot — production-grade refactor
Author: refactored by Claude for 24/7 stability on Render
"""

import asyncio
import logging
import os
import secrets
from contextlib import asynccontextmanager, contextmanager
from typing import Generator

import httpx
import psycopg2
import psycopg2.pool
from bs4 import BeautifulSoup
from telegram import Update
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

# Silence overly verbose libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ==========================
# Configuration
# ==========================

BOT_TOKEN: str = os.environ["BOT_TOKEN"]          # crash-fast if missing
DATABASE_URL: str = os.environ["DATABASE_URL"]     # crash-fast if missing
OWNER_ID: int = 2121957939
WEBHOOK_URL: str = os.environ.get(
    "WEBHOOK_URL", "https://nafezly-bot-5vdr.onrender.com"
).rstrip("/")
PORT: int = int(os.environ.get("PORT", 10000))
SECRET_TOKEN: str = os.environ.get("SECRET_TOKEN", secrets.token_hex(32))

NAFEZLY_URL: str = "https://nafezly.com/projects"
CHECK_INTERVAL: int = 30          # seconds between scrape cycles
RETRY_DELAY: int = 120            # seconds to wait after a request error
MAX_SEND_CONCURRENCY: int = 10    # parallel Telegram sends per batch

# ==========================
# Database — thread-safe pool
# ==========================

_db_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Return (and lazily create) the shared connection pool."""
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
    """
    Context manager: borrow a connection from the pool, yield a cursor,
    commit on success, rollback + return connection on any error.
    Always returns the connection to the pool.
    """
    pool = get_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def init_db() -> None:
    """Create tables if they don't exist yet."""
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


def db_add_subscriber(chat_id: int, username: str | None, first_name: str | None) -> bool:
    """Returns True if a new row was inserted, False if already existed."""
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


# ==========================
# HTTP client — single shared instance
# ==========================

_http_client: httpx.AsyncClient | None = None


def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=httpx.Timeout(15.0),
            follow_redirects=True,
        )
        logger.info("HTTP client created.")
    return _http_client


async def close_http_client() -> None:
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        logger.info("HTTP client closed.")


# ==========================
# Project monitor
# ==========================

def _scrape_project_links(html: str) -> list[tuple[str, str]]:
    """Return [(title, href), ...] for all project anchors on the page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for tag in soup.select("a[href*='/project/']"):
        href = tag["href"]
        title = tag.get_text(strip=True)
        results.append((title, href))
    return results


async def _seed_sent_projects(client: httpx.AsyncClient) -> set[str]:
    """
    First-run: populate sent_projects so we don't spam existing listings.
    Returns the set of already-known links.
    """
    logger.info("sent_projects is empty — seeding from current listings.")
    try:
        response = await client.get(NAFEZLY_URL)
        response.raise_for_status()
        projects = _scrape_project_links(response.text)
        links = {link for _, link in projects}
        for link in links:
            db_add_sent_project(link)
        logger.info("Seeded %d existing project links.", len(links))
        return links
    except Exception:
        logger.exception("Failed to seed sent_projects; will retry next cycle.")
        return set()


async def _send_to_subscribers(
    app: Application,
    message: str,
    subscribers: list[dict],
) -> None:
    """Send a message to all subscribers with bounded concurrency."""
    semaphore = asyncio.Semaphore(MAX_SEND_CONCURRENCY)

    async def _send(user: dict) -> None:
        async with semaphore:
            try:
                await app.bot.send_message(chat_id=user["chat_id"], text=message)
            except Exception:
                logger.warning(
                    "Failed to send to chat_id=%s.", user["chat_id"], exc_info=True
                )

    await asyncio.gather(*(_send(u) for u in subscribers))


async def check_projects(app: Application) -> None:
    """
    Infinite loop: poll Nafezly for new projects and notify subscribers.
    Designed to run as a long-lived asyncio Task; never raises unhandled exceptions.
    """
    logger.info("Project monitor started.")
    client = get_http_client()

    # Run database I/O in a thread to avoid blocking the event loop
    loop = asyncio.get_running_loop()

    sent_projects: set[str] = await loop.run_in_executor(None, db_load_sent_projects)

    if not sent_projects:
        sent_projects = await _seed_sent_projects(client)

    while True:
        try:
            response = await client.get(NAFEZLY_URL)
            response.raise_for_status()

            projects = _scrape_project_links(response.text)
            new_projects = [(t, l) for t, l in projects if l not in sent_projects]

            if new_projects:
                subscribers: list[dict] = await loop.run_in_executor(
                    None, db_load_subscribers
                )

                for title, link in new_projects:
                    sent_projects.add(link)
                    await loop.run_in_executor(None, db_add_sent_project, link)

                    message = (
                        "🚨 مشروع جديد على نفذلي\n\n"
                        f"📌 العنوان:\n{title}\n\n"
                        f"🔗 رابط المشروع:\n{link}"
                    )
                    logger.info("New project detected: %s", link)
                    await _send_to_subscribers(app, message, subscribers)

            await asyncio.sleep(CHECK_INTERVAL)

        except (httpx.TimeoutException, httpx.RequestError) as exc:
            logger.warning("HTTP error in monitor: %s — retrying in %ds.", exc, RETRY_DELAY)
            await asyncio.sleep(RETRY_DELAY)

        except asyncio.CancelledError:
            logger.info("Project monitor task cancelled — shutting down.")
            raise  # let asyncio handle clean cancellation

        except Exception:
            logger.exception("Unexpected error in project monitor — retrying in %ds.", RETRY_DELAY)
            await asyncio.sleep(RETRY_DELAY)


# ==========================
# Background task lifecycle
# ==========================

_monitor_task: asyncio.Task | None = None


async def post_init(app: Application) -> None:
    """Called by python-telegram-bot after the Application is initialised."""
    global _monitor_task
    _monitor_task = asyncio.create_task(
        check_projects(app), name="project_monitor"
    )
    # Log (and re-raise) if the task dies unexpectedly
    _monitor_task.add_done_callback(_on_monitor_done)
    logger.info("project_monitor task created.")


def _on_monitor_done(task: asyncio.Task) -> None:
    if task.cancelled():
        logger.info("project_monitor task was cancelled.")
    elif task.exception():
        logger.critical(
            "project_monitor task died with an exception!",
            exc_info=task.exception(),
        )


async def post_shutdown(app: Application) -> None:
    """Called by python-telegram-bot during graceful shutdown."""
    global _monitor_task
    if _monitor_task and not _monitor_task.done():
        _monitor_task.cancel()
        try:
            await asyncio.wait_for(_monitor_task, timeout=5)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        logger.info("project_monitor task stopped.")
    await close_http_client()
    if _db_pool:
        _db_pool.closeall()
        logger.info("Database pool closed.")


# ==========================
# Command handlers
# ==========================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name

    loop = asyncio.get_running_loop()
    inserted = await loop.run_in_executor(
        None, db_add_subscriber, chat_id, username, first_name
    )

    if inserted:
        await update.message.reply_text(
            "✅ تم الاشتراك بنجاح.\nستصلك إشعارات المشاريع الجديدة من نفذلي."
        )
    else:
        await update.message.reply_text("✅ أنت مشترك بالفعل.")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db_remove_subscriber, chat_id)
    await update.message.reply_text("❌ تم إلغاء الاشتراك.")


async def cmd_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    loop = asyncio.get_running_loop()
    users = await loop.run_in_executor(None, db_load_subscribers)
    await update.message.reply_text(f"👥 عدد المشتركين الحالي: {len(users)}")


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != OWNER_ID:
        return

    loop = asyncio.get_running_loop()
    users = await loop.run_in_executor(None, db_load_subscribers)

    if not users:
        await update.message.reply_text("لا يوجد مشتركون.")
        return

    lines = [f"👥 عدد المشتركين: {len(users)}\n"]
    for user in users:
        name = user.get("first_name") or "Unknown"
        uname = user.get("username")
        lines.append(f"• {name} (@{uname})" if uname else f"• {name}")

    await update.message.reply_text("\n".join(lines))


# ==========================
# Entry point
# ==========================

def main() -> None:
    # Initialise DB (tables) before anything else — fail fast if DB is down
    init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop",  cmd_stop))
    app.add_handler(CommandHandler("count", cmd_count))
    app.add_handler(CommandHandler("users", cmd_users))

    logger.info("Starting bot on port %d …", PORT)

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=BOT_TOKEN,
        webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        secret_token=SECRET_TOKEN,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
