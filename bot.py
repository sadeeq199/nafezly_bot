"""
Nafezly Projects Notifier Bot
==============================
A Telegram bot that monitors nafezly.com/projects and notifies subscribers
about new projects. Designed to run reliably on Render Web Service.

Architecture:
- Single asyncio event loop (no threading for the bot)
- Flask runs in a background thread (daemon) for Render's health check
- PTB Application uses run_polling() which manages its own loop internally
- Background monitoring task is launched via post_init hook
"""

import json
import asyncio
import logging
import os
import threading
import time

import requests
from bs4 import BeautifulSoup
from flask import Flask
from telegram import Update
from telegram.error import Forbidden, NetworkError
from telegram.ext import Application, CommandHandler, ContextTypes

# ===========================
# Logging Configuration
# ===========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("nafezly_bot")

# Silence noisy libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)


# ===========================
# Configuration
# ===========================
BOT_TOKEN: str = os.environ["BOT_TOKEN"]          # raises immediately if missing
OWNER_ID: int = 2121957939
NAFEZLY_URL: str = "https://nafezly.com/projects"
POLL_INTERVAL: int = 15        # seconds between site checks
REQUEST_TIMEOUT: int = 20      # seconds for HTTP requests
SUBSCRIBERS_FILE: str = "subscribers.json"
SENT_PROJECTS_FILE: str = "sent_projects.json"

# ===========================
# File Helpers
# ===========================

def _read_json(path: str, default):
    """Read a JSON file safely; return *default* on any error."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _write_json(path: str, data) -> None:
    """Write data to a JSON file atomically-ish (write then rename)."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=4)
    os.replace(tmp, path)


# ===========================
# Subscribers helpers
# ===========================

def load_subscribers() -> list[dict]:
    return _read_json(SUBSCRIBERS_FILE, [])


def save_subscribers(users: list[dict]) -> None:
    _write_json(SUBSCRIBERS_FILE, users)


def subscriber_exists(users: list[dict], chat_id: int) -> bool:
    return any(u["chat_id"] == chat_id for u in users)


# ===========================
# Sent-projects helpers
# ===========================

def load_sent_projects() -> set[str]:
    data = _read_json(SENT_PROJECTS_FILE, [])
    return set(data)


def save_sent_projects(sent: set[str]) -> None:
    _write_json(SENT_PROJECTS_FILE, list(sent))


# ===========================
# Scraping helper
# ===========================

def scrape_projects() -> list[dict]:
    """Return a list of {title, link} dicts from nafezly.com/projects."""
    response = requests.get(
        NAFEZLY_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; NafezlyBot/1.0)"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    results = []
    for tag in soup.select("a[href*='/project/']"):
        link = tag.get("href", "").strip()
        title = tag.get_text(strip=True)
        if link:
            results.append({"title": title, "link": link})
    return results


# ===========================
# Command Handlers
# ===========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name
    logger.info("/start from chat_id=%s username=%s", chat_id, username)

    users = load_subscribers()

    if subscriber_exists(users, chat_id):
        await update.message.reply_text("✅ أنت مشترك بالفعل.")
        return

    users.append({
        "chat_id": chat_id,
        "username": username,
        "first_name": first_name,
    })
    save_subscribers(users)
    logger.info("New subscriber: chat_id=%s (%s)", chat_id, first_name)
    await update.message.reply_text(
        "✅ تم الاشتراك بنجاح.\nستصلك إشعارات المشاريع الجديدة من نفذلي."
    )


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info("/stop from chat_id=%s", chat_id)

    users = load_subscribers()
    before = len(users)
    users = [u for u in users if u["chat_id"] != chat_id]
    save_subscribers(users)

    removed = before - len(users)
    logger.info("Unsubscribed chat_id=%s (removed=%d)", chat_id, removed)
    await update.message.reply_text("❌ تم إلغاء الاشتراك.")


async def count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("/count from chat_id=%s", update.effective_chat.id)
    users = load_subscribers()
    await update.message.reply_text(f"👥 عدد المشتركين الحالي: {len(users)}")


async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info("/users from chat_id=%s", chat_id)

    if chat_id != OWNER_ID:
        logger.warning("/users rejected: not owner (chat_id=%s)", chat_id)
        return

    users_list = load_subscribers()
    if not users_list:
        await update.message.reply_text("لا يوجد مشتركون.")
        return

    text = f"👥 عدد المشتركين: {len(users_list)}\n\n"
    for user in users_list:
        first_name = user.get("first_name", "Unknown")
        username = user.get("username")
        text += f"• {first_name} (@{username})\n" if username else f"• {first_name}\n"

    await update.message.reply_text(text)


# ===========================
# Background: Project Monitor
# ===========================

async def check_projects(app: Application) -> None:
    """
    Infinite loop that polls nafezly.com every POLL_INTERVAL seconds.
    Runs as a background asyncio task started via post_init.
    """
    logger.info("Project monitor: starting up...")

    sent_projects = load_sent_projects()

    # --- Seed phase: load existing projects without notifying ---
    if not sent_projects:
        logger.info("Project monitor: no sent_projects.json found, seeding initial list...")
        try:
            projects = scrape_projects()
            for p in projects:
                sent_projects.add(p["link"])
            save_sent_projects(sent_projects)
            logger.info("Project monitor: seeded %d existing projects, will notify only new ones.", len(projects))
        except Exception as exc:
            logger.error("Project monitor: seed phase failed: %s", exc)
    else:
        logger.info("Project monitor: loaded %d known project links.", len(sent_projects))

    # --- Monitor loop ---
    logger.info("Project monitor: entering polling loop (interval=%ds).", POLL_INTERVAL)
    while True:
        try:
            projects = scrape_projects()
            new_count = 0

            for project in projects:
                link = project["link"]
                title = project["title"]

                if link in sent_projects:
                    continue

                # New project found
                sent_projects.add(link)
                save_sent_projects(sent_projects)
                new_count += 1
                logger.info("Project monitor: NEW project detected — %s", title)

                subscribers = load_subscribers()
                if not subscribers:
                    logger.info("Project monitor: no subscribers to notify.")
                    continue

                message = (
                    "🚨 مشروع جديد على نفذلي\n\n"
                    f"📌 العنوان:\n{title}\n\n"
                    f"🔗 رابط المشروع:\n{link}"
                )

                for user in subscribers:
                    try:
                        await app.bot.send_message(
                            chat_id=user["chat_id"],
                            text=message,
                        )
                        logger.debug("Notified chat_id=%s", user["chat_id"])
                    except Forbidden:
                        # User blocked the bot – remove them
                        logger.warning("chat_id=%s blocked the bot, removing.", user["chat_id"])
                        all_users = load_subscribers()
                        all_users = [u for u in all_users if u["chat_id"] != user["chat_id"]]
                        save_subscribers(all_users)
                    except NetworkError as net_err:
                        logger.error("Network error sending to chat_id=%s: %s", user["chat_id"], net_err)
                    except Exception as exc:
                        logger.error("Unexpected error sending to chat_id=%s: %s", user["chat_id"], exc)

            if new_count:
                logger.info("Project monitor: notified subscribers about %d new project(s).", new_count)
            else:
                logger.debug("Project monitor: no new projects found.")

        except requests.Timeout:
            logger.error("Project monitor: request to nafezly.com timed out.")
        except requests.RequestException as req_err:
            logger.error("Project monitor: HTTP error: %s", req_err)
        except Exception as exc:
            logger.exception("Project monitor: unexpected error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL)


# ===========================
# post_init Hook
# ===========================

async def post_init(app: Application) -> None:
    """Called by PTB after the Application is fully initialized."""
    logger.info("post_init: Application initialized — launching background monitor task.")
    asyncio.create_task(check_projects(app))
    logger.info("post_init: Background monitor task created.")


# ===========================
# Flask (Render health-check)
# ===========================

web_app = Flask(__name__)


@web_app.route("/")
def home():
    return "Bot is running!", 200


@web_app.route("/health")
def health():
    return {"status": "ok", "subscribers": len(load_subscribers())}, 200


def run_flask() -> None:
    port = int(os.environ.get("PORT", 10000))
    logger.info("Flask: starting on port %d", port)
    web_app.run(host="0.0.0.0", port=port, use_reloader=False, debug=False)


# ===========================
# Entry Point
# ===========================

def main() -> None:
    logger.info("=" * 60)
    logger.info("Nafezly Bot starting up...")
    logger.info("OWNER_ID  : %d", OWNER_ID)
    logger.info("POLL_INTERVAL: %ds", POLL_INTERVAL)
    logger.info("=" * 60)

    # Start Flask in a daemon thread so Render gets a responding HTTP port
    flask_thread = threading.Thread(target=run_flask, name="flask", daemon=True)
    flask_thread.start()
    logger.info("Flask thread started.")

    # Small delay to let Flask bind before PTB starts (avoids port race on Render)
    time.sleep(1)

    # Build the PTB Application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("count", count))
    application.add_handler(CommandHandler("users", users_cmd))

    logger.info("PTB Application built, handlers registered.")
    logger.info("Starting polling — drop_pending_updates=True to avoid conflict errors.")

    # run_polling() creates and owns its own event loop.
    # drop_pending_updates=True prevents the Conflict error caused by
    # stale getUpdates calls left from a previous crashed instance.
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
        close_loop=False,
    )

    logger.info("Bot stopped.")


if __name__ == "__main__":
    main()
