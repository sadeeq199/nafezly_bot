import json
import os
import asyncio
import secrets
import requests

from bs4 import BeautifulSoup
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)


# ==========================
# Configuration
# ==========================

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = 2121957939
WEBHOOK_URL = "https://nafezly-bot-5vdr.onrender.com".rstrip("/")
PORT = int(os.environ.get("PORT", 10000))
SECRET_TOKEN = os.environ.get("SECRET_TOKEN", secrets.token_hex(32))

SUBSCRIBERS_FILE = "subscribers.json"
SENT_PROJECTS_FILE = "sent_projects.json"
NAFEZLY_URL = "https://nafezly.com/projects"
CHECK_INTERVAL = 60


# ==========================
# Subscribers Helpers
# ==========================

def load_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            users = json.load(f)
    except Exception:
        users = []

    migrated = []
    for user in users:
        if isinstance(user, int):
            migrated.append({
                "chat_id": user,
                "username": None,
                "first_name": "Unknown",
            })
        else:
            migrated.append(user)

    return migrated


def save_subscribers(users):
    with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=4)


# ==========================
# Sent Projects Helpers
# ==========================

def load_sent_projects():
    try:
        with open(SENT_PROJECTS_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_sent_projects(projects):
    with open(SENT_PROJECTS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(projects), f, ensure_ascii=False, indent=4)


# ==========================
# Project Monitor
# ==========================

async def check_projects(app: Application) -> None:
    sent_projects = load_sent_projects()

    if len(sent_projects) == 0:
        try:
            response = requests.get(
                NAFEZLY_URL,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            soup = BeautifulSoup(response.text, "html.parser")
            projects = soup.select("a[href*='/project/']")
            for project in projects:
                sent_projects.add(project["href"])
            save_sent_projects(sent_projects)
        except Exception:
            pass

    while True:
        try:
            response = requests.get(
                NAFEZLY_URL,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            soup = BeautifulSoup(response.text, "html.parser")
            projects = soup.select("a[href*='/project/']")
            users = load_subscribers()

            for project in projects:
                title = project.get_text(strip=True)
                link = project["href"]

                if link not in sent_projects:
                    sent_projects.add(link)
                    save_sent_projects(sent_projects)

                    message = (
                        "🚨 مشروع جديد على نفذلي\n\n"
                        f"📌 العنوان:\n{title}\n\n"
                        f"🔗 رابط المشروع:\n{link}"
                    )

                    for user in users:
                        try:
                            await app.bot.send_message(
                                chat_id=user["chat_id"],
                                text=message,
                            )
                        except Exception:
                            pass

            await asyncio.sleep(CHECK_INTERVAL)

        except requests.Timeout:
            await asyncio.sleep(120)

        except requests.RequestException:
            await asyncio.sleep(120)

        except Exception:
            await asyncio.sleep(120)


# ==========================
# Background Task
# ==========================

async def post_init(app: Application) -> None:
    asyncio.create_task(check_projects(app))


# ==========================
# Command Handlers
# ==========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name

    users = load_subscribers()
    exists = any(user["chat_id"] == chat_id for user in users)

    if not exists:
        users.append({
            "chat_id": chat_id,
            "username": username,
            "first_name": first_name,
        })
        save_subscribers(users)
        await update.message.reply_text(
            "✅ تم الاشتراك بنجاح.\nستصلك إشعارات المشاريع الجديدة من نفذلي."
        )
    else:
        await update.message.reply_text("✅ أنت مشترك بالفعل.")


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    users = load_subscribers()
    users = [user for user in users if user["chat_id"] != chat_id]
    save_subscribers(users)
    await update.message.reply_text("❌ تم إلغاء الاشتراك.")


async def count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    users = load_subscribers()
    await update.message.reply_text(f"👥 عدد المشتركين الحالي: {len(users)}")


async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != OWNER_ID:
        return

    users_list = load_subscribers()

    if not users_list:
        await update.message.reply_text("لا يوجد مشتركون.")
        return

    text = f"👥 عدد المشتركين: {len(users_list)}\n\n"

    for user in users_list:
        first_name = user.get("first_name", "Unknown")
        username = user.get("username")
        if username:
            text += f"• {first_name} (@{username})\n"
        else:
            text += f"• {first_name}\n"

    await update.message.reply_text(text)


# ==========================
# Application Entry Point
# ==========================

def main() -> None:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("count", count))
    app.add_handler(CommandHandler("users", users_cmd))

    print("Bot Started...")

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
