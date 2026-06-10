import os
import asyncio
import secrets
import httpx
import psycopg2

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


NAFEZLY_URL = "https://nafezly.com/projects"
CHECK_INTERVAL = 30

DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS subscribers (
    chat_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS sent_projects (
    link TEXT PRIMARY KEY
)
""")


# ==========================
# Subscribers Helpers
# ==========================
def load_subscribers():
    cursor.execute(
        "SELECT chat_id, username, first_name FROM subscribers"
    )

    rows = cursor.fetchall()

    return [
        {
            "chat_id": row[0],
            "username": row[1],
            "first_name": row[2],
        }
        for row in rows
    ]


def add_subscriber(chat_id, username, first_name):
    cursor.execute(
        """
        INSERT INTO subscribers(chat_id, username, first_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (chat_id) DO NOTHING
        """,
        (chat_id, username, first_name),
    )


def remove_subscriber(chat_id):
    cursor.execute(
        "DELETE FROM subscribers WHERE chat_id = %s",
        (chat_id,),
    )


def load_sent_projects():
    cursor.execute(
        "SELECT link FROM sent_projects"
    )

    return set(row[0] for row in cursor.fetchall())


def add_sent_project(link):
    cursor.execute(
        """
        INSERT INTO sent_projects(link)
        VALUES (%s)
        ON CONFLICT (link) DO NOTHING
        """,
        (link,),
    )

# ==========================
# Project Monitor
# ==========================

async def check_projects(app: Application) -> None:
    sent_projects = load_sent_projects()

    if not sent_projects:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    NAFEZLY_URL,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=10,
                )

            soup = BeautifulSoup(response.text, "html.parser")
            projects = soup.select("a[href*='/project/']")

            for project in projects:
                sent_projects.add(project["href"])
                add_sent_project(project["href"])

        except Exception:
            pass

    async with httpx.AsyncClient() as client:
        while True:
            try:
                response = await client.get(
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
                        add_sent_project(link)

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

            except httpx.TimeoutException:
                await asyncio.sleep(120)

            except httpx.RequestError:
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
        add_subscriber(
            chat_id,
            username,
            first_name
        )
        await update.message.reply_text(
            "✅ تم الاشتراك بنجاح.\nستصلك إشعارات المشاريع الجديدة من نفذلي."
        )
    else:
        await update.message.reply_text("✅ أنت مشترك بالفعل.")


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    remove_subscriber(chat_id)
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
