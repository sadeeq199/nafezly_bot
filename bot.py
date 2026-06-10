import json
import os
import threading

from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = 2121957939

SUBSCRIBERS_FILE = "subscribers.json"


# ==========================
# Subscribers Helpers
# ==========================
def load_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []


def save_subscribers(users):
    with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=4)


# ==========================
# Commands
# ==========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    chat_id = update.effective_chat.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name

    users = load_subscribers()

    exists = any(user["chat_id"] == chat_id for user in users)

    if not exists:

        users.append({
            "chat_id": chat_id,
            "username": username,
            "first_name": first_name
        })

        save_subscribers(users)

        await update.message.reply_text(
            "✅ تم الاشتراك بنجاح.\nستصلك إشعارات المشاريع الجديدة من نفذلي."
        )

    else:

        await update.message.reply_text(
            "✅ أنت مشترك بالفعل."
        )


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):

    chat_id = update.effective_chat.id

    users = load_subscribers()

    users = [
        user for user in users
        if user["chat_id"] != chat_id
    ]

    save_subscribers(users)

    await update.message.reply_text(
        "❌ تم إلغاء الاشتراك."
    )


async def count(update: Update, context: ContextTypes.DEFAULT_TYPE):

    users = load_subscribers()

    await update.message.reply_text(
        f"👥 عدد المشتركين: {len(users)}"
    )


async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if update.effective_chat.id != OWNER_ID:
        return

    users_list = load_subscribers()

    if not users_list:
        await update.message.reply_text(
            "لا يوجد مشتركون."
        )
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
# Flask
# ==========================
web_app = Flask(__name__)


@web_app.route("/")
def home():
    return "Bot is running!"


def run_web():
    web_app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )


threading.Thread(
    target=run_web,
    daemon=True
).start()


# ==========================
# Telegram
# ==========================
app = Application.builder().token(BOT_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("stop", stop))
app.add_handler(CommandHandler("count", count))
app.add_handler(CommandHandler("users", users))

print("Bot Started...")

app.run_polling(drop_pending_updates=True)