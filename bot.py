import json
import asyncio
import requests
import os
import threading
from bs4 import BeautifulSoup
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = 2121957939


# ===========================
# الاشتراك
# ===========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name

    try:
        with open("subscribers.json", "r", encoding="utf-8") as f:
            users = json.load(f)
    except:
        users = []

    exists = any(user["chat_id"] == chat_id for user in users)

    if not exists:

        users.append({
            "chat_id": chat_id,
            "username": username,
            "first_name": first_name
        })

        with open("subscribers.json", "w", encoding="utf-8") as f:
            json.dump(users, f, ensure_ascii=False, indent=4)

        await update.message.reply_text(
            "✅ تم الاشتراك بنجاح.\nستصلك إشعارات المشاريع الجديدة من نفذلي."
        )

    else:
        await update.message.reply_text(
            "✅ أنت مشترك بالفعل."
        )


# ===========================
# إلغاء الاشتراك
# ===========================
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    try:
        with open("subscribers.json", "r", encoding="utf-8") as f:
            users = json.load(f)
    except:
        users = []

    users = [user for user in users if user["chat_id"] != chat_id]

    with open("subscribers.json", "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=4)

    await update.message.reply_text("❌ تم إلغاء الاشتراك.")


# ===========================
# عدد المشتركين
# ===========================
async def count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with open("subscribers.json", "r", encoding="utf-8") as f:
            users = json.load(f)
    except:
        users = []

    await update.message.reply_text(
        f"👥 عدد المشتركين الحالي: {len(users)}"
    )


# ===========================
# عرض المشتركين
# ===========================
async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if update.effective_chat.id != OWNER_ID:
        return

    try:
        with open("subscribers.json", "r", encoding="utf-8") as f:
            users_list = json.load(f)
    except:
        users_list = []

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


# ===========================
# مراقبة المشاريع
# ===========================
async def check_projects(app):

    try:
        with open("sent_projects.json", "r", encoding="utf-8") as f:
            sent_projects = set(json.load(f))
    except:
        sent_projects = set()

    if len(sent_projects) == 0:

        response = requests.get(
            "https://nafezly.com/projects",
            headers={"User-Agent": "Mozilla/5.0"}
        )

        soup = BeautifulSoup(response.text, "html.parser")

        projects = soup.select("a[href*='/project/']")

        for project in projects:
            sent_projects.add(project["href"])

        with open("sent_projects.json", "w", encoding="utf-8") as f:
            json.dump(list(sent_projects), f, ensure_ascii=False, indent=4)

        print("Initial projects loaded.")

    while True:

        try:

            response = requests.get(
                "https://nafezly.com/projects",
                headers={"User-Agent": "Mozilla/5.0"}
            )

            soup = BeautifulSoup(response.text, "html.parser")

            projects = soup.select("a[href*='/project/']")

            for project in projects:

                title = project.get_text(strip=True)
                link = project["href"]

                if link not in sent_projects:

                    sent_projects.add(link)

                    with open("sent_projects.json", "w", encoding="utf-8") as f:
                        json.dump(list(sent_projects), f, ensure_ascii=False, indent=4)

                    try:
                        with open("subscribers.json", "r", encoding="utf-8") as f:
                            users_list = json.load(f)
                    except:
                        users_list = []

                    message = (
                        "🚨 مشروع جديد على نفذلي\n\n"
                        f"📌 العنوان:\n{title}\n\n"
                        f"🔗 رابط المشروع:\n{link}"
                    )

                    for user in users_list:

                        try:

                            await app.bot.send_message(
                                chat_id=user["chat_id"],
                                text=message
                            )

                        except Exception as e:
                            print(e)

                    print("New Project:", title)

            await asyncio.sleep(15)

        except Exception as e:
            print("Error:", e)
            await asyncio.sleep(30)


# ===========================
# تشغيل المراقبة
# ===========================
async def post_init(app):
    asyncio.create_task(check_projects(app))


app = (
    Application.builder()
    .token(BOT_TOKEN)
    .post_init(post_init)
    .build()
)

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("stop", stop))
app.add_handler(CommandHandler("count", count))
app.add_handler(CommandHandler("users", users))


# ===========================
# Flask لـ Render
# ===========================
web_app = Flask(__name__)


@web_app.route("/")
def home():
    return "Bot is running!"


def run_web():
    web_app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )


threading.Thread(target=run_web, daemon=True).start()

print("Bot Started...")

app.run_polling()