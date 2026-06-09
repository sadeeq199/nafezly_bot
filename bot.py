import json
import asyncio
import requests
from bs4 import BeautifulSoup
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import os

BOT_TOKEN = os.getenv("BOT_TOKEN")


# الاشتراك
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    try:
        with open("subscribers.json", "r") as f:
            users = json.load(f)
    except:
        users = []

    if chat_id not in users:
        users.append(chat_id)

        with open("subscribers.json", "w") as f:
            json.dump(users, f)

        await update.message.reply_text(
            "✅ تم الاشتراك بنجاح.\nستصلك إشعارات المشاريع الجديدة من نفذلي."
        )
    else:
        await update.message.reply_text(
            "✅ أنت مشترك بالفعل."
        )


# إلغاء الاشتراك
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    try:
        with open("subscribers.json", "r") as f:
            users = json.load(f)
    except:
        users = []

    if chat_id in users:
        users.remove(chat_id)

        with open("subscribers.json", "w") as f:
            json.dump(users, f)

        await update.message.reply_text(
            "❌ تم إلغاء الاشتراك."
        )
    else:
        await update.message.reply_text(
            "أنت غير مشترك."
        )


# عدد المشتركين
async def count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with open("subscribers.json", "r") as f:
            users = json.load(f)
    except:
        users = []

    await update.message.reply_text(
        f"👥 عدد المشتركين الحالي: {len(users)}"
    )


# مراقبة المشاريع
async def check_projects(app):
    try:
        with open("sent_projects.json", "r") as f:
            sent_projects = set(json.load(f))
    except:
        sent_projects = set()

    # أول تشغيل فقط
    if len(sent_projects) == 0:

        response = requests.get(
            "https://nafezly.com/projects",
            headers={"User-Agent": "Mozilla/5.0"}
        )

        soup = BeautifulSoup(response.text, "html.parser")

        projects = soup.select("a[href*='/project/']")

        for project in projects:
            link = project["href"]
            sent_projects.add(link)

        with open("sent_projects.json", "w") as f:
            json.dump(list(sent_projects), f)

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

                    with open("sent_projects.json", "w") as f:
                        json.dump(list(sent_projects), f)

                    with open("subscribers.json", "r") as f:
                        users = json.load(f)

                    message = (
                        "🚨 مشروع جديد على نفذلي\n\n"
                        f"📌 العنوان:\n{title}\n\n"
                        f"🔗 رابط المشروع:\n{link}\n\n"
                    )

                    for user in users:
                        try:
                            await app.bot.send_message(
                                chat_id=user,
                                text=message
                            )
                        except Exception as e:
                            print(e)

                    print("New Project:", title)

            await asyncio.sleep(15)

        except Exception as e:
            print("Error:", e)
            await asyncio.sleep(30)


# تشغيل مراقبة المشاريع
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

print("Bot Started...")

app.run_polling()