from flask import Flask, request
from typing import Union
import requests
import asyncpg
import os

app = Flask(__name__)

BOT_TOKEN = os.environ.get("SOLANA_BOT_TOKEN")
TELEGRAM_API = "https://api.telegram.org/bot"
DATABASE_URL = os.environ.get("SOLANA_BOT_DB_URL")

async def subscribe_user(chat_id: Union[int, str]) -> None:
    try:
        db = await asyncpg.connect(DATABASE_URL)
        users = await db.fetch("SELECT * FROM users WHERE chat_id = $1", chat_id)

        if len(users) > 0: # If user is already subscribed, do nothing.
            await db.close()
            send_message(chat_id, msg="You are already subscribed.")
        else: # If user is not yet subscribed, subscribe user.
            await db.execute("INSERT INTO users (chat_id) VALUES ($1)", chat_id)
            await db.close()
            send_message(chat_id, msg="You have successfully subscribed to Solana BOT Alerts.")
    except Exception:
        try:
            await db.close()
        except Exception:
            pass
        
        send_message(chat_id, msg="Could not process your request due to an internal server error.")

async def unsubscribe_user(chat_id: Union[int, str]) -> None:
    try:
        db = await asyncpg.connect(DATABASE_URL)
        users = await db.fetch("SELECT * FROM users WHERE chat_id = $1", chat_id)

        if len(users) == 0: # If user is not subscribed in the first place, do nothing.
            await db.close()
            send_message(chat_id, msg="You are not subscribed in the first place.")
        else: # If user is subscribed, unsubscribe user.
            await db.execute("DELETE FROM users WHERE chat_id = $1", chat_id)
            await db.close()
            send_message(chat_id, msg="You have successfully unsubscribed from Solan BOT Alerts. You will no longer receive price alerts for Solana tokens.")
    except Exception:
        try:
            await db.close()
        except Exception:
            pass
        
        send_message(chat_id, msg="Could not process your request due to an internal server error.")

async def handle_start(chat_id: Union[int, str]) -> None:
    message = """Hello there! Welcome to Solana BOT! With me, you can stay updated with the latest trends in the Solana blockchain!

Check my menu for the list of supported commands."""
    
    send_message(chat_id, msg=message)

def send_message(chat_id: Union[int, str], msg: str, parse_mode: Union[str, None]=None) -> None:
    method = "/sendMessage"

    if parse_mode:
        message = {
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": parse_mode
        }
    else:
        message = {
            "chat_id": chat_id,
            "text": msg
        }
    
    requests.post(url=f"{TELEGRAM_API}{BOT_TOKEN}{method}", params=message)

@app.route(f"/telegram", methods=["POST"])
async def process_message():
    commands = {
        "/subscribe": subscribe_user,
        "/unsubscribe": unsubscribe_user,
        "/start": handle_start
    }

    update = request.get_json()

    if "message" not in update:
        return "OK", 200
    
    chat_id = update["message"]["chat"]["id"]
    
    if "text" not in update["message"]:
        send_message(chat_id, msg="I only understand text messages.")
        return "OK", 200
    
    user_message = update["message"]["text"]

    if user_message not in commands:
        send_message(chat_id, msg="Invalid command. Please try again.")
        return "OK", 200
    
    await commands[user_message](chat_id)
    
    return "OK", 200

@app.route("/favicon.ico")
def favicon():
    return '', 204

@app.route("/")
def confirm_deployment():
    html = """<h1>Curious human, there is nothing to be seen here.</h1>
    <p>
        This page is here to confirm that this telegram bot is properly deployed and ready to listen to users.
        <br>
        To use the bot, click <a href="https://t.me/solana_scrutinizer_bot">here</a>.
    </p>"""

    return html, 200

if __name__ == "__main__":
    app.run(host="localhost", port=5000)
