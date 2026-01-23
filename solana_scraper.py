from curl_cffi import AsyncSession, requests
from asyncpg.pool import Pool
from asyncio import Semaphore
from functools import wraps
from typing import Union
import asyncpg
import asyncio
import logging
import time
import os

BOT_TOKEN = os.environ.get("SOLANA_BOT_TOKEN")
TELEGRAM_API = "https://api.telegram.org/bot"
DB_PASSWORD = os.environ.get("LOCAL_PG_PASSWORD")
BIRDEYE_INT_API_URL = "https://multichain-api.birdeye.so/[block_chain]/v3/gems"
POOL: Union[Pool, None] = None
MESSAGE_LIMIT = Semaphore(20)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def limit_concurrency(async_func):
    """This decorator limits the number of concurrent messages forwarded to users."""
    @wraps(async_func)
    async def wrapper(*args, **kwargs):
        async with MESSAGE_LIMIT:
            return await async_func(*args, **kwargs)

    return wrapper

def get_token_data() -> list:
    PAYLOAD = {"limit":100,"offset":0,"filters":[],"shown_time_frame":"24h","type":"trending","sort_by":"rank","sort_type":"asc"}
    REQ_HEADERS = {
        "content-type": "application/json",
        "origin": "https://birdeye.so",
        "referer": "https://birdeye.so/"
    }
    res = requests.post(url=BIRDEYE_INT_API_URL.replace("[block_chain]", "solana"), impersonate="edge", headers=REQ_HEADERS, json=PAYLOAD)
    tokens = res.json()["data"]["items"]

    token_data = [{"symbol": token.get("symbol", ""), "address": token.get("address", ""), "logo": token.get("logoURI", ""), "price": token.get("price", 0.0), "liquidity": token.get("liquidity", 0.0)} for token in tokens]

    return token_data

async def get_new_or_changed_tokens(tokens: list, db) -> list:
    new_or_changed_tokens = []
    for token in tokens:
        rows = await db.fetch("SELECT * FROM solana_tokens WHERE address = $1", token["address"])
        if rows: # Check if token is already in the db.
            stored_token = rows[0]
            if token["price"] != stored_token["price"] or token["liquidity"] != stored_token["liquidity"]: # If there is any change, store the token.
                new_or_changed_tokens.append((token, stored_token))
        else: # If not in the db, store the token.
            new_or_changed_tokens.append((token, {}))

    
    logging.info(f"Found {len(new_or_changed_tokens)} new or changed tokens.")
    return new_or_changed_tokens

async def get_alert_worthy_tokens(tokens: list) -> list:
    alert_worthy_tokens = []
    for token, stored_token in tokens:
        if not stored_token: # If token is not stored in the db, it is alert-worthy, but with a special alert.
            alert_worthy_token = dict(token)
            alert_worthy_token["new_token"] = True # Create a new key signifying it is a new token.
            alert_worthy_tokens.append(alert_worthy_token)
            continue

        price = token["price"]
        stored_price = stored_token["price"]
        liquidity = token["liquidity"]

        if liquidity >= 5_000_000:
            alert_threshold = 0.4
        elif 500_000 <= liquidity < 5_000_000:
            alert_threshold = 1
        elif 50_000 <= liquidity < 500_000:
            alert_threshold = 2
        else:
            alert_threshold = 6
        
        percent_change = ((price - stored_price) / stored_price) * 100

        if abs(percent_change) >= alert_threshold:
            alert_worthy_token = dict(token)
            alert_worthy_token["new_token"] = False
            alert_worthy_token["percent_change"] = percent_change
            alert_worthy_tokens.append(alert_worthy_token)


    logging.info(f"Found a total of {len(alert_worthy_tokens)} alert-worthy tokens.")
    return alert_worthy_tokens

async def init_pool() -> None:
    global POOL
    POOL = await asyncpg.create_pool(f"postgresql://postgres:{DB_PASSWORD}@localhost:5433/solana_bot")

@limit_concurrency
async def alert_user(tokens: list, user_chat_id: int, session: AsyncSession) -> None:
    for token in tokens:
        token_url = f"https://birdeye.so/solana/token/{token['address']}"
        if token.get("new_token"): # If this is a new token, give it the alert below:
            alert = f'[NEW TRENDING TOKEN]\n\nToken: {token['symbol']}\n\nPrice: {token['price']}\n\nAddress: {token['address']}\n\n<a href="{token_url}">Live Stats</a>'
        else:
            if token["percent_change"] > 0:
                 alert = f'[PRICE RISE ALERT]\n\n🟢 △ +{round(token["percent_change"], 2)}%\n\nToken: {token["symbol"]}\n\nPrice: {token["price"]}\n\nAddress: {token["address"]}\n\n<a href="{token_url}">Live Stats</a>'
            else:
                alert = f'[PRICE DROP ALERT]\n\n🔴 ▽ -{round(token["percent_change"], 2)}%\n\nToken: {token["symbol"]}\n\nPrice: {token["price"]}\n\nAddress: {token["address"]}\n\n<a href="{token_url}">Live Stats</a>'
        
    
        # Send alert:
        method = "/sendMessage"

        message = {
            "chat_id": user_chat_id,
            "text": alert,
            "parse_mode": "HTML"
        }

        res = await session.post(url=f"{TELEGRAM_API}{BOT_TOKEN}{method}", params=message)
        res.raise_for_status()
    
    logging.info(f"Alerted user with chat id {user_chat_id} for {', '.join([token['symbol'] for token in tokens])}.\n\n")

async def update_token_info(tokens: list, db) -> None:
    for token, stored_token in tokens:
        token_data = [token["symbol"], token["address"], token["logo"], token["price"], token["liquidity"]]
        result = await db.fetch("SELECT * FROM solana_tokens WHERE address = $1", token["address"])
        
        if not result: # If token is not stored in the db, insert it.
            insertion = await db.fetchrow("""INSERT INTO solana_tokens (symbol, address, logo, price, liquidity, last_updated)
                                            VALUES ($1, $2, $3, $4, $5, NOW()) RETURNING *""", *token_data)
            logging.info(f"NEW TOKEN INSERTED: {token['address']}.")
            continue
        
        stored_token = result[0]
        update = await db.fetchrow("UPDATE solana_tokens SET price = $1, liquidity = $2, last_updated = NOW() WHERE address = $3", token["price"], token["liquidity"], token["address"])
        logging.info(f"TOKEN UPDATED: {token['address']}.")

async def main() -> None:
    start_time = time.perf_counter()
    
    # Create database pool
    logging.info("STARTING SOLANA SCRAPER\n")
    await init_pool()

    async with POOL.acquire() as db:
        # Get subscribed users
        users = await db.fetch("SELECT * FROM users")

        # Get latest token data
        tokens = get_token_data()

        # Get changed or new token data
        new_tokens = await get_new_or_changed_tokens(tokens, db)

        # Get alert-worthy tokens
        alert_worthy_tokens = await get_alert_worthy_tokens(new_tokens)

        
        # Update database records
        await update_token_info(new_tokens, db)

    # Send alerts
    if alert_worthy_tokens and users:
        async with AsyncSession() as session:
            await asyncio.gather(*(alert_user(alert_worthy_tokens, user["chat_id"], session) for user in users))
    


    end_time = time.perf_counter()
    logging.info(f"SCRAPING FINISHED IN {round(end_time - start_time, 2)} SECONDS.")



if __name__ == "__main__":
    asyncio.run(main())
