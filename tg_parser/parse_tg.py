import pandas as pd
from telethon import TelegramClient
from dotenv import load_dotenv
import os

load_dotenv()

api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")

async def get_air_alarm_messages(client: TelegramClient, channel: str, limit: int = 1000):
    outp = []
    async for m in client.iter_messages(channel, limit=limit):
        outp.append({
            "id": m.id,
            "date": m.date,
            "text": m.text
        })
    
    df = pd.DataFrame(outp)
    return df

def detect_alarm_type(text: str) -> str:
    text = text.lower()

    if "міг" in text or "mig" in text or "зліт" in text:
        return "МіГ-31К"

    if "ракет" in text or "калібр" in text or "кр" in text:
        return "Ракетна"
    
    if "инджал" in text or "кинджал" in text:
        return "Кинджал"

    if "баліс" in text or "iskander" in text:
        return "Балістика"

    if "шахед" in text or "бпла" in text or "дрон" in text:
        return "БпЛА/Шахеди"

    return None 


def create_df(tg_df):
    outp = []

    started = None
    alarm_type = None
    need_type = False
    search_window = 0

    for alarm_tg in tg_df.iloc[::-1].itertuples():
        text = str(alarm_tg.text).lower()

        if "🔴" in text or "повітряна тривога" in text:
            started = pd.to_datetime(alarm_tg.date)

            alarm_type = detect_alarm_type(text)

            need_type = alarm_type is None

            search_window = 10
            continue

        if need_type and search_window > 0:
            detected = detect_alarm_type(text)
            search_window -= 1

            if detected:
                alarm_type = detected
                need_type = False
        
            
        if "🟢" in text or "відбій" in text:
            if started:
                finished = pd.to_datetime(alarm_tg.date)
                duration = (finished - started).total_seconds() / 60

                outp.append({
                    "started": started,
                    "finished": finished,
                    "duration_min": round(duration, 2),
                    "alarm_type": alarm_type if alarm_type else "Невідомо"
                })
            
            started = None
            alarm_type = None
            need_type = False
            search_window = 0

        

    return pd.DataFrame(outp)

            

if __name__ == "__main__":
    client = TelegramClient(
        "air_raid_stat_session",
        api_id,
        api_hash
    )
    async def main():
        df_messages = await get_air_alarm_messages(
            client,
            "https://t.me/LvivAlarm",
            limit=1500
        )
        df_messages.to_csv("tg_parser/lviv_alarm_messages.csv", index=False)
        return df_messages

    with client:
        df_messages = client.loop.run_until_complete(main())

    alarm_type_df = create_df(df_messages)
    print(alarm_type_df)
    alarm_type_df.to_csv("./datasets/tg_parsed_alarms.csv", index=False)
