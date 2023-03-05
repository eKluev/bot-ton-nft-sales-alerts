import requests
import time
import datetime
from threading import Thread
from core import connector as conn


class Daemon(Thread):
    def __init__(self, name, delay):
        Thread.__init__(self)
        self.name = name
        self.delay = delay

    def run(self):
        chats = ["-1001634937384", "-1001774539187"]
        
        while True:
            data = conn.make_query("select * from alerts as a left join sales as s on s.id = a.sales_id where a.notification_sent = 0")
            ton_usd = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd")
            if ton_usd.status_code != 200 or 'the-open-network' not in ton_usd.json():
                return

            ton_usd = ton_usd.json()['the-open-network']['usd']
            for d in data:
                if d['marketplace'] == "Getgems":
                    message = f"<a href='https://getgems.io/nft/{d['nft_address']}'>{d['name']}</a>"
                else:
                    message = f"<a href='https://beta.disintar.io/object/{d['nft_address']}'>{d['name']}</a>"
                    
                message += "\n\n"
                message += f"#{d['attr_type']}\n\n"
                usd_price = round(ton_usd * float(d['price']), 2)
                message += f"💎 Price: {d['price']} TON | $ {usd_price}\n\n"
                message += f"🛍 Marketplace: #{d['marketplace']}\n\n"
                message += f"🔖 Type of deal: {d['sale_type']}\n\n"
                message += f"🕓 Date & time: {datetime.datetime.fromtimestamp(d['utime'])}\n"
                
                
                for chat in chats:
                    while True:
                        data_msg = {"chat_id": chat, "photo": d['image'] + f'?a={time.time()}', "caption": message, "parse_mode": "HTML"}
                        response = requests.post(url=f"https://api.telegram.org/bot{os.environ.get('telegram_token')}/sendPhoto", data=data_msg)
                        if response.status_code == 200:
                            conn.make_query(f"update alerts set notification_sent=1 where sales_id={d['id']}", commit=True)
                            break
                        else:
                            time.sleep(3)


            time.sleep(self.delay)
