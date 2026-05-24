import os
import pandas as pd
import requests
from time import sleep


userdata    = pd.read_json(os.path.expanduser("~/kite_candles/userdata"))
api_key     = userdata['YOURNAME'].apikey
output_path = os.path.expanduser("~/kite_candles/instruments.csv")

url = "https://api.kite.trade/instruments"

while True:
    print("Fetching instruments list...")
    resp = requests.get(url, params={"api_key": api_key})

    if resp.status_code == 200 and len(resp.content) > 2048:
        break

    print(f"Unexpected response (status={resp.status_code}, size={len(resp.content)}), retrying in 15s")
    sleep(15)

with open(output_path, "w") as f:
    f.write(resp.content.decode("utf-8"))

print(f"Instruments saved to {output_path}")
