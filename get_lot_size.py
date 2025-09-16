
import pandas as pd

def lot_size(name):

    instruments = pd.read_csv("https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz")
    instruments['expiry'] = pd.to_datetime(instruments['expiry'], errors='coerce').dt.date
    indices = {"Nifty 50": "NIFTY","Nifty Bank": "BANKNIFTY", "Nifty Fin Service": "FINNIFTY", "NIFTY MID SELECT": "MIDCPNIFTY"}


    if name in indices:
        name = indices[name]
        instrument_type = "OPTIDX"
    else:
        instrument_type = "OPTSTK"

    filtered = instruments[
        (instruments['instrument_type'] == instrument_type) &
        (instruments['name'] == name)
        ]

    if filtered.empty:
        print("❌ No matching option instrument found")
        return

    if not filtered.empty:
        lot_size = filtered.iloc[0]['lot_size']
        print(lot_size)
        return lot_size
    else:
        print("❌ No matching option instrument found")
        return
