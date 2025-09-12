import datetime
from collections import deque
from time import sleep
import ta
import pandas as pd
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from logzero import logger
from tabulate import tabulate
import pytz
import requests
import http.client
import json


def number_to_interval(num):
    """
    Convert number to Angel One interval string.
    Supported: 1,3,5,10,15,30,60,1440
    """
    mapping = {
        1: "ONE_MINUTE",
        3: "THREE_MINUTE",
        5: "FIVE_MINUTE",
        10: "TEN_MINUTE",
        15: "FIFTEEN_MINUTE",
        30: "THIRTY_MINUTE",
        60: "ONE_HOUR",
        1440: "ONE_DAY"
    }
    return mapping.get(num, None)  # returns None if not found

def angleone_fetch_profile_and_balance(obj, refresh_token):

    profile_res = obj.getProfile(refresh_token)
    balance_res = obj.rmsLimit()
    data_profile = profile_res['data']
    data_balance = balance_res['data']
    profile= {
        "User ID": data_profile['clientcode'],
        "User Name": data_profile['name'],
        "Email": data_profile['email']
    }
    balance = {
        'Totla Balance': data_balance['net'],
        'Available Margin': data_balance['availablelimitmargin']
    }

    return profile, balance

def angleone_get_token_by_name(name):

    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    resp = requests.get(url)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())

    # Apply instrumenttype rule
    index_list = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
    if name in index_list:
        filtered = df[
            (df['name'] == name) &
            (df['instrumenttype'] == "AMXIDX") &
            (df['exch_seg'] == "NSE")
        ]
    else:
        filtered = df[
            (df['name'] == name) &
            (df['exch_seg'] == "NSE")
        ]

    if filtered.empty:
        return None  # No match found
    else:
        return filtered['token'].iloc[0]  # return first match

def angleone_get_nearest_option_details(api_key,auth_token, smart_api,name, spot_price, option_type, lots, tgt):

    # Index list
    index_list = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]
    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    df = pd.read_json(url)

    # Convert expiry column
    # Handle expiry with multiple formats
    def parse_expiry(x):
        for fmt in ("%Y-%m-%d", "%d%b%Y"):  # 2024-09-26 OR 29DEC2026
            try:
                return datetime.datetime.strptime(str(x), fmt).date()
            except Exception:
                continue
        return None

    df['expiry'] = df['expiry'].apply(parse_expiry)
    df['strike'] = pd.to_numeric(df['strike'], errors='coerce') / 100

    # Step 1: filter name and exchange segment
    df = df[df['name'] == name]
    df = df[df['exch_seg'] == "NFO"]

    # Step 2: instrument type
    if name in index_list:
        df = df[df['instrumenttype'] == "OPTIDX"]
    else:
        df = df[df['instrumenttype'] == "OPTSTK"]

    if df.empty:
        print(f"❌ No instruments found for {name}")
        return None

    # Step 3: find nearest expiry from today
    today = datetime.date.today()
    df = df[df['expiry'] >= today]
    df = df.sort_values("expiry")

    if df.empty:
        print(f"❌ No future expiry available for {name}")
        return None

    nearest_expiry = df['expiry'].iloc[0]
    df = df[df['expiry'] == nearest_expiry]

    # Step 4: choose strike closest to spot price
    df['strike_diff'] = abs(df['strike'] - spot_price)
    min_diff = df['strike_diff'].min()
    df = df[df['strike_diff'] == min_diff]

    # Step 5: filter by option_type from symbol (last 2 chars CE/PE)
    df = df[df['symbol'].str.endswith(option_type)]

    if df.empty:
        print(f"❌ No {option_type} option found for {name} at spot {spot_price}")
        return None

    nearest_option = df.iloc[0].to_dict()
    # Calculate total quantity (lots × lot size)
    lot_size = nearest_option['lotsize']
    total_qty = lots * lot_size
    symboltoken = nearest_option['token']
    tradingsymbol = nearest_option['symbol']
    strike = nearest_option['strike']
    option_tick_size = nearest_option['tick_size']

    option_buffer = deque(maxlen=500)
    ist = pytz.timezone('Asia/Kolkata')

    # Fetch latest intraday data
    option_intraday_data = angleone_get_historical_data(api_key,auth_token, smart_api,"NFO", symboltoken, "ONE_MINUTE")

    if option_intraday_data is None or option_intraday_data.empty or len(option_intraday_data) < 1:
        print("⚠️ Insufficient intraday data for option (need at least 1 candles).")
        return

    # Process only the last two candles
    latest_candle = option_intraday_data.iloc[-1]

    # Add latest candle to option_buffer
    dt_aware = latest_candle.name if latest_candle.name.tzinfo else ist.localize(latest_candle.name)
    candle = {
        'datetime': dt_aware,
        'open': latest_candle['open'],
        'high': latest_candle['high'],
        'low': latest_candle['low'],
        'close': latest_candle['close'],
    }
    option_buffer.append(candle)

    print("+---------------------+----------+----------+----------+----------+")
    print("| Time                | Open     | High     | Low      | Close    |")
    print("+---------------------+----------+----------+----------+----------+")

    for candle in [latest_candle]:
        dt_aware = candle.name if candle.name.tzinfo else ist.localize(candle.name)
        print("| {:<19} | {:>8.2f} | {:>8.2f} | {:>8.2f} | {:>8.2f} |".format(
            dt_aware.strftime('%Y-%m-%d %H:%M'),
            candle['open'],
            candle['high'],
            candle['low'],
            candle['close']
        ))

    print("+---------------------+----------+----------+----------+----------+")

    close_price = float(latest_candle["close"])
    target = (close_price * (100 + tgt)) / 100
    target_price = round(round(target / option_tick_size) * option_tick_size, 2)
    buy_price = close_price
    print(f"Strike Price is: {strike}  {option_type}  Entry: {buy_price},  Target : {target_price}")

    quantity = lots * lot_size

    positions = angleone_fetch_positions(api_key, auth_token)
    if positions:
        count = 0
        for pos in positions:
            quantity_old = pos['quantity']
            symbol = pos['tradingsymbol']
            option_type = symbol[-2:]

            if quantity_old > 0 and (option_type == "PE" or option_type == "CE"):
                print(
                    f"You have live position for the Trading symbol  {symbol}, Skipping the {option_type}Order placing")
            else:
                count += 1
                if count == 1:
                    angleone_gtt_order(api_key,auth_token, tradingsymbol, symboltoken, close_price, quantity)
    else:
        angleone_gtt_order(api_key,auth_token, tradingsymbol, symboltoken, close_price, quantity)

# ----------- Connect to Angel One -------------
def angleone_connect(api_key, client_id, password, TOTP_Secret):
    obj = SmartConnect(api_key=api_key)
    totp = pyotp.TOTP(TOTP_Secret).now()
    data = obj.generateSession(client_id, password, totp)
    refresh_token = data["data"]["refreshToken"]
    auth_token = data['data']['jwtToken']
    feed_token = obj.getfeedToken()
    return obj, refresh_token, auth_token,feed_token


# ----------- Historical Candle Data -------------
def angleone_get_historical_data(api_key,auth_token, smart_api,exchange, symboltoken, interval):

    now = datetime.datetime.now()
    from_dt = (now - datetime.timedelta(days=25)).strftime("%Y-%m-%d %H:%M")
    to_dt = now.strftime("%Y-%m-%d %H:%M")

    params = {
        "exchange": exchange,
        "symboltoken": symboltoken,
        "interval": interval,
        "fromdate": from_dt,
        "todate": to_dt
    }

    headers = {
        'X-PrivateKey': api_key,
        'Accept': 'application/json',
        'X-SourceID': 'WEB',
        'X-ClientLocalIP': 'CLIENT_LOCAL_IP',
        'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
        'X-MACAddress': 'MAC_ADDRESS',
        'X-UserType': 'USER',
        'Authorization': auth_token,
        'Accept': 'application/json',
        'X-SourceID': 'WEB',
        'Content-Type': 'application/json'
    }

    candles = smart_api.getCandleData(params)

    if candles["data"]:
        df = pd.DataFrame(candles['data'], columns=["timestamp", "open", "high", "low", "close", "volume"])

        # Convert timestamp format
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df.set_index('timestamp', inplace=True)
        return df
    else:
        print("❌ Failed to fetch historical data:", candles)


# ----------- Positions Fetch -------------
def angleone_fetch_positions(api_key, auth_token):
    conn = http.client.HTTPSConnection("apiconnect.angelone.in")
    payload = ''
    headers = {
      'Authorization': auth_token,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-UserType': 'USER',
      'X-SourceID': 'WEB',
      'X-ClientLocalIP': 'CLIENT_LOCAL_IP',
      'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
      'X-MACAddress': 'MAC_ADDRESS',
      'X-PrivateKey': api_key
    }
    conn.request("GET", "/rest/secure/angelbroking/order/v1/getPosition", payload, headers)

    res = conn.getresponse()
    data = res.read()
    decoded = data.decode("utf-8")  # Convert bytes → string
    parsed = json.loads(decoded)

    positions = parsed.get("data")
    return positions


# ----------- Place Normal Order -------------
def angle_place_order(api_key,auth_token, symbol, token, quantity,order_type, price):
    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/placeOrder"
    conn = http.client.HTTPSConnection(
        "apiconnect.angelone.in"
    )
    payload = {
        "exchange": "NFO",
        "tradingsymbol": symbol,
        "symboltoken": token,
        "quantity": quantity,
        "disclosedquantity": 0,
        "transactiontype": order_type,
        "ordertype": "MARKET",
        "variety": "NORMAL",
        "producttype": "CARRYFORWARD",
        "price": price,
        "triggerprice": 0,
        "duration": "DAY"
    }
    headers = {
        'Authorization': auth_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-UserType': 'USER',
        'X-SourceID': 'WEB',
        'X-ClientLocalIP': 'CLIENT_LOCAL_IP',
        'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
        'X-MACAddress': 'MAC_ADDRESS',
        'X-PrivateKey': api_key
    }
    response = requests.get(url, headers=headers, timeout=10)
    conn.request("POST", "/rest/secure/angelbroking/order/v1/placeOrder", body=json.dumps(payload), headers=headers)

    res = conn.getresponse()
    data = res.read()
    decoded = data.decode("utf-8")  # Convert bytes → string
    parsed = json.loads(decoded)  # Convert string → dict
    order_id = parsed.get("data", {}).get("orderid")
    return order_id


# ----------- Place GTT Order -------------
def angleone_gtt_order(api_key,auth_token, symbol, token, price, quantity):
    symbol = str(symbol)
    token = str(token)
    try:
        price = float(price.iloc[0] if hasattr(price, "iloc") else float(price))
    except Exception:
        price = float(price)
    quantity = int(quantity)

    conn = http.client.HTTPSConnection("apiconnect.angelone.in")
    payload = { "tradingsymbol": symbol,
                "symboltoken": token,
                "exchange": "NFO",
                "transactiontype": "BUY",
                "producttype": "CARRYFORWARD",
                "price": price,
                "qty": quantity,
                "triggerprice": price,
                "disclosedqty": 0
             }
    headers = {
        'Authorization': auth_token,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'X-UserType': 'USER',
        'X-SourceID': 'WEB',
        'X-ClientLocalIP': 'CLIENT_LOCAL_IP',
        'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
        'X-MACAddress': 'MAC_ADDRESS',
        'X-PrivateKey': api_key
    }

    # ✅ Convert dict to JSON string before sending
    conn.request("POST", "/rest/secure/angelbroking/gtt/v1/createRule", body=json.dumps(payload), headers=headers)

    res = conn.getresponse()
    data = res.read()
    decoded = data.decode("utf-8")  # Convert bytes → string
    parsed = json.loads(decoded)  # Convert string → dict
    id_value = parsed.get("data", {}).get("id")
    return  ( id_value)   # Just the id


# ----------- Close All Positions -------------
def angelone_close_position(obj, pos):

    qty = int(pos['netqty'])
    if qty != 0:
        side = "SELL" if qty > 0 else "BUY"
        position_close =obj.placeOrder({
            "variety": "NORMAL",
            "tradingsymbol": pos['tradingsymbol'],
            "symboltoken": pos['symboltoken'],
            "transactiontype": side,
            "exchange": pos['exchange'],
            "ordertype": "MARKET",
            "producttype": pos['producttype'],
            "duration": "DAY",
            "price": 0,
            "squareoff": "0",
            "stoploss": "0",
            "quantity": abs(qty)
        })
        return position_close

def angleone_trade_conditions_check(obj, auth_token, lots, tgt, indicators_df, credentials, stock,strategy):

    api_key = credentials['api_key']
    client_code = credentials['user_id']
    PIN = credentials['pin']
    TOTP_Secret = credentials['totp_secret']

    # ✅ Check for signal
    if strategy == "ADX_MACD_WillR_Supertrend":
        latest_adx = indicators_df["ADX"].iloc[-1]
        latest_adxema = indicators_df['ADX_EMA21'].iloc[-1]
        latest_willr = indicators_df['WillR_14'].iloc[-1]
        latest_supertrend = indicators_df['Supertrend'].iloc[-1]
        latest_macd = indicators_df['MACD'].iloc[-1]
        latest_macd_signal = indicators_df['MACD_signal'].iloc[-1]
        close_price = float(indicators_df['close'].iloc[-1])
        tgt = float(tgt)

        positions1 = angleone_fetch_positions(api_key, auth_token)
        if positions1:
            for pos in positions1:
                quantity = pos['quantity']
                if quantity > 0:
                    instrument_token = pos['instrument_token']
                    tradingsymbol = pos['tradingsymbol']
                    option_type = tradingsymbol[-2:]

                    if option_type == "CE" and ((latest_willr < -70 and latest_supertrend > close_price) or (
                            latest_willr < -70 and latest_macd < latest_macd_signal) or (
                                                        latest_supertrend > close_price and latest_macd < latest_macd_signal)):
                        angle_place_order(api_key, auth_token, tradingsymbol, instrument_token, quantity ,"SELL", close_price)
                    elif option_type == "PE" and ((latest_willr > -30 and latest_supertrend < close_price) or (
                            latest_willr > -30 and latest_macd > latest_macd_signal) or (
                                                          latest_supertrend < close_price and latest_macd < latest_macd_signal)):
                        angle_place_order(api_key, auth_token, tradingsymbol, instrument_token, quantity ,"SELL", close_price)

        positions = angleone_fetch_positions(api_key, auth_token)
        if latest_adx > latest_adxema and latest_willr > -30 and latest_supertrend < close_price and latest_macd > latest_macd_signal:
            print("🔼\033[92m BUY SIGNAL GENERATED\033[0m")
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "CE":
                            print(
                                f"The existing position is type CE with symbol {tradingsymbol}. No new CALL trade placed ")
                    else:
                        count += 1
                        if count == 1:
                            angleone_get_nearest_option_details(api_key,auth_token, obj,stock, close_price, "CE", lots, tgt)
            else:
                angleone_get_nearest_option_details(api_key,auth_token, obj,stock, close_price, "CE", lots, tgt)

        elif latest_adx > latest_adxema and latest_willr < -70 and latest_supertrend > close_price and latest_macd < latest_macd_signal:
            print("🔽\033[91m SELL SIGNAL GENERATED\033[0m")
            if positions:
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "PE":
                            print(f"The existing position is type PE with symbol {tradingsymbol}. No new PUT trade placed ")
                        angleone_get_nearest_option_details(api_key,auth_token, obj,stock, close_price, "PE", lots, tgt)
            else:
                angleone_get_nearest_option_details(api_key,auth_token, obj,stock, close_price, "PE", lots, tgt)
        else:
            print("⏸️\033[93m NO TRADE SIGNAL GENERATED\033[0m")

    elif strategy == "Ema10_Ema20_Supertrend":
        latest_Ema10 = indicators_df["ema10"].iloc[-1]
        latest_Ema20 = indicators_df['ema20'].iloc[-1]
        latest_supertrend = indicators_df['Supertrend'].iloc[-1]
        close_price = float(indicators_df['close'].iloc[-1])
        tgt = float(tgt)

        positions1 = angleone_fetch_positions(api_key, auth_token)
        if positions1:
            for pos in positions1:
                quantity = pos['quantity']
                if quantity > 0:
                    instrument_token = pos['instrument_token']
                    tradingsymbol = pos['tradingsymbol']
                    option_type = tradingsymbol[-2:]

                    if option_type == "CE" and (latest_Ema10 < latest_Ema20 or latest_supertrend > close_price):
                        angle_place_order(api_key, auth_token, tradingsymbol, instrument_token, quantity, "SELL",
                                          close_price)
                    elif option_type == "PE" and (latest_Ema10 > latest_Ema20 or latest_supertrend < close_price):
                        angle_place_order(api_key, auth_token, tradingsymbol, instrument_token, quantity, "SELL",
                                          close_price)

        positions = angleone_fetch_positions(api_key, auth_token)
        if latest_Ema10 > latest_Ema20 and latest_supertrend < close_price:
            print("🔼\033[92m BUY SIGNAL GENERATED\033[0m")
            if positions:
                count = 0
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "CE":
                            print(
                                f"The existing position is type CE with symbol {tradingsymbol}. No new CALL trade placed ")
                    else:
                        count += 1
                        if count == 1:
                            angleone_get_nearest_option_details(api_key, auth_token, obj, stock, close_price, "CE",
                                                                lots, tgt)
            else:
                angleone_get_nearest_option_details(api_key, auth_token, obj, stock, close_price, "CE", lots, tgt)

        elif latest_Ema10 < latest_Ema20 and latest_supertrend > close_price:
            print("🔽\033[91m SELL SIGNAL GENERATED\033[0m")
            if positions:
                for pos in positions:
                    quantity = pos['quantity']
                    if quantity > 0:
                        tradingsymbol = pos['tradingsymbol']
                        option_type = tradingsymbol[-2:]
                        if option_type == "PE":
                            print(
                                f"The existing position is type PE with symbol {tradingsymbol}. No new PUT trade placed ")
                        angleone_get_nearest_option_details(api_key, auth_token, obj, stock, close_price, "PE", lots,
                                                            tgt)
            else:
                angleone_get_nearest_option_details(api_key, auth_token, obj, stock, close_price, "PE", lots, tgt)
        else:
            print("⏸️\033[93m NO TRADE SIGNAL GENERATED\033[0m")