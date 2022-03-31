import logging
import websocket
import json
import _thread
import time
import sqlite3
import pandas as pd
import numpy as np
import math
import xlsxwriter
import os
from dataQuerry import writedata
import csv



# Define socket and connect to the database

cc = 'algousd'
interval = '1m'
socket = f'wss://stream.binance.com:9443/ws/{cc}t@kline_{interval}'

conn = sqlite3.connect('Algo_candles.db')
c = conn.cursor()

c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='main';")

Table = c.fetchone()
print(Table)
# create table for data streams
if Table == 'main':
    c.execute("""CREATE TABLE main (
        close real,
        low real,
        high real,
        time integer,
        symbol text )""")
    conn.commit()


def convert(list):
    return tuple(list)

def buy(price, size):
    url = "https://api.exchange.coinbase.com/orders"

    payload = {
        "profile_id": "default profile_id",
        "type": "limit",
        "side": "buy",
        "stp": "dc",
        "stop": "loss",
        "time_in_force": "GTC",
        "cancel_after": "min",
        "post_only": True,
        "price": f"{price}",
        "size": f"{size}",
        "product_id": "ALGO-USD",
        "client_oid": "496ffbba-7dae-4864-f7e8-a5bf52c6d0e2"
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.request("POST", url, json=payload, headers=headers)

    print(response.text)

def strategy(entry, lookback, qty, open_position=False):
    while True:
        
def on_message(ws, message):
    json_message = json.loads(message)
    candle = json_message['k']
    is_candle_closed = candle['x']
    close = candle['c']
    high = candle['h']
    low = candle['l']
    vol = candle['v']
    time = candle['T']
    symbol = candle['s']

    closes = []
    highs = []
    lows = []
    times = []
    Syms = []
    frame = []
    many_frames = []
    if is_candle_closed:
        closes.append(float(close))
        highs.append(float(high))
        lows.append(float(low))
        times.append(int(time))
        Syms.append(str(symbol))
        frame = closes + highs + lows + times + Syms
        frames = convert(frame)
        print(closes)
        # Connect to database
        # Create Cursor

        c.execute(" INSERT INTO  main VALUES (?,?,?,?,?)", frames)
        conn.commit()
        df = pd.read_sql_query("SELECT * FROM main", conn)
        print(df)



def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    print("Opened connection")


ws = websocket.WebSocketApp(socket, on_message=on_message, on_close=on_close)


ws.run_forever()

c.fetchall()
writedata()

