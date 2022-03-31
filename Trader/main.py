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

cc = 'btcusd'
interval = '15m'
socket = f'wss://stream.binance.com:9443/ws/{cc}t@kline_{interval}/ethusdt@kline_{interval}/mirusdt@kline_{interval}/kncusdt@kline_{interval}'

conn = sqlite3.connect('Crypto_candles.db')
c = conn.cursor()

c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='main';")

Table = c.fetchone()
print(Table)
# create table for data streams
if Table == 0:
    c.execute("""CREATE TABLE main (
        close real,
        low real,
        high real,
        time integer,
        symbol text )""")
    conn.commit()


def convert(list):
    return tuple(list)


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
        print(frames)
        # Connect to database
        # Create Cursor

        c.execute(" INSERT INTO  main VALUES (?,?,?,?,?)", frames)
        conn.commit()


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    print("Opened connection")


ws = websocket.WebSocketApp(socket, on_message=on_message, on_close=on_close)

df = pd.read_sql_query("SELECT * FROM main", conn)
print(df.head())
ws.run_forever()

c.fetchall()
writedata()

