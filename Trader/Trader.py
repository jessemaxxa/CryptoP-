import requests
import pandas as pd
import numpy as np
import sqlite3





conn = sqlite3.connect('Algo_candles.db')
c = conn.cursor()

df = pd.read_sql_query("SELECT * FROM main", conn)

dfc = df['close']

close = df['close']
symbol = df['symbol']

# tick = df.loc[:-1:-15, ['close']]
print(dfc.pct_change(periods=5))