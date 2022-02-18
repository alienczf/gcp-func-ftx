import datetime as dt
import requests
import pytz
import pandas as pd


def getAllToken():
    resp = requests.get('https://ftx.com/api/lt/tokens')
    data = resp.json()['result']
    temp = pd.DataFrame(data)
    return list(temp['name'])


def getRebalances(listOfAllToken):
    dfs = []
    for token in listOfAllToken:
        url = f'https://ftx.com/api/lt/{token}/major_rebalances'
        resp = None
        while resp is None:
            try:
                resp = requests.get(url)
            except Exception as exn:
                print(f'failed to get rebalance for {token}:{type(exn)}')
        df = pd.DataFrame(resp.json()['result'])
        if df.shape[0]:
            df['sym'] = token
            df['time'] = df['time'].astype('<M8[ns]')
            df['date'] = df['time'].dt.date
            dfs.append(df)
    return pd.concat(dfs)


def getPcf():
    resp = requests.get('https://ftx.com/api/lt')
    data = resp.json()['result']
    temp = pd.DataFrame(data)
    temp['pulledTime'] = dt.datetime.now(tz=pytz.utc)
    temp['date'] = temp['pulledTime'].dt.date
    temp['positionsPerShare'] = temp['positionsPerShare'].astype(str)
    temp['basket'] = temp['basket'].astype(str)
    temp['targetComponents'] = temp['targetComponents'].astype(str)
    temp['greeks'] = temp['greeks'].astype(str)
    return temp

def getFutures():
    resp = requests.get('https://ftx.com/api/futures')
    data = resp.json()['result']
    temp = pd.DataFrame(data)
    temp['pulledTime'] = dt.datetime.now(tz=pytz.utc)
    temp['date'] = temp['pulledTime'].dt.date
    return temp

def getFundings(futs):
    res = []
    for fut in futs:
        url = f'https://ftx.com/api/futures/{fut}/stats'
        resp = None
        while resp is None:
            try:
                resp = requests.get(url)
            except Exception as exn:
                print(f'failed to get rebalance for {token}:{type(exn)}')
        stats = resp.json()['result']
        stats['pulledTime'] = dt.datetime.now(tz=pytz.utc)
        stats['sym'] = fut
        res.append(stats)
    df = pd.DataFrame(res)
    df['date'] = df['pulledTime'].dt.date
    return df
