import time

from dataclasses import dataclass, fields

import async_timeout
import pandas as pd
from aioinflux import InfluxDBClient
import asyncio

from datetime import datetime, timedelta
from pandas import date_range

from xtrade_essential.utils.taskhandler.async_task import TaskHandler
from xtrade_essential.proto import quotation_pb2
from xtrade_essential.xlib import logger

from stockFeature_lineprotocol_replay2 import StockFeature
# from stockFeature_replay import StockFeature
from sys_module.datatype import TemplateControl
from collections import defaultdict

LOGGER = logger.getLogger()
LOGGER.setLevel("INFO")

import matplotlib.pyplot as plt
import plotly.graph_objects as go
def plotter(df, numP, threshold, threshold_2):
    df.reset_index(inplace = True)
    fig = plt.figure(figsize = (40,16))
    plt.plot(df['timestamp'], df['lastprice'], color = 'blue', linewidth = 0.8, label = "lastprice", alpha = 0.8)
    # plt.scatter(df[df['state'] == "WAVELESS"]['timestamp'], df[df['state'] == "WAVELESS"]['lastprice'], marker = '.', color = 'b', label = "WAVELESS", alpha = 0.5)
    plt.scatter(df[df['state'] == "UP"]['timestamp'], df[df['state'] == "UP"]['lastprice'], color='r', marker = '.', label = "UP")
    plt.scatter(df[df['state'] == "DOWN"]['timestamp'], df[df['state'] == "DOWN"]['lastprice'], color='grey', marker = '.', label = "DOWN")
    plt.title(f'numP = {numP}, threshold = {threshold}, threshold2 = {threshold_2}')
    plt.show()

# def plotly_plotter(df):
#     fig = go.Figure()
#     fig.add_trace(go.Scatter(
#         x=df['timestamp'],
#         y=df['lastprice'],
#         mode = "lines",
#         hovertemplate=
#         '<b>时间: %{x}</b><br><br>' +
#         '最新价: %{y:.2f}<br>' + '<extra></extra>'))

@dataclass
class TickParser:
    timestamp: float
    new_price: float
    ticker: str
    preclose: float

def getTodayDate(previous = True, days = 1):
    if previous:
        days = 1
    else:
        days = 0
    now = datetime.now()
    zero_today = now - timedelta(days = days, hours=now.hour, minutes=now.minute, seconds=now.second,
                                          microseconds=now.microsecond)
    last_today = zero_today + timedelta(days = days, hours=23, minutes=59, seconds=59)
    return zero_today, last_today



class ReplaySimulator:

    def __init__(self, stock_id, numP, threshold, threshold_2):
        self.__task_h = TaskHandler(debug = True)
        client = InfluxDBClient(host="ts-uf6344g88nhjcx1pc.influxdata.tsdb.aliyuncs.com", port=8086,
                                          username="admin", password="Vfgdsm(@12898", timeout=10000,
                                          db="graphna", mode="blocking", ssl=True, output="dataframe")
        zero_today, last_today = getTodayDate(days = 1)
        df = client.query(""" SELECT * FROM "Tick" WHERE time >= %ds AND time <= %ds AND "order_book_id" = '%s' """ %
        (zero_today.timestamp(), last_today.timestamp(), stock_id))
        df.reset_index(inplace = True)
        df['index'] = df['index'].apply(lambda x: int(x.timestamp()))
        self.dataset = df
        client.close()

        self.__replayClient = InfluxDBClient(host="ts-uf6344g88nhjcx1pc.influxdata.tsdb.aliyuncs.com", port=8086,
                                      username="admin", password="Vfgdsm(@12898", timeout = 10000,
                                      db="graphna", mode="async", ssl = True)
        self.queueTickers = asyncio.Queue()
        self.queueResults = asyncio.Queue()
        self.stockFeature = StockFeature(self.queueTickers, self.queueResults, numP=numP, threshold=threshold, threshold_2=threshold_2)

    async def onQuote(self):
        await asyncio.sleep(0)

    async def waveWriter(self):
        self.waves_to_write = []
        while True:
            res = await self.queueResults.get()
            try:
                self.waves_to_write.remove(res)
            except ValueError:
                self.waves_to_write.append(res)
            await asyncio.sleep(0)
            if len(self.waves_to_write) % 500 == 0:
                LOGGER.info(f'Wave Written at {datetime.now()}')
            #     waves_to_write = []

    async def monitorQuote(self):
        for i in self.dataset.index:
            row = self.dataset.loc[i]
            tick = TickParser(timestamp=row["index"],
                              new_price=row["newPrice"],
                              preclose=row["preclose"],
                              ticker=row["ticker"])
            await self.queueTickers.put(tick)



    def run(self):
        try:
            loop = self.__task_h.getLoop()
            # task_waveWriter = asyncio.wait_for(loop.run_in_executor(None, self.waveWriter), timeout=5)
            task_waveWriter = asyncio.ensure_future(self.waveWriter())
            task_feature = asyncio.ensure_future(self.stockFeature.validateTick())
            task_monitor = asyncio.ensure_future(self.monitorQuote())
            finished, unfinished = loop.run_until_complete(asyncio.wait([task_monitor, task_feature, task_waveWriter], timeout=5))
            [task.cancel() for task in unfinished]
            loop.run_until_complete(asyncio.wait(unfinished))
            result = [[v for k, v in item.__dict__.items()] for item in self.waves_to_write]
            df = pd.DataFrame(result, columns = [i.name for i in fields(self.waves_to_write[0])])
            df.drop('ticker', axis = 1, inplace = True)
            for c in df.columns:
                if 'time' in c:
                    df[c] = df[c].apply(lambda x: datetime.fromtimestamp(x / 1e9))

            return df
        except Exception as e:
            print("EXIT")



if __name__ == "__main__":
    # thres_list = [0.001, 0.0008, 0.0007, 0.0006, 0.0005, 0.0004, 0.0003]
    # thres2_list = [x - 0.0002 for x in thres_list]
    # numP = [0.8, 0.7, 0.6, 0.5]
    # for nump in numP:
    #     for i in range(len(thres_list)):
    #         server = ReplaySimulator("399006.SZ", numP = nump, threshold=thres_list[i], threshold_2=thres2_list[i])
    #         df = server.run()
    #         df.set_index("timestamp", inplace = True)
    #         zero_today, _ = getTodayDate()
    #         rangex = str(zero_today.date())
    #         plotter(df, numP = nump, threshold=thres_list[i], threshold_2=thres2_list[i])
    # print("over")

    server = ReplaySimulator("000300.SH", numP=0.8, threshold=0.0007, threshold_2=0.0005)
    df = server.run()
    df.set_index("timestamp", inplace = True)
    zero_today, _ = getTodayDate()
    rangex = str(zero_today.date())
    plotter(df, numP = 0.8, threshold=0.0007, threshold_2=0.0005)