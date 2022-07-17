# -*- coding: utf-8 -*-
import collections

import zmq
import zmq.asyncio
from aioinflux import InfluxDBClient
import asyncio
import time
import copy

from datetime import datetime, timedelta
from pandas import date_range
import pickle
from collections import deque, defaultdict

from xtrade_essential.utils.taskhandler.async_task import TaskHandler
from xtrade_essential.proto import quotation_pb2
from xtrade_essential.xlib import logger

from sgdPy.calculation_module.LoadRQData import RQDataPrep
from sgdPy.calculation_module.Calculation_utils import calculation
from sgdPy.utils.datatype import FactorTemplateControl

LOGGER = logger.getLogger()



class DemoSignalServer:
    def __init__(self):
        self.__task_h = TaskHandler(debug=True)
        self.__updating_dict_all = {}
        self.__updating_dict_refined = {}

        self.__DBClient = InfluxDBClient(host="ts-uf6344g88nhjcx1pc.influxdata.tsdb.aliyuncs.com", port=8086,
                                         username="admin", password="Vfgdsm(@12898",
                                         db="sample707",mode="async", ssl = True)
        self.__DBClient_all = InfluxDBClient(host="ts-uf6344g88nhjcx1pc.influxdata.tsdb.aliyuncs.com", port=8086,
                                         username="admin", password="Vfgdsm(@12898",
                                         db="graphna", mode="async", ssl = True)
        self.allFactorBound = defaultdict(float)
        self.bounds_to_write_all: list = []

        current_hour = time.localtime().tm_hour
        if current_hour < 12:
            self.morning = True
            # self.__updating_dict_all = {}
            # self.__updating_dict_refined = {}
        else:
            self.morning = False
            # with open("Barra_updating_dict_all.pkl", "rb") as f:
            #     self.__updating_dict_all = pickle.load(f)
            #     f.close()
            # with open("Barra_updating_dict_all.pkl", "rb") as f:
            #     self.__updating_dict_refined = pickle.load(f)
            #     f.close()
        self._prepare_data()


    def _prepare_data(self):
        rqdataprep = RQDataPrep()
        self._data_prep = rqdataprep.get_rq_data()
        self.factor_exposure = self._data_prep.reset_index()
        if self.morning:
            now = datetime.now()
        else:
            now = datetime.now() - timedelta(seconds=5400)
        self.factor_exposure.index = date_range(now - timedelta(seconds=0.5), now, len(self.factor_exposure))
        LOGGER.info("FACTOR EXPOSURE OBTAINED.")

        with open("../data/refined_list.pkl", "rb") as f:
            self.stocksRefined = pickle.load(f)
        self.stocksRefined = deque(self.stocksRefined)


    async def barraAll(self):
        """ 间隔执行

        :return:
        """
        if len(self.__updating_dict_all) >= 4250:
            try:
                df_params, Rsq, adj_Rsq, df_X_return, df_X_contrb = calculation(self.__updating_dict_all, self._data_prep, self.morning)
                rsq_dict = {'time': datetime.utcnow(),
                            'measurement': 'R_squared',
                            'fields': {'Rsq': round(Rsq, 5), 'AdjustedRsq': round(adj_Rsq, 5)}}

                for idx in range(len(df_params)):
                    item = df_params.iloc[idx]
                    factor_name = item['factor_name']
                    self.allFactorBound[factor_name] = max(self.allFactorBound[factor_name], 2 * abs(item["factor_return"]))
                    self.bounds_to_write_all.append(FactorTemplateControl(timestamp = df_params.index[idx],
                                                                 factor = factor_name,
                                                                 upperBound = self.allFactorBound[factor_name],
                                                                 lowerBound = -self.allFactorBound[factor_name]))

                await self.__DBClient_all.write(df_params, measurement = "Factor", tag_columns = ['factor_name'])
                await self.__DBClient_all.write(df_X_return, measurement="FactorDetail", tag_columns = ['order_book_id'])
                await self.__DBClient_all.write(df_X_contrb, measurement = "FactorRatio", tag_columns = ['order_book_id'])
                await self.__DBClient_all.write(rsq_dict)
                LOGGER.info("FACTOR DATA IS WRITTEN.")

                if len(self.bounds_to_write_all) >= 100:
                    await self.__DBClient_all.write(self.bounds_to_write_all)
                    self.bounds_to_write_all = []

            except ValueError:
                LOGGER.info("INDUSTRIES HAVE NOT BEEN AGGREGATED.")

    async def barraRefined(self):
        """ 间隔执行

        :return:
        """
        if len(self.__updating_dict_refined) >= 650:
            try:

                df_params, Rsq, adj_Rsq, df_X_return, df_X_contrb = calculation(self.__updating_dict_refined, self._data_prep, self.morning)
                rsq_dict = {'time': datetime.utcnow(),
                            'measurement': 'R_squared',
                            'fields': {'Rsq_707': round(Rsq, 5), 'AdjustedRsq_707': round(adj_Rsq, 5)}}

                await self.__DBClient.write(df_params, measurement = "FactorRefined", tag_columns = ['factor_name'])
                await self.__DBClient.write(df_X_return, measurement="FactorDetailRefined", tag_columns = ['order_book_id'])
                await self.__DBClient.write(df_X_contrb, measurement = "FactorRatioRefined", tag_columns = ['order_book_id'])
                await self.__DBClient.write(rsq_dict)


            except Exception:
                LOGGER.info("INDUSTRIES HAVE NOT BEEN AGGREGATED.")


    async def onQuote(self):
        pass


    async def monitorQuote(self, host, port):
        """接受行情并更新quotes
        :return:
        """
        counter = 0
        await self.__DBClient.write(self.factor_exposure, measurement="FactorExposure", tag_columns=['order_book_id'])
        await self.__DBClient_all.write(self.factor_exposure, measurement="FactorExposure", tag_columns=['order_book_id'])

        addr_str = f'tcp://{host}:{port}'
        ctx = zmq.asyncio.Context()
        socket = ctx.socket(zmq.SUB)
        socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
        socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)

        socket.connect(addr_str)
        socket.setsockopt_unicode(zmq.SUBSCRIBE, u"")

        if self.morning:
            LOGGER.info("Morning Session Start.")
            while True:
                raw = await socket.recv()
                type_, ticker, pb_str = raw.split(b'\t', 2)
                pb_ins = quotation_pb2.Message()
                pb_ins.ParseFromString(pb_str)
                pb_ins.tick_body.timestamp = datetime.strptime(pb_ins.tick_body.datetime_iso8601[:-3],
                                                               "%Y%m%d %H%M%S").timestamp()
                TICKER = pb_ins.tick_body.ticker
                if pb_ins.tick_body.new_price == 0:
                    try:
                        if not pb_ins.tick_body.bps[0] == 0:
                            pb_ins.tick_body.new_price = pb_ins.tick_body.bps[0]
                    except:
                        continue

                # 股票处理
                if pb_ins.security_type == 1:
                    # 连续竞价
                    if pb_ins.trading_session in {1, 2, 3}:
                        self.__updating_dict_all[TICKER] = {"preclose": pb_ins.tick_body.preclose,
                                                            "last": pb_ins.tick_body.new_price}
                        if TICKER in self.stocksRefined:
                            self.__updating_dict_refined[TICKER] = {"preclose": pb_ins.tick_body.preclose,
                                                                    "last": pb_ins.tick_body.new_price}

                    elif pb_ins.trading_session == 4:
                        self.__updating_dict_all[TICKER] = {"preclose": pb_ins.tick_body.preclose,
                                                            "last": pb_ins.tick_body.new_price}
                        if TICKER in self.stocksRefined:
                            self.__updating_dict_refined[TICKER] = {"preclose": pb_ins.tick_body.preclose,
                                                                    "last": pb_ins.tick_body.new_price}
                        counter += 1
                        if counter == 500:
                            temp_all = copy.copy(self.__updating_dict_all)
                            temp_707 = copy.copy(self.__updating_dict_refined)
                            self.__updating_dict_all = {}
                            self.__updating_dict_refined = {}
                            break

                    else:
                        await asyncio.sleep(0)
        LOGGER.info("Morning Session End.")

        # self.morning = False
        # self.__updating_dict_all = temp_all
        # self.__updating_dict_refined = temp_707
        # 下午
        if not self.morning:
            LOGGER.info("Afternoon Session Start.")
            while True:
                raw = await socket.recv()
                type_, ticker, pb_str = raw.split(b'\t', 2)
                pb_ins = quotation_pb2.Message()
                pb_ins.ParseFromString(pb_str)
                pb_ins.tick_body.timestamp = datetime.strptime(pb_ins.tick_body.datetime_iso8601[:-3],
                                                               "%Y%m%d %H%M%S").timestamp() - 5400

                TICKER = pb_ins.tick_body.ticker

                # 股票Tick的处理
                if pb_ins.security_type == 1:
                    if pb_ins.trading_session in {3, 5}:
                        self.__updating_dict_all[TICKER] = {"preclose": pb_ins.tick_body.preclose,
                                                            "last": pb_ins.tick_body.new_price}
                        if TICKER in self.stocksRefined:
                            self.__updating_dict_refined[TICKER] = {"preclose": pb_ins.tick_body.preclose,
                                                                    "last": pb_ins.tick_body.new_price}

                    elif pb_ins.trading_session == 6:
                        self.__updating_dict_all[TICKER] = {"preclose": pb_ins.tick_body.preclose,
                                                            "last": pb_ins.tick_body.new_price}
                        if TICKER in self.stocksRefined:
                            self.__updating_dict_refined[TICKER] = {"preclose": pb_ins.tick_body.preclose,
                                                                    "last": pb_ins.tick_body.new_price}
                        counter += 1
                        if counter == 5000:
                            break

                    else:
                        await asyncio.sleep(0)


    def run(self):
        host = "172.19.36.55"  # 行情地址
        port = "8200"

        loop = self.__task_h.getLoop()
        task_ticks = loop.create_task(self.monitorQuote(host, port))
        self.__task_h.runPeriodicAsyncJob(5, self.barraAll)
        self.__task_h.runPeriodicAsyncJob(5, self.barraRefined)
        loop.run_until_complete(task_ticks)


if __name__ == "__main__":
    server = DemoSignalServer()
    server.run()