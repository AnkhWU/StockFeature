# -*- coding: utf-8 -*-

import os
import asyncio

import multiprocessing
from multiprocessing import Process


import zmq
import zmq.asyncio
from aioinflux import InfluxDBClient
from collections import defaultdict, deque
from datetime import datetime, timedelta
import sys
import traceback
import time
import copy
# import dill

from xtrade_essential.utils.taskhandler.async_task import TaskHandler
from xtrade_essential.proto import quotation_pb2
from xtrade_essential.xlib import logger

import pandas as pd
from sgdPy.stockFeature import StockFeature
from sgdPy.utils.datatype import TemplateControl
from sgdPy.utils.tools import *
from functools import partial

import logging
LOGGER = logger.getLogger()
LOGGER.setLevel("INFO")
# multiprocessing.log_to_stderr(logging.DEBUG)

class BreakTimeError(Exception):
    pass


class TrendSubprocess:
    def __init__(self, process_queue, id_set, index_components, mix_component, preclose_df, process_number):
        current_hour = time.localtime().tm_hour
        if current_hour < 12:
            self.morning: bool = True
        else:
            self.morning = False
        self.tempIndexWave = defaultdict(partial(defaultdict, partial(defaultdict, defaultdict)))  # 需要temp


        self.__task_h = TaskHandler(debug = True)
        self.process_queue = process_queue # 用于进程间通讯
        self.id_set = id_set
        self.indexTickerSet = {'000001.SH', '000016.SH', '000300.SH', '000688.SH', '000905.SH',
                               '399006.SZ'}  # 指数列表，后面最好改成tuple
        self.index_components:defaultdict = index_components
        self.preclose_df = preclose_df.set_index('order_book_id')
        self.index_preclose = get_index_preclose()
        self.process_number = process_number

        self.queueTicks = asyncio.Queue() # 用于传递行情到趋势判断的模块中
        self.queueResults = asyncio.Queue()

        self.stockFeature = StockFeature(self.queueTicks, self.queueResults, mix_component, process_number) # 趋势判断

        self.__DBClient = InfluxDBClient(host="ts-uf6344g88nhjcx1pc.influxdata.tsdb.aliyuncs.com", port=8086,
                                         username="admin", password="Vfgdsm(@12898",
                                         db="graphna",mode="async", ssl = True) # 此Client专用于写入趋势
        self.index_preclose = get_index_preclose()


    async def waveWriter(self):
        waves_up = []
        waves_down = []
        wave_counter = 0
        while True:
            res = await self.queueResults.get()
            wave_counter += 1
            if res.ticker in self.indexTickerSet and res.lastprice != 0.0 and res.show == True:
                try:
                    await self.calculateTrendRatio(res)
                except Exception as e:
                    print("_______________________")
                    print(res.ticker)
                    # Get information about the exception that is currently being handled
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    print('e.message:\t', exc_value)
                    print("Note, object e and exc of Class %s is %s the same." %
                          (type(exc_value), ('not', '')[exc_value is e]))
                    print('traceback.print_exc(): ', traceback.print_exc())
                    print('traceback.format_exc():\n%s' % traceback.format_exc())
                    print("_______________________")
            if res.state == "UP":
                waves_up.append(res)
            elif res.state == "DOWN":
                waves_down.append(res)

            if wave_counter>=6000:
                try:
                    await self.__DBClient.write(waves_up)
                    await self.__DBClient.write(waves_down)
                    LOGGER.info(f'Wave Written at {datetime.now()}')
                    waves_up = []
                    waves_down = []
                    wave_counter = 0
                except Exception as e:
                    LOGGER.warning(e)


    async def QuotaReceiver(self):
        while True:
            await asyncio.sleep(0)
            pb_str = self.process_queue.get()
            if self.process_queue == 0:
                self.process_queue.close()
                break
            pb_ins = quotation_pb2.Message()
            pb_ins.ParseFromString(pb_str)
            # pb_ins.tick_body.timestamp = datetime.strptime(pb_ins.tick_body.datetime_iso8601[:-3],
            #                                                "%Y%m%d %H%M%S").timestamp()
            await self.queueTicks.put(pb_ins)



    async def calculateTrendRatio(self, index_wave):
        """
        Trend Ratio 写入 measurement IndexTrendRatio
        """
        if index_wave.state == "WAVELESS":
            return

        INDEX =  index_wave.ticker
        len_currentwave = copy.copy(len(self.stockFeature.dequeCurrentWave[INDEX]))

        wave_value = index_wave.lastprice / index_wave.waveStartPrice - 1
        indexStartTime = index_wave.startTime
        indexCurrentTime = index_wave.timestamp

        self.tempIndexWave = defaultdict(partial(defaultdict, partial(defaultdict, defaultdict)))

        for TICKER in self.index_components[INDEX]:
            # 先更新最新价格
            component_ticks = self.stockFeature.bufferDict[TICKER]
            if not len(component_ticks) == 0:
                for component_tick in reversed(component_ticks):
                    if component_tick.timestamp <= indexCurrentTime:
                        self.tempIndexWave[INDEX][TICKER]['last_price'] = component_tick.new_price
                        break
                # self.tempIndexTime[INDEX].append(indexStartTime)

            component_waves = self.stockFeature.dequeWaveRecord[TICKER]
            if len(component_waves) > 0:
                for component_wave in reversed(component_waves):
                    if component_wave.timestamp <= indexStartTime:
                        if component_wave.state == "WAVELESS":
                            self.tempIndexWave[INDEX][TICKER]['target_price'] = component_wave.lastprice
                        else:
                            self.tempIndexWave[INDEX][TICKER]['target_price'] = component_wave.waveStartPrice
                        break
            else:
                try:
                    self.tempIndexWave[INDEX][TICKER]["target_price"] = self.stockFeature.bufferDict[TICKER][
                        0].new_price
                except:
                    pass

        if wave_value == 0:
            return

        res_df = pd.DataFrame.from_dict(self.tempIndexWave[INDEX], "index", dtype=float)

        if not res_df.empty:
            daily_return_index = index_wave.lastprice / self.index_preclose[INDEX] - 1
            res_df = res_df.dropna(axis = 0)
            res_df = res_df.loc[~((res_df==0).any(axis = 1))]

            res_df.index.name = "order_book_id"
            res_df = res_df.astype(float)
            res_df = pd.merge(res_df, self.preclose_df, left_index = True, right_index=True)
            res_df['current_return'] = res_df['last_price'] / res_df['preclose'] - 1
            res_df['daily_return'] = res_df['current_return'] / daily_return_index
            res_df['ratio'] = (res_df['last_price'] / res_df['target_price'] - 1) / wave_value
            res_df.reset_index(inplace = True)
            res_df['INDEX'] = INDEX
            res_df['index_id'] = INDEX

            res_df.reset_index(inplace = True)
            index_time = datetime.utcfromtimestamp(indexCurrentTime / 1e9)
            res_df.index = pd.date_range(index_time, index_time, len(res_df))

            await self.__DBClient.write(res_df, measurement = 'IndexState', tag_columns = ['order_book_id', 'INDEX'])
            LOGGER.warning(f"{os.getpid()}: {INDEX} ratio data is written at {datetime.now()}")

        else:
            await asyncio.sleep(0)
            return

    # 10分钟运行一次
    async def time_checker(self):
        """ Periodically check the time. """

        print("here")
        now = datetime.now()
        morning_range1 = datetime(year = now.year, month = now.month, day = now.day, hour = 11, minute = 31, second = 0)
        morning_range2 = datetime(year=now.year, month=now.month, day=now.day, hour=12, minute=50, second=0)
        afternoon_range1 = datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=1, second=0)
        # if now >= morning_range1 and now <= morning_range2:
        if now >= afternoon_range1:
            file_dir = "/home/workspace/wuzhh/sgdPy/temptation/"

            LOGGER.warning(f"{os.getpid()} is to shutdown.")
            try:
                buffer_dict_to_write = {}
                buffer_dict = self.stockFeature.bufferDict.copy()
                for k, v in buffer_dict:
                    buffer_dict_to_write[k] = deque([v_value.SerializeToString() for v_value in v])
                # print("Buffer ticks are serialized.")
                dill.dump(self.tempIndexWave, open(f'{file_dir}tempIndexWave_{self.process_number}.pkl', "wb"))
                dill.dump(self.stockFeature.dequeWaveRecord, open(f'{file_dir}dequeWaveRecord_{self.process_number}.pkl', "wb"))
                dill.dump(self.stockFeature.dequeCurrentWave, open(f'{file_dir}dequeCurrentWave_{self.process_number}.pkl', "wb"))
                dill.dump(self.stockFeature.dequeLast1Wave, open(f'{file_dir}dequeLast1Wave_{self.process_number}.pkl', "wb"))
                dill.dump(self.stockFeature.dequeLast2Wave, open(f'{file_dir}dequeLast2Wave_{self.process_number}.pkl', "wb"))
                dill.dump(self.stockFeature.dequeLast3Wave, open(f'{file_dir}dequeLast3Wave_{self.process_number}.pkl', "wb"))
                dill.dump(buffer_dict_to_write, open(f'{file_dir}bufferDict_{self.process_number}.pkl', "wb"))
                dill.dump(self.stockFeature.dequePriceS, open(f'{file_dir}dequePriceS_{self.process_number}.pkl', "wb"))
                dill.dump(self.stockFeature.dequePriceL, open(f'{file_dir}dequePriceL_{self.process_number}.pkl', "wb"))
                dill.dump(self.stockFeature.dequeValue, open(f'{file_dir}dequeValue_{self.process_number}.pkl', "wb"))
                for task in self.task_list:
                    task.cancel()

            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                print('e.message:\t', exc_value)
                print("Note, object e and exc of Class %s is %s the same." %
                      (type(exc_value), ('not', '')[exc_value is e]))
                print('traceback.print_exc(): ', traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
                for task in self.task_list:
                    task.cancel()

        # if now >= afternoon_range1:
        #     for task in self.task_list:
        #         task.cancel()

            # self.task_receiver.cancel()


    def run(self):
        print('exec Data Process process id : %s, parent process id : %s' % (os.getpid(), os.getppid()))
        # loop = asyncio.get_event_loop()

        self.loop = self.__task_h.getLoop()
        # loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(loop)
        task_receiver = self.loop.create_task(self.QuotaReceiver())
        task_feature = self.loop.create_task(self.stockFeature.validateTick())
        task_waveWriter = self.loop.create_task(self.waveWriter())
        self.task_list = [task_receiver, task_feature, task_waveWriter]
        # task_receiver = asyncio.ensure_future(self.QuotaReceiver())
        # task_feature = asyncio.ensure_future(self.stockFeature.validateTick())
        # task_waveWriter = asyncio.ensure_future((self.waveWriter()))
        self.__task_h.runPeriodicAsyncJob(300, self.time_checker)
        self.loop.run_until_complete(asyncio.gather(*self.task_list))
        raise BreakTimeError




class MultiprocessServer:

    def __init__(self, id_sets):
        self.__DBClient_tick = InfluxDBClient(host="ts-uf6344g88nhjcx1pc.influxdata.tsdb.aliyuncs.com", port=8086,
                                         username="admin", password="Vfgdsm(@12898",
                                         db="graphna",mode="async", ssl = True)
        self.__DBClient_bound = InfluxDBClient(host="ts-uf6344g88nhjcx1pc.influxdata.tsdb.aliyuncs.com", port=8086,
                                               username="admin", password="Vfgdsm(@12898",
                                               db="graphna", mode="async", ssl=True,)
        self._eof = False
        self.id_sets = id_sets
        self.tick_to_distribute = asyncio.Queue()
        self.queueTickersBound = asyncio.Queue()


    async def calMinMax(self):
        minmaxList:list = []
        while True:
            tick = await self.queueTickersBound.get()
            TICKER = tick.tick_body.ticker
            if tick.tick_body.upper_limit == 0:
                tick.tick_body.upper_limit = tick.tick_body.preclose * 1.1
            bound = max(abs(tick.tick_body.high - tick.tick_body.preclose), abs(tick.tick_body.low - tick.tick_body.preclose))
            if bound < (tick.tick_body.upper_limit - tick.tick_body.preclose)/2:
                bound *= 2
            upperBound = tick.tick_body.preclose + bound
            lowerBound = tick.tick_body.preclose - bound

            minmaxList.append(TemplateControl(timestamp = datetime.utcfromtimestamp(tick.tick_body.timestamp),
                                              order_book_id = TICKER,
                                              upperBound = upperBound,
                                              lowerBound = lowerBound))
            if len(minmaxList) == 8000:
                await self.__DBClient_bound.write(minmaxList)
                minmaxList: list = []
                # LOGGER.info(f"Axis DATA IS WRITTEN AT {datetime.now()}")


    async def monitorQuote(self, host, port, dequeDistributor):
        current_hour = time.localtime().tm_hour
        counter = 0

        ctx = zmq.asyncio.Context()
        socket = ctx.socket(zmq.SUB)
        socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 10)
        socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)

        addr_str = f'tcp://{host}:{port}'
        socket.connect(addr_str)
        socket.setsockopt_unicode(zmq.SUBSCRIBE, u"")

        if current_hour < 12:
            ticks_to_write = []
            while True:
                raw = await socket.recv()
                type_, ticker, pb_str = raw.split(b'\t', 2)
                pb_ins = quotation_pb2.Message()
                pb_ins.ParseFromString(pb_str)
                pb_ins.tick_body.timestamp = datetime.strptime(pb_ins.tick_body.datetime_iso8601[:-3],
                                                               "%Y%m%d %H%M%S").timestamp()
                # 指数的处理
                if pb_ins.security_type == 0:
                    tick = indexTickDict(pb_ins.tick_body)
                    ticks_to_write.append(tick)
                    await self.queueTickersBound.put(pb_ins)
                    if len(ticks_to_write) >= 1000:
                        await self.__DBClient_tick.write(ticks_to_write)
                        ticks_to_write = []
                        # LOGGER.info(f"DATA IS WRITTEN AT {datetime.now()}")
                    await self.tick_to_distribute.put((pb_ins.tick_body.ticker, pb_ins.SerializeToString()))

                # 股票处理
                elif pb_ins.security_type == 1:
                    # 连续竞价
                    if pb_ins.trading_session == 3:
                        tick = tickDict(pb_ins.tick_body)
                        ticks_to_write.append(tick)
                        await self.queueTickersBound.put(pb_ins)
                        if len(ticks_to_write) >= 5000:
                            await self.__DBClient_tick.write(ticks_to_write)
                            ticks_to_write = []
                            # LOGGER.info(f"DATA IS WRITTEN AT {datetime.now()}")

                        await self.tick_to_distribute.put((pb_ins.tick_body.ticker, pb_ins.SerializeToString()))

                    # 开盘集合竞价
                    elif pb_ins.trading_session == 1:

                        tick = tickDict_opening(pb_ins.tick_body)
                        if not tick:
                            continue
                        ticks_to_write.append(tick)
                        await self.queueTickersBound.put(pb_ins)
                        if len(ticks_to_write) >= 800:
                            try:
                                await self.__DBClient_tick.write(ticks_to_write)
                                ticks_to_write = []
                                # LOGGER.info(f"DATA IS WRITTEN AT {datetime.now()}")
                            except:
                                ticks_to_write.pop()

                        await self.tick_to_distribute.put((pb_ins.tick_body.ticker, pb_ins.SerializeToString()))


                    # 开盘集合竞价后
                    elif pb_ins.trading_session == 2:
                        tick = tickDict_opening(pb_ins.tick_body)
                        if not tick:
                            continue
                        ticks_to_write.append(tick)
                        await self.queueTickersBound.put(pb_ins)
                        if len(ticks_to_write) >= 100:
                            try:
                                await self.__DBClient_tick.write(ticks_to_write)
                                ticks_to_write = []
                                # LOGGER.info(f"DATA IS WRITTEN AT {datetime.now()}")
                            except:
                                ticks_to_write.pop()
                        await self.tick_to_distribute.put((pb_ins.tick_body.ticker, pb_ins.SerializeToString()))

                    # 中午休市
                    elif pb_ins.trading_session == 4:
                        await self.tick_to_distribute.put((pb_ins.tick_body.ticker, pb_ins.SerializeToString()))
                        counter += 1
                        if counter == 500:
                            if not len(ticks_to_write) == 0:
                                await self.__DBClient_tick.write(ticks_to_write)
                            for task in self.task_list:
                                task.cancel()
                            await asyncio.sleep(0)
                            break

                    else:
                        await asyncio.sleep(0)

        # 下午
        if current_hour > 12:

            ticks_to_write = []
            while True:

                raw = await socket.recv()
                type_, ticker, pb_str = raw.split(b'\t', 2)
                pb_ins = quotation_pb2.Message()
                pb_ins.ParseFromString(pb_str)
                pb_ins.tick_body.timestamp = datetime.strptime(pb_ins.tick_body.datetime_iso8601[:-3],
                                                               "%Y%m%d %H%M%S").timestamp()-5400

                # 指数的处理
                if pb_ins.security_type == 0:
                    tick = indexTickDict(pb_ins.tick_body)
                    ticks_to_write.append(tick)
                    await self.queueTickersBound.put(pb_ins)
                    if len(ticks_to_write) >= 2000:
                        await self.__DBClient_tick.write(ticks_to_write)
                        ticks_to_write = []
                        # LOGGER.info(f"DATA IS WRITTEN AT {datetime.now()}")
                    await self.tick_to_distribute.put((pb_ins.tick_body.ticker, pb_ins.SerializeToString()))

                # 股票Tick的处理
                elif pb_ins.security_type == 1:
                    if pb_ins.trading_session == 3:
                        tick = tickDict(pb_ins.tick_body)
                        ticks_to_write.append(tick)
                        await self.queueTickersBound.put(pb_ins)
                        if len(ticks_to_write) == 5000:
                            await self.__DBClient_tick.write(ticks_to_write)
                            ticks_to_write = []
                            # LOGGER.info(f"DATA IS WRITTEN AT {datetime.now()}")
                        await self.tick_to_distribute.put((pb_ins.tick_body.ticker, pb_ins.SerializeToString()))
                        # await self.queueTickers.put(pb_ins)

                    # 收盘前集合竞价
                    elif pb_ins.trading_session == 5:
                        tick = tickDict_opening(pb_ins.tick_body)
                        if not tick:
                            continue
                        ticks_to_write.append(tick)
                        await self.queueTickersBound.put(pb_ins)

                        if len(ticks_to_write) >= 100:
                            try:
                                await self.__DBClient_tick.write(ticks_to_write)
                                ticks_to_write = []
                            except Exception as e:
                                ticks_to_write.pop()

                        await self.tick_to_distribute.put((pb_ins.tick_body.ticker, pb_ins.SerializeToString()))

                    elif pb_ins.trading_session == 6:

                        # break

                        counter += 1
                        await self.tick_to_distribute.put((pb_ins.tick_body.ticker, pb_ins.SerializeToString()))
                        if counter == 5:
                            await asyncio.sleep(0)
                            if not len(ticks_to_write) == 0:
                                await self.__DBClient_tick.write(ticks_to_write)
                            for i in list(range(len(self.id_sets))):
                                if ticker in self.id_sets[i]:
                                    dequeDistributor[i].put(0)
                            for task in self.task_list:
                                task.cancel()
                            await asyncio.sleep(0)
                            break

                    else:
                        await asyncio.sleep(0)


    async def tickDistributor(self, dequeDistributor):
        tgt_list = list(range(len(self.id_sets)))
        while True:
            ticker, tick_pending = await self.tick_to_distribute.get()
            for i in tgt_list:
                if ticker in self.id_sets[i]:
                    dequeDistributor[i].put(tick_pending)


    def run(self, dequeDistributor):
        print('exec Monitor Engine child process id : %s, parent process id : %s' % (os.getpid(), os.getppid()))
        host = "172.19.36.55"  # 行情地址
        port = "8200"

        loop = asyncio.get_event_loop()
        # loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(loop)
        task_monitor = loop.create_task(self.monitorQuote(host, port, dequeDistributor))
        task_distributor = loop.create_task(self.tickDistributor(dequeDistributor))
        task_MinMax = loop.create_task(self.calMinMax())
        self.task_list = [task_monitor, task_distributor, task_MinMax]
        loop.run_until_complete(asyncio.gather(*self.task_list))
        raise BreakTimeError



if __name__ == "__main__":


    def main():
        # process_count = multiprocessing.cpu_count() - 1
        process_count = 2
        dequeDistributor = deque([multiprocessing.Queue() for i in range(process_count)])
        id_sets, component_sets, mix_components, preclose_dfs = prepare_data(batch_num = process_count)


        server = MultiprocessServer(id_sets)
        trendSubprocessDeque = deque()

        for i in range(process_count):
            trendSubprocessDeque.append(TrendSubprocess(dequeDistributor[i], id_sets[i], component_sets[i], mix_components[i], preclose_dfs[i], i))

        # processes = [SubHandlerProcess(target=trendSubprocessDeque[i].run, args=()) for i in range(process_count)]
        processes = [Process(target=server.run, args = (dequeDistributor,))] \
                       + [Process(target=trendSubprocessDeque[i].run, args=()) for i in range(process_count)]
        for p in processes:
            # p.daemon = True
            p.start()


    main()