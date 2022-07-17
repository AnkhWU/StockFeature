import pdb
from datetime import datetime, timedelta
from functools import partial
from aioinflux import InfluxDBClient
from collections import deque
import traceback
import sys
import pickle

from xtrade_essential.xlib import logger
import logging
import copy

import numpy as np
from utils.datatype_replay import *
from xtrade_essential.proto import quotation_pb2

LOGGER = logger.getLogger()
LOGGER.setLevel("INFO")
# handler = logging.FileHandler(f'log_20210901.txt')
# handler.setLevel(logging.ERROR)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# LOGGER.addHandler(handler)


class StockFeature:

    def __init__(self, queue_bars, queue_results, *args, **kwargs):
        self.interval = int(30 * 1e9)
        self.waveInterval = int(60 * 1e9)
        self.queueTicks = queue_bars
        self.queueResults = queue_results

        self.dequeQuotaWave = defaultdict(deque)  # 存放QuotaDataAndWave对象的双端队列
        self.bufferDict = defaultdict(partial(deque, maxlen=90))
        self.dequePriceS = defaultdict(partial(deque, maxlen=5)) # 存放Wave的deque
        self.dequePriceL = defaultdict(partial(deque, maxlen=30))
        self.dequeValue = defaultdict(partial(deque, maxlen=30))

    async def validateTick(self) -> None:
        # 创建一个值限定为 最大长度90 deque的defaultdict，若键ticker不存在，会返回一个空的deque，长度为0

        while True:
            tick = await self.queueTicks.get() # 此处queueTicker为全局变量，后作修改
            TICKER = tick.ticker
            # 若最新价格为0，替换为前一天收盘价或者上一个最新价格
            if tick.new_price == 0.0:
                if len(self.bufferDict[TICKER]) == 0:
                    tick.new_price = tick.preclose
                else:
                    tick.new_price = self.bufferDict[TICKER][-1].new_price
            self.bufferDict[TICKER].append(tick)
            # if tick.timestamp == 1630495844000000000/1e9:
            #     print("here")
            # 若ticker的deque长度大于5，调用 getWave 方法
            await self.getWave(TICKER)

    async def getWave(self, TICKER: str) -> None:
        """
        :param pb_deque:
        :return:
        """
        pb_deque = self.bufferDict[TICKER] # 取出该ticker的队列
        # 初始化其对应的数据类QuotaDataAndWave的deque
        self.dequePriceS[TICKER].append(pb_deque[-1].new_price)
        self.dequePriceL[TICKER].append(pb_deque[-1].new_price)

        if len(pb_deque) < 6:
            current_timestamp = int(pb_deque[-1].timestamp * 1e9)
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp = current_timestamp,
                                        order_book_id = TICKER,
                                        ticker = TICKER,
                                        lastprice = pb_deque[-1].new_price))
            # tick数量少于6时不做趋势判断
            return

        # 确定threshold
        self.__getThreshold(TICKER, pb_deque[-1].preclose)
        # 在deque里收集收益率
        self.dequeValue[TICKER].append(pb_deque[-1].new_price/pb_deque[-2].new_price)

        # 此处的[-1]索引指向上一个tick
        try:
            if self.dequeQuotaWave[TICKER][-1].state == WaveType.WAVELESS:
                self.__switchFromWaveless(pb_deque, TICKER)

            elif self.dequeQuotaWave[TICKER][-1].state == WaveType.UP:
                self.__switchFromUp(pb_deque, TICKER)

            elif self.dequeQuotaWave[TICKER][-1].state == WaveType.DOWN:
                self.__switchFromDown(pb_deque, TICKER)
        except ZeroDivisionError:
            print("there")

        # 后面的这些[-1]的索引都是指向当前的tick
        await self.queueResults.put(self.dequeQuotaWave[TICKER][-1])

        lastHighLevelTime = self.dequeQuotaWave[TICKER][-1].lastHighLevelTime
        lastLowLevelTime = self.dequeQuotaWave[TICKER][-1].lastLowLevelTime
        current_timestamp = int(pb_deque[-1].timestamp * 1e9)

        if lastLowLevelTime != -1 or lastHighLevelTime != -1:
            try:
                if self.dequeQuotaWave[TICKER][-1].lastprice >= self.dequeQuotaWave[TICKER][-1].lastHighLevelPrice \
                    and (current_timestamp - lastHighLevelTime) < self.waveInterval:
                    await self.__modifyBackUp(TICKER)
                elif self.dequeQuotaWave[TICKER][-1].lastprice <= self.dequeQuotaWave[TICKER][-1].lastLowLevelPrice \
                    and (current_timestamp - lastLowLevelTime) < self.waveInterval:
                    await self.__modifyBackDown(TICKER)


            except Exception as e:
                print('str(Exception):\t', str(Exception))
                print('str(e):\t\t', str(e))
                print('repr(e):\t', repr(e))
                print(TICKER)
                # with open(f'ExceptionFiles/{TICKER}.pkl', 'wb') as f:
                #     pickle.dump(self.dequeQuotaWave[TICKER], f)
                #     f.close()
                # Get information about the exception that is currently being handled
                exc_type, exc_value, exc_traceback = sys.exc_info()
                print('e.message:\t', exc_value)
                print("Note, object e and exc of Class %s is %s the same." %
                      (type(exc_value), ('not', '')[exc_value is e]))
                print('traceback.print_exc(): ', traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())


    def __switchFromWaveless(self, pb_deque: deque, TICKER: str) -> None:
        """
        :param current_tick:
        :param TICKER:
        :return:
        """
        lastDequeQuotaWave: QuotaDataAndWave = self.dequeQuotaWave[TICKER][-1]
        current_tick = copy.copy(pb_deque[-1])
        current_timestamp = int(current_tick.timestamp * 1e9)
        short_MIN = self.dequePriceS[TICKER][np.argmin(self.dequePriceS[TICKER])] # 用于判断快速上涨
        short_MAX = self.dequePriceS[TICKER][np.argmax(self.dequePriceS[TICKER])] # 用于判断快速下跌
        long_MIN = self.dequePriceL[TICKER][np.argmin(self.dequePriceL[TICKER])] # 用于判断慢速上涨
        long_MAX = self.dequePriceL[TICKER][np.argmax(self.dequePriceL[TICKER])] # 用于判断慢速下跌

        # 快速上涨趋势开始
        if (self.dequePriceS[TICKER][-1]/short_MIN - 1) >= self.threshold_1:
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp=current_timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice = current_tick.new_price,
                            state=WaveType.UP,
                            waveStartPrice=current_tick.new_price,
                            value=current_tick.new_price / short_MIN - 1,
                            price=current_tick.new_price,
                            highLowValue=current_tick.new_price / short_MIN - 1,
                            startTime=current_timestamp,
                            waveOver=0,
                            endTime= -1,
                            highLowTime=current_timestamp,
                            lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                            lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                            lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                            lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                            lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                            lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                            lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                            lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                            ))
        # 快速下跌趋势开始
        elif (self.dequePriceS[TICKER][-1]/short_MAX - 1) <= -self.threshold_1:
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp=current_timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=current_tick.new_price,
                            state=WaveType.DOWN,
                            waveStartPrice=current_tick.new_price,
                            value=current_tick.new_price / short_MAX - 1,
                            price=current_tick.new_price,
                            highLowValue=current_tick.new_price / short_MAX - 1,
                            startTime=current_timestamp,
                            waveOver=0,
                            endTime=-1,
                            highLowTime=current_timestamp,
                            lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                            lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                            lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                            lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                            lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                            lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                            lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                            lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                            ))

        # 慢速上涨趋势开始，先保证前面没有快速上涨和下跌
        elif (self.dequePriceS[TICKER][-1]/long_MIN - 1) >= self.threshold_2 \
            and len(np.argwhere(np.array(self.dequeValue[TICKER]) > 0)) > len(self.dequeValue[TICKER])*0.8:
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp=current_timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=current_tick.new_price,
                            state=WaveType.UP,
                            waveStartPrice=current_tick.new_price,
                            value=current_tick.new_price / long_MIN - 1,
                            price=current_tick.new_price,
                            highLowValue=current_tick.new_price / long_MIN - 1,
                            startTime=current_timestamp,
                            waveOver=0,
                            endTime=-1,
                            highLowTime=current_timestamp,
                            lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                            lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                            lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                            lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                            lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                            lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                            lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                            lastDownStartTime=lastDequeQuotaWave.lastDownStartTime))

        # 慢速下跌趋势开始
        elif (self.dequePriceS[TICKER][-1] / long_MAX - 1) < -self.threshold_2 \
            and len(np.argwhere(np.array(self.dequeValue[TICKER]) > 0)) < len(self.dequeValue[TICKER]) * 0.2:
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp=current_timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=current_tick.new_price,
                            state=WaveType.DOWN,
                            waveStartPrice=current_tick.new_price,
                            value=current_tick.new_price / long_MAX - 1,
                            price=current_tick.new_price,
                            highLowValue=current_tick.new_price / long_MAX - 1,
                            startTime=current_timestamp,
                            waveOver=0,
                            endTime=-1,
                            highLowTime=current_timestamp,
                            lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                            lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                            lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                            lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                            lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                            lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                            lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                            lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                            ))
            LOGGER.info(f'{TICKER}: Slow down trend starts.')

        else:
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp=current_timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=current_tick.new_price,
                            price=current_tick.new_price,
                            lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                            lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                            lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                            lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                            lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                            lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                            lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                            lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                            ))


    def __switchFromUp(self, pb_deque: quotation_pb2.Message, TICKER: str) -> None:
        lastDequeQuotaWave: StateRecord = self.dequeQuotaWave[TICKER][-1]
        current_tick = copy.copy(pb_deque[-1])
        current_timestamp = int(current_tick.timestamp * 1e9)
        previous_tick = copy.copy(pb_deque[-2])

        time_from_HL = current_timestamp - lastDequeQuotaWave.highLowTime
        run_time = current_timestamp - lastDequeQuotaWave.startTime

        # 价格创新高，趋势继续
        if (current_tick.new_price >= lastDequeQuotaWave.price):
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp=current_timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=current_tick.new_price,
                            state=WaveType.UP,
                            waveStartPrice = lastDequeQuotaWave.waveStartPrice,
                            value=(current_tick.new_price / previous_tick.new_price - 1)
                                  + lastDequeQuotaWave.value,
                            price=current_tick.new_price,
                            highLowValue=(current_tick.new_price / previous_tick.new_price - 1)
                                         + lastDequeQuotaWave.value,
                            startTime=lastDequeQuotaWave.startTime,
                            waveOver=0,
                            endTime=-1,
                            highLowTime=current_timestamp,
                            lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                            lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                            lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                            lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                            lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                            lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                            lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                            lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                            ))

        elif (current_tick.new_price >= (1 - self.threshold_end) * lastDequeQuotaWave.price):
            if (current_tick.new_price < lastDequeQuotaWave.price):
                # 价格盘整，趋势继续
                if time_from_HL <= self.interval:
                    self.dequeQuotaWave[TICKER].append(
                        StateRecord(timestamp=current_timestamp,
                                    order_book_id=TICKER,
                                    ticker=TICKER,
                                    lastprice=current_tick.new_price,
                                    state=WaveType.UP,
                                    waveStartPrice=lastDequeQuotaWave.waveStartPrice,
                                    value=(current_tick.new_price / previous_tick.new_price - 1)
                                          + lastDequeQuotaWave.value,
                                    price=lastDequeQuotaWave.price,
                                    highLowValue=lastDequeQuotaWave.highLowValue,
                                    highLowTime = lastDequeQuotaWave.highLowTime,
                                    startTime=lastDequeQuotaWave.startTime,
                                    waveOver=0,
                                    endTime=-1,
                                    lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                    lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                    lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                    lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                    lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                    lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                    lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                    lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                                    ))

                # 价格盘整，超出时间，趋势初步结束
                else:
                    self.dequeQuotaWave[TICKER].append(
                        StateRecord(timestamp=current_timestamp,
                                    order_book_id=TICKER,
                                    ticker=TICKER,
                                    lastprice=current_tick.new_price,
                                    state=WaveType.WAVELESS,
                                    waveStartPrice=-1.0,
                                    value=-1.0,
                                    price=-1.0,
                                    highLowValue=-1.0,
                                    startTime=-1,
                                    waveOver=1,
                                    endTime=current_timestamp,
                                    highLowTime=-1,
                                    lastHighLevelTime=lastDequeQuotaWave.highLowTime,
                                    lastHighLevelValue=lastDequeQuotaWave.highLowValue,
                                    lastHighLevelPrice=lastDequeQuotaWave.price,
                                    lastUpStartTime=lastDequeQuotaWave.startTime,
                                    lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                    lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                    lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                    lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                                    ))

            # 涨停情况时
            elif (current_tick.new_price <= lastDequeQuotaWave.price) and (time_from_HL > self.interval):
                self.dequeQuotaWave[TICKER].append(
                    StateRecord(timestamp=current_timestamp,
                                order_book_id=TICKER,
                                ticker=TICKER,
                                lastprice=current_tick.new_price,
                                state=WaveType.WAVELESS,
                                waveStartPrice=-1.0,
                                value=-1.0,
                                price=-1.0,
                                highLowValue=-1.0,
                                startTime=-1,
                                waveOver=1,
                                endTime=current_timestamp,
                                highLowTime=-1,
                                lastHighLevelTime=lastDequeQuotaWave.highLowTime,
                                lastHighLevelValue=lastDequeQuotaWave.highLowValue,
                                lastHighLevelPrice=lastDequeQuotaWave.price,
                                lastUpStartTime=lastDequeQuotaWave.startTime,
                                lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                                ))


        # 回调大于阈值，趋势初步结束，下跌趋势开始
        elif (current_tick.new_price < (1 - self.threshold_end) * lastDequeQuotaWave.price):
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp=current_timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=current_tick.new_price,
                            state=WaveType.DOWN,
                            waveStartPrice=current_tick.new_price,
                            value=current_tick.new_price / lastDequeQuotaWave.price - 1,
                            price=current_tick.new_price,
                            highLowValue=current_tick.new_price / lastDequeQuotaWave.price - 1,
                            startTime=current_timestamp,
                            waveOver=1,
                            endTime=current_timestamp,
                            highLowTime=current_timestamp,
                            lastHighLevelTime=lastDequeQuotaWave.highLowTime,
                            lastHighLevelValue=lastDequeQuotaWave.highLowValue,
                            lastHighLevelPrice=lastDequeQuotaWave.price,
                            lastUpStartTime=lastDequeQuotaWave.startTime,
                            lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                            lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                            lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                            lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                            ))


    def __switchFromDown(self, pb_deque: deque, TICKER: str) -> None:
        """
        :param pb_deque:
        :param TICKER:
        :return:
        """

        lastDequeQuotaWave: Wave_ = self.dequeQuotaWave[TICKER][-1]
        current_tick = copy.copy(pb_deque[-1])
        current_timestamp = int(current_tick.timestamp * 1e9)
        previous_tick = copy.copy(pb_deque[-2])
        # previous_tick.timestamp = int(previous_tick.timestamp * 10**9)
        time_from_HL = current_timestamp - lastDequeQuotaWave.highLowTime
        run_time = current_timestamp - lastDequeQuotaWave.startTime

        # 价格创新低，趋势继续
        if (current_tick.new_price <= lastDequeQuotaWave.price):
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp=current_timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=current_tick.new_price,
                            state=WaveType.DOWN,
                            waveStartPrice=lastDequeQuotaWave.waveStartPrice,
                            value=(current_tick.new_price / previous_tick.new_price - 1)
                                  + lastDequeQuotaWave.value,
                            price=current_tick.new_price,
                            highLowValue=(current_tick.new_price / previous_tick.new_price - 1)
                                         + lastDequeQuotaWave.value,
                            startTime=lastDequeQuotaWave.startTime,
                            waveOver=0,
                            endTime=-1,
                            highLowTime=current_timestamp,
                            lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                            lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                            lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                            lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                            lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                            lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                            lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                            lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                            ))

        elif (current_tick.new_price <= (1 + self.threshold_end) * lastDequeQuotaWave.price):
            if (current_tick.new_price > lastDequeQuotaWave.price):
                # 价格盘整，趋势继续
                if time_from_HL <= self.interval:
                    self.dequeQuotaWave[TICKER].append(
                        StateRecord(timestamp=current_timestamp,
                                    order_book_id=TICKER,
                                    ticker=TICKER,
                                    lastprice=current_tick.new_price,
                                    state=WaveType.DOWN,
                                    waveStartPrice=lastDequeQuotaWave.waveStartPrice,
                                    value=(current_tick.new_price / previous_tick.new_price - 1)
                                          + lastDequeQuotaWave.value,
                                    price=lastDequeQuotaWave.price,
                                    highLowValue=lastDequeQuotaWave.highLowValue,
                                    startTime=lastDequeQuotaWave.startTime,
                                    waveOver=0,
                                    endTime=-1,
                                    highLowTime=lastDequeQuotaWave.highLowTime,
                                    lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                    lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                    lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                    lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                    lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                    lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                    lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                    lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                                    ))

                # 价格盘整，超出时间，趋势初步结束
                else:
                    self.dequeQuotaWave[TICKER].append(
                        StateRecord(timestamp=current_timestamp,
                                    order_book_id=TICKER,
                                    ticker=TICKER,
                                    lastprice=current_tick.new_price,
                                    state=WaveType.WAVELESS,
                                    waveStartPrice=-1.0,
                                    value=-1.0,
                                    price=-1.0,
                                    highLowValue=-1.0,
                                    startTime=-1,
                                    waveOver=1,
                                    endTime=current_timestamp,
                                    highLowTime=-1,
                                    lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                    lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                    lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                    lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                    lastLowLevelTime=lastDequeQuotaWave.highLowTime,
                                    lastLowLevelValue=lastDequeQuotaWave.highLowValue,
                                    lastLowLevelPrice=lastDequeQuotaWave.price,
                                    lastDownStartTime=lastDequeQuotaWave.startTime
                                    ))

            # 跌停情况时
            elif (current_tick.new_price >= lastDequeQuotaWave.price) and (time_from_HL > self.interval):
                self.dequeQuotaWave[TICKER].append(
                    StateRecord(timestamp=current_timestamp,
                                order_book_id=TICKER,
                                ticker=TICKER,
                                lastprice=current_tick.new_price,
                                state=WaveType.WAVELESS,
                                waveStartPrice=-1.0,
                                value=-1.0,
                                price=-1.0,
                                highLowValue=-1.0,
                                startTime=-1,
                                waveOver=1,
                                endTime=current_timestamp,
                                highLowTime=-1,
                                lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                lastLowLevelTime=lastDequeQuotaWave.highLowTime,
                                lastLowLevelValue=lastDequeQuotaWave.highLowValue,
                                lastLowLevelPrice=lastDequeQuotaWave.price,
                                lastDownStartTime=lastDequeQuotaWave.startTime
                                ))



        # 回调大于阈值，趋势初步结束，上涨趋势开始
        elif (current_tick.new_price > (1 + self.threshold_end) * lastDequeQuotaWave.price):
            self.dequeQuotaWave[TICKER].append(
                StateRecord(timestamp=current_timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=current_tick.new_price,
                            state=WaveType.UP,
                            waveStartPrice=current_tick.new_price,
                            value=current_tick.new_price / lastDequeQuotaWave.price - 1,
                            price=current_tick.new_price,
                            highLowValue=current_tick.new_price / lastDequeQuotaWave.price - 1,
                            startTime=current_timestamp,
                            waveOver=1,
                            endTime=current_timestamp,
                            highLowTime=current_timestamp,
                            lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                            lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                            lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                            lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                            lastLowLevelTime=lastDequeQuotaWave.highLowTime,
                            lastLowLevelValue=lastDequeQuotaWave.highLowValue,
                            lastLowLevelPrice=lastDequeQuotaWave.price,
                            lastDownStartTime=lastDequeQuotaWave.startTime
                            ))


    async def __modifyBackUp(self, TICKER) -> None:
        """
        :return:
        """

        # 1min内创新高，趋势继续，修复之前的回调。
        tickerQuotaWave = self.dequeQuotaWave[TICKER]
        target_wave = \
            list(filter(lambda p: (p.timestamp == tickerQuotaWave[-1].lastHighLevelTime)
                                  and (p.value == tickerQuotaWave[-1].lastHighLevelValue),
                        tickerQuotaWave))[0]
        start_idx = tickerQuotaWave.index(target_wave)
        for j in range(start_idx + 1, len(tickerQuotaWave)):
            await self.queueResults.put(self.dequeQuotaWave[TICKER][j])
            self.dequeQuotaWave[TICKER][j] = \
                StateRecord(timestamp=tickerQuotaWave[j].timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=tickerQuotaWave[j].lastprice,
                            state=WaveType.UP,
                            waveStartPrice=tickerQuotaWave[j - 1].waveStartPrice,
                            value=tickerQuotaWave[j - 1].value
                                  + tickerQuotaWave[j].lastprice / tickerQuotaWave[j - 1].lastprice - 1,
                            price=tickerQuotaWave[j - 1].price,
                            highLowValue=tickerQuotaWave[j - 1].highLowValue,
                            startTime=tickerQuotaWave[j - 1].startTime,
                            waveOver=0,
                            endTime=-1,
                            highLowTime=tickerQuotaWave[j - 1].highLowTime,
                            lastHighLevelTime=tickerQuotaWave[j - 1].lastHighLevelTime,
                            lastHighLevelValue=tickerQuotaWave[j - 1].lastHighLevelValue,
                            lastHighLevelPrice=tickerQuotaWave[j - 1].lastHighLevelPrice,
                            lastUpStartTime=tickerQuotaWave[j - 1].lastUpStartTime,
                            lastLowLevelTime=tickerQuotaWave[j - 1].lastLowLevelTime,
                            lastLowLevelValue=tickerQuotaWave[j - 1].lastLowLevelValue,
                            lastLowLevelPrice=tickerQuotaWave[j - 1].lastLowLevelPrice,
                            lastDownStartTime=tickerQuotaWave[j - 1].lastDownStartTime
                            )
            await self.queueResults.put(self.dequeQuotaWave[TICKER][j])
        LOGGER.info(f'{TICKER} IS MODIFIED TO UP')

    async def __modifyBackDown(self, TICKER) -> None:
        """
        :return:
        """

        # 1min内创新低,趋势继续，修复之前的回调。
        tickerQuotaWave = self.dequeQuotaWave[TICKER]
        target_wave = \
            list(filter(lambda p: (p.timestamp == tickerQuotaWave[-1].lastLowLevelTime)
                                  and (p.value == tickerQuotaWave[-1].lastLowLevelValue),
                        tickerQuotaWave))[0]
        start_idx = tickerQuotaWave.index(target_wave)
        for j in range(start_idx + 1, len(tickerQuotaWave)):
            await self.queueResults.put(self.dequeQuotaWave[TICKER][j])
            self.dequeQuotaWave[TICKER][j] = \
                StateRecord(timestamp=tickerQuotaWave[j].timestamp,
                            order_book_id=TICKER,
                            ticker=TICKER,
                            lastprice=tickerQuotaWave[j].lastprice,
                            state=WaveType.DOWN,
                            waveStartPrice=tickerQuotaWave[j - 1].waveStartPrice,
                            value=tickerQuotaWave[j - 1].value
                                  + tickerQuotaWave[j].lastprice / tickerQuotaWave[j - 1].lastprice - 1,
                            price=tickerQuotaWave[j - 1].price,
                            highLowValue=tickerQuotaWave[j - 1].highLowValue,
                            startTime=tickerQuotaWave[j - 1].startTime,
                            waveOver=0,
                            endTime=-1,
                            highLowTime=tickerQuotaWave[j - 1].highLowTime,
                            lastHighLevelTime=tickerQuotaWave[j - 1].lastHighLevelTime,
                            lastHighLevelValue=tickerQuotaWave[j - 1].lastHighLevelValue,
                            lastHighLevelPrice=tickerQuotaWave[j - 1].lastHighLevelPrice,
                            lastUpStartTime=tickerQuotaWave[j - 1].lastUpStartTime,
                            lastLowLevelTime=tickerQuotaWave[j - 1].lastLowLevelTime,
                            lastLowLevelValue=tickerQuotaWave[j - 1].lastLowLevelValue,
                            lastLowLevelPrice=tickerQuotaWave[j - 1].lastLowLevelPrice,
                            lastDownStartTime=tickerQuotaWave[j - 1].lastDownStartTime
                            )
            await self.queueResults.put(self.dequeQuotaWave[TICKER][j])
        LOGGER.info(f'{TICKER} IS MODIFIED TO DOWN')


    def __getThreshold(self, TICKER, preclose) -> None:
        """
        :param TICKER:
        :return:
        """
        # self.threshold_1 = 0.0003
        # self.threshold_end = 0.0003
        # self.threshold_2 = 0.0001

        try:
            if preclose == 0:
                self.threshold_1 = 0.0015
                self.threshold_end = 0.0015
            else:
                self.threshold_1 = max(0.02 / preclose, 0.0015)
            self.threshold_2 = max(self.threshold_1, 0.003)
            self.threshold_end = max(0.0015, 0.02 / preclose)
        except ZeroDivisionError:
            print('here')
