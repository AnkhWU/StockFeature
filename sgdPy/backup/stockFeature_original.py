from datetime import datetime
from functools import partial
from aioinflux import InfluxDBClient
from collections import deque

from xtrade_essential.xlib import logger
import logging
import copy

import numpy as np
from utils.datatype import *

def stateParser(QuotaWave, TICKER, state: str):
    tick = StateRecord(timestamp = int(QuotaWave.timestamp),
                       order_book_id = TICKER,
                       state = QuotaWave.state,
                       ticker = TICKER,
                       price = Quota.new_price)
    return tick


LOGGER = logger.getLogger()
LOGGER.setLevel("INFO")


class StockFeature:

    def __init__(self, queue_bars):
        self.interval = 30
        self.waveInterval = 60
        self.calPeriod = 90
        self.__DBClient_tick = InfluxDBClient(host="localhost", port=8086,
                                              username="t0", password="123456",
                                              db="graphna", mode="async")
        self.queueTicks = queue_bars
        self.bufferDict = AsyncDefaultdict(partial(deque, maxlen=90))
        self.dequeQuotaWave = AsyncDefaultdict(deque)  # 存放QuotaDataAndWave对象的双端队列
        self.dequePriceS = AsyncDefaultdict(partial(deque, maxlen=5)) # 存放Wave的deque
        self.dequePriceL = AsyncDefaultdict(partial(deque, maxlen=90))
        self.dequeValue = AsyncDefaultdict(partial(deque, maxlen=90))

    async def validateTick(self) -> None:
        # 创建一个值限定为 最大长度90 deque的defaultdict，若键ticker不存在，会返回一个空的deque，长度为0

        while True:
            tick = await self.queueTicks.get() # 此处queueTicker为全局变量，后作修改
            TICKER = tick.tick_body.ticker
            # 若最新价格为0，替换为前一天收盘价或者上一个最新价格
            # if tick.tick_body.new_price == 0.0:
            #     if len(self.bufferDict[TICKER]) == 0:
            #         tick.tick_body.new_price = tick.tick_body.preclose
            #     elif len(self.bufferDict[TICKER]) > 0:
            #         tick.tick_body.new_price = self.bufferDict[TICKER][-1].tick_body.new_price
            self.bufferDict[TICKER].append(tick)

            # 若ticker的deque长度大于5，调用 getWave 方法
            await self.getWave(TICKER)


    async def getWave(self, TICKER: str) -> None:
        """
        :param pb_deque:
        :return:
        """
        pb_deque = self.bufferDict[TICKER] # 取出该ticker的队列
        # 初始化其对应的数据类QuotaDataAndWave的deque

        tempQuotaWave = QuotaDataAndWave(new_price=pb_deque[-1].tick_body.new_price,
                                         timestamp= pb_deque[-1].tick_body.timestamp)
        self.dequeQuotaWave[TICKER].append(tempQuotaWave)
        self.dequePriceS[TICKER].append(pb_deque[-1].tick_body.new_price)
        self.dequePriceL[TICKER].append(pb_deque[-1].tick_body.new_price)

        # tick数量少于6时不做趋势判断
        if len(pb_deque) < 6:
            return

        # 确定threshold
        self.__getThreshold(TICKER)
        # 在deque里收集收益率
        self.dequeValue[TICKER].append(pb_deque[-1].tick_body.new_price/pb_deque[-2].tick_body.new_price)

        if self.dequeQuotaWave[TICKER][-2].state == WaveType.WAVELESS:
            self.__switchFromWaveless(pb_deque, TICKER)
            if self.dequeQuotaWave[TICKER][-1].state == WaveType.UP:
                await self.__DBClient_tick.write(stateParser(pb_deque[-1].tick_body, 'UP'))
            elif self.dequeQuotaWave[TICKER][-1].state == WaveType.DOWN:
                await self.__DBClient_tick.write(stateParser(pb_deque[-1].tick_body, 'DOWN'))
        elif self.dequeQuotaWave[TICKER][-2].state == WaveType.UP:
            self.__switchFromUp(pb_deque, TICKER)
            if self.dequeQuotaWave[TICKER][-1].state == WaveType.DOWN:
                await self.__DBClient_tick.write(stateParser(pb_deque[-1].tick_body, 'DOWN'))
            elif self.dequeQuotaWave[TICKER][-1].state == WaveType.WAVELESS:
                await self.__DBClient_tick.write(stateParser(pb_deque[-1].tick_body, 'WAVELESS'))
        elif self.dequeQuotaWave[TICKER][-2].state == WaveType.DOWN:
            self.__switchFromDown(pb_deque, TICKER)
            if self.dequeQuotaWave[TICKER][-1].state == WaveType.UP:
                await self.__DBClient_tick.write(stateParser(pb_deque[-1].tick_body, 'UP'))
            elif self.dequeQuotaWave[TICKER][-1].state == WaveType.WAVELESS:
                await self.__DBClient_tick.write(stateParser(pb_deque[-1].tick_body, 'WAVELESS'))

        lastHighLevelTime = self.dequeQuotaWave[TICKER][-1].lastHighLevelTime
        lastLowLevelTime = self.dequeQuotaWave[TICKER][-1].lastLowLevelTime

        if lastHighLevelTime and lastLowLevelTime:
            if (lastHighLevelTime > lastLowLevelTime) \
                    and ((pb_deque[-1].tick_body.timestamp - lastHighLevelTime) < self.waveInterval)\
                    and (self.dequeQuotaWave[TICKER][-1].state != WaveType.UP):
                self.__modifyBackUp(pb_deque, TICKER)

            elif (lastHighLevelTime < lastLowLevelTime) \
                    and ((pb_deque[-1].tick_body.timestamp - lastLowLevelTime) < self.waveInterval)\
                    and (self.dequeQuotaWave[TICKER][-1].state != WaveType.DOWN):
                self.__modifyBackDown(pb_deque, TICKER)

        elif lastHighLevelTime:
            if ((pb_deque[-1].tick_body.timestamp - lastHighLevelTime) < self.waveInterval) \
                    and (self.dequeQuotaWave[TICKER][-1].state != WaveType.UP):
                self.__modifyBackUp(pb_deque, TICKER)

        elif lastLowLevelTime:
            if ((pb_deque[-1].tick_body.timestamp - lastLowLevelTime) < self.waveInterval) \
                    and (self.dequeQuotaWave[TICKER][-1].state != WaveType.DOWN):
                self.__modifyBackDown(pb_deque, TICKER)



    def __switchFromWaveless(self, pb_deque: deque, TICKER: str) -> None:
        """
        :param current_tick:
        :param TICKER:
        :return:
        """
        lastDequeQuotaWave: QuotaDataAndWave = self.dequeQuotaWave[TICKER][-2]
        current_tick = copy.copy(pb_deque[-1])
        short_MIN = self.dequePriceS[TICKER][np.argmin(self.dequePriceS[TICKER])] # 用于判断快速上涨
        short_MAX = self.dequePriceS[TICKER][np.argmax(self.dequePriceS[TICKER])] # 用于判断快速下跌
        long_MIN = self.dequePriceL[TICKER][np.argmin(self.dequePriceL[TICKER])] # 用于判断慢速上涨
        long_MAX = self.dequePriceL[TICKER][np.argmax(self.dequePriceL[TICKER])] # 用于判断慢速下跌

        # 快速上涨趋势开始
        if (self.dequePriceS[TICKER][-1]/short_MIN - 1) >= self.threshold_1:
            self.dequeQuotaWave[TICKER][-1] = \
                QuotaDataAndWave(state=WaveType.UP,
                                 value=current_tick.tick_body.new_price / short_MIN - 1,
                                 price=current_tick.tick_body.new_price,
                                 highLowValue=current_tick.tick_body.new_price / short_MIN - 1,
                                 startTime=current_tick.tick_body.timestamp,
                                 waveOver=0,
                                 endTime=None,
                                 highLowTime=current_tick.tick_body.timestamp,
                                 lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                 lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                 lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                 lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                 lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                 lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                 lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                 lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                                 )
            LOGGER.info(f'{TICKER}: Fast up trend starts.')

        # 快速下跌趋势开始
        elif (self.dequePriceS[TICKER][-1]/short_MAX - 1) <= -self.threshold_1:
            self.dequeQuotaWave[TICKER][-1] = \
                QuotaDataAndWave(state=WaveType.DOWN,
                                 value=current_tick.tick_body.new_price / short_MAX - 1,
                                 price=current_tick.tick_body.new_price,
                                 highLowValue=current_tick.tick_body.new_price / short_MAX - 1,
                                 startTime=current_tick.tick_body.timestamp,
                                 waveOver=0,
                                 endTime=None,
                                 highLowTime=current_tick.tick_body.timestamp,
                                 lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                 lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                 lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                 lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                 lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                 lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                 lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                 lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                                 )
            LOGGER.info(f'{TICKER}: Fast down trend starts.')

        # 慢速上涨趋势开始，先保证前面没有快速上涨和下跌
        elif (self.dequePriceS[TICKER][-1]/long_MIN - 1) >= self.threshold_2 \
            and len(np.argwhere(np.array(self.dequeValue[TICKER]) > 0)) > len(self.dequeValue[TICKER])*0.8:
            self.dequeQuotaWave[TICKER][-1].Wave = \
                QuotaDataAndWave(state=WaveType.UP,
                                 value=current_tick.tick_body.new_price / long_MIN - 1,
                                 price=current_tick.tick_body.new_price,
                                 highLowValue=current_tick.tick_body.new_price / long_MIN - 1,
                                 startTime=current_tick.tick_body.timestamp,
                                 waveOver=0,
                                 endTime=None,
                                 highLowTime=current_tick.tick_body.timestamp,
                                 lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                 lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                 lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                 lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                 lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                 lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                 lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                 lastDownStartTime=lastDequeQuotaWave.lastDownStartTime)
            LOGGER.info(f'{TICKER}: Slow up trend starts.')

        # 慢速下跌趋势开始
        elif (self.dequePriceS[TICKER][-1] / long_MAX - 1) < -self.threshold_2 \
            and len(np.argwhere(np.array(self.dequeValue[TICKER]) > 0)) < len(self.dequeValue[TICKER]) * 0.2:
            self.dequeQuotaWave[TICKER][-1].Wave = \
                QuotaDataAndWave(state=WaveType.DOWN,
                                 value=current_tick.tick_body.new_price / long_MAX - 1,
                                 price=current_tick.tick_body.new_price,
                                 highLowValue=current_tick.tick_body.new_price / long_MAX - 1,
                                 startTime=current_tick.tick_body.timestamp,
                                 waveOver=0,
                                 endTime=None,
                                 highLowTime=current_tick.tick_body.timestamp,
                                 lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                 lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                 lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                 lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                 lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                 lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                 lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                 lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                                 )
            LOGGER.info(f'{TICKER}: Slow down trend starts.')

        else:
            self.dequeQuotaWave[TICKER][-1].Wave = \
                QuotaDataAndWave(price=current_tick.tick_body.new_price,
                                 lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                 lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                 lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                 lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                 lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                 lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                 lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                 lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                                 )


    def __switchFromUp(self, pb_deque: quotation_pb2.Message, TICKER: str) -> None:
        lastDequeQuotaWave = self.dequeQuotaWave[TICKER][-2]
        current_tick = copy.copy(pb_deque[-1])
        previous_tick = copy.copy(pb_deque[-2])

        time_from_HL = current_tick.tick_body.timestamp - lastDequeQuotaWave.highLowTime
        run_time = current_tick.tick_body.timestamp - lastDequeQuotaWave.startTime

        # 价格创新高，趋势继续
        if (current_tick.tick_body.new_price >= lastDequeQuotaWave.price):
            self.dequeQuotaWave[TICKER][-1].Wave = \
                QuotaDataAndWave(state=WaveType.UP,
                      value=(current_tick.tick_body.new_price / lastDequeQuotaWave.price - 1)
                            + lastDequeQuotaWave.value,
                      price=current_tick.tick_body.new_price,
                      highLowValue=(current_tick.tick_body.new_price / lastDequeQuotaWave.price - 1)
                                   + lastDequeQuotaWave.value,
                      startTime=lastDequeQuotaWave.startTime,
                      waveOver=0,
                      endTime=None,
                      highLowTime=current_tick.tick_body.timestamp,
                      lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                      lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                      lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                      lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                      lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                      lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                      lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                      lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                      )

        elif (current_tick.tick_body.new_price >= (1 - self.threshold_end) * lastDequeQuotaWave.price):
            if (current_tick.tick_body.new_price < lastDequeQuotaWave.price):
                # 价格盘整，趋势继续
                if time_from_HL <= self.interval:
                    self.dequeQuotaWave[TICKER][-1].Wave = \
                        QuotaDataAndWave(state=WaveType.UP,
                                         value=(current_tick.tick_body.new_price / previous_tick.tick_body.new_price - 1)
                                               + lastDequeQuotaWave.value,
                                         price=lastDequeQuotaWave.price,
                                         highLowValue=lastDequeQuotaWave.highLowValue,
                                         startTime=lastDequeQuotaWave.startTime,
                                         waveOver=0,
                                         endTime=None,
                                         lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                                         lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                                         lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                                         lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                                         lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                                         lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                                         lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                                         lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                                         )

                # 价格盘整，超出时间，趋势初步结束
                else:
                    timeSpan = max(3, run_time)
                    if timeSpan > 5400:
                        timeSpan = timeSpan - 5400
                    self.dequeQuotaWave[TICKER][-1].Wave = \
                        QuotaDataAndWave(state=WaveType.WAVELESS,
                              value=None,
                              price=None,
                              highLowValue=None,
                              startTime=None,
                              waveOver=1,
                              endTime=current_tick.tick_body.timestamp,
                              highLowTime=None,
                              lastHighLevelTime=lastDequeQuotaWave.highLowTime,
                              lastHighLevelValue=lastDequeQuotaWave.highLowValue,
                              lastHighLevelPrice=lastDequeQuotaWave.price,
                              lastUpStartTime=lastDequeQuotaWave.startTime,
                              lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                              lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                              lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                              lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                              )
                    LOGGER.info(f'{TICKER}:Up trend ended with speed:{self.dequeQuotaWave[TICKER][-1].Wave.waveEnd.SPEED}.')

            # 涨停情况时
            elif (current_tick.tick_body.new_price <= lastDequeQuotaWave.price) and (time_from_HL > self.interval):
                timeSpan = max(3, run_time)
                if timeSpan > 5400:
                    timeSpan = timeSpan - 5400
                self.dequeQuotaWave[TICKER][-1].Wave = \
                    QuotaDataAndWave(state=WaveType.WAVELESS,
                          value=None,
                          price=None,
                          highLowValue=None,
                          startTime=None,
                          waveOver=1,
                          endTime=current_tick.tick_body.timestamp,
                          highLowTime=None,
                          lastHighLevelTime=lastDequeQuotaWave.highLowTime,
                          lastHighLevelValue=lastDequeQuotaWave.highLowValue,
                          lastHighLevelPrice=lastDequeQuotaWave.price,
                          lastUpStartTime=lastDequeQuotaWave.startTime,
                          lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                          lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                          lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                          lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                          )
                LOGGER.info(f'{TICKER}:Up trend ended with 涨停')

            else:
                timeSpan = max(3, run_time)
                if timeSpan > 5400:
                    timeSpan = timeSpan - 5400
                self.dequeQuotaWave[TICKER][-1].Wave = \
                    QuotaDataAndWave(state=WaveType.UP,
                          value=(current_tick.tick_body.new_price / previous_tick.tick_body.new_price - 1)
                                + lastDequeQuotaWave.value,
                          price=lastDequeQuotaWave.price,
                          highLowValue=lastDequeQuotaWave.highLowValue,
                          startTime=lastDequeQuotaWave.startTime,
                          waveOver=0,
                          endTime=None,
                          highLowTime=lastDequeQuotaWave.highLowTime,
                          lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                          lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                          lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                          lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                          lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                          lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                          lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                          lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                          )

        # 回调大于阈值，趋势初步结束，下跌趋势开始
        elif (current_tick.tick_body.new_price < (1 - self.threshold_end) * lastDequeQuotaWave.price):
            timeSpan = max(3, run_time)
            if timeSpan > 5400:
                timeSpan = timeSpan - 5400
            self.dequeQuotaWave[TICKER][-1].Wave = \
                QuotaDataAndWave(state=WaveType.DOWN,
                      value=current_tick.tick_body.new_price / lastDequeQuotaWave.price - 1,
                      price=current_tick.tick_body.new_price,
                      highLowValue=current_tick.tick_body.new_price / lastDequeQuotaWave.price - 1,
                      startTime=current_tick.tick_body.timestamp,
                      waveOver=1,
                      endTime=current_tick.tick_body.timestamp,
                      highLowTime=current_tick.tick_body.timestamp,
                      lastHighLevelTime=lastDequeQuotaWave.highLowTime,
                      lastHighLevelValue=lastDequeQuotaWave.highLowValue,
                      lastHighLevelPrice=lastDequeQuotaWave.price,
                      lastUpStartTime=lastDequeQuotaWave.startTime,
                      lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                      lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                      lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                      lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                      )
            LOGGER.info(f'{TICKER}:Up trend ended with speed:{self.dequeQuotaWave[TICKER][-1].Wave.waveEnd.SPEED}, Down trend starts.')


    def __switchFromDown(self, pb_deque: deque, TICKER: str) -> None:
        """
        :param pb_deque:
        :param TICKER:
        :return:
        """

        lastDequeQuotaWave: Wave_ = self.dequeQuotaWave[TICKER][-2].Wave
        current_tick = copy.copy(pb_deque[-1])
        previous_tick = copy.copy(pb_deque[-2])
        time_from_HL = current_tick.tick_body.timestamp - lastDequeQuotaWave.highLowTime
        run_time = current_tick.tick_body.timestamp - lastDequeQuotaWave.startTime

        # 价格创新低，趋势继续
        if (current_tick.tick_body.new_price <= lastDequeQuotaWave.price):
            self.dequeQuotaWave[TICKER][-1].Wave = \
                QuotaDataAndWave(state=WaveType.DOWN,
                      value=(current_tick.tick_body.new_price / lastDequeQuotaWave.price - 1)
                            + lastDequeQuotaWave.value,
                      price=current_tick.tick_body.new_price,
                      highLowValue=(current_tick.tick_body.new_price / lastDequeQuotaWave.price - 1)
                                   + lastDequeQuotaWave.value,
                      startTime=lastDequeQuotaWave.startTime,
                      waveOver=0,
                      endTime=None,
                      highLowTime=current_tick.tick_body.timestamp,
                      lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                      lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                      lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                      lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                      lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                      lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                      lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                      lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                      )

        elif (current_tick.tick_body.new_price <= (1 + self.threshold_end) * lastDequeQuotaWave.price):
            if (current_tick.tick_body.new_price > lastDequeQuotaWave.price):
                # 价格盘整，趋势继续
                if time_from_HL <= self.interval:
                    self.dequeQuotaWave[TICKER][-1].Wave = \
                        QuotaDataAndWave(state=WaveType.DOWN,
                              value=(current_tick.tick_body.new_price / previous_tick.tick_body.new_price - 1)
                                    + lastDequeQuotaWave.value,
                              price=lastDequeQuotaWave.price,
                              highLowValue=lastDequeQuotaWave.highLowValue,
                              startTime=lastDequeQuotaWave.startTime,
                              waveOver=0,
                              endTime=None,
                              highLowTime=lastDequeQuotaWave.highLowTime,
                              lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                              lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                              lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                              lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                              lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                              lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                              lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                              lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                              )

                # 价格盘整，超出时间，趋势初步结束
                else:
                    timeSpan = max(3, run_time)
                    if timeSpan > 5400:
                        timeSpan = timeSpan - 5400
                    self.dequeQuotaWave[TICKER][-1].Wave = \
                        QuotaDataAndWave(state=WaveType.WAVELESS,
                              value=None,
                              price=None,
                              highLowValue=None,
                              startTime=None,
                              waveOver=1,
                              endTime=current_tick.tick_body.timestamp,
                              highLowTime=None,
                              lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                              lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                              lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                              lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                              lastLowLevelTime=lastDequeQuotaWave.highLowTime,
                              lastLowLevelValue=lastDequeQuotaWave.highLowValue,
                              lastLowLevelPrice=lastDequeQuotaWave.price,
                              lastDownStartTime=lastDequeQuotaWave.startTime
                              )
                    LOGGER.info(f'{TICKER}:Down trend ended with speed:{self.dequeQuotaWave[TICKER][-1].Wave.waveEnd.SPEED}.')

            # 跌停情况时
            elif (current_tick.tick_body.new_price >= lastDequeQuotaWave.price) and (time_from_HL > self.interval):
                timeSpan = max(3, run_time)
                if timeSpan > 5400:
                    timeSpan = timeSpan - 5400
                self.dequeQuotaWave[TICKER][-1].Wave = \
                    QuotaDataAndWave(state=WaveType.WAVELESS,
                          value=None,
                          price=None,
                          highLowValue=None,
                          startTime=None,
                          waveOver=1,
                          endTime=current_tick.tick_body.timestamp,
                          highLowTime=None,
                          lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                          lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                          lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                          lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                          lastLowLevelTime=lastDequeQuotaWave.highLowTime,
                          lastLowLevelValue=lastDequeQuotaWave.highLowValue,
                          lastLowLevelPrice=lastDequeQuotaWave.price,
                          lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                          )
                LOGGER.info(f'{TICKER}:Down trend ended with 跌停.')

            else:
                timeSpan = max(3, run_time)
                if timeSpan > 5400:
                    timeSpan = timeSpan - 5400
                self.dequeQuotaWave[TICKER][-1].Wave = \
                    QuotaDataAndWave(state=WaveType.DOWN,
                          value=(current_tick.tick_body.new_price / previous_tick.tick_body.new_price - 1)
                                + lastDequeQuotaWave.value,
                          price=lastDequeQuotaWave.price,
                          highLowValue=lastDequeQuotaWave.highLowValue,
                          startTime=lastDequeQuotaWave.startTime,
                          waveOver=0,
                          endTime=None,
                          highLowTime=lastDequeQuotaWave.highLowTime,
                          lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                          lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                          lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                          lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                          lastLowLevelTime=lastDequeQuotaWave.lastLowLevelTime,
                          lastLowLevelValue=lastDequeQuotaWave.lastLowLevelValue,
                          lastLowLevelPrice=lastDequeQuotaWave.lastLowLevelPrice,
                          lastDownStartTime=lastDequeQuotaWave.lastDownStartTime
                          )

        # 回调大于阈值，趋势初步结束，上涨趋势开始
        elif (current_tick.tick_body.new_price > (1 + self.threshold_end) * lastDequeQuotaWave.price):
            timeSpan = max(3, run_time)
            if timeSpan > 5400:
                timeSpan = timeSpan - 5400
            self.dequeQuotaWave[TICKER][-1].Wave = \
                QuotaDataAndWave(state=WaveType.UP,
                      value=current_tick.tick_body.new_price / lastDequeQuotaWave.price - 1,
                      price=current_tick.tick_body.new_price,
                      highLowValue=current_tick.tick_body.new_price / lastDequeQuotaWave.price - 1,
                      startTime=current_tick.tick_body.timestamp,
                      waveOver=1,
                      endTime=current_tick.tick_body.timestamp,
                      highLowTime=current_tick.tick_body.timestamp,
                      lastHighLevelTime=lastDequeQuotaWave.lastHighLevelTime,
                      lastHighLevelValue=lastDequeQuotaWave.lastHighLevelValue,
                      lastHighLevelPrice=lastDequeQuotaWave.lastHighLevelPrice,
                      lastUpStartTime=lastDequeQuotaWave.lastUpStartTime,
                      lastLowLevelTime=lastDequeQuotaWave.highLowTime,
                      lastLowLevelValue=lastDequeQuotaWave.highLowValue,
                      lastLowLevelPrice=lastDequeQuotaWave.price,
                      lastDownStartTime=lastDequeQuotaWave.startTime
                      )
            LOGGER.info(
                f'{TICKER}:Down trend ended with speed:{self.dequeQuotaWave[TICKER][-1].Wave.waveEnd.SPEED}, Up trend starts.')


    def __modifyBackUp(self, pb_deque: deque, TICKER) -> None:
        """
        :return:
        """

        # 1min内创新高，趋势继续，修复之前的回调。
        if self.dequeQuotaWave[TICKER][-1].Wave.lastHighLevelPrice and \
                pb_deque[-1].tick_body.new_price > self.dequeQuotaWave[TICKER][-1].Wave.lastHighLevelPrice:
            tickerQuotaWave = self.dequeQuotaWave[TICKER]
            target_tick = \
                list(filter(lambda p: (p.QuotaData.tick_body.tick_body.timestamp == tickerQuotaWave[-1].wave.lastHighLevelTime)
                                      and (p.wave.value == tickerQuotaWave[-1].wave.lastHighLevelValue),
                            tickerQuotaWave))[0]
            start_idx = pb_deque.index(target_tick)
            print(start_idx)
            for j in range(start_idx + 1, len(pb_deque)):
                self.dequeQuotaWave[TICKER][-1].Wave = \
                    QuotaDataAndWave(state=WaveType.UP,
                          value=tickerQuotaWave[j - 1].wave.value
                                + pb_deque[j].tick_body.new_price / pb_deque[j - 1].tick_body.new_price - 1,
                          price=tickerQuotaWave[j - 1].wave.price,
                          highLowValue=tickerQuotaWave[j - 1].wave.highLowValue,
                          startTime=tickerQuotaWave[j - 1].wave.startTime,
                          waveOver=0,
                          endTime=None,
                          highLowTime=tickerQuotaWave[j - 1].wave.highLowTime,
                          lastHighLevelTime=tickerQuotaWave[j - 1].wave.lastHighLevelTime,
                          lastHighLevelValue=tickerQuotaWave[j - 1].wave.lastHighLevelValue,
                          lastHighLevelPrice=tickerQuotaWave[j - 1].wave.lastHighLevelPrice,
                          lastUpStartTime=tickerQuotaWave[j - 1].wave.lastUpStartTime,
                          lastLowLevelTime=tickerQuotaWave[j - 1].wave.lastLowLevelTime,
                          lastLowLevelValue=tickerQuotaWave[j - 1].wave.lastLowLevelValue,
                          lastLowLevelPrice=tickerQuotaWave[j - 1].wave.lastLowLevelPrice,
                          lastDownStartTime=tickerQuotaWave[j - 1].wave.lastDownStartTime
                          )
            LOGGER.info(f'{TICKER}: Trend was modified to Up.')


    def __modifyBackDown(self, pb_deque: deque, TICKER) -> None:
        """
        :return:
        """

        # 1min内创新低,趋势继续，修复之前的回调。
        if self.dequeQuotaWave[TICKER][-1].Wave.lastLowLevelPrice \
                and pb_deque[-1].tick_body.new_price <= self.dequeQuotaWave[TICKER][-1].Wave.lastLowLevelPrice:
            tickerQuotaWave = self.dequeQuotaWave[TICKER]
            target_tick = \
                list(filter(lambda p: (p.QuotaData.tick_body.tick_body.timestamp == tickerQuotaWave[-1].wave.lastLowLevelTime)
                                      and (p.wave.value == tickerQuotaWave[-1].wave.lastLowLevelValue),
                            tickerQuotaWave))[0]
            start_idx = pb_deque.index(target_tick)
            for j in range(start_idx + 1, len(pb_deque)):
                self.dequeQuotaWave[TICKER][-1].Wave = \
                    QuotaDataAndWave(state=WaveType.DOWN,
                          value=tickerQuotaWave[j - 1].wave.value
                                + pb_deque[j].tick_body.new_price / pb_deque[j - 1].tick_body.new_price - 1,
                          price=tickerQuotaWave[j - 1].wave.price,
                          highLowValue=tickerQuotaWave[j - 1].wave.highLowValue,
                          startTime=tickerQuotaWave[j - 1].wave.startTime,
                          waveOver=0,
                          endTime=None,
                          highLowTime=tickerQuotaWave[j - 1].wave.highLowTime,
                          lastHighLevelTime=tickerQuotaWave[j - 1].wave.lastHighLevelTime,
                          lastHighLevelValue=tickerQuotaWave[j - 1].wave.lastHighLevelValue,
                          lastHighLevelPrice=tickerQuotaWave[j - 1].wave.lastHighLevelPrice,
                          lastUpStartTime=tickerQuotaWave[j - 1].wave.lastUpStartTime,
                          lastLowLevelTime=tickerQuotaWave[j - 1].wave.lastLowLevelTime,
                          lastLowLevelValue=tickerQuotaWave[j - 1].wave.lastLowLevelValue,
                          lastLowLevelPrice=tickerQuotaWave[j - 1].wave.lastLowLevelPrice,
                          lastDownStartTime=tickerQuotaWave[j - 1].wave.lastDownStartTime
                          )
            LOGGER.info(f'{TICKER}: Trend was modified to Down.')


    def __getThreshold(self, TICKER) -> None:
        """
        :param TICKER:
        :return:
        """

        if self.dequeQuotaWave[TICKER][0].QuotaData.tick_body.preclose == 0:
            self.threshold_1 = 0.0015
        else:
            self.threshold_1 = max(0.02 / self.dequeQuotaWave[TICKER][0].QuotaData.tick_body.preclose, 0.0015)
        self.threshold_2 = max(self.threshold_1, 0.003)
        self.threshold_end = max(0.0015, 0.02 / self.dequeQuotaWave[TICKER][0].QuotaData.tick_body.preclose)
