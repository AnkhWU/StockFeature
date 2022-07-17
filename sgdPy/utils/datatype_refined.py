import asyncio
from dataclasses import dataclass
from collections import defaultdict
from typing import NamedTuple

import async_timeout
from aioinflux.serialization.usertype import *
from xtrade_essential.proto import quotation_pb2

class WaveType:
    UP = "UP"
    DOWN = "DOWN"
    WAVELESS = 'WAVELESS'


@lineprotocol
@dataclass
class Tick:
    timestamp: TIMEDT
    order_book_id: TAG

    ticker: STR
    new_price: FLOAT
    open: FLOAT
    high: FLOAT
    low: FLOAT
    volume: FLOAT
    amount: FLOAT
    preclose: FLOAT
    bought_qty: FLOAT
    sold_qty: FLOAT
    vwap_buy: FLOAT
    vwap_sell: FLOAT
    number_of_trades: FLOAT
    upper_limit: FLOAT
    lower_limit: FLOAT
    # aps1: FLOAT
    # aps2: FLOAT
    # aps3: FLOAT
    # aps4: FLOAT
    # aps5: FLOAT
    # aps6: FLOAT
    # aps7: FLOAT
    # aps8: FLOAT
    # aps9: FLOAT
    # aps10: FLOAT
    # bps1: FLOAT
    # bps2: FLOAT
    # bps3: FLOAT
    # bps4: FLOAT
    # bps5: FLOAT
    # bps6: FLOAT
    # bps7: FLOAT
    # bps8: FLOAT
    # bps9: FLOAT
    # bps10: FLOAT
    # avs1: FLOAT
    # avs2: FLOAT
    # avs3: FLOAT
    # avs4: FLOAT
    # avs5: FLOAT
    # avs6: FLOAT
    # avs7: FLOAT
    # avs8: FLOAT
    # avs9: FLOAT
    # avs10: FLOAT
    # bvs1: FLOAT
    # bvs2: FLOAT
    # bvs3: FLOAT
    # bvs4: FLOAT
    # bvs5: FLOAT
    # bvs6: FLOAT
    # bvs7: FLOAT
    # bvs8: FLOAT
    # bvs9: FLOAT
    # bvs10: FLOAT


@lineprotocol
@dataclass
class StateRecord:
    timestamp: TIMEINT
    order_book_id: TAG

    ticker: STR
    lastprice: FLOAT

    state: TAG = WaveType.WAVELESS
    waveStartPrice: FLOAT = -1.0
    value: FLOAT = -1.0 # 收益
    price: FLOAT = -1.0 # 价格
    highLowValue: FLOAT = -1.0
    startTime: INT = -1
    endTime: INT = -1
    highLowTime: INT = -1

    waveOver: INT = 0

    lastHighLevelValue: FLOAT = -1.0
    lastHighLevelPrice: FLOAT = -1.0
    lastHighLevelTime: FLOAT = -1.0
    lastUpStartTime: INT = -1

    lastLowLevelValue: FLOAT = -1.0
    lastLowLevelPrice: FLOAT = -1.0
    lastLowLevelTime: FLOAT = -1.0
    lastDownStartTime: INT = -1


class AsyncDefaultdict(defaultdict):
    def __init__(self, *args, **kwargs):
        super(AsyncDefaultdict, self).__init__(*args, **kwargs)

    def __aiter__(self):
        self.avalues = iter(self.values())
        return self

    async def __anext__(self):
        try:
            v = next(self.avalues)
            return v[0]
        except StopIteration:
            raise StopAsyncIteration


@dataclass
class QuotaDataAndWave:

    new_price: float or None = None
    timestamp: float = None

    state: str = WaveType.WAVELESS  # 0:无wave;1:上涨；-1：下跌
    value: float or None = None  # 收益
    price: float or None = None  # 价格
    highLowValue: float or None = None
    startTime: float or None = None
    endTime: float or None = None
    highLowTime: float or None = None

    waveOver: int = 0

    lastHighLevelValue: float or None = None
    lastHighLevelPrice: float or None = None
    lastHighLevelTime: float or None = None
    lastUpStartTime: float or None = None
    lastLowLevelValue: float or None = None
    lastLowLevelPrice: float or None = None
    lastLowLevelTime: float or None = None
    lastDownStartTime: float or None = None


if __name__ == "__main__":
    import random
    from collections import deque
    import time
    from datetime import datetime
    import pickle

    sr = StateRecord(timestamp = 10087, order_book_id = "000001.SH", ticker = "000001.SH", lastprice = 3.4)
    # j = 1
    # tick = StateRecord(time.time()*1e9, '000001.sh', '000001.sh',j)
    # tick.to_lineprotocol()
    # 测试了1000个股票各4800个wave,大小约为2g, 存为pickle约500m
    # for i in range(1000):
    #     for j in range(4800):
    #         tick = StateRecord(datetime.now(),'000001.sh', 'waveless','000001.sh' ,j,j,j,j,j,j,j,j,j,j,j,j,j,j,j)
    #         dequeWaveDict[i].append(tick)
    # with open("savingtest.pkl", "wb") as f:
    #     pickle.dump(dequeWaveDict, f)
    print("hello")




