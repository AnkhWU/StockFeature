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
    timestamp: TIMEDT
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
    lastHighLevelTime: INT = -1
    lastUpStartTime: INT = -1

    lastLowLevelValue: FLOAT = -1.0
    lastLowLevelPrice: FLOAT = -1.0
    lastLowLevelTime: INT = -1
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
    from collections import deque, defaultdict, AsyncIterator
    import time
    import random

    # 行情批量写入操作简化版
    q = asyncio.Queue()

    async def func2(q):
        klist = []
        count = 0
        while True:
            a = await q.get()
            klist.append(a)
            if len(klist) == 1000:
                klist = []
                count += 1
                if count == 1000:
                    print(count)





    async def func(q):

        for i in range(1000000):
            temp_qdw = random.random()
            await q.put(temp_qdw)


    asyncio.get_event_loop().run_until_complete(asyncio.gather(*[func(q), func2(q)]))




