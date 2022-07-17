import rqdatac as rq
rq.init("15626436420", "vista2211655")
# rq.init(18123610100,123456)
from datetime import datetime
from collections import defaultdict
from functools import partial
from aioinflux import InfluxDBClient
import pandas as pd


def indexTickDict(pb):
    tick = {'time': datetime.utcfromtimestamp(pb.timestamp),
            'measurement': 'Tick',
            'tags': {'order_book_id': pb.ticker},
            'fields': {'ticker': pb.ticker,
                       'newPrice': pb.new_price,
                       'open': pb.open,
                       'high': pb.high,
                       'low': pb.low,
                       'volume': pb.volume,
                       'amount': pb.amount,
                       'bought_amount': pb.bought_amount,
                       'sold_amount': pb.sold_amount,
                       'preclose': pb.preclose,
                       'vwap_buy': pb.vwap_buy,
                       'vwap_sell': pb.vwap_sell,
                       'number_of_trades': pb.number_of_trades,
                       'upper_limit': pb.upper_limit,
                       'lower_limit': pb.lower_limit}
            }
    return tick


def tickDict(pb):
    try:
        tick = {'time': datetime.utcfromtimestamp(pb.timestamp),
                'measurement': 'Tick',
                'tags': {'order_book_id': pb.ticker},
                'fields': {'ticker': pb.ticker,
                           'newPrice': pb.new_price,
                           'open': pb.open,
                           'high': pb.high,
                           'low': pb.low,
                           'volume': pb.volume,
                           'amount': pb.amount,
                           'bought_amount':pb.bought_amount,
                           'sold_amount': pb.sold_amount,
                           'preclose': pb.preclose,
                           'vwap_buy': pb.vwap_buy,
                           'vwap_sell': pb.vwap_sell,
                           'number_of_trades': pb.number_of_trades,
                           'upper_limit': pb.upper_limit,
                           'lower_limit': pb.lower_limit}
                }
        return tick
    except:
        raise IndexError

def tickDict_opening(pb):
        if pb.new_price != 0.0:
            tick = {'time': datetime.utcfromtimestamp(pb.timestamp),
                    'measurement': 'Tick',
                    'tags': {'order_book_id': pb.ticker},
                    'fields': {'ticker': pb.ticker,
                               'newPrice': pb.new_price,
                               'open': pb.open,
                               'high': pb.high,
                               'low': pb.low,
                               'volume': pb.volume,
                               'amount': pb.amount,
                               'bought_amount': pb.bought_amount,
                               'sold_amount': pb.sold_amount,
                               'preclose': pb.preclose,
                               'vwap_buy': pb.vwap_buy,
                               'vwap_sell': pb.vwap_sell,
                               'number_of_trades': pb.number_of_trades,
                               'upper_limit': pb.upper_limit,
                               'lower_limit': pb.lower_limit}
                    }
            return tick

        else:
            try:
                if pb.bps[0] != 0.0:
                    pb.new_price = pb.bps[0]
                    tick = {'time': datetime.utcfromtimestamp(pb.timestamp),
                            'measurement': 'Tick',
                            'tags': {'order_book_id': pb.ticker},
                            'fields': {'ticker': pb.ticker,
                                       'newPrice': pb.bps[0],
                                       'open': pb.open,
                                       'high': pb.high,
                                       'low': pb.low,
                                       'volume': pb.volume,
                                       'amount': pb.amount,
                                       'bought_amount':pb.bought_amount,
                                       'sold_amount': pb.sold_amount,
                                       'preclose': pb.preclose,
                                       'vwap_buy': pb.vwap_buy,
                                       'vwap_sell': pb.vwap_sell,
                                       'number_of_trades': pb.number_of_trades,
                                       'upper_limit': pb.upper_limit,
                                       'lower_limit': pb.lower_limit}
                            }
                    return tick

            except IndexError or TypeError:
                raise IndexError

def prepare_data(batch_num):
    SUFFIX_MAPPING = {"XSHG": "SH", "XSHE": "SZ"}
    index_list = ['000001.XSHG', '000016.XSHG', '000300.XSHG', '000688.XSHG',
                                                           '000905.XSHG', '399006.XSHE']
    # 获取当天交易的股票和指数代码
    df = rq.all_instruments(type=['CS', 'INDX'], date=datetime.now())
    # 过滤掉非XSHE或XSHG为后缀的股票代码
    df = df[~df.iloc[:, 0].str.contains(r'INDX')]
    df = df[~df.iloc[:, 0].str.startswith('H')]

    # 存储代码为列表
    stocks = df['order_book_id'].tolist()

    #计算并分配到各进程的代码数量
    batch_size = int(len(stocks) / batch_num) + 1
    combinations = [
        set(stocks[i * batch_size:(i + 1) * batch_size]) for i in range(batch_num)]

    # 存下各指数在各进程中的成分股，defaultdict
    component_sets = list(map(get_each_components, combinations))

    # 本步目的为将一个进程中的各指数各成分并到一起
    mix_components = [set() for i in range(batch_num)]
    for i in range(batch_num):
        for k, v in component_sets[i].items():
            mix_components[i] = mix_components[i].union(v)

    df_copy = df.copy()
    df_copy['order_book_id'] = df['order_book_id'].apply(
        lambda x: f"{x[:7]}{SUFFIX_MAPPING[x[7:]]}")
    preclose_dfs = []
    previous_trading_day = rq.get_previous_trading_date(datetime.today())
    for comp_set in mix_components:
        preclose = rq.get_price(list(comp_set), previous_trading_day, previous_trading_day, fields=['open', 'close'], expect_df=True).reset_index()
        preclose.drop(['date', 'open'], axis = 1, inplace = True)
        preclose.columns = ['order_book_id', 'preclose']
        preclose['order_book_id'] = preclose['order_book_id'].apply(lambda x: x[:7]+SUFFIX_MAPPING[x[7:]])
        preclose.set_index('order_book_id', inplace = True)
        preclose = preclose.merge(df_copy[['order_book_id', 'symbol']], left_index=True, right_on="order_book_id")
        preclose_dfs.append(preclose)


    # 将后缀映射为SZ或SH

    stocks = list(map(lambda x: x[:7] + SUFFIX_MAPPING[x[7:]], df['order_book_id'].tolist()))
    index_list = ['000001.SH', '000016.SH', '000300.SH', '000688.SH',
                  '000905.SH', '399006.SZ']
    combinations = [
        stocks[i * batch_size:(i + 1) * batch_size] for i in range(batch_num)]
    combinations = [set(comb+index_list) for comb in combinations]
    component_sets = list(map(partial(get_each_components, suffix = True), combinations))

    mix_components = [set(map(lambda x: x[:7] + SUFFIX_MAPPING[x[7:]], mix_component)) for mix_component in mix_components]

    # write_index_weights()

    return combinations, component_sets, mix_components, preclose_dfs

def write_index_weights():
    client = InfluxDBClient(host="ts-uf6344g88nhjcx1pc.influxdata.tsdb.aliyuncs.com", port=8086,
                                      username="admin", password="Vfgdsm(@12898", timeout=20,
                                      db="graphna", mode="blocking", ssl=True, output="dataframe")
    SUFFIX_MAPPING = {"XSHG": "SH", "XSHE": "SZ"}
    now = datetime.now()
    index_list = ['000001.XSHG', '000016.XSHG', '000300.XSHG', '000688.XSHG',
                  '000905.XSHG', '399006.XSHE']
    for index in index_list:
        weights = rq.index_weights(index, now)
        weights = pd.DataFrame(weights)
        weights.columns = ['weight']
        weights.reset_index(inplace=True)
        weights.order_book_id = weights.order_book_id.apply(lambda x: f"{x[:7]}{SUFFIX_MAPPING[x[7:]]}")

        weights.index = pd.date_range(now, now, len(weights))
        TICKER = f"{index[:7]}{SUFFIX_MAPPING[index[7:]]}"
        weights['INDEX'] = TICKER
        client.write(weights, measurement='IndexWeights', tag_columns=['order_book_id', 'INDEX'])

    client.close()


def get_stock_chunks(batch_num=3) -> list:
    SUFFIX_MAP = {"XSHG": "SH", "XSHE": "SZ"}
    index_list = ['000001.SH', '000016.SH', '000300.SH', '000688.SH',
                                                           '000905.SH', '399006.SZ']

    df = rq.all_instruments(type=['CS', 'INDX'], date=datetime.now())
    df = df[~df.iloc[:, 0].str.contains(r'INDX')]
    df['order_book_id'] = df['order_book_id'].apply(
        lambda x: f"{x[:7]}{SUFFIX_MAP[x[7:]]}")

    stocks = df['order_book_id'].tolist()
    # stocks.sort()
    batch_size = int(len(stocks) / batch_num) + 1

    combinations = [
        set(stocks[i * batch_size:(i + 1) * batch_size] + index_list) for i in range(batch_num)]
    return combinations


def get_each_components(combination, suffix = False):
    SUFFIX_MAP = {"XSHG": "SH", "XSHE": "SZ"}
    index_list = ['000001.XSHG', '000016.XSHG', '000300.XSHG', '000688.XSHG',
                  '000905.XSHG', '399006.XSHE']
    index_components = defaultdict(set)
    for idx in index_list:
        if suffix:
            components = list(map(lambda x: f"{x[:7]}{SUFFIX_MAP[x[7:]]}", rq.index_weights(idx, datetime.now()).index.tolist()))
            idx = idx[:7] + SUFFIX_MAP[idx[7:]]
        else:
            components = rq.index_weights(idx, datetime.now()).index.tolist()
        for comb in combination:
            if comb in components:
                index_components[idx].add(comb)

    return index_components

def get_index_preclose():
    SUFFIX_MAPPING = {"XSHG": "SH", "XSHE": "SZ"}
    index_list = ['000001.XSHG', '000016.XSHG', '000300.XSHG', '000688.XSHG',
                  '000905.XSHG', '399006.XSHE']

    previous_trading_day = rq.get_previous_trading_date(datetime.today())
    preclose = rq.get_price(index_list, previous_trading_day, previous_trading_day, fields=['open', 'close'],
                            expect_df=True).reset_index()
    preclose.order_book_id = preclose.order_book_id.apply(lambda x: f"{x[:7]}{SUFFIX_MAPPING[x[7:]]}")
    index_preclose = {preclose.loc[x, 'order_book_id']: preclose.loc[x, 'close'] for x in preclose.index}

    return index_preclose


if __name__ == "__main__":
    # a,b,c,d = prepare_data(2)
    write_index_weights()
    print("here")