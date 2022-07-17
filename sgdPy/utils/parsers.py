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