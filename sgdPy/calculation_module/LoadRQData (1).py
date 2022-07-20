import rqdatac as rq
import datetime
rq.init('','')

exchange_map = {"XSHG": "SH", "XSHE": "SZ"}

class RQDataPrep:
    def __init__(self):
        self.today = datetime.datetime.today()
        self._get_stock_list()
        self._get_weight()
        self._get_factor_exposure()

    def _get_stock_list(self):
        self.pre_day = rq.get_previous_trading_date(self.today)
        cur_stocks = rq.all_instruments(type='CS', date=self.today)['order_book_id'].to_list()
        pre_stocks = rq.all_instruments(type='CS', date=self.pre_day)['order_book_id'].to_list()
        self.stock_list = list(set(cur_stocks).intersection(pre_stocks))

    def _get_weight(self):
        weight = rq.get_factor(self.stock_list, ["a_share_market_val", "a_share_market_val_in_circulation"],
                               self.pre_day, self.pre_day, universe=None)
        weight.reset_index(inplace=True)
        weight.drop('date', axis=1, inplace=True)
        weight['order_book_id'] = weight['order_book_id'].apply(lambda x: f"{x[:7]}{exchange_map[x[7:]]}")
        weight.set_index('order_book_id', inplace=True)
        weight.columns = ['mkt_cap', 'mkt_cap_root']
        weight['mkt_cap_root'] = weight['mkt_cap'].pow(0.5)
        self.weight = weight / weight.sum()

    def _get_factor_exposure(self):
        factors = rq.get_factor_exposure(self.stock_list, self.pre_day, self.pre_day)
        factors.reset_index(inplace=True)
        factors['order_book_id'] = factors['order_book_id'].apply(lambda x: f"{x[:7]}{exchange_map[x[7:]]}")
        factors.drop('date', axis=1, inplace=True)
        factors.dropna(axis=0, inplace=True)
        self.factors = factors.set_index('order_book_id')


    def get_rq_data(self):
        return self.factors.merge(self.weight, how = 'inner', on = 'order_book_id')
