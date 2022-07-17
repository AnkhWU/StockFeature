import pandas as pd
import torch
from numpy import float64

from datetime import datetime, timedelta

from calculation_module.constants import *


def calculation(updating_dict, data_prep):
    bars_df = pd.DataFrame(updating_dict).T
    bars_df.columns = ["preclose", "return"]
    bars_df.index.name = "order_book_id"
    bars_df = bars_df.pct_change(axis=1)
    df = pd.merge(data_prep, bars_df, how="inner", on="order_book_id")
    
    df.drop("preclose", axis=1, inplace=True)
    df.sort_values(by="order_book_id")

    model = BarraRegressor(df['return'], df[all_factor], df['mkt_cap_root'])
    model.fit()
    params, Rsq, adj_Rsq, X_ctrb, X_ratio = model.calRsq()
    
    now = datetime.utcnow()
    params = pd.DataFrame(params.astype(float64), index=all_factor).reset_index()
    params.index = pd.date_range(now - timedelta(seconds = 0.5), now , params.shape[0])
    X_ctrb = pd.DataFrame(X_ctrb.astype(float64), index=df.index, columns=all_factor_ctrb).reset_index()
    X_ctrb.index = pd.date_range(now - timedelta(seconds = 0.5), now , X_ctrb.shape[0])
    X_ratio = pd.DataFrame(X_ratio.astype(float64), index=df.index, columns=all_factor_ratio).reset_index()
    X_ratio.index = pd.date_range(now - timedelta(seconds=0.5), now, X_ratio.shape[0])
    
    return params, Rsq, adj_Rsq, X_ctrb, X_ratio


class BarraRegressor:
    def __init__(self, y, X, w):
        self.y = torch.Tensor(y.values)
        self.X = torch.Tensor(X.values)
        self.w = torch.Tensor(w.values)
        self.W = torch.diag_embed(self.w)
        self.tickers = y.index.tolist()


    def generate_S(self):
        S = self.w.T @ self.X
        S = torch.diag(S)
        S[:11, :11] = torch.eye(11)
        S[11] = - torch.sum(S[12:], 0) / S[11, 11]
        S[12:, 12:] = torch.eye(S.shape[1] - 12)
        self.S = S[:, torch.arange(S.size(1))!=11]


    def fit(self):
        self.generate_S()
        XS = self.X @ self.S
        XSW = XS.T @ self.W
        self.params = self.S @ torch.inverse(XSW @ XS) @ XSW @ self.y

    def calRsq(self):
        X_ctrb = self.X * self.params
        explained_y = torch.sum(X_ctrb, 1)
        X_ratio = X_ctrb / torch.sum(X_ctrb, 0)
        SS_total = self.w.T @ self.y ** (2)
        SS_error = self.w.T @ (self.y - explained_y) ** (2)
        rsq = 1 - SS_error / SS_total
        adj_rsq = 1 - (1 - rsq) * (len(self.y) - 1) / (len(self.y) - len(self.params) - 1)
        return self.params.numpy(), float(rsq), float(adj_rsq), X_ctrb.numpy(), X_ratio.numpy()
