import pandas as pd
import torch
import numpy as np
from numpy import float64
from numpy.linalg import pinv

from datetime import datetime, timedelta

from calculation_module.constants import *


def calculation(updating_dict, data_prep, morning):
    bars_df = pd.DataFrame(updating_dict).T
    bars_df.columns = ["preclose", "return"]
    bars_df.index.name = "order_book_id"
    bars_df = bars_df.pct_change(axis=1)
    df = pd.merge(data_prep, bars_df, how="inner", on="order_book_id")
    df = df.astype(float64)
    
    df.drop("preclose", axis=1, inplace=True)
    df.sort_values(by="order_book_id")

    model = BarraRegressor(df['return'], df[all_factor], df['mkt_cap_root'])
    model.fit()
    params, Rsq, adj_Rsq, X_return, X_contrb = model.calRsq()

    if morning:
        now = datetime.utcnow()
    else:
        now = datetime.utcnow() - timedelta(seconds = 5400)

    params = pd.DataFrame(params.astype(float64), index=all_factor).reset_index()
    params.index = pd.date_range(now - timedelta(seconds = 0.5), now , params.shape[0])
    params.columns = ["factor_name", "factor_return"]
    X_return = pd.DataFrame(X_return.astype(float64), index=df.index, columns=all_factor).reset_index()
    X_return.index = pd.date_range(now - timedelta(seconds=0.5), now, X_return.shape[0])
    X_contrb = pd.DataFrame(X_contrb.astype(float64), index=df.index, columns=all_factor).reset_index()
    X_contrb.index = pd.date_range(now - timedelta(seconds=0.5), now, X_contrb.shape[0])
    
    return params, Rsq, adj_Rsq, X_return, X_contrb


class BarraRegressor:
    def __init__(self, y, X, w):
        self.y = torch.Tensor(y.values)
        self.X = torch.Tensor(X.values)
        self.w = torch.Tensor(w.values)
        self.W = torch.diag_embed(self.w)

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
        self.theta = self.S @ torch.inverse(XSW @ XS) @ XSW


    def calRsq(self):
        params = self.theta @ self.y
        if np.isnan(params).any():
            raise ValueError("Error occurred when calculating theta.")
        X_return = (self.theta * self.y).T
        explained_y = self.X @ params
        X_contrb = X_return / torch.sum(X_return, 0)

        SS_total = self.w.T @ (self.y ** 2)
        a = (self.y - explained_y) ** 2
        SS_error = self.w.T @ ((self.y - explained_y) ** 2)
        rsq = float(1 - SS_error / SS_total)
        adj_rsq = 1 - (1 - rsq) * (self.y.shape[0] - 1) / (self.y.shape[0] - params.shape[0] - 1)

        return params.numpy(), rsq, adj_rsq, X_return.numpy(), X_contrb.numpy()
