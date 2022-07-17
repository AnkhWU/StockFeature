import pandas as pd
import torch
import time


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
