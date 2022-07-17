import numpy as np


class BarraRegressor:
    def __init__(self, y, X, w):
        self.y = y.values
        self.X = X.values
        self.w = w.values
        self.W = np.diag(w.values)

    def generate_S(self):
        S = self.w.T @ self.X
        S = np.diag(S)
        S[:11, :11] = np.eye(11)
        S.flags.writeable = True
        S[11] = - np.sum(S[12:], axis=0) / S[11, 11]
        S[12:, 12:] = np.eye(27)
        self.S = np.delete(S, 11, 1)

    def fit(self):
        self.generate_S()
        XS = self.X @ self.S
        XSW = XS.T @ self.W
        self.params = self.S @ np.linalg.inv(XSW @ XS) @ XSW @ self.y

    def calRsq(self):
        X_ctrb = self.X * self.params
        explained_y = np.sum(X_ctrb, axis = 1)
        X_ratio = X_ctrb / np.sum(X_ctrb, axis=0)
        SS_total = self.w.T @ self.y ** (2)
        SS_error = self.w.T @ (self.y - explained_y) ** (2)
        rsq = 1 - SS_error / SS_total
        adj_rsq = 1 - (1 - rsq) * (len(self.y) - 1) / (len(self.y) - len(self.params) - 1)


        X_ctrb = self.X[:, 0] * self.params[0]


        # return [rsq, adj_rsq, X_ctrb, X_ratio]