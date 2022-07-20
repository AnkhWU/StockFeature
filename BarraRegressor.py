import torch

class BarraRegressor:
    def __init__(self, y, X, w):
        """
        :param y: 各支股票的收益率
        :param X: 各支股票的因子暴露,前11个为风险因子和comovement,后28个为申万一级行业
        :param w: 各支股票的根号市值权重
        """
        self.y = torch.Tensor(y.values)
        self.X = torch.Tensor(X.values)
        self.w = torch.Tensor(w.values)
        self.W = torch.diag_embed(self.w)

    def generate_S(self):
        """
        生成约束矩阵，总共39个因子，前11个为非行业因子（包含了comovement)
        以第1个行业因子作为线性约束的中介，生成约束矩阵S
        :return:
        """
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
        explained_y = self.X @ params

        SS_total = self.w.T @ (self.y ** 2)
        SS_error = self.w.T @ ((self.y - explained_y) ** 2)
        rsq = float(1 - SS_error / SS_total)
        adj_rsq = 1 - (1 - rsq) * (self.y.shape[0] - 1) / (self.y.shape[0] - params.shape[0] - 1)

        return params.numpy(), rsq, adj_rsq