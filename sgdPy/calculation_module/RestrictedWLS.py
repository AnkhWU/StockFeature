import numpy as np
class RWLS:
    def __init__(self, data_merged, factor_names):
        self.X = data_merged[factor_names].values
        self.y = data_merged['ret'].values
        self.weight = data_merged['mkt_cap_root'].values

    def _building_mat_S(self):
        vec_s = self.weight.T @ self.X
        S = np.diag(vec_s)
        S[:11, :11] = np.diag(np.ones(11))
        S.flags.writeable = True
        S[11] = -np.sum(S[12:], axis = 0)/ S[11, 11]
        S[12:, 12:] = np.diag(np.ones(27))
        self.S = np.delete(S, 11, 1)


    def fit(self):
        self._building_mat_S()
        XS = self.X @ self.S
        self.W = np.diag(self.weight)
        theta = self.S @ np.linalg.inv(XS.T @ self.W @ XS) @ XS.T @ self.W
        return theta