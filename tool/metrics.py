'''
Autor: Mijie Pang
Date: 2023-09-14 21:08:27
LastEditTime: 2024-02-20 19:50:09
Description: 
'''
import numpy as np


class MetricOneD:

    def __init__(self, *args) -> None:

        if len(args) == 1:
            self.x = np.array(args[0])
        else:
            ValueError('length of input data must be 1')

    def calculate(self, method: str) -> float:

        methods = {'std': self.STD, 'var': self.Var}
        try:
            return methods.get(method)()
        except:
            raise ValueError('Invalid method')

    ### Standard Deviation ###
    def STD(self, ) -> float:
        return np.sqrt(np.mean((self.x - np.mean(self.x))**2))

    ### Variance ###
    def Var(self, ) -> float:
        return np.mean((self.x - np.mean(self.x))**2)


class MetricTwoD:

    def __init__(self, *args) -> None:  # input : obs-model

        if len(args) == 2:
            if len(args[0]) == len(args[1]):
                self.x1 = np.array(args[0])
                self.x2 = np.array(args[1])
            else:
                raise ValueError('length of input data must be equal')
        else:
            raise ValueError('length of input data must be 2')

    def calculate(self, method: str) -> float:

        methods = {
            'rmse': self.RMSE,
            'mb': self.MB,
            'mae': self.MAE,
            'nmb': self.NMB,
            'nme': self.NME,
            'r': self.R
        }
        try:
            return methods.get(method)()
        except:
            raise ValueError('Invalid method')

    ### Root Mean Square Error ###
    def RMSE(self, ) -> float:
        return np.sqrt(np.mean((self.x1 - self.x2)**2))

    ### Mean Bias ###
    def MB(self, ) -> float:
        return np.mean(self.x2 - self.x1)

    ### Mean Absolute Error ###
    def MAE(self, ) -> float:
        return np.mean(np.abs(self.x2 - self.x1))

    ### Normalized Mean Bias ###
    def NMB(self, ) -> float:
        return np.nansum(self.x2 - self.x1) / np.nansum(self.x1)

    ### Normalized Mean Error ###
    def NME(self, ) -> float:
        return np.nansum(np.abs(self.x2 - self.x1)) / np.nansum(self.x1)

    ### Correlation ###
    def R(self, ) -> float:
        return np.corrcoef(self.x1, self.x2)[1, 0]


if __name__ == '__main__':

    metric = MetricTwoD([33, 70, 100, 300, 667, 32], [2, 6, 8, 2, 5, 6])
    print(metric.calculate('r'))
