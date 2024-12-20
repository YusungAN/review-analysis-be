import numpy as np


def minmax_scaler(x):
    _max = x.max()
    _min = x.min()
    _denom = _max - _min
    return (x - _min) / _denom


def pearson_corr(a, b):
    return np.dot((a - np.mean(a)), (b - np.mean(b))) / ((np.linalg.norm(a - np.mean(a))) * (np.linalg.norm(b - np.mean(b))))


def mse(a, b):
    return ((a-b)**2).mean()


def distance(a, b):
    return abs(a - b)


def dynamic_time_warping(x, y):
    m = len(x)
    n = len(y)

    dtw = np.zeros((m+1, n+1))
    dtw[0, 1:] = np.inf

    dtw[1:, 0] = np.inf

    for i in range(1, m+1):
        for j in range(1, n+1):
            cost = distance(x[i-1], y[j-1])
            dtw[i, j] = cost + min(dtw[i-1, j], dtw[i, j-1], dtw[i-1, j-1])

    return dtw[m, n]