import numpy as np


def RSE(pred, true):
    return (
        np.sqrt(np.sum((true - pred) ** 2)) / np.sqrt(np.sum((true - true.mean()) ** 2))
    ).item()


def MAE(pred, true):
    return np.mean(np.abs(pred - true)).item()


def MSE(pred, true):
    return np.mean((pred - true) ** 2).item()


def RMSE(pred, true):
    return np.sqrt(MSE(pred, true)).item()


def MAPE(pred, true):
    return np.mean(np.abs((pred - true) / true)).item()


def MSPE(pred, true):
    return np.mean(np.square((pred - true) / true)).item()


def metric(pred, true):
    mae = MAE(pred, true)
    mse = MSE(pred, true)
    rmse = RMSE(pred, true)
    mape = MAPE(pred, true)
    mspe = MSPE(pred, true)
    rse = RSE(pred, true)

    return mae, mse, rmse, mape, mspe, rse
