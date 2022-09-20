# -*- coding: utf-8 -*-
import numpy as np

from prefeitura_rio.metrics import (
    brier,
    carabetta,
    fbeta,
    mse,
    nash_sutcliffe,
    pet,
    rmse,
    sp,
)

sample_y_true = np.array([0, 1, 1, 0])
sample_y_pred = np.array([0.1, 0.4, 0.7, 0.5])


def compare_results(func, expected):
    """
    Compares the results of a function with an expected value.

    Args:
        func (function): The function to be tested.
        expected (float): The expected value.

    Returns:
        bool: `True` if the results match, `False` otherwise.
    """
    result = func(sample_y_true, sample_y_pred)
    print(result)
    return abs(result - expected) < 1e-6


def test_brier():
    """
    Tests the `brier` function.
    """
    expected = 0.1775
    assert compare_results(brier, expected)


def test_carabetta():
    """
    Tests the `carabetta` function.
    """
    pass


def test_fbeta():
    """
    Tests the `fbeta` function.
    """
    expected = 2 / 3
    assert compare_results(
        lambda y_true, y_pred: fbeta(y_true, y_pred, beta=1, threshold=0.5), expected
    )


def test_mse():
    """
    Tests the `mse` function.
    """
    expected = 0.1775
    assert compare_results(mse, expected)


def test_nash_sutcliffe():
    """
    Tests the `nash_sutcliffe` function.
    """
    pass


def test_pet():
    """
    Tests the `pet` function.
    """
    expected = np.tanh(0.1775)
    assert compare_results(pet, expected)


def test_rmse():
    """
    Tests the `rmse` function.
    """
    expected = np.sqrt(0.1775)
    assert compare_results(rmse, expected)


def test_sp():
    """
    Tests the `sp` function.
    """
    expected = 0.7282376575609851
    assert compare_results(
        lambda y_true, y_pred: sp(y_true, y_pred, threshold=0.5), expected
    )


if __name__ == "__main__":
    test_brier()
    test_carabetta()
    test_fbeta()
    test_mse()
    test_nash_sutcliffe()
    test_pet()
    test_rmse()
    test_sp()
