# -*- coding: utf-8 -*-
import numpy as np

from prefeitura_rio.metrics import (
    brier,
    carabetta,
    evaluate,
    fbeta,
    mse,
    nash_sutcliffe,
    pet,
    rmse,
    sp,
)

sample_y_true = np.array([0, 1, 1, 0])
sample_y_pred = np.array([0.1, 0.4, 0.7, 0.5])

expected_results = {
    "Brier": 0.1775,
    "Carabetta": 5.7,
    "F-Beta": 2 / 3,
    "MSE": 0.1775,
    "Nash-Sutcliffe": 0.29,
    "PET": np.tanh(0.1775),
    "RMSE": np.sqrt(0.1775),
    "SP": 0.7282376575609851,
}


def compare_numeric_dictionaries(d1, d2):
    """
    Compares two dictionaries of numeric values.

    Args:
        d1 (dict): The first dictionary.
        d2 (dict): The second dictionary.

    Returns:
        bool: `True` if the dictionaries match, `False` otherwise.
    """
    for key in d1:
        if abs(d1[key] - d2[key]) >= 1e-6:
            return False
    return True


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
    return abs(result - expected) < 1e-6


def test_brier():
    """
    Tests the `brier` function.
    """
    expected = expected_results["Brier"]
    assert compare_results(brier, expected)


def test_carabetta():
    """
    Tests the `carabetta` function.
    """
    expected = expected_results["Carabetta"]
    assert compare_results(carabetta, expected)


def test_fbeta():
    """
    Tests the `fbeta` function.
    """
    expected = expected_results["F-Beta"]
    assert compare_results(
        lambda y_true, y_pred: fbeta(y_true, y_pred, beta=1, threshold=0.5), expected
    )


def test_mse():
    """
    Tests the `mse` function.
    """
    expected = expected_results["MSE"]
    assert compare_results(mse, expected)


def test_nash_sutcliffe():
    """
    Tests the `nash_sutcliffe` function.
    """
    expected = expected_results["Nash-Sutcliffe"]
    assert compare_results(nash_sutcliffe, expected)


def test_pet():
    """
    Tests the `pet` function.
    """
    expected = expected_results["PET"]
    assert compare_results(pet, expected)


def test_rmse():
    """
    Tests the `rmse` function.
    """
    expected = expected_results["RMSE"]
    assert compare_results(rmse, expected)


def test_sp():
    """
    Tests the `sp` function.
    """
    expected = expected_results["SP"]
    assert compare_results(lambda y_true, y_pred: sp(y_true, y_pred, threshold=0.5), expected)


def test_evaluate():
    """
    Tests the `evaluate` function.
    """
    result = evaluate(sample_y_true, sample_y_pred)
    assert compare_numeric_dictionaries(result, expected_results)


if __name__ == "__main__":
    test_brier()
    test_carabetta()
    test_fbeta()
    test_mse()
    test_nash_sutcliffe()
    test_pet()
    test_rmse()
    test_sp()
    test_evaluate()
