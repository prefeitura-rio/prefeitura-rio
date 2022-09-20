# -*- coding: utf-8 -*-
"""
Metrics implementations for the prefeitura_rio package.

In this module, we define the metrics that will be used to evaluate the
performance of the models. The metrics are defined as functions that take
the true and predicted values as input and return a single value.

In the `agnostic` module, we define metrics that are agnostic to any
framework.
"""
from typing import Callable, Dict

from .agnostic import (
    brier,
    carabetta,
    fbeta,
    mse,
    nash_sutcliffe,
    pet,
    rmse,
    sp,
)


def evaluate(
    y_true,
    y_pred,
    *,
    metrics: Dict[str, Callable] = None,
    beta=1,
    threshold=0.5,
) -> Dict[str, float]:
    """
    Evaluates the performance of a model.

    Args:
        y_true (array-like): The true values.
        y_pred (array-like): The predicted values.
        metrics (dict, optional): A dictionary of metrics to be evaluated.
            Defaults to `None`.
        beta (int, optional): The beta value for the F-beta score. Defaults to
            `1`.
        threshold (float, optional): The threshold for the F-beta score.
            Defaults to `0.5`.

    Returns:
        dict: A dictionary with the results of the metrics.
    """
    if metrics is None:
        metrics = {
            "Brier": brier,
            "Carabetta": carabetta,
            "F-Beta": lambda y_true, y_pred: fbeta(
                y_true, y_pred, beta=beta, threshold=threshold
            ),
            "MSE": mse,
            "Nash-Sutcliffe": nash_sutcliffe,
            "PET": pet,
            "RMSE": rmse,
            "SP": lambda y_true, y_pred: sp(y_true, y_pred, threshold=threshold),
        }
    return {name: func(y_true, y_pred) for name, func in metrics.items()}
