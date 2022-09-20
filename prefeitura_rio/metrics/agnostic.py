# -*- coding: utf-8 -*-
"""
Framework-agnotic metrics implementations for the prefeitura_rio package.
Most of them can usually be imported from the `sklearn.metrics` module.
"""

import warnings

import numpy as np
from sklearn.metrics import (
    brier_score_loss,
    confusion_matrix,
    fbeta_score,
    mean_squared_error,
)


def brier(
    y_true,
    y_pred,
    *,
    sample_weight=None,
    pos_label=None,
):
    """
    Computes the Brier score loss.

    Args:
        y_true (array-like): The true values.
        y_pred (array-like): The predicted values.
        sample_weight (array-like, optional): The sample weights. Defaults to
            `None`.
        pos_label (int, optional): The positive label. Defaults to `None`.

    Returns:
        float: The Brier score.
    """
    return brier_score_loss(
        y_true, y_pred, sample_weight=sample_weight, pos_label=pos_label
    )


def carabetta(y_true, y_pred):
    """
    Computes the Carabetta score.

    Args:
        y_true (array-like): The true values.
        y_pred (array-like): The predicted values.

    Returns:
        float: The Carabetta score.
    """
    warnings.warn(
        "The `carabetta` metric is not implemented yet. Returning zero.",
        UserWarning,
    )
    return 0.0


def fbeta(
    y_true,
    y_pred,
    *,
    beta,
    labels=None,
    pos_label=1,
    average="binary",
    sample_weight=None,
    zero_division="warn",
    threshold=None,
) -> float:
    """
    Computes the F-beta score.

    Args:
        y_true (array-like): The true values.
        y_pred (array-like): The predicted values.
        beta (float): The beta value.
        labels (array-like, optional): The labels to consider. If `None`, all
            labels are considered. Defaults to `None`.
        pos_label (int, optional): The positive label. Defaults to `1`.
        average (str, optional): The averaging method. Defaults to `'binary'`.
        sample_weight (array-like, optional): The sample weights. Defaults to
            `None`.
        zero_division (str, optional): The value to return when there is a
            zero division. Defaults to `'warn'`.
        threshold (float, optional): The threshold to use when converting
            `y_pred` to binary. Defaults to `None`.

    Returns:
        float: The F-beta score.
    """
    if threshold:
        y_pred = np.array(y_pred) > threshold
    try:
        return fbeta_score(
            y_true,
            y_pred,
            beta=beta,
            labels=labels,
            pos_label=pos_label,
            average=average,
            sample_weight=sample_weight,
            zero_division=zero_division,
        )
    except ValueError as exc:
        # Check for exception message
        if "mix of binary and continuous" in str(exc):
            raise ValueError(
                "`y_true` and `y_pred` must be binary. If your `y_pred` is not"
                " binary, try setting the `threshold` parameter."
            ) from exc
        raise exc


def mse(y_true, y_pred, *, sample_weight=None, multioutput="uniform_average"):
    """
    Computes the MSE score.

    Args:
        y_true (array-like): The true values.
        y_pred (array-like): The predicted values.
        sample_weight (array-like, optional): The sample weights. Defaults to
            `None`.
        multioutput (str, optional): The averaging method. Defaults to
            `'uniform_average'`.

    Returns:
        float: The MSE score.
    """
    return mean_squared_error(
        y_true, y_pred, sample_weight=sample_weight, multioutput=multioutput
    )


def nash_sutcliffe(
    y_true,
    y_pred,
):
    """
    Computes the Nash-Sutcliffe score.

    Args:
        y_true (array-like): The true values.
        y_pred (array-like): The predicted values.

    Returns:
        float: The Nash-Sutcliffe score.
    """
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    return 1 - np.sum((y_true - y_pred) ** 2) / np.sum((y_true - np.mean(y_true)) ** 2)


def pet(y_true, y_pred, *, sample_weight=None, multioutput="uniform_average"):
    """
    Computes the PET score.

    Args:
        y_true (array-like): The true values.
        y_pred (array-like): The predicted values.
        sample_weight (array-like, optional): The sample weights. Defaults to
            `None`.
        multioutput (str, optional): The averaging method. Defaults to
            `'uniform_average'`.

    Returns:
        float: The PET score.
    """
    return np.tanh(
        mean_squared_error(
            y_true, y_pred, sample_weight=sample_weight, multioutput=multioutput
        )
    )


def rmse(y_true, y_pred, *, sample_weight=None, multioutput="uniform_average"):
    """
    Computes the RMSE score.

    Args:
        y_true (array-like): The true values.
        y_pred (array-like): The predicted values.
        sample_weight (array-like, optional): The sample weights. Defaults to
            `None`.
        multioutput (str, optional): The averaging method. Defaults to
            `'uniform_average'`.

    Returns:
        float: The RMSE score.
    """
    return np.sqrt(
        mean_squared_error(
            y_true, y_pred, sample_weight=sample_weight, multioutput=multioutput
        )
    )


def sp(
    y_true,
    y_pred,
    *,
    labels=None,
    sample_weight=None,
    normalize=None,
    threshold=None,
):
    """
    Computes the SP score.

    Args:
        y_true (array-like): The true values.
        y_pred (array-like): The predicted values.
        labels (array-like, optional): List of labels to index the matrix. This may be used to
            reorder or select a subset of labels. If None is given, those that appear at least
            once in y_true or y_pred are used in sorted order.
        sample_weight (array-like, optional): The sample weights. Defaults to
            `None`.
        normalize (str, optional): Normalizes confusion matrix over the true (rows), predicted
            (columns) conditions or all the population. If None, confusion matrix will not be
            normalized.
        threshold (float, optional): The threshold to use when converting
            `y_pred` to binary. Defaults to `None`.

    Returns:
        float: The SP score.
    """
    if threshold:
        y_pred = np.array(y_pred) > threshold
    try:
        conf_matrix = confusion_matrix(
            y_true,
            y_pred,
            labels=labels,
            sample_weight=sample_weight,
            normalize=normalize,
        )
        true_negative = conf_matrix[0][0]
        false_negative = conf_matrix[1][0]
        true_positive = conf_matrix[1][1]
        false_positive = conf_matrix[0][1]
        fa = false_positive / (true_negative + false_positive + np.finfo(float).eps)
        pd = true_positive / (true_positive + false_negative + np.finfo(float).eps)
        sp = np.sqrt(np.sqrt(pd * (1 - fa)) * (0.5 * (pd + (1 - fa))))
        return sp
    except ValueError as exc:
        # Check for exception message
        if "mix of binary and continuous" in str(exc):
            raise ValueError(
                "`y_true` and `y_pred` must be binary. If your `y_pred` is not"
                " binary, try setting the `threshold` parameter."
            ) from exc
        raise exc
