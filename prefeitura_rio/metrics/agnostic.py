# -*- coding: utf-8 -*-
"""
Framework-agnotic metrics implementations for the prefeitura_rio package.
Most of them can usually be imported from the `sklearn.metrics` module.
"""
import numpy as np

try:
    from sklearn.metrics import (
        brier_score_loss,
        confusion_matrix,
        fbeta_score,
        mean_squared_error,
    )
except ImportError:
    raise ImportError(
        "Unmet dependencies for the `prefeitura_rio.metrics` submodules. "
        "Please install the prefeitura-rio[metrics] extra."
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
    raise NotImplementedError("The Carabetta score is not implemented yet.")


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

    Returns:
        float: The F-beta score.
    """
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
    raise NotImplementedError("The Nash-Sutcliffe score is not implemented yet.")


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

    Returns:
        float: The SP score.
    """
    conf_matrix = confusion_matrix(
        y_true, y_pred, labels=labels, sample_weight=sample_weight, normalize=normalize
    )
    false_positive = conf_matrix.sum(axis=0) - np.diag(conf_matrix)
    false_negative = conf_matrix.sum(axis=1) - np.diag(conf_matrix)
    true_positive = np.diag(conf_matrix)
    true_negative = conf_matrix.sum() - (
        false_positive + false_negative + true_positive
    )
    fa = false_positive / (true_negative + false_positive + np.finfo(float).eps)
    pd = true_positive / (true_positive + false_negative + np.finfo(float).eps)
    sp = np.sqrt(np.sqrt(pd * (1 - fa)) * (0.5 * (pd + (1 - fa))))
    return sp
