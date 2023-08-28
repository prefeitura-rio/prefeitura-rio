# -*- coding: utf-8 -*-
from typing import List, Union


def assert_dependencies(dependencies: List[str], extras: Union[str, List[str]]):
    """
    Decorator that asserts dependencies are met before running a function.

    Args:
        dependencies (list): A list of dependencies to be checked.
        extras (list): A list of extras to warn about.

    Raises:
        ImportError: If any of the dependencies or extras is not installed.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            base_assert_dependencies(dependencies, extras)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def base_assert_dependencies(dependencies: List[str], extras: Union[str, List[str]]):
    """
    Asserts that the dependencies are installed.

    Args:
        dependencies (list): A list of dependencies to be checked.
        extras (list): A list of extras to warn about.

    Raises:
        ImportError: If any of the dependencies or extras is not installed.
    """
    extras = [extras] if isinstance(extras, str) else extras
    missing_deps = []
    for dep in dependencies:
        try:
            __import__(dep)
        except ImportError:
            missing_deps.append(dep)
    if missing_deps:
        raise ImportError(
            "The following dependencies are missing for what you're trying to do: "
            f"{', '.join(missing_deps)}.\n"
            f"Please install them with `pip install prefeitura_rio[{','.join(extras)}]`."
        )
