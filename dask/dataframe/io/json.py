# -*- coding: utf-8
from __future__ import absolute_import, division, print_function

from fnmatch import fnmatch
from glob import glob
import os
import uuid
from warnings import warn

import pandas as pd
from toolz import merge

from .io import _link, from_pandas
from ..core import DataFrame, new_dd_object
from ... import multiprocessing
from ...base import tokenize, compute_as_if_collection
from ...bytes.utils import build_name_function
from ...compatibility import PY3
from ...context import _globals
from ...delayed import Delayed, delayed
from ...local import get_sync
from ...utils import effective_get, get_scheduler_lock


def to_json(df, path, append=False, compute=True, **kwargs):
    """Store Dask.dataframe to line-delimited JSON files"""
    pass


def read_json(pattern, npartitions=None, chunksize=1000000, **kwargs):
    """
    Read JSON files (line-delimited) into a Dask DataFrame

    Read json files into a dask dataframe. This function is like
    ``pandas.read_json``, except it can read from a single large file, or from
    multiple files.

    Parameters
    ----------
    pattern : string, list
        File pattern (string), buffer to read from, or list of file
        paths. Can contain wildcards.
    orient : string of expected JSON string format.
        Possible values: "split", "records", "index", "columns", "values".
    chunksize : positive integer, optional
        Maximal number of rows per partition (default is 1000000).
    typ : type of object to recover (series or frame), default ‘frame’
    dtype : boolean or dict, default True
        If True, infer dtypes, if a dict of column to dtype, then use those, if False,
        then don’t infer dtypes at all, applies only to the data.
    convert_axes : boolean, default True
        Try to convert the axes to the proper dtypes.
    convert_dates : boolean, default True
        List of columns to parse for dates; If True, then try to parse datelike columns.
        Default is True. A column label is datelike if:
        * it ends with '_at',
        * it ends with '_time',
        * it begins with 'timestamp',
        * it is 'modified', or
        * it is 'date'

    Returns
    -------
    dask.DataFrame

    Examples
    --------
    Load single file

    >>> dd.read_json('myfile.1.json')  # doctest: +SKIP

    Load multiple files

    >>> dd.read_json('myfile.*.json')  # doctest: +SKIP

    >>> dd.read_json(['myfile.1.json', 'myfile.2.json'])  # doctest: +SKIP
    """
    if isinstance(pattern, str):
        paths = sorted(glob(pattern))
    else:
        paths = pattern
    if chunksize <= 0:
        raise ValueError("Chunksize must be a positive integer")

    from ..multi import concat
    return concat([from_pandas(pd.read_json(path, **kwargs), npartitions=npartitions,
                               chunksize=chunksize)
                   for path in paths])


if PY3:
    from ..core import _Frame
    _Frame.to_json.__doc__ = to_json.__doc__

