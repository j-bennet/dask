# -*- coding: utf-8
from __future__ import absolute_import, division, print_function

from glob import glob

import pandas as pd

from .io import from_pandas
from ...compatibility import PY3
from ...bytes import open_files
from ...delayed import delayed


def pd_to_json(df, path_or_buf, **kwargs):
    """Use pandas to write one dataframe chunk to JSON."""
    with path_or_buf as f:
        df.to_json(f, **kwargs)


def to_json(df, path, compute=True, get=None, name_function=None, encoding='utf-8',
            compression=None, **kwargs):
    """Store Dask.dataframe to JSON files.

    encoding : string, optional
        A string representing the encoding to use in the output file,
        defaults to 'ascii' on Python 2 and 'utf-8' on Python 3.
    """
    mode = 'w' if PY3 else 'wb'
    files = open_files(path, compression=compression, mode=mode, encoding=encoding,
                       name_function=name_function, num=df.npartitions)

    write_chunk = delayed(pd_to_json, pure=False)
    values = [write_chunk(d, f, **kwargs)
              for d, f
              in zip(df.to_delayed(), files)]
    if compute:
        delayed(values).compute(get=get)
        return [f.path for f in files]
    else:
        return values


def read_json(pattern, npartitions=None, chunksize=1000000, **kwargs):
    """Read JSON files into a Dask DataFrame.

    This function is like ``pandas.read_json``, except it can read from a single large file,
    or from multiple files. Please see the Pandas docstring for more detailed information
    about shared keyword arguments.

    Parameters
    ----------
    pattern : string, list
        File pattern (string), buffer to read from, or list of file
        paths. Can contain wildcards.
    npartitions : int, optional
        The number of partitions of the index to create. Note that depending on
        the size and index of the dataframe, the output may have fewer
        partitions than requested.
    chunksize : positive integer, optional
        Maximal number of rows per partition (default is 1000000).

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

    missing_index_info = kwargs.get('orient', None) in ['records', 'values']

    from ..multi import concat
    df = concat([from_pandas(pd.read_json(path, **kwargs),
                             npartitions=npartitions,
                             chunksize=chunksize)
                 for path in paths],
                interleave_partitions=missing_index_info)

    # If we did not have indexed chunks, let's reindex the dataframe, otherwise its index
    # will be created by stacking chunk indices, and those chunk indices don't make any sense
    # together.
    if missing_index_info:
        df = df.reset_index(drop=True)

    return df


if PY3:
    from ..core import _Frame
    _Frame.to_json.__doc__ = to_json.__doc__

