# -*- coding: utf-8
import os
import pandas as pd
import pandas.util.testing as tm

import pytest
import dask.dataframe as dd

from dask.utils import tmpfile, tmpdir


@pytest.mark.parametrize('df, compare', [
    (pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                   'y': [1, 2, 3, 4]}),
     tm.assert_frame_equal)
])
def test_read_json(df, compare):
    with tmpfile('json') as f:
        df.to_json(f, orient='records', lines=True)
        actual = dd.read_json(f, orient='records', lines=True)
        actual_pd = pd.read_json(f, orient='records', lines=True)

        compare(actual.compute(), df)
        compare(actual.compute(), actual_pd)

        assert (sorted(actual.dask) ==
                sorted(dd.read_json(f, orient='records', lines=True).dask))


@pytest.mark.parametrize('df, compare', [
    (dd.from_pandas(pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                                  'y': [1, 2, 3, 4]}),
                    npartitions=2),
     tm.assert_frame_equal)
])
def test_write_json(df, compare):
    with tmpdir() as path:
        df.to_json(path, orient='records', lines=True)
        actual = dd.read_json(os.path.join(path, '*'), orient='records', lines=True)
        compare(actual.compute(), df.compute())
