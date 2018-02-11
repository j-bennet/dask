# -*- coding: utf-8
import numpy as np
import pandas as pd
import pandas.util.testing as tm

import sys
import os
import dask
import pytest

from time import sleep

import dask.dataframe as dd

from dask.utils import tmpfile, tmpdir, dependency_depth

from dask.dataframe.utils import assert_eq

import numpy as np
import pandas as pd
import pandas.util.testing as tm

import sys
import os
import dask
import pytest

from time import sleep

import dask.dataframe as dd

from dask.utils import tmpfile, tmpdir, dependency_depth

from dask.dataframe.utils import assert_eq


@pytest.mark.parametrize('data, compare', [
    (pd.DataFrame({'x': ['a', 'b', 'c', 'd'],
                   'y': [1, 2, 3, 4]}),
     tm.assert_frame_equal)
])
def test_read_json(data, compare):
    with tmpfile('json') as f:
        data.to_json(f, orient='records')
        actual = dd.read_json(f, orient='records')

        compare(actual.compute(), data)

        compare(dd.read_json(f).compute(), pd.read_json(f))

        assert (sorted(dd.read_json(f, orient='records').dask) ==
                sorted(dd.read_json(f, orient='records').dask))
