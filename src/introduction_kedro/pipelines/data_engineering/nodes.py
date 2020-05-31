# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example code for the nodes in the example pipeline. This code is meant
just for illustrating basic Kedro features.

PLEASE DELETE THIS FILE ONCE YOU START WORKING ON YOUR OWN PROJECT!
"""

from typing import Any, Dict

import pandas as pd

import requests

from kedro.extras.datasets.pandas import CSVDataSet


def hktvmall_conn_node(link: str) -> dict:
    r = requests.get(link)
    cook = []
    for c in r.cookies:
        cook.append("{}".format(c.name) + "=" + "{}".format(c.value))
    headers = {
        'Cookie': "; ".join(cook)
    }
    return headers


def request_hktvmall_product_raw(headers: dict, link: str, page_size: str) -> pd.DataFrame:
    url = str(link).format(page_size)
    product_raw = requests.request("GET", url, headers=headers).json()['products']

    assert all(len(i.keys()) for i in product_raw), \
        "HKTV Mall raw data each products' dictionary key not the same"

    df = pd.DataFrame(product_raw)
    # data_set = CSVDataSet(filepath="data/01_raw")
    # data_set.save(df)
    # reloaded = data_set.load()

    return df


def concat_data_sets(df_discounted: pd.DataFrame, df_top100: pd.DataFrame) -> pd.DataFrame:
    assert all(i.columns for i in [df_discounted, df_top100])

    from functools import reduce
    df = reduce(lambda x, y: pd.merge(x, y, on='code', how='inner'), [df_discounted, df_top100])

    return df


def get_product_comment(headers: dict, product_code: list, comment_url: str, page_size: str) -> pd.DataFrame:
    concatted__comment_raw = []
    for code in product_code:
        url = comment_url.format(code, page_size)
        comment_raw = requests.request("GET", url, headers=headers).json()['reviews']
        assert all(i.keys() for i in comment_raw)
        concatted__comment_raw.append(pd.DataFrame(comment_raw))

    return pd.concat(concatted__comment_raw, ignore_index=True)


def make_scatter_plot(df: pd.DataFrame):
    import matplotlib.pyplot as plt
    fg, ax = plt.subplots()
    for idx, item in enumerate(list(df.species.unique())):
        df[df["species"] == item].plot.scatter(
            x='petal_width',
            y='petal_length',
            label=item,
            color=f"C{idx}",
            ax=ax)
    fg.set_size_inches(12, 12)

    return fg


def split_data(data: pd.DataFrame, example_test_data_ratio: float) -> Dict[str, Any]:
    """Node for splitting the classical Iris data set into training and test
    sets, each split into features and labels.
    The split ratio parameter is taken from conf/project/parameters.yml.
    The data and the parameters will be loaded and provided to your function
    automatically when the pipeline is executed and it is time to run this node.
    """
    data.columns = [
        "sepal_length",
        "sepal_width",
        "petal_length",
        "petal_width",
        "target",
    ]
    classes = sorted(data["target"].unique())
    # One-hot encoding for the target variable
    data = pd.get_dummies(data, columns=["target"], prefix="", prefix_sep="")

    # Shuffle all the data
    data = data.sample(frac=1).reset_index(drop=True)

    # Split to training and testing data
    n = data.shape[0]
    n_test = int(n * example_test_data_ratio)
    training_data = data.iloc[n_test:, :].reset_index(drop=True)
    test_data = data.iloc[:n_test, :].reset_index(drop=True)

    # Split the data to features and labels
    train_data_x = training_data.loc[:, "sepal_length":"petal_width"]
    train_data_y = training_data[classes]
    test_data_x = test_data.loc[:, "sepal_length":"petal_width"]
    test_data_y = test_data[classes]

    # When returning many variables, it is a good practice to give them names:
    return dict(
        train_x=train_data_x,
        train_y=train_data_y,
        test_x=test_data_x,
        test_y=test_data_y,
    )
