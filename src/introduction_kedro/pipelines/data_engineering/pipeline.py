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

Delete this when you start working on your own Kedro project.
"""

from kedro.pipeline import Pipeline, node

from .nodes import split_data, request_hktvmall_product_raw, make_scatter_plot, hktvmall_conn_node, concat_data_sets


def create_pipeline(**kwargs):
    return Pipeline(
        [
            # node(
            #     split_data,
            #     inputs=["example_iris_data", "params:example_test_data_ratio"],
            #     outputs=dict(
            #         train_x="example_train_x",
            #         train_y="example_train_y",
            #         test_x="example_test_x",
            #         test_y="example_test_y",
            #     ),
            # ),
            # node(
            #     make_scatter_plot,
            #     inputs="example_iris_data",
            #     outputs="iris_scatter_plot",
            # ),
            node(
                hktvmall_conn_node,
                inputs="params:hktvmall_home_url",
                outputs="hktvmall_header",
            ),
            node(
                request_hktvmall_product_raw,
                inputs=["hktvmall_header", "params:hktvmall_discount_url", "params:hktv_mall_page_size"],
                outputs="hktvmall_discounted_raw",
            ),
            node(
                request_hktvmall_product_raw,
                inputs=["hktvmall_header", "params:hktvmall_top100_url", "params:hktv_mall_page_size"],
                outputs="hktvmall_top100_raw",
            ),
            # node(
            #     concat_data_sets,
            #     inputs=["hktvmall_discounted_raw", "hktvmall_top100_raw"],
            #     outputs="hktvmall_exist_in_all_df",
            # ),

        ]
    )
