from kedro.pipeline import Pipeline, node

from .nodes import request_hktvmall_product_raw, hktvmall_conn_node, request_hktvmall_catagory_code, gen_hktvmall_product_link


def create_pipeline(**kwargs):
    pipe = []

    # conn start
    pipe.append(
        node(
            hktvmall_conn_node,
            inputs="params:hktvmall_home_url",
            outputs="hktvmall_header",
        )
    )

    # categories
    pipe.append(
        node(
            request_hktvmall_catagory_code,
            inputs=["hktvmall_header", "params:hktvmall_category_diction_url"],
            outputs="hktv_mall_category",
        )
    )

    # generate links
    pipe.append(
        node(
            gen_hktvmall_product_link,
            inputs=['params:hktvmall_catagory_code', 'params:hktvmall_browse_method', "params:hktvmall_hotpicks_url"],
            outputs="all_links_for_req"
        )
    )

    # hot picks sets
    pipe.append(
        node(
            request_hktvmall_product_raw,
            inputs=["hktvmall_header", "all_links_for_req", "params:hktv_mall_page_size"],
            outputs="hktvmall_exist_in_all_df"
        )
    )

    return Pipeline(pipe)
