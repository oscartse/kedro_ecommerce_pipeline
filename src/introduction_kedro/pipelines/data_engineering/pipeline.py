from kedro.pipeline import Pipeline, node

from .nodes import hktvmall_conn_node, gen_hktvmall_full_site_links, \
    request_hktvmall_catagory_code, gen_hktvmall_product_by_method_and_cat_links, raw_etl, \
    categories_df_etl, multi_threading_req, df_to_kedro_csvdataset


def create_pipeline(**kwargs):

    # conn start
    start_conn = [
        node(
            hktvmall_conn_node,
            inputs="params:hktvmall_home_url",
            outputs="HktvmallHeader",
            tags="Preparation"
        )
    ]

    # get HKTV mall categories
    get_category = [
        node(
            request_hktvmall_catagory_code,
            inputs=["HktvmallHeader", "params:hktvmall_category_diction_url"],
            outputs="category_raw_req",
            tags="Preparation"
        ),
        node(
            categories_df_etl,
            inputs="category_raw_req",
            outputs="category_df",
            tags="Preparation"
        )
    ]

    # generate urls by type for requests
    gen_url_list = [
        node(
            gen_hktvmall_product_by_method_and_cat_links,
            inputs=['params:hktvmall_catagory_code', 'params:hktvmall_browse_method',
                    "params:product_by_method_catcode_url"],
            outputs=dict(
                method1="promotiondiff_url_list",
                method2="hotpickorder_url_list"
            ),
            tags="Preparation"
        ),
        node(
            gen_hktvmall_full_site_links,
            inputs=["category_df", 'params:hktvmall_cat_product_url'],
            outputs="fullsite_url_list",
            tags="Preparation"
        )
    ]

    # multi threading requires for raw data
    req_raw_df = [
        node(
            multi_threading_req,
            inputs=["HktvmallHeader", "promotiondiff_url_list"],
            outputs="promotiondiff_raw_list",
            tags="Requests"
        ),
        node(
            multi_threading_req,
            inputs=["HktvmallHeader", "hotpickorder_url_list"],
            outputs="hotpickorder_raw_list",
            tags="Requests"
        ),
        node(
            multi_threading_req,
            inputs=["HktvmallHeader", 'fullsite_url_list'],
            outputs="fullsite_raw_list",
            tags="Requests"
        )
    ]

    # ETL on df columns for proper columns
    etl_on_df = [
        node(
            raw_etl,
            inputs="promotiondiff_raw_list",
            outputs="promotiondiff_raw_df",
            tags="ETL"
        ),
        node(
            raw_etl,
            inputs="hotpickorder_raw_list",
            outputs="hotpickorder_raw_df",
            tags="ETL"
        ),
        node(
            raw_etl,
            inputs="fullsite_raw_list",
            outputs="fullsite_raw_df",
            tags="ETL"
        ),
    ]

    # turn df to CSVDataSet
    df_to_csv = [
        node(
            df_to_kedro_csvdataset,
            inputs=["category_df", "params:category_path"],
            outputs="category_raw",
            tags="Saving Data"
        ),
        node(
            df_to_kedro_csvdataset,
            inputs=["promotiondiff_raw_df", "params:promotiondiff_path"],
            outputs="promotiondiff_raw",
            tags="Saving Data"
        ),
        node(
            df_to_kedro_csvdataset,
            inputs=["hotpickorder_raw_df", "params:hotpickorder_path"],
            outputs="hotpickorder_raw",
            tags="Saving Data"
        ),
        node(
            df_to_kedro_csvdataset,
            inputs=["fullsite_raw_df", "params:fullsite_path"],
            outputs="fullsite_raw",
            tags="Saving Data"
        )
    ]

    pipe = start_conn + get_category + gen_url_list + req_raw_df + etl_on_df + df_to_csv

    # pipe = start_conn + get_category
    # pipe.append(
    #     node(
    #         gen_hktvmall_full_site_links,
    #         inputs=["category_df", 'params:hktvmall_cat_product_url'],
    #         outputs="fullsite_url_list",
    #         tags="Preparation"
    #     )
    # )
    # pipe.append(
    #     node(
    #         multi_threading_req,
    #         inputs=["HktvmallHeader", 'fullsite_url_list'],
    #         outputs="fullsite_raw_list",
    #         tags="Requests"
    #     )
    # )
    # pipe.append(
    #     node(
    #         raw_etl,
    #         inputs="fullsite_raw_list",
    #         outputs="fullsite_raw_df",
    #         tags="ETL"
    #     ),
    # )
    # pipe.append(
    #     node(
    #         df_to_kedro_csvdataset,
    #         inputs=["fullsite_raw_df", "params:fullsite_path"],
    #         outputs="fullsite_raw",
    #         tags="Saving Data"
    #     )
    # )

    return Pipeline(pipe)
