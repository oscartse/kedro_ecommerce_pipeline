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
            outputs="HktvmallHeader"
        )
    ]

    # get HKTV mall categories
    get_category = [
        node(
            request_hktvmall_catagory_code,
            inputs=["HktvmallHeader", "params:hktvmall_category_diction_url"],
            outputs="Categories_raw",
        ),
        node(
            categories_df_etl,
            inputs="Categories_raw",
            outputs="CategoriesExtracted_df"
        )
    ]

    # generate urls by type for requests
    gen_url_list = [
        node(
            gen_hktvmall_product_by_method_and_cat_links,
            inputs=['params:hktvmall_catagory_code', 'params:hktvmall_browse_method',
                    "params:product_by_method_catcode_url"],
            outputs=dict(
                method1="PromotionDifferenceURL_list",
                method2="HotPickOrderURL_list"
            )
        ),
        node(
            gen_hktvmall_full_site_links,
            inputs=["CategoriesExtracted_df", 'params:hktvmall_cat_product_url'],
            outputs="full_site_url"
        )
    ]

    # multi threading requires for raw data
    req_raw_df = [
        node(
            multi_threading_req,
            inputs=["HktvmallHeader", "PromotionDifferenceURL_list"],
            outputs="PromotionDifference_raw_list"
        ),
        node(
            multi_threading_req,
            inputs=["HktvmallHeader", "HotPickOrderURL_list"],
            outputs="HotPickOrder_raw_list"
        ),
        node(
            multi_threading_req,
            inputs=["HktvmallHeader", 'full_site_url'],
            outputs="FullSite_raw_list"
        )
    ]

    # ETL on df columns for proper columns
    etl_on_df = [
        node(
            raw_etl,
            inputs="FullSite_raw_list",
            outputs="FullSite_raw_df"
        ),
        node(
            raw_etl,
            inputs="PromotionDifference_raw_list",
            outputs="PromotionDifference_raw_df"
        ),
        node(
            raw_etl,
            inputs="HotPickOrder_raw_list",
            outputs="HotPickOrder_raw_df"
        )

    ]

    # turn df to CSVDataSet
    df_to_csv = [
        node(
            df_to_kedro_csvdataset,
            inputs=["PromotionDifference_raw_df", "params:promotion_diff_path"],
            outputs="PromotionDifference_raw"
        ),
        node(
            df_to_kedro_csvdataset,
            inputs=["HotPickOrder_raw_df", "params:hotpick_path"],
            outputs="HotPickOrder_raw"
        ),
        node(
            df_to_kedro_csvdataset,
            inputs=["CategoriesExtracted_df", "params:catalog_path"],
            outputs="Catelog_elt"
        ),
        node(
            df_to_kedro_csvdataset,
            inputs=["FullSite_raw_df", "params:fullsite_path"],
            outputs="FullSite_raw"
        )
    ]

    pipe = start_conn + get_category + gen_url_list + req_raw_df + etl_on_df + df_to_csv

    return Pipeline(pipe)
