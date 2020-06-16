from kedro.pipeline import Pipeline, node

from .nodes import hktvmall_conn_node, gen_hktvmall_full_site_links, \
    request_hktvmall_catagory_code, gen_hktvmall_product_by_method_and_cat_links, raw_etl, \
    categories_df_etl, multi_threading_req, df_to_kedro_csvdataset


def create_pipeline(**kwargs):
    pipe = []
    # conn start
    pipe.append(node(
        hktvmall_conn_node,
        inputs="params:hktvmall_home_url",
        outputs="HktvmallHeader",)
    )
    # categories
    pipe.append(node(
        request_hktvmall_catagory_code,
        inputs=["HktvmallHeader", "params:hktvmall_category_diction_url"],
        outputs="Categories_raw",)
    )
    # categories ETL
    pipe.append(node(
        categories_df_etl,
        inputs="Categories_raw",
        outputs="CategoriesExtracted_df",)
    )
    # generate links by method+category
    pipe.append(node(
        gen_hktvmall_product_by_method_and_cat_links,
        inputs=['params:hktvmall_catagory_code', 'params:hktvmall_browse_method', "params:product_by_method_catcode_url"],
        outputs=dict(
            method1="PromotionDifferenceURL_list",
            method2="HotPickOrderURL_list"
        ))
    )
    # gen full site url
    pipe.append(node(
        gen_hktvmall_full_site_links,
        inputs=["CategoriesExtracted_df", 'params:hktvmall_cat_product_url'],
        outputs="full_site_url")
    )
    # get PromotionDifference_raw_df (method 1)
    pipe.append(node(
        multi_threading_req,
        inputs=["HktvmallHeader", "PromotionDifferenceURL_list"],
        outputs="PromotionDifference_raw_list")
    )
    # get HotPickOrder_raw_df (method 2)
    pipe.append(node(
        multi_threading_req,
        inputs=["HktvmallHeader", "HotPickOrderURL_list"],
        outputs="HotPickOrder_raw_list")
    )
    # get FullSite_raw_df
    pipe.append(node(
        multi_threading_req,
        inputs=["HktvmallHeader", 'full_site_url'],
        outputs="FullSite_raw_list")
    )
    # filtering df necessary columns
    pipe.append(node(
        raw_etl,
        inputs="FullSite_raw_list",
        outputs="FullSite_raw_df")
    )
    pipe.append(node(
        raw_etl,
        inputs="PromotionDifference_raw_list",
        outputs="PromotionDifference_raw_df")
    )
    pipe.append(node(
        raw_etl,
        inputs="HotPickOrder_raw_list",
        outputs="HotPickOrder_raw_df")
    )
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################
    ####################################################################################################################

    # turn PromotionDifference_raw_df to PromotionDifference_raw
    pipe.append(node(
        df_to_kedro_csvdataset,
        inputs=["PromotionDifference_raw_df", "params:promotion_diff_path"],
        outputs="PromotionDifference_raw")
    )
    # turn HotPickOrder_raw_df to HotPickOrder_raw
    pipe.append(node(
        df_to_kedro_csvdataset,
        inputs=["HotPickOrder_raw_df", "params:hotpick_path"],
        outputs="HotPickOrder_raw")
    )
    # turn hktv_mall_category_extracted to HotPickOrder_raw
    pipe.append(node(
        df_to_kedro_csvdataset,
        inputs=["CategoriesExtracted_df", "params:catalog_path"],
        outputs="Catelog_elt")
    )
    # turn PromotionDifference_raw_df to PromotionDifference_raw
    pipe.append(node(
        df_to_kedro_csvdataset,
        inputs=["FullSite_raw_df", "params:fullsite_path"],
        outputs="FullSite_raw")
    )

    return Pipeline(pipe)
