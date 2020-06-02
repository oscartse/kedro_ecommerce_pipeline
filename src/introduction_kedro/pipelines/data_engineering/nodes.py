from typing import Any, Dict
import pandas as pd
import requests
import random
from kedro.extras.datasets.pandas import CSVDataSet

LUMINATI_PASS = "hja29x3mhtyy"
LUMINATI_USER = "lum-customer-klook-zone-shared_data_center"
LUMINATI_HOST = "zproxy.lum-superproxy.io"
LUMINATI_PORT = 22225
user_agent_list = [
    # Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    # Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]


def get_proxy_credentials():
    proxy = "{}:{}@{}:{}".format(LUMINATI_USER, LUMINATI_PASS, LUMINATI_HOST, LUMINATI_PORT)
    # lum-customer-klook-zone-shared_data_center:hja29x3mhtyy@zproxy.lum-superproxy.io:22225
    return proxy


def proxy_server():
    proxies = {
        "http": get_proxy_credentials(),
        "https": get_proxy_credentials()
    }
    return proxies


def hktvmall_conn_node(link: str) -> dict:
    r = requests.get(link)
    cook = []
    for c in r.cookies:
        cook.append("{}".format(c.name) + "=" + "{}".format(c.value))
    user_agent = random.choice(user_agent_list)
    headers = {
        'Cookie': "; ".join(cook),
        'User-Agent': user_agent,
    }
    return headers


def request_hktvmall_catagory_code(headers: dict, category_directory_url: str) \
        -> CSVDataSet:
    from lxml import html
    from datetime import datetime
    import time

    category_directory_html = requests.get(category_directory_url).content
    tree = html.fromstring(category_directory_html)
    category_list = tree.xpath('//div[@class="directory-navbar"]/ul/a/li/@data-zone')
    all_categories = []

    # category raw dta
    for i in category_list:
        get_categories_url = "https://www.hktvmall.com/hktv/zh/ajax/getCategories?categoryCode={}".format(i)
        catalog_raw = requests.request("GET", get_categories_url, headers=headers).json()['categories']
        all_categories.append(catalog_raw)

    # get needed from category raw data
    tmp = []
    for directory in all_categories:
        for category in directory:
            try:
                for subcat in category['subCats']:
                    tmp.append(
                        {
                            "CategoryCode": category['categoryCode'],
                            "CategoryName": category['name'],
                            "SubcategoryCode": subcat['categoryCode'],
                            "SubcategoryName": subcat['name'],
                            "Count": str(subcat['count'])
                        }
                    )
            except TypeError:
                tmp.append(
                    {
                        "CategoryCode": category['categoryCode'],
                        "CategoryName": category['name'],
                        "SubcategoryCode": None,
                        "SubcategoryName": None,
                        "Count": str(category['count'])
                    }
                )
    catalog = pd.DataFrame(tmp)
    catalog['scrap_date'] = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    data_set = CSVDataSet(filepath="data/01_raw/hktv_mall_category.csv")
    data_set.save(catalog)
    reloaded = data_set.load()
    return reloaded


def request_hktvmall_product_raw(headers: dict, links: list, page_size: str) \
        -> CSVDataSet:
    proxies = proxy_server()
    total_df = []
    for link in links:
        url = link.format(page_size)
        product_raw = requests.request("GET", url, headers=headers, proxies=proxies).json()['products']
        try:
            assert all(len(i.keys()) for i in product_raw), "HKTV Mall raw data each products' dictionary key not the same"
            total_df.append(pd.DataFrame(product_raw))
        except AssertionError:
            pass

    data = pd.concat(total_df, ignore_index=True, sort=False)
    data_set = CSVDataSet(filepath="data/01_raw/hktvmall_exist_in_all_df.csv")
    data_set.save(data)
    reloaded = data_set.load()

    return reloaded


def gen_hktvmall_product_link(categories: dict, methods: dict, url: str) -> list:
    all_links = []
    for method in methods.values():
        for code in categories.values():
            thislink = url.format(code, method, code, code) + "&pageSize={}"
            all_links.append(thislink)

    return all_links


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
