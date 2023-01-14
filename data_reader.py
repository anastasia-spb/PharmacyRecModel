import os

import numpy as np
import pandas as pd
import warnings


def convert_dtype(x):
    try:
        return int(x)
    except:
        return np.nan


def convert_dtype_with_comma(x):
    try:
        return int(x.split(",")[0])
    except:
        return np.nan


class DataReader:
    def __init__(self, data_path):
        self.receipts_data = self.__read_receipts_data(data_path)
        self.products_list = self.__read_products_list(data_path)

    def __read_receipts_data(self, data_path):
        """Read input data and apply base preprocessing
        such as converting column values to expected types,
        removing invalid values and drop duplicates
        """
        receipts_data_path = os.path.join(data_path, "чековые данные.csv")
        receipts_data = pd.read_csv(receipts_data_path,
                                         parse_dates=['sale_date_date'],
                                         on_bad_lines='skip',
                                         converters={"contact_id": convert_dtype,
                                                     "shop_id": convert_dtype,
                                                     "product_id": convert_dtype,
                                                     "product_sub_category_id": convert_dtype,
                                                     "product_category_id": convert_dtype,
                                                     "brand_id": convert_dtype,
                                                     "quantity": convert_dtype_with_comma})
        receipts_data.dropna(inplace=True)
        receipts_data = receipts_data.astype({'contact_id': 'int32', 'shop_id': 'int32',
                                                        'product_id': 'int32', 'product_sub_category_id': 'int32',
                                                        'product_category_id': 'int32', 'brand_id': 'int32',
                                                        'quantity': 'int32'})

        receipts_data.drop_duplicates(inplace=True)
        return receipts_data

    def __read_products_list(self, data_path):
        product_data_path = os.path.join(data_path, "products.csv")
        return pd.read_csv(product_data_path)

    def get_product_description_by_id(self, index: int):
        return self.products_list["product"].loc[self.products_list['product_id'] == index].values[0]

    def save(self, path):
        file_path = os.path.join(path, "receipts_data_filtered.csv")
        if not os.path.exists(file_path):
            self.receipts_data.to_csv(file_path)
        else:
            warnings.warn("File already exists")

    def remove_rows_with_product_ids(self, ids_to_remove: list):
        self.receipts_data = self.receipts_data.loc[~self.receipts_data['product_id'].isin(ids_to_remove)]

    def get_receipts_data(self):
        return self.receipts_data

    def get_products_description(self):
        return self.products_list

