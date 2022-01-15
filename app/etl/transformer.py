from datetime import date, datetime, timedelta
import numpy as np
import re

import pandas as pd


def get_lambda_to_apply(data=None):

    lambda_to_apply = lambda x: x
    if not data:
        return lambda_to_apply

    data_type = data["type"]

    if "value" in dict.keys(data):
        data_value = data["value"]

        lambda_to_apply = lambda x: data_value

        if data_type == "date":
            if data_value == "date.max":
                return lambda x: date.max
            if data_value == "date.min":
                return lambda x: date.min
            return lambda_to_apply

        if data_type == "integer":
            return lambda_to_apply

        return lambda_to_apply

    if "format" in dict.keys(data):
        format = data["format"]
        if data_type == "date":
            return lambda x: datetime.strptime(
                (x + data["suffix"]) if ("suffix" in dict.keys(data)) else (x), format
            ).date()
        if data_type == "string":
            return lambda x: (format).format(x)

    if "subtract" in dict.keys(data):
        subtract = data["subtract"]
        if data_type == "date":
            days = 0
            if "days" in dict.keys(subtract):
                days = subtract["days"]
            return lambda x: x - timedelta(days=days)
        pass

    if "get" in dict.keys(data):
        get = data["get"]
        if get == "weeknum":
            return lambda x: ((x).isocalendar()[1])
        if get == "year":
            return lambda x: ((x).isocalendar()[0])

    if "find" in dict.keys(data) and "replace" in dict.keys(data):
        find = data["find"]
        replace = data["replace"]
        if (replace == "np.nan"):
            replace = np.nan
            # return lambda x: np.nan
        return lambda x: re.sub(find, replace, x)

    return lambda_to_apply


class Transformer:
    def __init__(self, transforms):
        self.transforms = transforms

    def transform(self, data_frame: pd.DataFrame):
        for transform in self.transforms:
            data = None
            if "data" in dict.keys(transform):
                data = transform["data"]
            data_frame = getattr(self, transform["type"])(data_frame, data)
        return data_frame

    def drop_columns(self, data_frame, data_transform):
        columns = data_transform["columns"]
        data_frame.drop(columns, axis=1, inplace=True)
        return data_frame

    def rename_columns(self, data_frame, data_transform):
        columns = data_transform["columns"]
        data_frame.rename(columns=columns, inplace=True)
        return data_frame

    # def replace_invalid_with_na(self, data_frame, data_transform):
    #     replace_value = data_transform["replace_value"]
    #     data_frame.replace(replace_value, np.nan, inplace=True)
    #     return data_frame

    def replace(self, data_frame, data_transform):
        to_replace = data_transform["to_replace"]
        value = data_transform["value"]
        if (value == "np.nan"):
            value = np.nan
        inplace = data_transform["inplace"]
        data_frame.replace(to_replace=to_replace, value=value, inplace=inplace)
        return data_frame

    # def drop_previous_years(self, data_frame, data_transform):
    #     data_frame.drop(
    #         data_frame[data_frame.year < data_frame["year"].max()].index, inplace=True
    #     )
    #     return data_frame

    def drop_na(self, data_frame, data_transform):
        data_frame.dropna(inplace=True)
        return data_frame

    def update_value(self, data_frame, data_transform):
        column = data_transform["column"]

        if "update" in dict.keys(data_transform):
            lambda_update = get_lambda_to_apply(data_transform["update"])
            data_frame[column] = data_frame[column].apply(lambda_update)

        if "current_value" in dict.keys(data_transform):
            current_value = data_transform["current_value"]

            lambda_if_true = get_lambda_to_apply()
            if "value_if_true" in dict.keys(data_transform):
                lambda_if_true = get_lambda_to_apply(data_transform["value_if_true"])

            lambda_if_false = get_lambda_to_apply()
            if "value_if_false" in dict.keys(data_transform):
                lambda_if_false = get_lambda_to_apply(data_transform["value_if_false"])

            data_frame[column] = np.where(
                data_frame[column] == current_value,
                data_frame[column].apply(lambda_if_true),
                data_frame[column].apply(lambda_if_false),
            )

        if "type" in dict.keys(data_transform):
            data_type = data_transform["type"]
            if data_type == "integer":
                data_frame[column] = pd.to_numeric(data_frame[column])
            if data_type == "string":
                data_frame[column] = data_frame[column].apply(str)

        return data_frame

    def add_column(self, data_frame, data_transform):
        data_transform_keys = dict.keys(data_transform)
        column = data_transform["column"]
        columnfrom = column

        if "column_from" in data_transform_keys:
            columnfrom = data_transform["column_from"]

        if "update" in data_transform_keys:
            update = data_transform["update"]
            lambda_to_apply = get_lambda_to_apply(update)
            data_frame[column] = data_frame[columnfrom].apply(lambda_to_apply)

        if "static_value" in data_transform_keys:
            static_value = data_transform["static_value"]

            data_type = static_value["type"]
            if data_type == "integer":
                data_value = static_value["value"]
                data_frame[column] = data_value
                return data_frame

        return data_frame

    def group_by(self, data_frame, data_transform):
        columns = data_transform["columns"]
        aggregate = data_transform["aggregate"]

        groupedby = data_frame.groupby(columns)
        if aggregate["type"] == "sum":
            data_frame = groupedby.sum().reset_index()

        return data_frame

    def split_column(self, data_frame, data_transform):
        column = data_transform["column"]
        new_columns = data_transform["new_columns"]
        delimiter = data_transform["delimiter"]

        data_frame[new_columns] = data_frame[column].str.split(delimiter, expand=True)

        return data_frame
