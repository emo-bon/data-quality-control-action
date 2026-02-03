"""
emobon-specific extensions of existing functionality
"""
import pandas as pd
from py_data_rules.data_type import DataType


class EMOBONRange(DataType):
    @staticmethod
    def match(instance):
        assert instance
        try:
            a, b = map(str.strip, instance.split("-"))
            assert float(a) < float(b)
            return True
        except (ValueError, AssertionError):
            return False


class EMOBONList(DataType):
    @staticmethod
    def match(instance):
        assert instance
        if "," in instance:  # list must not contain commas
            return False
        else:
            return True


def read_emobon_csv(path):
    df = (
        pd.read_csv(path, dtype=object, keep_default_na=False)
        .astype(str)
        .map(lambda _: _.strip())
        .map(lambda _: "" if _ == "nan" else _)
        .map(lambda _: "" if (_ == "NA") or (_ =="ΝΑ") else _)  # prevent unicode confusion by replacing both greek and latin NA)
    )
    df = df[~(df == "").all(axis=1)]  # drop empty rows
    return df
