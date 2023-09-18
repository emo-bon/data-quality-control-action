import numpy as np
import pandas as pd
from py_data_rules.data_model import DataModel
from py_data_rules.data_type import (
    DataType,
    XSDBoolean,
    XSDDate,
    XSDDateTime,
    XSDFloat,
    XSDInteger,
    XSDString,
    XSDAnyURI,
)
from py_data_rules.schema import Schema


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
    return (
        pd.read_csv(path, dtype=object, keep_default_na=False, na_values=[""])
        .astype(str)
        .applymap(lambda x: x.strip())
        .replace("", "nan")
        .replace("nan", np.nan)
        .dropna(how="all")
        .replace(np.nan, "")
    )


def generate_schema(habitat, sheet, config):
    dtype_lookup = {
        "xsd:string": XSDString(),
        "xsd:float": XSDFloat(),
        "xsd:integer": XSDInteger(),
        "xsd:datetime": XSDDateTime(["%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M%z"]),
        "xsd:date": XSDDate(),
        "xsd:boolean": XSDBoolean(),
        "range": EMOBONRange(),
        "xsd:list": EMOBONList(),
    }
    schema = Schema()
    for _, row in config.iterrows():
        lty = row["LogsheetType"].lower().strip()
        lta = row["LogsheetTab"].lower().strip()
        lct = row["LogsheetColumnTitle"].strip()
        dty = row["DataTypeOut"].lower().strip()
        req = row["Requirement"].lower().strip()
        base_uri = row["BaseURI"].lower().strip()
        dty = "xsd:float" if dty == "xd:float" else dty
        if dty == "xsd:anyuri":
            data_type = XSDAnyURI(base_uri=base_uri)
        else:
            data_type = dtype_lookup[dty]
        if habitat == "w" and sheet == "measured" and lct == "ph":
            req = "optional"
        if (habitat in lty) and (sheet in lta):
            schema.add_column(
                label=lct,
                data_type=data_type,
                nullable=(req == "optional"),
                trim="both",
            )
    return schema


def generate_data_model(logsheets_path, alias2basename):
    config = pd.read_csv(
        "https://raw.githubusercontent.com/emo-bon/observatory-profile/main/logsheet_schema_extended.csv"
    ).astype(str)
    data_model = {}
    for alias, base_name in alias2basename.items():
        habitat = base_name[0]
        sheet = base_name.split("_")[1]
        logsheet_path = logsheets_path / f"{base_name}.csv"
        schema = generate_schema(habitat, sheet, config)
        data_model.update(
            {
                alias: {
                    "path": logsheet_path,
                    "reader": read_emobon_csv,
                    "schema": schema,
                }
            }
        )
    return DataModel(description=data_model, na_literal="NA")
