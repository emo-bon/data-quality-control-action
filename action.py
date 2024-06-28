#!/usr/bin/env python
import logging
import numpy as np
import os
import pandas as pd
import yaml
from datetime import date
from github import Github
from pathlib import Path
from py_data_rules.rule_engine import RuleEngine
from pipeline.data_model import generate_data_model
from pipeline.pipeline import Pipeline
from pipeline.rules import generate_rules

GITHUB_WORKSPACE = Path(os.getenv("GITHUB_WORKSPACE", "/github/workspace"))
PAT = os.getenv("PAT")
REPO = os.getenv("REPO")
ASSIGNEE = os.getenv("ASSIGNEE")
LOGSHEETS_PATH = GITHUB_WORKSPACE / "logsheets/raw"
LOGSHEETS_FILTERED_PATH = GITHUB_WORKSPACE / "logsheets/filtered"
LOGSHEETS_FILTERED_PATH.mkdir(parents=True, exist_ok=True)
LOGSHEETS_TRANSFORMED_PATH = GITHUB_WORKSPACE / "logsheets/transformed"
LOGSHEETS_TRANSFORMED_PATH.mkdir(parents=True, exist_ok=True)
DQC_PATH = GITHUB_WORKSPACE / "data-quality-control"
DQC_PATH.mkdir(parents=True, exist_ok=True)


def filter_logsheets(
    habitat,
):  # i.e. discarding samples and measurements taken after the data_quality_control_threshold_date
    df_sampling = pd.read_csv(
        LOGSHEETS_PATH / f"{habitat}_sampling.csv", dtype=object, keep_default_na=False
    )
    df_sampling.loc[pd.to_datetime(df_sampling["collection_date"]) >= THRESHOLD] = ""
    df_sampling.to_csv(LOGSHEETS_FILTERED_PATH / f"{habitat}_sampling.csv", index=False)

    df_measured = pd.read_csv(
        LOGSHEETS_PATH / f"{habitat}_measured.csv", dtype=object, keep_default_na=False
    )
    df_measured.loc[
        ~df_measured["source_mat_id"].isin(df_sampling["source_mat_id"])
    ] = ""
    df_measured.to_csv(LOGSHEETS_FILTERED_PATH / f"{habitat}_measured.csv", index=False)

    df_observatory = pd.read_csv(
        LOGSHEETS_PATH / f"{habitat}_observatory.csv",
        dtype=object,
        keep_default_na=False,
    )
    df_observatory.to_csv(
        LOGSHEETS_FILTERED_PATH / f"{habitat}_observatory.csv", index=False
    )


def create_report(input_path, output_path):
    df = pd.read_csv(input_path)
    df_report = pd.DataFrame()
    df_report["Diagnosis"] = df["diagnosis"]
    df_report["LogsheetType"] = np.select(
        [df["table"].str[0] == "s", df["table"].str[0] == "w"],
        ["sediment", "water"],
    )
    df_report["LogsheetTab"] = np.select(
        [
            df["table"].str[1] == "m",
            df["table"].str[1] == "o",
            df["table"].str[1] == "s",
        ],
        ["measured", "observatory", "sampling"],
    )
    df_report["Column"] = df["column"]
    df_report["Row"] = df["row"]
    df_report["Value"] = np.where(df["value"].isna(), "<empty>", df["value"])
    df_report["Repair"] = df["repair"]
    df_report["ExtendedDiagnosis"] = df["extended_diagnosis"]
    df_report["ExtendedDiagnosis"] = np.where(
        df["extended_diagnosis"].isna(), "\\", df["extended_diagnosis"]
    )
    df_report["FilePath"] = df["file_path"]
    df_report["DataType"] = df["data_type"]
    df_report["Requirement"] = np.select(
        [df["nullable"] == True, df["nullable"] == False],
        ["optional", "mandatory"],
    )
    df_report = df_report[df_report["Repair"].isna()]
    df_report = df_report.drop(columns=["Repair"])
    df_report.to_csv(output_path, index=False)


def create_issue():
    repo = Github(PAT).get_repo(REPO)
    repo.create_issue(
        title=f"Data Quality Control {date.today()}",
        body=(
            f"A new [logfile](https://github.com/{REPO}/blob/main/data-quality-"
            f"control/logfile) and [report](https://github.com/{REPO}/blob/main"
            "/data-quality-control/report.csv) are available. Have a look at th"
            "e logfile first to see if any problems were encountered during the"
            " data quality control.\n\n"
            f"Data were controlled up to {THRESHOLD}, this date can be changed "
            "by modifying the `data_quality_control_threshold_date` in [governa"
            "nce-data/logsheets.csv](https://github.com/emo-bon/governance-data"
            "/blob/main/logsheets.csv) (date format is YYYY-MM-DD)."
        ),
        assignee=f"{ASSIGNEE}",
    )


if __name__ == "__main__":
    logging.basicConfig(filename=DQC_PATH / "logfile", filemode="w", level=logging.INFO)
    wp = yaml.load(
        open(GITHUB_WORKSPACE / "config/workflow_properties.yml"),
        Loader=yaml.BaseLoader,
    )
    THRESHOLD = wp["data_quality_control_threshold_date"]

    alias2basename_sediment = {
        "sm": "sediment_measured",
        "so": "sediment_observatory",
        "ss": "sediment_sampling",
    }
    alias2basename_water = {
        "wm": "water_measured",
        "wo": "water_observatory",
        "ws": "water_sampling",
    }

    if (wp["sediment"] != "nan") and (wp["water"] != "nan"):
        habitat = "all"
        alias2basename = {**alias2basename_sediment, **alias2basename_water}
        filter_logsheets("sediment")
        filter_logsheets("water")
    elif wp["sediment"] != "nan":
        habitat = "sediment"
        alias2basename = alias2basename_sediment
        filter_logsheets("sediment")
    elif wp["water"] != "nan":
        habitat = "water"
        alias2basename = alias2basename_water
        filter_logsheets("water")
    else:
        raise AssertionError("invalid workflow properties")

    # data quality control
    data_model = generate_data_model(
        logsheets_path=LOGSHEETS_FILTERED_PATH,
        alias2basename=alias2basename,
    )

    rules = generate_rules(habitat=habitat)

    RuleEngine(
        data_model=data_model,
        rules=rules,
    ).execute(report_path=DQC_PATH / "dqc.csv")

    # notify end user of new dqc report
    create_report(
        input_path=DQC_PATH / "dqc.csv",
        output_path=DQC_PATH / "report.csv",
    )

    create_issue()

    # data transformation
    Pipeline(
        input_path=LOGSHEETS_FILTERED_PATH,
        output_path=LOGSHEETS_TRANSFORMED_PATH,
        dqc_path=DQC_PATH / "dqc.csv",
        alias2basename=alias2basename,
    ).run()
