import logging
import re
import requests
import uritemplate
import py_data_rules.rule_factory as rf
from bs4 import BeautifulSoup
from inspect import getmembers, isfunction
from typing import List
from py_data_rules.data_model import DataModel
from py_data_rules.rule import Rule
from py_data_rules.violation import Violation

logger = logging.getLogger(__name__)


class CommonRuleArray:
    """
    rules in common to all habitats
    """
    def __init__(self, aliases_measured, aliases_observatory, aliases_sampling):
        self.aliases_measured = aliases_measured
        self.aliases_observatory = aliases_observatory
        self.aliases_sampling = aliases_sampling

        # rule factory
        self.biomass = rf.regex("biomass", r"^(.+\s+\d+\.?\d*E?[-|+]?\d*;?\s*)+$", self.aliases_measured)
        self.chem_administration = rf.regex("chem_administration", r"^(CHEBI:\d{5}\s+\d{4}-\d{2}-\d{2};?\s*)+$", self.aliases_measured)
        self.ship_date_after_samp_store_date = rf.x_after_y("ship_date", "samp_store_date", self.aliases_sampling)
        self.ship_date_seq_after_ship_date = rf.x_after_y("ship_date_seq", "ship_date", self.aliases_sampling)
        self.arr_date_hq_after_ship_date = rf.x_after_y("arr_date_hq", "ship_date", self.aliases_sampling)
        self.arr_date_seq_after_arr_date_hq = rf.x_after_y("arr_date_seq", "arr_date_hq", self.aliases_sampling)
        self.arr_date_seq_after_ship_date_seq = rf.x_after_y("arr_date_seq", "ship_date_seq", self.aliases_sampling)

        # one-offs
        def depth(data_model: DataModel) -> List[Violation]:
            violations = []
            for alias_observatory, alias_sampling in zip(self.aliases_observatory, self.aliases_sampling):
                dfo = data_model[alias_observatory]
                dfs = data_model[alias_sampling]
                tot_depth_water_col = dfo.at[0, "tot_depth_water_col"]
                for index, row in dfs.iterrows():
                    if not data_model.isna(row["depth"]):
                        if row["depth"] > tot_depth_water_col:  # TODO absolute value?
                            violations.append(
                                Violation(
                                    diagnosis="illegal depth",
                                    table=alias_sampling,
                                    column="depth",
                                    row=index + 1,
                                    value=row["depth"],
                                    extended_diagnosis=f"depth must be less than or equal to tot_depth_water_col ({tot_depth_water_col})",
                                ),
                            )
            return violations
        
        self.depth = depth
        
        def source_mat_id(data_model: DataModel) -> List[Violation]:
            violations = []
            for alias in self.aliases_sampling:
                df = data_model[alias]
                for index, row in df.iterrows():
                    if not data_model.isna(row["source_mat_id"]):
                        collection_date = row["collection_date"][2:10].replace("-", "")
                        if alias.startswith("s"):
                            so_id = data_model["so"].at[0, "so_id"].replace(" ", "_")
                            pattern = f'^EMOBON_{so_id}_{collection_date}_{row["comm_samp"]}_{row["replicate"]}$'
                        else:
                            wa_id = data_model["wo"].at[0, "wa_id"].replace(" ", "_")
                            if str(row["size_frac_up"]).endswith(".0"):
                                size_frac_up = str(row["size_frac_up"])[:-2]
                            else:
                                size_frac_up = row["size_frac_up"]
                            pattern = f'^EMOBON_{wa_id}_{collection_date}_{size_frac_up}um_{row["replicate"]}$'
                        if not re.match(pattern, row["source_mat_id"]):
                            violations.append(
                                Violation(
                                    diagnosis="source_mat_id error",
                                    table=alias,
                                    column="source_mat_id",
                                    row=index + 1,
                                    value=row["source_mat_id"],
                                    extended_diagnosis=f"source_mat_id should match {pattern}",
                                )
                            )

            return violations
        
        self.source_mat_id = source_mat_id
        
        def tax_id_versus_scientific_name(data_model: DataModel) -> List[Violation]:
            tax_id2scientific_name = {}
            violations = []
            for alias in self.aliases_sampling:
                df = data_model[alias]
                for index, row in df.iterrows():
                    if not data_model.isna(row["tax_id"]) and not data_model.isna(row["scientific_name"]):
                        tax_id = row["tax_id"]
                        if not tax_id in tax_id2scientific_name.keys():
                            uri = uritemplate.expand(
                                "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id={tax_id}",
                                {"tax_id": tax_id}
                            )
                            r = requests.get(uri)
                            soup = BeautifulSoup(r.text, "html.parser")
                            scientific_name = soup.title.string.split("(")[1][:-1]
                            tax_id2scientific_name.update({tax_id: scientific_name})  # TODO handle the case where the scientific name could not be retrieved
                        if tax_id2scientific_name[tax_id] != row["scientific_name"]:
                            violations.append(
                                Violation(
                                    diagnosis="scientific name error",
                                    table=alias,
                                    column="scientific_name",
                                    row=index + 1,
                                    value=row["scientific_name"],
                                    extended_diagnosis=f"scientific_name should be {tax_id2scientific_name[tax_id]}",
                                )
                            )
                    elif data_model.isna(row["tax_id"]) and data_model.isna(row["scientific_name"]):
                        pass
                    else:
                        raise AssertionError
            return violations
        
        self.tax_id_versus_scientific_name = tax_id_versus_scientific_name
        
        def orcid(person_orcid, person_name, aliases):
            def fn(data_model: DataModel):
                orcid2name = {}
                violations = []
                for alias in aliases:
                    df = data_model[alias]
                    for index, row in df.iterrows():
                        if not data_model.isna(row[person_orcid]):
                            if not data_model.isna(row[person_name]):
                                if not row[person_orcid] in orcid2name.keys():
                                    uri = uritemplate.expand(
                                        "https://pub.orcid.org/v3.0/{orcid}",
                                        {"orcid": row[person_orcid]},
                                    )
                                    r = requests.get(uri, headers={"Accept": "application/json"})
                                    if r.status_code == 200:
                                        r = r.json()["person"]["name"]
                                        name = r["given-names"]["value"] + " " + r["family-name"]["value"]
                                        orcid2name.update({row[person_orcid]: name})
                                    else:
                                        logger.error(f"orcid api failure for {row[person_orcid]}")
                                if (row[person_orcid] in orcid2name.keys()) and (orcid2name[row[person_orcid]] != row[person_name]): 
                                    violations.append(
                                        Violation(
                                            diagnosis="orcid error",
                                            table=alias,
                                            column=person_name,
                                            row=index + 1,
                                            value=row[person_name],
                                            extended_diagnosis=f"orcid {row[person_orcid]} corresponds to person name {orcid2name[row[person_orcid]]}",
                                        )
                                    )
                            else:
                                violations.append(
                                    Violation(
                                        diagnosis="orcid error",
                                        table=alias,
                                        column=person_orcid,
                                        row=index + 1,
                                        value=row[person_orcid],
                                        extended_diagnosis=f"no person name was provided for this orcid",
                                    )
                                )
                return violations
            return fn
            
        self.contact_orcid = orcid("contact_orcid", "contact_name", aliases=self.aliases_observatory)
        self.other_person_orcid = orcid("other_person_orcid", "other_person", aliases=self.aliases_sampling)
        self.sampl_person_orcid = orcid("sampl_person_orcid", "sampl_person", aliases=self.aliases_sampling)
        self.store_person_orcid = orcid("store_person_orcid", "store_person", aliases=self.aliases_sampling)

        def organization_edmoid(data_model: DataModel) -> List[Violation]:
            prefix = "https://edmo.seadatanet.org/report/"
            violations = []
            for alias in self.aliases_observatory:
                df = data_model[alias]
                for index, row in df.iterrows():
                    edmo = row["organization_edmoid"]
                    if not data_model.isna(edmo):
                        # TODO handle the case where the input is already repaired (i.e. a list of URIs)
                        try:
                            repair = ";".join([f"{prefix}{int(i.strip())}" for i in edmo.split(";")])  # int conversion to assert the input is a list of integers
                            violations.append(
                                Violation(
                                    diagnosis="organization edmoid error",
                                    table=alias,
                                    column="organization_edmoid",
                                    row=index + 1,
                                    value=edmo,
                                    extended_diagnosis="organization edmoid should be a list of URIs",
                                    repair=repair,
                                )
                            )
                        except ValueError:
                            violations.append(
                                Violation(
                                    diagnosis="organization edmoid error",
                                    table=alias,
                                    column="organization_edmoid",
                                    row=index + 1,
                                    value=edmo,
                                    extended_diagnosis="organization edmoid should be a list of URIs",
                                )
                            )
            return violations
    
        self.organization_edmoid = organization_edmoid


class SedimentRuleArray:
    """
    rules exclusive to sediment
    """
    def __init__(self, aliases_measured, aliases_observatory, aliases_sampling):
        self.aliases_measured = aliases_measured
        self.aliases_observatory = aliases_observatory
        self.aliases_sampling = aliases_sampling
        self.comm_samp = rf.membership("comm_samp", ["micro", "meio", "macro", "blank"], self.aliases_sampling)


class WaterRuleArray:
    """
    rules exclusive to water
    """
    def __init__(self, aliases_measured, aliases_observatory, aliases_sampling):
        self.aliases_measured = aliases_measured
        self.aliases_observatory = aliases_observatory
        self.aliases_sampling = aliases_sampling
        ...


def generate_rules(habitat):
    # TODO clean up this boilerplate 
    assert habitat in ("all", "sediment", "water")

    all_measured = ["sm", "wm"]
    all_observatory = ["so", "wo"]
    all_sampling = ["ss", "ws"]
    sediment_measured = ["sm"]
    sediment_observatory = ["so"]
    sediment_sampling = ["ss"]
    water_measured = ["wm"]
    water_observatory = ["wo"]
    water_sampling = ["ws"]

    if habitat == "sediment":
        all_measured = sediment_measured
        all_observatory = sediment_observatory
        all_sampling = sediment_sampling

    if habitat == "water":
        all_measured = water_measured
        all_observatory = water_observatory
        all_sampling = water_sampling

    rule_arrays = [CommonRuleArray(all_measured, all_observatory, all_sampling)]
    sediment_rule_array = SedimentRuleArray(sediment_measured, sediment_observatory, sediment_sampling)
    water_rule_array = WaterRuleArray(water_measured, water_observatory, water_sampling)

    if habitat == "sediment":
        rule_arrays.append(sediment_rule_array)
    if habitat == "water":
        rule_arrays.append(water_rule_array)
    if habitat == "all":
        rule_arrays.append(sediment_rule_array)
        rule_arrays.append(water_rule_array)

    rules = []
    for array in rule_arrays:
        for name, value in getmembers(array, isfunction):
            if not name.startswith("__"):
                rules.append(Rule(value, name))

    return rules