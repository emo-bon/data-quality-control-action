import pandas as pd
from .data_model import read_emobon_csv

class Pipeline:
    def __init__(self, input_path, output_path, dqc_path, alias2basename):
        self.input_path = input_path
        self.output_path = output_path
        self.dqc_path = dqc_path
        self.alias2basename = alias2basename

    def quick_fix(self):
        df_repair = self.dqc[~self.dqc["repair"].isna()]
        for _, row in df_repair.iterrows():
            self.dfs[row["table"]].at[row["row"] - 1, row["column"]] = row["repair"]
    
    def run(self):
        # read input
        self.dqc = pd.read_csv(self.dqc_path)
        self.dfs = {}
        for alias, base_name in self.alias2basename.items():
            self.dfs[alias] = read_emobon_csv(self.input_path / f"{base_name}.csv")

        # quick fixes
        self.quick_fix()

        # create missing columns
        ...

        # rename existing columns
        ...

        # drop excess columns
        ...

        # write output
        for alias, df in self.dfs.items():
            base_name = self.alias2basename[alias]
            df.to_csv(self.output_path / f"{base_name}.csv", index=False)
