# data-quality-control-action

## Usage

Include the action in the workflow by adding these lines to some `./.github/worfklow/my-wf.yml`

```
on: [push]
jobs:
  job:
    runs-on: ubuntu-latest
    steps:
      - name: checkout
        uses: actions/checkout@v3
      - name: data-quality-control-action
        uses: emo-bon/data-quality-control-action@main
        env:
          PAT: ${{ secrets.GITHUB_TOKEN }}
          REPO: ${{ github.repository }}
          ASSIGNEE: <github_username>
```

with:

* `PAT`: a personal access token or automatic authentication token
* `REPO`: repo in which to create an issue for end user notification
* `ASSIGNEE`: github username of end user to notify


## Description

This GitHub Action automates the quality control (QC) of EMO-BON sampling data. 
It processes CSV files located in the `./observatory-*-crate/logsheets/filtered/` directories, applies a set of validation and transformation rules, and outputs the QC’ed versions to the corresponding `./observatory-*-crate/logsheets/transformed/` directories. 
This ensures that the data is cleaned and standardized for downstream automated processing tasks such as semantic uplifting and analysis.

During QC, the action uses the [py-data-rules](https://github.com/vliz-be-opsci/py-data-rules) framework, which allows for defining and evaluating rules at the level of individual table cells. When deviations from expected values are detected, these rules specify how to transform or replace the faulty values. Violations are logged in structured reports (e.g. JSON or CSV), making the process transparent and reproducible. The framework is extensible through custom rule definitions and benefits from familiarity with the Pandas library.

The contents of this package — available at [https://github.com/emo-bon/data-quality-control-action](https://github.com/emo-bon/data-quality-control-action) — include:
- A GitHub Action workflow for automated execution;
- Python scripts and configuration files implementing the QC logic;
- Predefined data rules for consistent validation;
- A standardized folder structure for filtered and transformed data files;
- Reporting utilities to track and document QC outcomes.

Together, these components provide a robust and reusable solution for maintaining the integrity and usability of EMO-BON sampling data in automated pipelines.
