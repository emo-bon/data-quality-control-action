# data-quality-control-action

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
