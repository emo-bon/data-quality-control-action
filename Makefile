# assuming you have make, python and git installed
# point GITHUB_WORKSPACE to the repository you want to test the action against
GITHUB_WORKSPACE = https://github.com/octocat/hello-world

.DEFAULT_GOAL := run

ifeq ($(OS),Windows_NT)
    PYTHON := venv/Scripts/python.exe
else
    PYTHON := venv/bin/python
endif

init:
	rm -rf venv
	python -m venv venv
	$(PYTHON) -m pip install -r requirements.txt
	rm -rf github-workspace
	git clone $(GITHUB_WORKSPACE) github-workspace 
	rm -rf github-workspace/.git

run:
	$(PYTHON) -m action --dev
