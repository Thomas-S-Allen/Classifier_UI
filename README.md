# Django Classifier DB UI

This repository contains a Django-based UI for browsing and updating
classifier records stored in PostgreSQL.

## Install

From the repository root:

```bash
./venv/bin/pip install -r requirements.txt
```

## Run

```bash
./venv/bin/python manage.py runserver 127.0.0.1:8010
```

Then open [http://127.0.0.1:8010/](http://127.0.0.1:8010/).

## Behavior

- Connects to the classifier PostgreSQL database from the web UI
- Supports querying recent, validated, unvalidated, and filtered records
- Allows updating collection and override values for individual records
- Score column is sortable by clicking the table header
- ADS title and abstract lookup support is included
