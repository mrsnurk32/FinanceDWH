# dbt + ClickHouse setup

This repository contains a minimal scaffold to run dbt against a local ClickHouse instance.

Quick steps

1. Start ClickHouse:

```bash
docker-compose up -d
```

2. Create a Python virtualenv and install dbt + adapter:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

3. Copy `profiles.yml.sample` to your dbt profiles location and update credentials:

```bash
mkdir -p ~/.dbt
cp profiles.yml.sample ~/.dbt/profiles.yml
# edit ~/.dbt/profiles.yml to match your environment
```

4. Run dbt (from repo root with virtualenv active):

```bash
dbt debug
dbt run --models example
```

Files

- [docker-compose.yml](docker-compose.yml) — ClickHouse service
- [dbt_project.yml](dbt_project.yml) — dbt project config
- [profiles.yml.sample](profiles.yml.sample) — sample dbt profile for ClickHouse
- [models/example.sql](models/example.sql) — example model
- [requirements.txt](requirements.txt) — packages to install locally
- [Makefile](Makefile) — convenience targets

Notes

- Install versions of `dbt-core` and `dbt-clickhouse` that are compatible. If you use the dbt CLI docker image or Homebrew, adapt accordingly.
- The adapter `type` in `profiles.yml.sample` is `clickhouse` — ensure the adapter package you install provides that adapter name.
