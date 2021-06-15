#!/bin/bash
rm -rf retrosheet_analytics &&
dbt init retrosheet_analytics &&
cp -r /dbt_init/. /dbt/retrosheet_analytics &&
rm -rf /dbt/retrosheet_analytics/models/example &&
mv /dbt/retrosheet_analytics/profiles.yml /root/.dbt/profiles.yml
cd /dbt/retrosheet_analytics &&
dbt debug &&
dbt deps &&
dbt run --no-version-check &&
dbt test --no-version-check &&
dbt docs generate --no-version-check