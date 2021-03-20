#!/bin/bash
dbt init retrosheet_analytics &&
cp -r /dbt_init/. /dbt/retrosheet_analytics &&
rm -rf /dbt/retrosheet_analytics/models/example &&
mv /dbt/retrosheet_analytics/profiles.yml /root/.dbt/profiles.yml
cd /dbt/retrosheet_analytics &&
dbt debug &&
dbt deps &&
dbt run &&
dbt test &&
dbt docs generate &&
dbt docs serve