-- check that the total number of active teams is exactly 30
with active_teams as (
    select 
        count(*) as active_teams
    from {{ ref('agg_team_stats') }}
)
select * 
from active_teams
where active_teams > 30