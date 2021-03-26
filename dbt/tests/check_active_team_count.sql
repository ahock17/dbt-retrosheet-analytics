-- check that the total number of active teams is exactly 30
with active_teams as (
    select 
        count(*) as active_teams
    from {{ ref('dim_team') }}
    where is_active = TRUE
)
select * 
from active_teams
where active_teams > 30