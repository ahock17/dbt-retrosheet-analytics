with gamelog as (
    select *
    from {{ ref('fact_gamelog') }}
),
teams as (
    select *
    from {{ ref('dim_team') }}
),
unpivoted as (
    SELECT 
        unnest(array['visiting', 'home']) as team_type,
        unnest(array[vis_team_key, home_team_key]) as team_key,
        unnest(array[result.vis_team_result, result.home_team_result]) as result,
        unnest(array[vis_team_score, home_team_score]) as runs,
        unnest(array[vis_team_hits, home_team_hits]) as hits,
        unnest(array[vis_team_doubles, home_team_doubles]) as doubles,
        unnest(array[vis_team_triples, home_team_triples]) as triples,
        unnest(array[vis_team_hr, home_team_hr]) as hr,
        unnest(array[vis_team_strikeouts, home_team_strikeouts]) as strikeouts,
        game_len_mins,
        attendance
    from gamelog
    cross join lateral (
        select
            case 
                when vis_team_score > home_team_score then 'w'
                when vis_team_score = home_team_score then 't'
                else 'l'
            end as vis_team_result,
            case
                when home_team_score > vis_team_score then 'w'
                when home_team_score = vis_team_score then 't'
                else 'l'
            end as home_team_result
    ) as result
)
select 
    unpivoted.team_key,
    count(*) as games_played,
    sum(case when result = 'w' then 1 else 0 end) as wins,
    sum(case when result = 'l' then 1 else 0 end) as losses,
    sum(case when result = 't' then 1 else 0 end) as ties,
    sum(case when result = 'w' then 1 else 0 end) / count(*)::numeric as win_pct,
    sum(runs) as tot_runs_all_time,
    avg(runs) as avg_runs_per_game,
    avg(hits) as avg_hits_per_game,
    avg(doubles) as avg_doubles_per_game,
    avg(triples) as avg_triples_per_game,
    sum(hr) as tot_hr_all_time,
    avg(hr) as avg_hr_per_game,
    avg(strikeouts) as avg_strikeouts_per_game,
    avg(game_len_mins) as avg_game_len_mins,
    avg(case when team_type = 'visiting' then attendance else null::int end) as avg_away_attendance,
    avg(case when team_type = 'home' then attendance else null::int end) as avg_home_attendance
from unpivoted
left join teams on unpivoted.team_key = teams.team_key
where teams.is_active = TRUE
group by unpivoted.team_key