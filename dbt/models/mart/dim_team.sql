with source as (
    select
        *
    from {{ source('stage', 'teams') }}
),
final as (
    select
        {{ dbt_utils.surrogate_key(['team_abbr', 'league']) }} as team_key,
        upper(trim(team_abbr)) as team_abbr,
        upper(trim(league)) as league,
        city,
        nickname as team_name,
        first_year as first_year,
        case 
            when last_year = 2010 
                then date_part('year', CURRENT_DATE) 
            else last_year 
        end as last_year,
        coalesce(
            nullif(last_year, 2010), 
            date_part('year', CURRENT_DATE)
        ) - first_year as tot_years
    from source
)
select
    *
from final