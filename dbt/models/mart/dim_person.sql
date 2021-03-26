{{
    config(
        tags="daily"
    )
}}

with source as (
    select
        *
    from {{ source('stage', 'people') }}
),
final as (
    select
        upper(trim(id)) as person_id,
        last_name,
        first_name,
        play_debut,
        mgr_debut,
        coach_debut,
        ump_debut,
        case when play_debut is not null then TRUE else FALSE end as is_player,
        case when mgr_debut is not null then TRUE else FALSE end as is_mgr,
        case when coach_debut is not null then TRUE else FALSE end as is_coach,
        case when ump_debut is not null then TRUE else FALSE end as is_ump
    from source
    union all
    select
        '00000000'::char(8),
        'UNKNOWN',
        'UNKNOWN',
        '1700-01-01'::date,
        '1700-01-01'::date,
        '1700-01-01'::date,
        '1700-01-01'::date,
        FALSE::boolean,
        FALSE::boolean,
        FALSE::boolean,
        FALSE::boolean
)
select
    *
from final