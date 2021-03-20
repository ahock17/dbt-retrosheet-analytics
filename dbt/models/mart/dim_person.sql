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
)
select
    *
from final