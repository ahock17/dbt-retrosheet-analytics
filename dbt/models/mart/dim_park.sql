{{
    config(
        tags="hourly"
    )
}}

with source as (
    select
        *
    from {{ source('stage', 'park_code') }}
),
final as (
    select
        upper(trim(parkid)) as park_key,
        upper(trim(name)) as park_name,
        aka as park_name_aka,
        city,
        state,
        start_date,
        end_date,
        league
    from source
    union all
    select
        '00000',
        'UNKNOWN',
        'UNKNOWN',
        'UNKNOWN',
        'UNKNOWN',
        '1700-01-01'::date,
        '1700-01-01'::date,
        'UNKNOWN'
)
select
    *
from final