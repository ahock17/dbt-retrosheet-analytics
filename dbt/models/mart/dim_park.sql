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
)
select
    *
from final