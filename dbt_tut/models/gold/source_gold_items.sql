with dedup_query as (
    select *, row_number() over(partition by id order by update_date desc) as dedup_id
    from {{ source('source', 'items') }}
)
select id, name, category, update_date
from dedup_query
where dedup_id = 1