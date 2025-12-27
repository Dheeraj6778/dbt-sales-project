with returns as (
    select sales_id,
    store_sk,
    product_sk,
    return_reason,
    refund_amount
    from {{ ref('bronze_returns') }}
),
product as (
    select product_sk,
    product_name,
    category
    from {{ ref('bronze_product') }}
),
customer as (
    select customer_sk,
    gender,
    loyalty_tier
    from {{ ref('bronze_customer') }}
),
sales as (
    select sales_id,customer_sk
    from {{ ref('bronze_sales') }}
),
joined_data_by_gender_and_category as (
    select p.category,r.return_reason, c.gender, sum(r.refund_amount) as total_refunds
    from returns r
    join sales s
        on r.sales_id = s.sales_id
    join product p
        on r.product_sk = p.product_sk
    join customer c
        on s.customer_sk = c.customer_sk
    group by p.category,c.gender,r.return_reason
    order by 1 asc, 3 desc
),
refund_by_category as (
    select category, sum(total_refunds) as total_refunds
    from joined_data_by_gender_and_category
    group by category
    order by 2 desc
),
refund_by_return_reason as (
    select return_reason, sum(total_refunds) as total_refunds
    from joined_data_by_gender_and_category
    group by return_reason
    order by 2 desc
)
select *
from joined_data_by_gender_and_category


