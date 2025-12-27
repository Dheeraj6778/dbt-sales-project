with sales as (
    select sales_id,
            product_sk,
            customer_sk,
            {{multiply('unit_price','quantity')}} as calculated_gross_amount,
            payment_method
    from {{ ref('bronze_sales') }}
),
product as (
    select product_sk,
            product_name,
            category,
            list_price
    from {{ ref('bronze_product') }}
),
customer as (
    select customer_sk,
            gender,
            loyalty_tier
    from {{ ref('bronze_customer') }}
),
joined_data as (
    select s.sales_id,s.calculated_gross_amount,s.payment_method,p.category,c.gender
    from sales s
    join product p
        on s.product_sk = p.product_sk
    join customer c
        on s.customer_sk = c.customer_sk
)
select category, gender, sum(calculated_gross_amount) as total_sales
from joined_data
group by category, gender
order by 1 asc, 2 asc


