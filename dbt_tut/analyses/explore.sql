select payment_method, count(*) as total_transactions, sum(net_amount) as total_amount
from 
{{ ref('bronze_sales') }}
group by payment_method