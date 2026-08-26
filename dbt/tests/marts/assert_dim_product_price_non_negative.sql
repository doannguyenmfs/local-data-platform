select product_id
from {{ ref('dim_product') }}
where price < 0