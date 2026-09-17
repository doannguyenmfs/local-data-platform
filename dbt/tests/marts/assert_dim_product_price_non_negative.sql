-- Reassert a source-domain rule after extraction/transformation; source
-- constraints alone cannot prove a downstream model preserved the value.
select product_id
from {{ ref('dim_product') }}
where price < 0
