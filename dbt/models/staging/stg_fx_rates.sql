select
    rate_date,
    base        as base_currency,
    currency    as quote_currency,
    rate,
    source_file,
    loaded_at
from {{ source('raw', 'fx_rates') }}