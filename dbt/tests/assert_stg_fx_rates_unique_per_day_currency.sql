-- Fails if any (rate_date, base_currency, quote_currency) appears more than once.
-- A dbt singular test passes when it returns zero rows.
select
    rate_date,
    base_currency,
    quote_currency,
    count(*) as row_count
from {{ ref('stg_fx_rates') }}
group by rate_date, base_currency, quote_currency
having count(*) > 1