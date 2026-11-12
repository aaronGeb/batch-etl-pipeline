with rates as (

    select
        rate_date,
        base_currency,
        quote_currency,
        rate
    from {{ ref('stg_fx_rates') }}

),

with_previous as (

    select
        *,
        lag(rate) over (
            partition by base_currency, quote_currency
            order by rate_date
        ) as previous_rate
    from rates

)

select
    rate_date,
    base_currency,
    quote_currency,
    rate,
    previous_rate,
    round(((rate / nullif(previous_rate, 0)) - 1) * 100, 4) as pct_change
from with_previous