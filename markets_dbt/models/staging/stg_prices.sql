-- SILVER: cleaned, typed version of the raw prices
select
    ticker,
    trade_date,
    open,
    high,
    low,
    close,
    volume
from {{ source('raw', 'daily_prices') }}
where close is not null
  and close > 0