select
    ticker,
    trade_date,
    open, high, low, close, volume,
    close - lag(close) over (partition by ticker order by trade_date)
        as change_vs_prev_day
from {{ ref('stg_prices') }}