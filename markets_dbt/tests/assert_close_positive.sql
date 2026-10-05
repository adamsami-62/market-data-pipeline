select ticker, trade_date, close
from {{ ref('fact_daily_prices') }}
where close <= 0