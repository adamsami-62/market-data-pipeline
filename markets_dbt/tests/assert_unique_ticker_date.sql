select ticker, trade_date, count(*) as n
from {{ ref('fact_daily_prices') }}
group by ticker, trade_date
having count(*) > 1