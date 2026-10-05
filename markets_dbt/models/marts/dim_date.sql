select distinct
    trade_date,
    extract(year  from trade_date) as year,
    extract(month from trade_date) as month,
    dayname(trade_date)            as weekday
from {{ ref('stg_prices') }}