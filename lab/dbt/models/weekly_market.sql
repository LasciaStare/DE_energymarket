select
    date_trunc('week', day)::date as week,
    count(price_cop_kwh) as price_days,
    avg(price_cop_kwh) as mean_price_cop_kwh,
    count(demand_gwh) as demand_days,
    sum(demand_gwh) as demand_gwh,
    avg(reservoir_pct) as mean_reservoir_pct
from {{ source('market', 'market_daily') }}
group by 1
