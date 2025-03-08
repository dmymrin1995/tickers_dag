 SELECT                   
    eod.symbol AS ticker,
    AVG(eod.high - eod.low) AS avg_price_fluctuation
FROM end_of_day_data eod
JOIN tickers_info t ON eod.symbol = t.ticker 
JOIN exchanges ex ON eod.exchange = ex.exchange_mic
WHERE
    lower(ex.city) = 'new york'
    AND eod.date >= '2024-09-01'
    AND eod.date <= '2024-09-30'
GROUP BY
    eod.symbol
;