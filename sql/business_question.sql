-- 1. Highest AQI this week
SELECT MAX(aqi) AS highest_aqi_this_week
FROM bangkok_aqi
WHERE timestamp >= NOW() - INTERVAL '7 days';

-- 2. Lowest AQI in last 3 months
SELECT MIN(aqi) AS lowest_aqi_last_3_months
FROM bangkok_aqi
WHERE timestamp >= NOW() - INTERVAL '3 months';

-- 3. Average AQI this week
SELECT AVG(aqi) AS avg_aqi_this_week
FROM bangkok_aqi
WHERE timestamp >= NOW() - INTERVAL '7 days';

-- 4. Number of AQI readings above 100 in last 30 days
SELECT COUNT(*) AS count_aqi_above_100_last_30_days
FROM bangkok_aqi
WHERE aqi > 100 AND timestamp >= NOW() - INTERVAL '30 days';

-- 5. Average Temperature on days when AQI > 100 (last month)
SELECT AVG(temperature) AS avg_temp_aqi_above_100_last_month
FROM bangkok_aqi
WHERE aqi > 100 AND timestamp >= NOW() - INTERVAL '1 month';
