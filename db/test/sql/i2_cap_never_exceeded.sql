-- I2: the cap is never exceeded
SELECT count(*) AS n FROM session_guard WHERE total_events > 5000;
