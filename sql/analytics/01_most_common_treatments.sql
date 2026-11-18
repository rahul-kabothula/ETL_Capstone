-- Most common treatments
SELECT treatment_type,
       COUNT(*)                AS treatment_count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM fact_treatment
GROUP BY treatment_type
ORDER BY treatment_count DESC;
