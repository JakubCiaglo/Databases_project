SELECT
e.employee_id,
COUNT(*) AS success
FROM trips t
JOIN feedback f ON f.trip_id = t.trip_id
JOIN employee_assignments ea ON ea.trip_id = t.trip_id
JOIN employees e ON ea.employee_id = e.employee_id
WHERE rating > 3
GROUP BY ea.employee_id
ORDER BY success DESC
