SELECT
    tp.client_id,
    tp.trip_id,
    t.departure_datetime
FROM trip_participants tp
JOIN trips t ON tp.trip_id = t.trip_id
ORDER BY tp.client_id, t.departure_datetime;