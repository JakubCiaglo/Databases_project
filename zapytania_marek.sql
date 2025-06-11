SELECT
    tp.client_id,
    t.trip_id,
    t.departure_datetime
FROM
    trip_participants tp
JOIN trips t ON tp.trip_id = t.trip_id
JOIN trip_types tt ON t.trip_type_id = tt.trip_type_id
WHERE
    t.departure_datetime = (
        SELECT MAX(t2.departure_datetime)
        FROM trip_participants tp2
        JOIN trips t2 ON tp2.trip_id = t2.trip_id
        WHERE tp2.client_id = tp.client_id
    )
ORDER BY tp.client_id;