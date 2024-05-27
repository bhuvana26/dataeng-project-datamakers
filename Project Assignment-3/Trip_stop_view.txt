CREATE VIEW trip_stop_view as 
SELECT  DISTINCT t.trip_id, t.vehicle_id, s.route_number, s.service_key, s.direction 
FROM trip t, stopevent s 
WHERE s.trip_id = t.trip_id;