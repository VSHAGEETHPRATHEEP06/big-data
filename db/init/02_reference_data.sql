-- Load reference data from the files mounted at /opt/fleet/config
COPY ref.vehicles (vehicle_id, driver_id, vehicle_type, plate_no, home_zone, profile,
                   km_per_litre, shift_start_hour, shift_hours)
FROM '/opt/fleet/config/fleet_registry.csv' WITH (FORMAT csv, HEADER true);

INSERT INTO ref.zones (zone_id, zone_name, lat, lon, area_type)
SELECT zone_id, zone_name, lat, lon, area_type
FROM json_to_recordset((pg_read_file('/opt/fleet/config/zones.json')::json) -> 'zones')
     AS z(zone_id text, zone_name text, lat double precision, lon double precision, area_type text);
