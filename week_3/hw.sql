select count(1) from `de-zoomcamp-338904.trips_data_all.external_fhv_tripdata`
where extract(YEAR from pickup_datetime) = 2019;

select count(distinct(dispatching_base_num)) from `de-zoomcamp-338904.trips_data_all.external_fhv_tripdata`
where extract(YEAR from pickup_datetime) = 2019;

select count(*) from trips_data_all.fhv_tripdata_partitioned
where pickup_datetime between '2019-01-01' and '2019-03-31' AND
dispatching_base_num in ('B00987', 'B02060', 'B02279');