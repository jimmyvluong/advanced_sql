---- Part 1: Get cities and geolocation data column.
-- Start by removing duplicates in the us_cities table and select one city to approximate the geolocation.
-- In reality, even if a city had multiple geolocations, the difference between the geolocations is likely neglible in this case.
---- Part 2: Get customer information and add geolocation data column.
-- Join the customer table onto the cities table.
---- Part 3: Get suppliers information and add geolocation data column.
---- Part 4: For each customer, calculate the distance between the customer and each of the 10 suppliers. 
-- A cross join makes it such that for the ~2400 customers, 10 rows are created for a total of ~24,000 rows.
-- Then, select only the row with the minimal distance per customer.

---- Part 1: Get cities and geolocation data column.
with cities_cte as (
    
    select
        lower(trim(city_name)) as city_name,
        lower(trim(state_abbr)) as state_abbr,
        geo_location
    from VK_DATA.RESOURCES.US_CITIES
    qualify row_number() over (partition by lower(trim(city_name)), lower(trim(state_abbr)) order by city_name) = 1
)

---- Part 2: Get customer information and add geolocation data column.
, customers_cte as (
    
    select 
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email,
        lower(trim(ca.customer_city)) as customer_city,
        lower(trim(ca.customer_state)) as customer_state,
        cities_cte.geo_location as customer_geo_location
    from CUSTOMER_DATA as cd
    inner join CUSTOMER_ADDRESS as ca
        on cd.customer_id = ca.customer_id
    inner join cities_cte
        on lower(trim(ca.customer_city)) = lower(trim(cities_cte.city_name)) and lower(ca.customer_state) = lower(cities_cte.state_abbr)
)
---- Part 3: Get suppliers information and add geolocation data column.
, suppliers_cte as (
    
    select
        supplier_id,
        supplier_name,
        lower(trim(si.supplier_city)) as supplier_city,
        lower(trim(si.supplier_state)) as supplier_state,
        cities_cte.geo_location as supplier_geo_location
    from VK_DATA.SUPPLIERS.SUPPLIER_INFO as si
    inner join cities_cte 
        on lower(trim(si.supplier_city)) = cities_cte.city_name and lower(trim(si.supplier_state)) = cities_cte.state_abbr
)
---- Part 4: For each customer, calculate the distance between the customer and each of the 10 suppliers. Then, select only the minimal distance per customer.
, final_cte as (
    select 
        customer_id,
        first_name,
        last_name,
        email,
        supplier_id,
        supplier_name,
        st_distance(customers_cte.customer_geo_location ,suppliers_cte.supplier_geo_location)/1609 as distance_crow_flies_miles
    from customers_cte
    cross join suppliers_cte
    qualify row_number() over (partition by customer_id order by distance_crow_flies_miles asc) = 1
    order by last_name, first_name
)
select * from final_cte;