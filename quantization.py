'''
The Goal of this Python Script is to Automate the entire Quantization process as part of the Discrete Global Grid
System (DGGS) Cell Based Analytics Project, which we have performed based on a five step process by using
ClickHouse OLAP.
'''


# Importing the essential Libraries
import json
from clickhouse_driver import Client
import os


# Configuration environment settings for ClickHouse connection
CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', '34.91.49.74')
CLICKHOUSE_PORT = os.environ.get('CLICKHOUSE_PORT', '9000')
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', 'clickhouse')
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE', 'default')


# Function 1: Function for loading data from an input GeoJSON file
def load_geojson(file_path):
    with open(file_path, 'r') as f:
        geojson_data = json.load(f)
    return geojson_data


# Function 2: Function for automating the entire Quantization Process
def ingest_data_to_clickhouse(file_path):
    # Connecting to ClickHouse with authentication
    client = Client(host=CLICKHOUSE_HOST,
                    port=CLICKHOUSE_PORT,
                    user=CLICKHOUSE_USER,
                    password=CLICKHOUSE_PASSWORD,
                    database=CLICKHOUSE_DATABASE)

    # Step 1A: Create temporary_raw_data table
    create_temp_raw_query = """
    CREATE TABLE IF NOT EXISTS temporary_raw_data (
        lat Float64,
        lon Float64,
        attributes Nested(
        name String,
        value String
    )
    ) ENGINE = MergeTree()
    ORDER BY tuple();
    """
    client.execute(create_temp_raw_query)

    # Step 1B: Load data directly from GeoJSON file into ClickHouse
    geojson_data = load_geojson(file_path)

    data = []  # This will store our formatted data
    for feature in geojson_data['features']:
        properties = feature['properties']
        geometry = feature['geometry']

        # Extract coordinates for latitude and longitude
        longitude, latitude = geometry['coordinates']

        # Build the nested attributes structure
        attribute_names = list(properties.keys())
        attribute_values = [str(properties[key]) for key in attribute_names]

        data_point = {
            'lat': latitude,
            'lon': longitude,
            'attributes.name': attribute_names,
            'attributes.value': attribute_values
        }

        data.append(data_point)

    # Insert data into ClickHouse using JSONEachRow format
    client.execute("INSERT INTO temporary_raw_data FORMAT JSONEachRow", data)

    # Step 1C: Create and populate temporary_filtered_data table
    create_temp_filtered_query = """
    CREATE TABLE IF NOT EXISTS temporary_filtered_data
    ENGINE = MergeTree()
    ORDER BY lat AS
    SELECT
    lat,
    lon,
    arrayFilter((v, t) -> ((t = 'int') OR (t = 'float')), attributes.name, ['string', 'int', 'float']) AS filtered_names,
    arrayFilter((v, t) -> ((t = 'int') OR (t = 'float')), attributes.value, ['string', 'int', 'float']) AS filtered_values
    FROM temporary_raw_data;
    """
    client.execute(create_temp_filtered_query)

    print("STEP 1: Data Ingestion and Filtering Complete!")


    # Step 2: Create and populate temporary_geohash_grid table
    create_temp_geohash_grid_query = """
    CREATE TABLE temporary_geohash_grid
    ENGINE = MergeTree()
    ORDER BY (geohash, resolution) AS
    SELECT
        t.lat,
        t.lon,
        t.filtered_names,
	    t.filtered_values,
        geohashEncode(t.lat, t.lon, r.resolution) AS geohash,
        r.resolution
    FROM
        temporary_filtered_data AS t
    CROSS JOIN
        (SELECT number + 1 AS resolution FROM numbers(12)) AS r;
    """
    client.execute(create_temp_geohash_grid_query)

    print("STEP 2: Geohash Grid Creation Complete!")


    # Step 3: Create and populate temporary_stats table
    create_temp_stats_query = """
    CREATE TABLE temporary_stats
    ENGINE = MergeTree()
    ORDER BY (geohash, resolution)
    AS
    WITH aggregated AS (
        SELECT 
            geohash,
            resolution,
            any(filtered_names) AS attributes_name,
            arrayMap(x -> if(isNull(toFloat64OrZero(x)), 0, toFloat64OrZero(x)), filtered_values) AS transformed_values
        FROM 
            temporary_geohash_grid
        GROUP BY 
            geohash, resolution, filtered_values
    )
    , stats AS (
        SELECT
            geohash,
            resolution,
            attributes_name,
            arrayFlatten(groupArray(transformed_values)) AS all_values,
            count() AS point_count
        FROM aggregated
        GROUP BY geohash, resolution, attributes_name
    )
    SELECT 
        geohash,
        resolution,
        point_count,
        attributes_name,
        sum(arrayJoin(all_values)) AS attributes_sum,
        min(arrayJoin(all_values)) AS attributes_min,
        max(arrayJoin(all_values)) AS attributes_max,
        avg(arrayJoin(all_values)) AS attributes_mean,
        stddevSamp(arrayJoin(all_values)) AS attributes_stddev,
        
        -- Use the square of stddevSamp to compute variance:
        
        pow(stddevSamp(arrayJoin(all_values)), 2) AS attributes_variance,
        quantile(0.5)(arrayJoin(all_values)) AS attributes_median,
        topK(1)(arrayJoin(all_values))[1] AS attributes_mode
    FROM 
        stats
    GROUP BY 
        geohash, resolution, point_count, attributes_name;
    """
    client.execute(create_temp_stats_query)

    print("STEP 3: Statistics Table Creation Complete!")


    # Step 4: Create and populate temporary_transformed_data table
    create_temp_transformed_data_query = """
    CREATE TABLE dggs_data_nested (
        grid_id String,
        resolution Int32,
        geometry_type String,
        source_record_link String,
        point_count Int32,
        `attributes.name` Array(String),
        `attributes.sum` Float64,
        `attributes.min` Float64,
        `attributes.max` Float64,
        `attributes.mean` Float64,
        `attributes.stddev` Float64,
        `attributes.variance` Float64,
        `attributes.median` Float64,
        `attributes.mode` Float64
    )
    ENGINE = MergeTree()
    ORDER BY (grid_id, resolution)
    AS
    SELECT
        geohash AS grid_id,
        resolution,
        'Point' AS geometry_type,
        'link' AS source_record_link,
        point_count,
        attributes_name AS `attributes.name`,
        attributes_sum AS `attributes.sum`,
        attributes_min AS `attributes.min`,
        attributes_max AS `attributes.max`,
        attributes_mean AS `attributes.mean`,
        attributes_stddev AS `attributes.stddev`,
        attributes_variance AS `attributes.variance`,
        attributes_median AS `attributes.median`,
        attributes_mode AS `attributes.mode`
    FROM temporary_stats;
    """
    client.execute(create_temp_transformed_data_query)

    print("STEP 4: Transformed Data Table Creation Complete!")


    # Step 5: Cleanup: Dropping Temporary Tables
    tables_to_drop = [
    "temporary_raw_data",
    "temporary_filtered_data",
    "temporary_geohash_grid",
    "temporary_stats"
    ]

    for table in tables_to_drop:
        drop_query = f"DROP TABLE IF EXISTS {table};"
        client.execute(drop_query)

    print("STEP 5: Cleanup of Temporary Tables Complete!")


# End of the Python Script
