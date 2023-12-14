'''
The Goal of this Python Script is to convert an input Point .geojson Geo-Spatial Dataset into JSONEachRow
Nested Structure for inserting it into ClickHouse, which is used as part of the Data Upload Process which is
part of the Project DGGS Cell Based Analytics
'''

# Importing the essential library
import json

# Function to convert point data in .geojson format to jsoneachrow format supported by clickhouse
def geojson_to_jsoneachrow(input_file_path, output_file_path):
    # Read the GeoJSON file
    with open(input_file_path, 'r') as f:
        data = json.load(f)

    # Convert to desired JSONEachRow format and save
    with open(output_file_path, 'w') as f:
        for feature in data['features']:
            properties = feature['properties']
            geometry = feature['geometry']

            # Extract coordinates for latitude and longitude
            longitude, latitude = geometry['coordinates']

            # Build the nested attributes structure
            attribute_names = list(properties.keys())
            attribute_values = [str(properties[key]) for key in attribute_names]

            json_line = {
                'lat': latitude,
                'lon': longitude,
                'attributes.name': attribute_names,
                'attributes.value': attribute_values
            }

            f.write(json.dumps(json_line) + '\n')

# End of the Python Script
