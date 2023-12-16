# Quantization using ClickHouse

In geospatial data processing, the transformation of raw data into a standardized format is essential. "Quantization using ClickHouse" plays an important role in this transformation within the DGGS Cell Based Analytics Project. This Github Repository delves into the sub-parts of this service, focusing on its functionalities, and the mechanics behind its backend operations.

## What is Quantization using ClickHouse?

Quantization using ClickHouse is a service in for Converting Non-DGGS data to DGGS Reference System (https://docs.ogc.org/per/20-039r2.html). Its function is to transform geospatial datasets (e.g. Point .geojson file in our case) into the DGGS Geohash Grid ID system, having resolutions from 1 to 12. This transformation is important for ensuring that the data aligns with the Discrete Global Grid System (DGGS) standards.

### Key Features:

1. DGGS Conversion: The service takes non-DGGS geospatial datasets and converts them into the DGGS Geohash grid id system, covering all resolutions from 1 to 12.

2. Statistics Calculation: Post alignment with the DGGS Geohash grid ids, ClickHouse computes a plethora of statistics for intersecting points across all resolutions. These statistics encompass:

2.1 Count: Total points within a cell.

2.2 Attribute Analysis: For numeric attributes in the dataset, the service calculates Sum, Minimum, Maximum, Mean(Average), Standard Deviation, Variance, Median, and Mode.

### How Does It Work?

The subsequent steps detail out the systematic procedure adopted by the "Quantization using ClickHouse" service. With each step, we showcase the inner mechanics of the back-end, ensuring that the geospatial data aligns perfectly with the DGGS Geohash Grid ID system. The steps are as follows:

1. Data Ingestion from .json in JSONEachRow Format
2. Geohash Grid Generation
3. Compute Statistics
4. Transform Data to Desired Nested Schema
5. Cleanup - Remove Temporary Tables

## Contact

Author - Akhil Chhibber

LinkedIn: https://www.linkedin.com/in/akhilchhibber/
