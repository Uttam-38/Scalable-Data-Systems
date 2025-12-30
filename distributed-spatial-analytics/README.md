# Distributed Spatial Analytics with Apache Spark

## Overview
This project implements scalable spatial query processing using Apache Spark
and SparkSQL. The objective is to efficiently execute spatial queries over
large geospatial datasets using distributed computation.

The system supports range-based and distance-based spatial queries by
implementing custom spatial logic and leveraging Spark’s parallel execution
model.

## Key Features
- Distributed spatial query execution using SparkSQL
- Custom spatial functions for geometric containment and distance checks
- Support for range queries, distance queries, and spatial joins
- Scalable processing over large geospatial datasets

## Implemented Queries
- **Range Query**: Find all points within a given rectangular boundary
- **Range Join Query**: Match points with enclosing spatial regions
- **Distance Query**: Identify points within a given distance of a reference point
- **Distance Join Query**: Find point pairs within a specified distance threshold

## Architecture
- Apache Spark for distributed data processing
- SparkSQL for query execution
- Scala for implementing spatial logic and UDFs
- Docker-based environment for reproducible execution

## Technologies Used
- Apache Spark
- SparkSQL
- Scala
- Distributed Systems

## Dataset
The project operates on geospatial point and region datasets representing
real-world location data. Due to size and licensing constraints, datasets are
not included in this repository.

To run the project locally, provide the required CSV input files as described
in the execution instructions.

## Learning Outcomes
- Distributed query processing at scale
- Spatial data modeling and computation
- SparkSQL optimization and UDF design
- Scalable analytics system design

## Notes
This project emphasizes performance-aware design and distributed execution
rather than single-node spatial computation.
