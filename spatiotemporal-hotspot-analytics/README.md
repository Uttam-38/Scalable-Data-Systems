# Spatiotemporal Hotspot Analytics with Apache Spark

## Overview
This project implements scalable spatiotemporal hotspot analysis using Apache
Spark. The goal is to identify statistically significant spatial and
spatiotemporal clusters in large geospatial datasets through distributed
computation.

The system focuses on detecting high-activity regions by analyzing spatial
density and temporal patterns, enabling insights for operational and strategic
decision-making.

## Key Components
- **Hot Zone Analysis**: Aggregates point data within spatial regions to measure
  relative spatial density using distributed range joins.
- **Hot Cell Analysis**: Applies the Getis-Ord Gi* statistic to identify
  statistically significant spatiotemporal hotspots.

## Architecture
- Apache Spark for distributed data processing
- Scala for implementing spatial and statistical logic
- Grid-based spatial partitioning for scalable computation
- Spatiotemporal indexing using space–time cell coordinates

## Technologies Used
- Apache Spark
- SparkSQL
- Scala
- Distributed Systems
- Spatial & Spatiotemporal Analytics

## Dataset
The project operates on large geospatial point datasets representing
spatiotemporal activity. Due to size and licensing constraints, datasets are not
included in this repository.

To execute the pipeline locally, provide the required input datasets as
described in the execution instructions.

## Learning Outcomes
- Distributed spatial and spatiotemporal analytics
- Implementation of spatial statistics at scale
- Performance-aware Spark programming
- Large-scale data processing and aggregation

## Notes
This project emphasizes scalable analytics and statistical correctness over
single-node implementations.
