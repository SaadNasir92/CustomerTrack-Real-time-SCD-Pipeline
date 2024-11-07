# CustomerTrack: Real-time SCD Pipeline

## Overview
CustomerTrack is a sophisticated real-time data pipeline that demonstrates enterprise-grade data engineering capabilities through the implementation of both SCD (Slowly Changing Dimension) Type 1 and Type 2 methodologies. The pipeline showcases the ability to handle streaming data while maintaining historical records and current state data efficiently.

[Architecture Diagram Placeholder]

## Technical Highlights
- Real-time data streaming pipeline
- Containerized data generation service
- Cloud-based infrastructure (AWS)
- Automated data ingestion and processing
- Implementation of both SCD1 and SCD2 patterns
- Efficient data versioning and historical tracking

## Architecture Components

### Data Generation Layer
- Custom Python script using Faker library
- Docker containerization
- Volume mounting for data sharing
- Configurable data generation intervals

### Data Ingestion Layer
- Apache NiFi for data flow management
- Apache ZooKeeper for distributed coordination
- AWS EC2 for hosting services
- S3 bucket for data lake storage

[NiFi Flow Configuration Screenshot Placeholder]

### Data Processing Layer
- Snowflake for data warehousing
- Snowpipe for automated data loading
- Stream-based change data capture
- Scheduled data processing tasks

[Snowflake Pipeline Screenshot Placeholder]

## Pipeline Workflow

1. **Data Generation**
   ```python
   # Simulated real-time data generation
   # Reference: app.py
   ```

2. **Data Ingestion**
   - NiFi monitors mounted volume
   - Files are processed and moved to S3
   - Snowpipe automatically loads data to staging

3. **Data Processing**
   - Scheduled procedure runs every minute
   - Updates SCD1 main table
   - Captures changes via Snowflake streams
   - Maintains historical records in SCD2 table

[Data Flow Diagram Placeholder]

## Technical Design Decisions

### SCD1 and SCD2 Implementation
The project implements a dual-table strategy:
- **SCD1 Main Table**: 
  - Maintains current, accurate data
  - Optimizes query performance
  - Eliminates redundancy
  
- **SCD2 History Table**:
  - Tracks historical changes
  - Maintains data lineage
  - Enables point-in-time analysis

### Performance Optimization
- Separation of current and historical data reduces compute time
- Efficient merge operations for real-time updates
- Stream-based change capture minimizes processing overhead

### Scalability Considerations
- Containerized architecture enables easy scaling
- Cloud-based infrastructure allows for flexible resource allocation
- Parallel processing capability through Snowflake

## Real-World Applications

This pipeline architecture demonstrates enterprise-ready capabilities for:
- Customer data management systems
- Financial transaction tracking
- Product inventory management
- User profile management
- Compliance and audit systems

The separation of SCD1 and SCD2 tables reflects real-world requirements where:
- Business teams need quick access to current data
- Audit teams require historical tracking
- Analytics teams need point-in-time analysis capabilities

## Code Structure

### Docker Configuration
```yaml
# Reference: docker-compose.yml
# Container setup for NiFi and ZooKeeper
```

### Data Processing Logic
```sql
# Reference: procedure_and_task.sql
# Automated data processing procedure
```

### Data Versioning
```sql
# Reference: scd2.sql
# Historical data tracking implementation
```

## Technical Capabilities Demonstrated

1. **Data Engineering**
   - Stream processing
   - Data pipeline architecture
   - ETL/ELT implementation

2. **Cloud & Infrastructure**
   - Docker containerization
   - AWS service integration
   - Distributed systems management

3. **Data Modeling**
   - Dimensional modeling
   - SCD pattern implementation
   - Change data capture

4. **Automation**
   - Scheduled procedures
   - Automated data loading
   - Continuous data processing

## Project Significance

This project showcases the ability to:
- Design and implement enterprise-grade data pipelines
- Handle real-time streaming data effectively
- Apply industry-standard data modeling patterns
- Build scalable, maintainable data solutions
- Balance performance and historical tracking requirements

[Additional Screenshots/Diagrams Placeholder]

---
