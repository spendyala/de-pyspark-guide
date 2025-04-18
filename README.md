# PySpark Learning Environment

This Docker-based environment provides a ready-to-use PySpark setup with Jupyter notebooks covering various aspects of Apache Spark, from basic operations to advanced internals.

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop/) installed on your system
- Basic familiarity with terminal/command-line

### Running the Environment

#### Option 1: Using Docker Compose (Recommended)

1. **Start the container using Docker Compose**:

   ```bash
   docker compose up -d
   ```
   
   If you've made changes to the Dockerfile and need to rebuild:
   
   ```bash
   docker compose up --build -d
   ```

2. **Stop the container when done**:

   ```bash
   docker compose down
   ```

#### Option 2: Using Docker Run

If you prefer to use Docker directly:

```bash
docker run -it --rm -p 8888:8888 -p 4040:4040 --name pyspark_jupyter pyspark_jupyter
```

### Accessing the Environment

1. **JupyterLab**: 
   - Open your browser and navigate to: `http://localhost:8888`
   - No password is required - you'll be taken directly to the JupyterLab interface

2. **Spark UI**:
   - When running Spark applications, access the Spark UI at: `http://localhost:4040`
   - If multiple Spark applications are running, try ports 4041, 4042, etc.
   - The Spark UI provides insights into job execution, stages, tasks, and performance metrics

## Available Notebooks

The environment includes several notebooks covering different aspects of PySpark:

0. **00-PySpark-Internals.ipynb**
   - Deep dive into Spark architecture
   - Executors, slots, and resource management
   - DAG optimization and lazy execution
   - Memory management and performance tuning

1. **01-UDF-Examples.ipynb**
   - Basic User-Defined Functions in PySpark
   - Python/PySpark function integration

2. **02-PySpark-Best-Practices.ipynb**
   - Performance optimization
   - Data handling recommendations
   - Common pitfalls to avoid

3. **03-Flatten-Complex-Objects.ipynb**
   - Working with nested data structures
   - Flattening arrays, maps, and structs

4. **04-JSON-Processing.ipynb**
   - Reading and parsing JSON data
   - Handling nested JSON structures

5. **05-Dictionary-UDFs.ipynb**
   - Creating UDFs that return complex data types
   - Dictionary-like structures in PySpark

6. **06-File-Formats.ipynb**
   - Working with CSV, JSON, Parquet, and Avro
   - Format comparisons and best practices

8. **08-Spark-UI-Guide.ipynb**
   - Understanding and navigating the Spark UI
   - Monitoring job execution and performance
   - Identifying bottlenecks and optimization opportunities
   - Analyzing memory usage, shuffles, and data skew

9. **09-Spark-SQL-Guide.ipynb**
   - SQL query basics and best practices
   - Creating and using temporary views and tables
   - Advanced SQL features: window functions, CTEs, and subqueries
   - UDFs in SQL context and performance considerations
   - Optimization techniques specific to Spark SQL

10. **10-Spark-Plan-Analysis.ipynb**
   - Understanding Spark's logical and physical execution plans
   - Analyzing plans to identify performance bottlenecks
   - Optimization techniques for scan operations, joins, and shuffles
   - Handling data skew and implementing best practices
   - End-to-end optimization examples with performance comparisons

11. **11-Advanced-Spark-Optimization.ipynb**
   - Deep dive into logical and physical query plans
   - Advanced scan operation analysis and tuning
   - Filter and projection pushdown optimization
   - Detailed join strategy analysis and selection
   - Advanced shuffle operation optimization
   - Data skew detection and handling techniques
   - Adaptive Query Execution internals
   - Performance benchmarking across optimization techniques

## Working with Notebooks

- **Running a cell**: Click a cell and press `Shift+Enter`
- **Adding a cell**: Click the `+` button in the toolbar
- **Changing cell type**: Use the dropdown in the toolbar to select code or markdown
- **Saving work**: Notebooks auto-save, but you can explicitly save with `Ctrl+S`

## Managing Packages

If you need additional Python packages, you can install them within a notebook:

```python
!pip install matplotlib pandas seaborn
```

## Data Persistence

Data created within the container will be lost when the container stops unless:

1. You mount a volume when starting the container:

   ```bash
   docker compose up -d  # Volumes are already configured in docker-compose.yml
   ```

2. You copy data out of the container before stopping it:

   ```bash
   docker cp pyspark_jupyter:/opt/spark/work-dir/yourfile.csv ./yourfile.csv
   ```

## Troubleshooting

### Container Doesn't Start

Check if ports 8888 or 4040 are already in use:

```bash
lsof -i :8888
lsof -i :4040
```

If so, stop the conflicting service or modify docker-compose.yml to use different ports.

### Memory Issues

If you encounter memory errors, allocate more memory to Docker in Docker Desktop settings, or adjust Spark memory settings in your notebooks:

```python
spark = SparkSession.builder \
    .appName("Your App") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()
```

### Missing Libraries

If you get module not found errors, install the required packages:

```python
!pip install numpy pandas matplotlib
```

### Can't Access Spark UI

1. Make sure you're running a Spark application (the UI only appears when Spark is active)
2. Verify port 4040 is exposed in docker-compose.yml
3. Try restarting the container with the proper port mappings

## Learning Path Recommendation

If you're new to PySpark, we recommend this learning path:

1. Start with **01-UDF-Examples.ipynb** for basic PySpark operations
2. Move to **06-File-Formats.ipynb** to understand data loading and saving
3. Continue with **04-JSON-Processing.ipynb** and **03-Flatten-Complex-Objects.ipynb** for data structures
4. Study **02-PySpark-Best-Practices.ipynb** for optimization tips
5. Explore **09-Spark-SQL-Guide.ipynb** for SQL operations and optimization
6. Learn plan analysis with **10-Spark-Plan-Analysis.ipynb** to understand execution optimization
7. Dive into advanced optimization with **11-Advanced-Spark-Optimization.ipynb** for deeper performance tuning
8. Study **00-PySpark-Internals.ipynb** for how Spark works under the hood
9. Finally, use **08-Spark-UI-Guide.ipynb** to learn how to monitor and debug your applications

Happy learning! 