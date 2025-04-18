# PySpark Learning Environment

This Docker-based environment provides a ready-to-use PySpark setup with Jupyter notebooks covering various aspects of Apache Spark, from basic operations to advanced internals.

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop/) installed on your system
- Basic familiarity with terminal/command-line

### Running the Environment

1. **Start the Docker container**:

   ```bash
   docker run -it --rm -p 8888:8888 --name pyspark_jupyter pyspark_jupyter
   ```
   
   If you need to rebuild the container first, run:
   
   ```bash
   docker build -t pyspark_jupyter .
   ```

2. **Access JupyterLab**:

   Open your browser and navigate to:
   
   ```
   http://localhost:8888
   ```
   
   No password is required - you'll be taken directly to the JupyterLab interface.

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
   docker run -it --rm -p 8888:8888 -v $(pwd)/data:/opt/spark/work-dir/data pyspark_jupyter
   ```

2. You copy data out of the container before stopping it:

   ```bash
   docker cp pyspark_jupyter:/opt/spark/work-dir/yourfile.csv ./yourfile.csv
   ```

## Troubleshooting

### Container Doesn't Start

Check if port 8888 is already in use:

```bash
lsof -i :8888
```

If so, stop the conflicting service or use a different port:

```bash
docker run -it --rm -p 8889:8888 --name pyspark_jupyter pyspark_jupyter
```

Then access using http://localhost:8889

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

## Stopping the Environment

When you're done, you can stop the container:

1. Press `Ctrl+C` in the terminal where the container is running
2. Or run this in a separate terminal:

   ```bash
   docker stop pyspark_jupyter
   ```

## Learning Path Recommendation

If you're new to PySpark, we recommend this learning path:

1. Start with **01-UDF-Examples.ipynb** for basic PySpark operations
2. Move to **06-File-Formats.ipynb** to understand data loading and saving
3. Continue with **04-JSON-Processing.ipynb** and **03-Flatten-Complex-Objects.ipynb** for data structures
4. Study **02-PySpark-Best-Practices.ipynb** for optimization tips
5. Finally explore **00-PySpark-Internals.ipynb** for a deep dive into how Spark works

Happy learning! 