# Data Engineering Project: PySpark, Airflow, and Kubernetes Orchestration

## Overview

This project demonstrates advanced data engineering skills by building a robust ETL pipeline using **PySpark**, orchestrated with **Apache Airflow** on **Kubernetes**. The pipeline ingests, transforms, and aggregates large-scale data from S3, and loads analytical results into Greenplum via external tables.
- This project is a part of a **Data Engineer** course by **Karpov.Courses**. You can check it out [here](https://karpov.courses/dataengineer).

---

## Key Features & Technologies

- **PySpark**: Distributed data processing and transformation using Spark DataFrames.
- **Apache Airflow**: Workflow orchestration with dynamic DAGs, task dependencies, and retries.
- **Kubernetes**: Spark jobs are submitted and monitored as Kubernetes resources for scalability and reliability.
- **AWS S3**: Data lake storage for raw and processed data using the `s3a` protocol.
- **Greenplum**: Analytical database integration via external tables for downstream analytics.
- **Environment Variables & Security**: All sensitive credentials are loaded from environment variables using `.env` files (never committed to source control).
- **Modular & Reusable Code**: Each entity (orders, suppliers, customers, etc.) has its own PySpark job for maintainability and clarity.
- **Cloud technologies**: I've used [VK.Cloud](https://cloud.vk.com/) to run the K8S Cluster, Spark, Airflow and everything else.
    - I recommend going through [this](https://github.com/stockblog/webinar_mlflow) and [this](https://github.com/stockblog/jupyterhub_k8s_mcs_slurm_intel) examples as a starting point for launching your own solution on VK.Cloud. It doesn't cover the entire process - but is a great starting point. The examples there should be replicable to other cloud solutions without too much hassle, as I've used the managed K8S cluster solution.
<img width="1081" height="781" alt="image" src="https://github.com/user-attachments/assets/cb913cd0-922e-48be-9ad9-35759cdaca4c" />

---

## Skills Demonstrated

- **Big Data Processing**: Efficiently joining, aggregating, and transforming multi-million row datasets with PySpark.
- **Workflow Orchestration**: Building dynamic, maintainable Airflow DAGs with custom operators and sensors.
- **Cloud & DevOps**: Running Spark jobs on Kubernetes, integrating with S3, and automating data pipelines.
- **SQL & Data Modeling**: Creating external tables in Greenplum for seamless analytics.
- **Secure Coding Practices**: Managing secrets via environment variables and `.env` files, with `.gitignore` to prevent leaks.
- **Production-Ready Engineering**: Error handling, retries, modularization, and clear separation of configuration and code.

---

## Project Structure
de-project/  
├── dags/  
│   ├── spark-job-orders.py  
│   ├── spark-job-suppliers.py  
│   ├── ... (other spark jobs)  
│   ├── ... (associated .yaml files) 
│   └── de-project-s-malyarov-dag.py  
├── .env.example  
├── .gitignore  
├── .requirements.txt
└── README.md  

---

## How It Works
In general, the project assumes the classic DAMA DMBOK architecture - Extracting data from a separate Data Lake, Transforming it and Loading the resulting processed data into the analytical DWH. 
<img width="1972" height="1204" alt="image" src="https://github.com/user-attachments/assets/b2cd3546-b08f-42dc-be5e-6536f686523c" />

1. **PySpark Jobs**: Each job reads raw data from S3, performs joins and aggregations, and writes results back to S3 in Parquet format.
2. **Airflow DAG**: Dynamically loops through all entities, submitting Spark jobs to Kubernetes, monitoring their completion, and then running SQL DDL to create/update Greenplum external tables.
4. **Kubernetes**: Spark jobs are run as Kubernetes resources for scalability and fault tolerance.
5. **Greenplum Integration**: Results are exposed as external tables for analytics and BI. I'm not covering that part here :)
    <img width="857" height="371" alt="image" src="https://github.com/user-attachments/assets/16d7afb4-a6df-4d60-932b-cfafd7baca48" />
---

## Example: Orders ETL (PySpark)

- Reads orders, customers, and nation data from S3.
- Joins and aggregates by month, nation, and order priority.
- Calculates order counts, price stats, and status breakdowns.
- Writes the result as Parquet to S3.




