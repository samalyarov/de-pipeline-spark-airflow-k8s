import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ───── CONFIGURATION ──────────────────────
# I kept it here instead of moving over to .env (for visibility) - but you might consider doing that
K8S_SPARK_NAMESPACE = "k8_spark_namespace_name"
K8S_CONNECTION_ID = "connection_id"
GREENPLUM_ID = 'greenplum_id'
USER_ID = "s-malyarov"

# Tables and related Spark/DDL info
TABLES = {
    "lineitems": {
        "application_file": "spark_submit_lineitems.yaml",
        "s3_path": f"de-project/{USER_ID}/lineitems_report",
        "ddl": f"""
            DROP EXTERNAL TABLE IF EXISTS "{USER_ID}".lineitems;
            CREATE EXTERNAL TABLE "{USER_ID}".lineitems (
                L_ORDERKEY BIGINT,
                count BIGINT,
                sum_extendprice FLOAT8,
                mean_discount FLOAT8,
                mean_tax FLOAT8,
                delivery_days FLOAT8,
                A_return_flags BIGINT,
                R_return_flags BIGINT,
                N_return_flags BIGINT
            )
            LOCATION ('pxf://de-project/{USER_ID}/lineitems_report?PROFILE=s3:parquet&SERVER=default')
            ON ALL
            FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
            ENCODING 'UTF8';
        """
    },
    
    "orders": {
        "application_file": "spark_submit_orders.yaml",
        "s3_path": f"de-project/{USER_ID}/orders_report",
        "ddl": f"""
            DROP EXTERNAL TABLE IF EXISTS "{USER_ID}".orders;
            CREATE EXTERNAL TABLE "{USER_ID}".orders (
                O_MONTH TEXT,
                N_NAME TEXT,
                O_ORDERPRIORITY TEXT,
                orders_count BIGINT,
                avg_order_price FLOAT8,
                sum_order_price FLOAT8,
                min_order_price FLOAT8,
                max_order_price FLOAT8,
                f_order_status BIGINT,
                o_order_status BIGINT,
                p_order_status BIGINT
            )
            LOCATION ('pxf://de-project/{USER_ID}/orders_report?PROFILE=s3:parquet&SERVER=default')
            FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
            ENCODING 'UTF8'
        """
    },
    
    "customers": {
         "application_file": "spark_submit_customers.yaml",
         "s3_path": f"de-project/{USER_ID}/customers_report",
         "ddl": f"""
            DROP EXTERNAL TABLE IF EXISTS "{USER_ID}".customers;
            CREATE EXTERNAL TABLE "{USER_ID}".customers (
                R_NAME TEXT,
                N_NAME TEXT,
                C_MKTSEGMENT TEXT,
                unique_customers_count BIGINT,
                avg_acctbal FLOAT8,
                mean_acctbal FLOAT8,
                min_acctbal FLOAT8,
                max_acctbal FLOAT8
            )
            LOCATION ('pxf://de-project/{USER_ID}/customers_report?PROFILE=s3:parquet&SERVER=default')
            FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
            ENCODING 'UTF8'
        """
    },
    
    "parts": {
        "application_file": "spark_submit_parts.yaml",
        "s3_path": f"de-project/{USER_ID}/parts_report",
        "ddl": f"""
            DROP EXTERNAL TABLE IF EXISTS "{USER_ID}".parts;
            CREATE EXTERNAL TABLE "{USER_ID}".parts (
                N_NAME TEXT,
				P_TYPE TEXT,
				P_CONTAINER TEXT,
				parts_count BIGINT,
				avg_retailprice FLOAT8,
				size BIGINT,
				mean_retailprice FLOAT8,
				min_retailprice FLOAT8,
				max_retailprice FLOAT8,
				avg_supplycost FLOAT8,
				mean_supplycost FLOAT8,
				min_supplycost FLOAT8,
				max_supplycost FLOAT8
			)
            LOCATION ('pxf://de-project/{USER_ID}/parts_report?PROFILE=s3:parquet&SERVER=default')
            FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
            ENCODING 'UTF8'
        """
    },
    
    "suppliers": {
        "application_file": "spark_submit_suppliers.yaml",
        "s3_path": f"de-project/{USER_ID}/suppliers_report",
         "ddl": f"""
            DROP EXTERNAL TABLE IF EXISTS "{USER_ID}".suppliers;
            CREATE EXTERNAL TABLE "{USER_ID}".suppliers (
                R_NAME TEXT,
				N_NAME TEXT,
				unique_supplers_count BIGINT,
				avg_acctbal FLOAT8,
				mean_acctbal FLOAT8,
				min_acctbal FLOAT8,
				max_acctbal FLOAT8
			)
            LOCATION ('pxf://de-project/{USER_ID}/suppliers_report?PROFILE=s3:parquet&SERVER=default')
            FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')
            ENCODING 'UTF8'
        """
    }
}

# ───── HELPERS ──────────────────────────────────────────────────
def build_submit_operator(task_id: str, application_file: str, dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=dag,
    )

def build_sensor(task_id: str, application_name, dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=dag,
    )

# ───── DAG DEFINITION ───────────────────────────────────────────
default_args = {
    "owner": USER_ID,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=120),
}

with DAG(
    dag_id=f"de-project-{USER_ID}-dag",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 7, 22, tz="UTC"),
    catchup=False,
    tags=[USER_ID],
    default_args=default_args,
    description="Loop-based multi-table Spark-K8S DAG",
) as dag:

    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    for table_name, table_cfg in TABLES.items():
        submit_task = build_submit_operator(
            task_id=f"submit_{table_name}",
            application_file=table_cfg["application_file"],
            dag=dag
        )

        sensor_task = build_sensor(
            task_id=f"sensor_{table_name}",
            application_name=f"{{{{task_instance.xcom_pull(task_ids='submit_{table_name}')['metadata']['name'] }}}}",
            dag=dag
        )

        datamart_task = SQLExecuteQueryOperator(
            task_id=f"{table_name}_datamart",
            conn_id=GREENPLUM_ID,
            sql=table_cfg["ddl"],
            split_statements=True,
            autocommit=True
        )

        # Set dependencies
        start_task >> submit_task >> sensor_task >> datamart_task >> end_task