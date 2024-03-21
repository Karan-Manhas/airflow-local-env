from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define connection details (replace with your actual credentials)
MYSQL_CONN_ID = {
    'user': 'root',
    'password': '$hjR7cnhfbn8KmDX',
    'host': 'localhost',
    'database': 'poke_data',
}

def load_data_to_mysql(scraped_data):
    """
    Loads scraped data into a MySQL table.

    Args:
        data (list): List of dictionaries containing scraped data
                    (e.g., [{'name': '...', 'price': '...', ...}])
    """
    import mysql.connector

    # Connect to MySQL database
    connection = mysql.connector.connect(conn_name=MYSQL_CONN_ID)
    cursor = connection.cursor()

    try:
        # Assuming data is a list of dictionaries
        insert_query = """
            INSERT INTO ebay_raw_data (name, price, link, postage_cost)
            VALUES (%(name)s, %(price)s, %(link)s, %(postage_cost)s)
        """

        cursor.executemany(insert_query, scraped_data)
        connection.commit()

    except Exception as e:
        print(f"Error loading data into MySQL: {e}")

    finally:
        cursor.close()
        connection.close()

with DAG(
    "load_scraped_data_to_mysql",
    start_date=datetime(2024, 3, 22),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Assuming 'scraped_data' is a variable or XCom result from the previous task
    load_data_task = PythonOperator(
        task_id="load_data_to_mysql",
        python_callable=load_data_to_mysql,
        op_args=[ "{{ ti.xcom_pull(task_ids='previous_scraping_task') }}"],
    )