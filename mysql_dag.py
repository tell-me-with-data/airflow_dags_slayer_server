"""
First mysql operator use
"""
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

def get_years_of_customers():
    request='SELECT DISTINCT YEAR(birht_date) FROM sales_db.customer LIMIT 5;'
    mysql_hook = MySqlHook(mysql_conn_id = 'mysql_sales_db',schema='sales_db')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    years = cursor.fetchall()
    if len(years) == 0:
        logging.warning("There aren't years to create view")
        return None
    years = [year for y in years for year in y]
    for year in years:
        logging.info('It will create a view with the next year: '+str(year))
    return years

with DAG(
    dag_id='mysql_sales_db_dag',
    start_date=datetime(2022,8,29),
    default_args={'mysql_conn_id':'mysql_sales_db'},
    tags=['development','test'],
    catchup=False
) as dag:

    start_task = DummyOperator(task_id = 'Start', dag=dag)
    finish_task = DummyOperator(task_id = 'Finish', dag=dag)

    for year in get_years_of_customers():

        drop_view_customers_by_years_task = MySqlOperator(
            task_id = f'drop_view_customers_by_{year}',
            sql=r"""DROP VIEW IF EXISTS sales_db.customer_"""+str(year)+r""";""",
            dag=dag
        )

        create_view_customers_by_year_task = MySqlOperator(
            task_id = f'create_view_customers_by_{year}',
            sql=r"""CREATE OR REPLACE VIEW sales_db.customer_"""+str(year)+r""" 
                    AS SELECT * FROM sales_db.customer
                    WHERE YEAR(birht_date) = """+str(year)+r""";""",
            dag=dag
        )
        start_task >> drop_view_customers_by_years_task >> create_view_customers_by_year_task >> finish_task