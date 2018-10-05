from unittest.mock import patch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from airflow.models import BaseOperator
from airflow.models import TaskInstance

from foo_dag import MyOperator, python_method


@patch('foo_dag.logger.info')
def test_my_operator(info):
    dag = DAG(dag_id='foo', start_date=datetime(2018, 1, 1))
    task = MyOperator(dag=dag,
                      task_id='foo',
                      # provide_context=True,
                      x=3)
    ti = TaskInstance(
        task=task, execution_date=datetime.now())
    task.execute(ti.get_template_context())

    info.assert_called()
    info.assert_called_once_with('self.x=3 - year=2018')


def test_python_operator():
    dag = DAG(dag_id='foo', start_date=datetime.now())
    doit = PythonOperator(
        task_id='doit',
        provide_context=True,
        python_callable=python_method,
        dag=dag)
    ti = TaskInstance(
        task=doit, execution_date=datetime.now())
    doit.execute(ti.get_template_context())
