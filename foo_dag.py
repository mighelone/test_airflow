from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime

import logging

logger = logging.getLogger(__name__)


class MyOperator(BaseOperator):

    @apply_defaults
    def __init__(self, x, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.x = x

    def execute(self, context, **kwargs):
        exec_date = context['execution_date']
        logger.info(f'self.x={self.x} - year={exec_date:%Y}')
        return 'foo'


def python_method(ds, **kwargs):
    exec_date = kwargs['execution_date']
    logger.info(f'{exec_date:%Y-%m-%d}')


if __name__.startswith('unusual_prefix'):
    dag = DAG(dag_id='foo', start_date=datetime.now())
    MyOperator(dag=dag, task_id='foo', x=34)
    task = MyOperator(dag=dag, task_id='foo')
    task1 = PythonOperator(task_id='doit',
                           provide_context=True,
                           python_callable=python_method,
                           dag=dag)
    task >> task1
