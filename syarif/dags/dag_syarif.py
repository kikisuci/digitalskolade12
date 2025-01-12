#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_syarif",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    run_this_last = EmptyOperator(
        task_id="datamart_layer",
    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id="joined_layer",
        bash_command="echo 1",
    )
    # [END howto_operator_bash]

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id="data_ingestion_layer_" + str(i),
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    latest = BashOperator(
        task_id="machine_learning",
        bash_command="echo 1",
    )
    run_this >> latest

if __name__ == "__main__":
    dag.test()