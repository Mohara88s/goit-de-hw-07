from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.task.trigger_rule import TriggerRule as tr
from airflow.utils.state import State
from airflow.providers.common.sql.sensors.sql import SqlSensor
from datetime import datetime
import random
import time

# Функція для примусового встановлення статусу DAG як успішного


def mark_dag_success(ti, **kwargs):
    dag_run = kwargs['dag_run']
    dag_run.set_state(State.SUCCESS)

# Функція для генерації медалей


def generate_medal():
    medal = random.choice(["Gold", "Silver", "Bronze"])
    print(f"Generated medal: {medal}")
    return medal

#  Функція вибору значення що запускає одне із трьох завдань (розгалуження).


def branching_on_medal(**kwargs):
    medal = kwargs["ti"].xcom_pull(task_ids="generate_medal")
    if medal == "Gold":
        return "count_gold_medals"
    elif medal == "Silver":
        return "count_silver_medals"
    else:
        return "count_bronze_medals"

# Функція затримки часу


def delay_func():
    print("Sleeping for 34 seconds...")
    time.sleep(35)


# Визначення DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 23, 0, 0),
}

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db"

with DAG(
        'vitalii_vasylets_de_hw7_dag',
        default_args=default_args,
        schedule=None,
        catchup=False,
        tags=["vitalii_vasylets_de_hw7_dag"]
) as dag:

    # Завдання для створення схеми бази даних (якщо не існує)
    create_schema = SQLExecuteQueryOperator(
        task_id='create_schema',
        conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS vitalii_vasylets;
        """
    )

    # Завдання для створення таблиці для зберігання медалей (якщо не існує)
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS vitalii_vasylets.medals(
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(6),
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Випадково обирає одне із трьох значень ['Bronze', 'Silver', 'Gold'].
    generate_medal_task = PythonOperator(
        task_id='generate_medal',
        python_callable=generate_medal,
    )

    # Залежно від обраного значення запускає одне із трьох завдань (розгалуження).
    branching_task = BranchPythonOperator(
        task_id="branching_on_medal",
        python_callable=branching_on_medal,
    )

    # Завдання рахує кількість записів у таблиці olympic_dataset.athlete_event_results,
    # що містять запис Bronze у полі medal, та записує отримане значення в таблицю, створену в пункті 1,
    # разом із типом медалі та часом створення запису.
    count_bronze_medals_task = SQLExecuteQueryOperator(
        task_id="count_bronze_medals",
        conn_id=connection_name,
        sql="""
           INSERT INTO vitalii_vasylets.medals (medal_type, count)
           SELECT 'Bronze', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Bronze';
           """,
    )

    # Завдання рахує кількість записів у таблиці olympic_dataset.athlete_event_results,
    # що містять запис Silver у полі medal, та записує отримане значення в таблицю, створену в пункті 1,
    # разом із типом медалі та часом створення запису.
    count_silver_medals_task = SQLExecuteQueryOperator(
        task_id="count_silver_medals",
        conn_id=connection_name,
        sql="""
           INSERT INTO vitalii_vasylets.medals (medal_type, count)
           SELECT 'Silver', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Silver';
           """,
    )

    # Завдання рахує кількість записів у таблиці olympic_dataset.athlete_event_results,
    # що містять запис Gold у полі medal, та записує отримане значення в таблицю, створену в пункті 1,
    # разом із типом медалі та часом створення запису.
    count_gold_medals_task = SQLExecuteQueryOperator(
        task_id="count_gold_medals",
        conn_id=connection_name,
        sql="""
           INSERT INTO vitalii_vasylets.medals (medal_type, count)
           SELECT 'Gold', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Gold';
           """,
    )

    # Запускає затримку виконання наступного завдання.
    delay_task = PythonOperator(
        task_id="delay",
        python_callable=delay_func,
        trigger_rule=tr.ONE_SUCCESS,
    )

    # Перевіряє за допомогою сенсора, чи найновіший запис у таблиці, створеній на етапі 1,
    # не старший за 30 секунд (порівнюючи з поточним часом). Ідея в тому, щоб упевнитися,
    # чи справді відбувся запис у таблицю.
    check_if_last_record_task = SqlSensor(
        task_id="check_if_last_record",
        conn_id=connection_name,
        sql="""
            WITH count_in_medals AS (
                SELECT COUNT(*) as nrows
                FROM vitalii_vasylets.medals
                WHERE created_at >= NOW() - INTERVAL 30 SECOND
            )
            SELECT nrows > 0 FROM count_in_medals;
        """,
        mode="poke",  # Перевірка умови періодично
        poke_interval=10,  # Інтервал перевірки (10 секунд)
        timeout=30,  # Тайм-аут перевірки (30 секунд)
    )

    # Завдання для примусового встановлення статусу DAG як успішного в разі невдачі
    mark_success_task = PythonOperator(
        task_id='mark_success',
        # Виконати, якщо хоча б одне попереднє завдання завершилося невдачею
        trigger_rule=tr.ONE_FAILED,
        python_callable=mark_dag_success,
    )

    # Встановлення залежностей
    create_schema >> create_table >> generate_medal_task >> branching_task
    (branching_task >> [count_bronze_medals_task,
     count_silver_medals_task, count_gold_medals_task] >> delay_task)
    delay_task >> check_if_last_record_task
    [
        count_bronze_medals_task,
        count_silver_medals_task,
        count_gold_medals_task,
        delay_task,
        check_if_last_record_task
    ] >> mark_success_task
