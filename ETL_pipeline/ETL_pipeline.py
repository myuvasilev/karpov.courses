from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20231020',
              'user':'student',
              'password':'dpo_python_2020'}

test_connection = {'host': 'https://clickhouse.lab.karpov.courses',
                   'database':'test',
                   'user':'student-rw',
                   'password':'656e2b0c9c'}




default_args = {
    'owner': 'maksim-vasilev',
    'depends_on_past': False, 
    'retries': 2, 
    'retry_delay': timedelta(minutes=5), 
    'start_date': datetime(2023, 11, 9), 
}


# Функция для CH
def ch_get_df(query):
    result = ph.read_clickhouse(query = query, connection = connection)
    return result


# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def m_vasilev_etl():
    

    @task()
    def extract_feed():
        query = """SELECT 
                   toDate(time) as event_date,
                   count(distinct user_id) as DAU,
                   sum(action = 'view') as views,
                   sum(action = 'like') as likes,
                   countIf(action = 'like')/countIf(action = 'view') as CTR
                FROM 
                    simulator_20231020.feed_actions 
                having toDate(time) between today() - 7 and today() - 1
                group by
                    event_date
                """
        
        df_cube_feed = ch_get_df(query)
        return df_cube_feed
    
    @task()
    def extract_messages():
        query = """SELECT event_date, user_id, os, gender, age, messages_sent, messages_received, users_sent, users_received
                   FROM(
                   SELECT
                   toDate(time) as event_date,
                   user_id,
                   os,
                   gender,
                   age,
                   count(receiver_id) as messages_sent,
                   count(distinct receiver_id) as users_sent
                FROM 
                    simulator_20231020.message_actions 
                where 
                    toDate(time) = toDate(yesterday())
                group by
                    event_date,
                    user_id,
                    os,
                    gender,
                    age)
                    as sent_mes
                
                full outer join
                
                (SELECT
                   toDate(time) as event_date,
                   receiver_id,
                   os,
                   gender,
                   age,
                   count(user_id) as messages_received, 
                   count(distinct user_id) as users_received
                FROM 
                    simulator_20231020.message_actions 
                where 
                    toDate(time) = toDate(yesterday())
                group by
                    event_date,
                    receiver_id,
                    os,
                    gender,
                    age)
                as receive_mes
                on sent_mes.user_id = receive_mes.receiver_id
                """
              
        df_cube_messages = ch_get_df(query)
        return df_cube_messages
    
    @task()
    def connect_tables(df_cube_feed, df_cube_messages):
        connected_table = df_cube_feed.merge(df_cube_messages, how = 'inner', on = ['event_date', 'user_id', 'os', 'gender', 'age'])
        return connected_table
    
    @task
    def transform_os(connected_table):
        connected_table['dimension'] = 'os'
        df_cube_os = connected_table[['event_date', 'dimension', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent', 'os']]\
            .groupby(['event_date', 'dimension', 'os'])\
            .sum()\
            .reset_index()\
            .rename(columns={'os': 'dimension_value'})
        return df_cube_os
    
    @task
    def transform_gender(connected_table):
        connected_table['dimension'] = 'gender'
        df_cube_gender = connected_table[['event_date', 'dimension', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent', 'gender']]\
            .groupby(['event_date', 'dimension',  'gender'])\
            .sum()\
            .reset_index()\
            .rename(columns={'gender': 'dimension_value'})
        return df_cube_gender
    
    @task
    def transform_age(connected_table):
        connected_table['dimension'] = 'age'
        df_cube_age = connected_table[['event_date', 'dimension', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent', 'age']]\
            .groupby(['event_date', 'dimension', 'age'])\
            .sum()\
            .reset_index()\
            .rename(columns={'age': 'dimension_value'})
        return df_cube_age
    
    @task
    def load(df_cube_os, df_cube_gender, df_cube_age):
        context = get_current_context()
        ds = context['ds']

        df_final = pd.concat([df_cube_os, df_cube_gender, df_cube_age])
        print('Feed and messages')
        print(df_final.to_csv(index=False, sep='\t'))

       
        query_table = """CREATE TABLE IF NOT EXISTS test.dag_m_vasilev(
                            event_date Date, 
                            dimension String, 
                            dimension_value String, 
                            views Int32, 
                            likes Int32, 
                            messages_received Int32, 
                            messages_sent Int32, 
                            users_received Int32, 
                            users_sent Int32
                        ) ENGINE = MergeTree()
                        ORDER BY event_date"""

        
        ph.execute(query = query_table, connection = test_connection)
        result = ph.to_clickhouse(df_final, table = 'dag_m_vasilev', connection = test_connection, index = False)
    
    df_cube_feed = extract_feed()
    df_cube_messages = extract_messages()
    connected_table = connect_tables(df_cube_feed, df_cube_messages)
    
    df_cube_os = transform_os(connected_table)
    df_cube_gender = transform_gender(connected_table)
    df_cube_age = transform_age(connected_table)
    
    load(df_cube_os, df_cube_gender, df_cube_age)

m_vasilev_etl = m_vasilev_etl()
 
 