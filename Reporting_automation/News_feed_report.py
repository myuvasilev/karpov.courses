import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
from datetime import datetime, timedelta
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

sns.set()


connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20231020',
              'user':'student',
              'password':'dpo_python_2020'}


# Функция для CH
def ch_get_df(query):
    result = ph.read_clickhouse(query = query, connection = connection)
    return result

# Подключение к боту в telegram

chat_id = -4042591545
my_token = '6791782762:AAG4Uh9oaaK1T1FmYxAB8PMMNdHex4gW0So'
bot = telegram.Bot(token = my_token) 

    
default_args = {
    'owner': 'maksim-vasilev',
    'depends_on_past': False, 
    'retries': 2, 
    'retry_delay': timedelta(minutes=5), 
    'start_date': datetime(2023, 11, 19), 
}


schedule_interval = '0 11 * * *'


@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def m_vasilev_monitoring_bot():
    
    # выгружаем нужные нам метрики
    
    @task()
    def extract_metrics():   
        query = """SELECT 
                   count(distinct user_id) as DAU,
                   sum(action = 'view') as views,
                   sum(action = 'like') as likes,
                   countIf(action = 'like')/countIf(action = 'view') as CTR,
                   toDate(time) as event_date
                FROM 
                    simulator_20231020.feed_actions 
                group by event_date
                having toDate(time) between toDate(today() - 8) and toDate(today() - 1)
                order by event_date
                """
        df_metrics = ch_get_df(query)
        return df_metrics
     
    
    
    @task
    def make_report(df_metrics):
        yesterday = (datetime.now() - timedelta(days = 1)).strftime('%Y-%m-%d')
        feed_report = f'''
        Отчет по ключевым метрикам новостной ленты за {yesterday} :
        - DAU: {df_metrics[df_metrics['event_date'] == yesterday]['DAU'].iloc[0]}
        - Views: {df_metrics[df_metrics['event_date'] == yesterday]['views'].iloc[0]}
        - Likes: {df_metrics[df_metrics['event_date'] == yesterday]['likes'].iloc[0]}
        - CTR: {round(df_metrics[df_metrics['event_date'] == yesterday]['CTR'].iloc[0], 2)}
        '''
        return feed_report
    
    
    
    @task
    def make_image_report(df_metrics):
        fig, axes = plt.subplots(2, 2, figsize = (19, 15))
    
        sns.lineplot(ax = axes[0, 0], data = df_metrics, x = 'event_date', y = 'DAU')
        axes[0, 0].set_title('Уникальные пользователи')
    
        sns.lineplot(ax = axes[0, 1], data = df_metrics, x = 'event_date', y = 'views')
        axes[0, 1].set_title('Views')
    
        sns.lineplot(ax = axes[1, 0], data = df_metrics, x = 'event_date', y = 'likes')
        axes[1, 0].set_title('Likes')
    
        sns.lineplot(ax = axes[1, 1], data = df_metrics, x = 'event_date', y = 'CTR')
        axes[1, 1].set_title('CTR')
    
        for ax in axes.flat:
            ax.set(xlabel = None, ylabel = None)
        
        fig.suptitle('Отчет по ключевым метрикам новостной ленты за прошедшую неделю')
        plt.tight_layout()
    
        img_report = io.BytesIO()
        plt.savefig(img_report, format = 'png')
        img_report.seek(0)
        return img_report

    
    @task
    def send_tg_msg(feed_report):
        chat_id = -4042591545
        my_token = '6791782762:AAG4Uh9oaaK1T1FmYxAB8PMMNdHex4gW0So'
        bot = telegram.Bot(token = my_token)
        bot.send_message(chat_id = chat_id, text = feed_report)

        
    @task
    def send_tg_img(img_report):
        chat_id = -4042591545
        my_token = '6791782762:AAG4Uh9oaaK1T1FmYxAB8PMMNdHex4gW0So'
        bot = telegram.Bot(token = my_token)
        bot.send_photo(chat_id = chat_id, photo = img_report)    
        
    
    metrics_data = extract_metrics()
    report = make_report(metrics_data)
    image = make_image_report(metrics_data)
    
    send_tg_msg(report)
    send_tg_img(image)

    
m_vasilev_monitoring_bot = m_vasilev_monitoring_bot()   