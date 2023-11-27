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
    result = ph.read_clickhouse(query, connection = connection)
    return result

# Подключение к боту в telegram

chat_id = -938659451
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
def m_vasilev_app_bot():
    
    # выгружаем нужные нам метрики 
    
    
    @task()
    def extract_onlyfeed():   
        onlyfeed_query = """SELECT countIf(distinct user_id, feed_actions != 0) AS "News Feed Users"
FROM
  (SELECT user_id, date, age,
                         city,
                         country,
                         exp_group,
                         gender,
                         os,
                         source,
                         feed_actions,
                         messages
   FROM
     (SELECT user_id,
             COUNT(action) AS feed_actions,
             toDate(time) AS date,
             age,
             city,
             country,
             exp_group,
             gender,
             os,
             source
      FROM simulator_20231020.feed_actions
      GROUP BY user_id,
               toDate(time),
               age,
               city,
               country,
               exp_group,
               gender,
               os,
               source) feed
   FULL OUTER JOIN
     (SELECT user_id,
             COUNT(receiver_id) AS messages,
             toDate(time) AS date,
             age,
             city,
             country,
             exp_group,
             gender,
             os,
             source
      FROM simulator_20231020.message_actions
      GROUP BY user_id,
               toDate(time),
               age,
               city,
               country,
               exp_group,
               gender,
               os,
               source) message USING user_id, date, age,
                                                    city,
                                                    country,
                                                    exp_group,
                                                    gender,
                                                    os,
                                                    source
   ORDER BY date) AS feed_table
WHERE ((messages = 0));
                """
        df_onlyfeed = ch_get_df(onlyfeed_query)
        return df_onlyfeed
    
    
    
    @task()
    def extract_onlymes():   
        onlymes_query = """SELECT countIf(distinct user_id, messages != 0) AS "Messenger Users"
FROM
  (SELECT user_id, date, age,
                         city,
                         country,
                         exp_group,
                         gender,
                         os,
                         source,
                         feed_actions,
                         messages
   FROM
     (SELECT user_id,
             COUNT(action) AS feed_actions,
             toDate(time) AS date,
             age,
             city,
             country,
             exp_group,
             gender,
             os,
             source
      FROM simulator_20231020.feed_actions
      GROUP BY user_id,
               toDate(time),
               age,
               city,
               country,
               exp_group,
               gender,
               os,
               source) feed
   FULL OUTER JOIN
     (SELECT user_id,
             COUNT(receiver_id) AS messages,
             toDate(time) AS date,
             age,
             city,
             country,
             exp_group,
             gender,
             os,
             source
      FROM simulator_20231020.message_actions
      GROUP BY user_id,
               toDate(time),
               age,
               city,
               country,
               exp_group,
               gender,
               os,
               source) message USING user_id, date, age,
                                                    city,
                                                    country,
                                                    exp_group,
                                                    gender,
                                                    os,
                                                    source
   ORDER BY date) AS mes_table
WHERE ((feed_actions = 0));
                """
        df_onlymes = ch_get_df(onlymes_query)
        return df_onlymes
    
    
    
    @task()
    def extract_feed():   
        feed_query = """SELECT 
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
        df_feed = ch_get_df(feed_query)
        return df_feed
    
    
    
    @task()
    def extract_feed_two():   
        feed_two_query = """SELECT 
                   count(distinct user_id) as DAU,
                   sum(action = 'view') as views,
                   sum(action = 'like') as likes,
                   countIf(action = 'like')/countIf(action = 'view') as CTR,
                   toDate(time) as event_date
                FROM 
                    simulator_20231020.feed_actions 
                group by event_date
                having toDate(time) between toDate(today() - 15) and toDate(today() - 8)
                order by event_date
                """
        df_feed_two = ch_get_df(feed_two_query)
        return df_feed_two
    
    
    
    @task()
    def extract_mes():   
        mes_query = """SELECT 
                   count(distinct user_id) as DAU,
                   count(user_id) as messages_sent,
                   messages_sent / DAU as avg_mes,
                   toDate(time) as event_date
                FROM 
                    simulator_20231020.message_actions 
                group by event_date
                having toDate(time) between toDate(today() - 8) and toDate(today() - 1)
                order by event_date
                """
        df_mes = ch_get_df(mes_query)
        return df_mes
    
    
    
    @task()
    def extract_audience():   
        audience_query = """SELECT toStartOfDay(toDateTime(this_week)) AS __timestamp,
       status AS status,
       AVG(num_users) AS "AVG(num_users)"
FROM
  (SELECT this_week,
          previous_week, -uniq(user_id) as num_users,
                          status
   FROM
     (SELECT user_id,
             groupUniqArray(toMonday(toDate(time))) as weeks_visited,
             addWeeks(arrayJoin(weeks_visited), +1) this_week,
             if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status,
             addWeeks(this_week, -1) as previous_week
      FROM simulator_20231020.feed_actions
      group by user_id)
   where status = 'gone'
   group by this_week,
            previous_week,
            status
   HAVING this_week != addWeeks(toMonday(today()), +1)
   union all SELECT this_week,
                    previous_week,
                    toInt64(uniq(user_id)) as num_users,
                    status
   FROM
     (SELECT user_id,
             groupUniqArray(toMonday(toDate(time))) as weeks_visited,
             arrayJoin(weeks_visited) this_week,
             if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status,
             addWeeks(this_week, -1) as previous_week
      FROM simulator_20231020.feed_actions
      group by user_id)
   group by this_week,
            previous_week,
            status) AS virtual_table
GROUP BY status,
         toStartOfDay(toDateTime(this_week))
ORDER BY "AVG(num_users)" DESC
LIMIT 1000;
                """
        df_audience = ch_get_df(audience_query)
        return df_audience
    
    
    @task
    def make_report(df_onlyfeed, df_onlymes, df_feed, df_mes):
        yesterday = (datetime.now() - timedelta(days = 1)).strftime('%Y-%m-%d')
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        df_feed['DAU'] = df_feed['DAU'].astype(float)
        dau = df_feed[df_feed['event_date'] == yesterday]['DAU'].iloc[0]
        dau_week_ago = df_feed[df_feed['event_date'] == week_ago]['DAU'].iloc[0]
        views = df_feed[df_feed['event_date'] == yesterday]['views'].iloc[0]
        views_week_ago = df_feed[df_feed['event_date'] == week_ago]['views'].iloc[0]
        likes = df_feed[df_feed['event_date'] == yesterday]['likes'].iloc[0]
        likes_week_ago = df_feed[df_feed['event_date'] == week_ago]['likes'].iloc[0]
        ctr = df_feed[df_feed['event_date'] == yesterday]['CTR'].iloc[0]
        ctr_week_ago = df_feed[df_feed['event_date'] == week_ago]['CTR'].iloc[0]
        dau_mes = df_mes[df_mes['event_date'] == yesterday]['DAU'].iloc[0]
        dau_mes_week_ago = df_mes[df_mes['event_date'] == week_ago]['DAU'].iloc[0]
        df_mes['messages_sent'] = df_mes['messages_sent'].astype(float)
        messages = df_mes[df_mes['event_date'] == yesterday]['messages_sent'].iloc[0]
        messages_week_ago = df_mes[df_mes['event_date'] == week_ago]['messages_sent'].iloc[0]
        avg_mes = df_mes[df_mes['event_date'] == yesterday]['avg_mes'].iloc[0]
        avg_mes_week_ago = df_mes[df_mes['event_date'] == week_ago]['avg_mes'].iloc[0]
              
    
        dau_diff = ((dau - dau_week_ago) / dau_week_ago) * 100
        dau = int(dau)
        ctr_diff = ((ctr - ctr_week_ago) / ctr_week_ago) * 100
        dau_mes_diff = ((dau_mes - dau_mes_week_ago) / dau_mes_week_ago) * 100
        messages_diff = ((messages - messages_week_ago) / messages_week_ago) * 100
        messages = int(messages)
        avg_mes_diff = ((avg_mes - avg_mes_week_ago) / avg_mes_week_ago) * 100
        
        app_report = f'''
    Отчет по количеству пользователей за {yesterday} :
    - Пользуются только лентой: {df_onlyfeed.iloc[0]['News Feed Users']}
    - Пользуются только мессенджером: {df_onlymes.iloc[0]['Messenger Users']}
    
    Отчет по ключевым метрикам новостной ленты за {yesterday} :
    - DAU: {dau}, Изменение относительно недели назад: {dau_diff:.2f}%
    - CTR: {ctr:.2f}, Изменение относительно недели назад: {ctr_diff:.2f}%

    Отчет по ключевым метрикам мессенджера за {yesterday} :
    - DAU: {dau_mes}, Изменение относительно недели назад: {dau_mes_diff:.2f}%
    - Number of messages: {messages}, Изменение относительно недели назад: {messages_diff:.2f}%
    - Сообщений на пользователя: {avg_mes:.2f}, Изменение относительно недели назад: {avg_mes_diff:.2f}%
    '''
        return app_report
    

    @task
    def make_audience_report(df_audience):
        df_audience['__timestamp'] = pd.to_datetime(df_audience['__timestamp'])
        dfp = df_audience.pivot(index='__timestamp', columns='status', values='AVG(num_users)')
        dfp.index = dfp.index.date

        dfp.plot.bar(stacked=True, figsize=(22, 17), xlabel = 'Date', ylabel = 'Number of Users', fontsize=14)
        plt.title(label='Пользователи', fontsize=20)
        plt.legend(title='Status', bbox_to_anchor=(0.9, 1), loc='upper left', fontsize=18)

        aud_img = io.BytesIO()
        plt.savefig(aud_img, format='png')
        aud_img.seek(0)
        return aud_img
    
    
    @task
    def make_image_report(df_feed, df_feed_two):
        
        fig, axes = plt.subplots(2, 2, figsize = (19, 15))
        
        # Зеленый цвет отображает данные за прошедшую неделю, а красный за неделю до нее
    
        sns.lineplot(ax = axes[0, 0], data = df_feed, x = 'event_date', y = 'DAU', color='green', label='DAU')
        sns.lineplot(ax=axes[0, 0], data = df_feed_two, x='event_date', y='DAU', color='red', label='DAU (Week Ago)')
        axes[0, 0].set_title('Уникальные пользователи')
    
        sns.lineplot(ax=axes[0, 1], data=df_feed, x='event_date', y='views', color='green', label='Views')
        sns.lineplot(ax=axes[0, 1], data=df_feed_two, x='event_date', y='views', color='red', label='Views (Week Ago)')
        axes[0, 1].set_title('Views')
    
        sns.lineplot(ax = axes[1, 0], data = df_feed, x = 'event_date', y = 'likes', color='green', label='Likes')
        sns.lineplot(ax=axes[1, 0], data=df_feed_two, x='event_date', y='likes', color='red', label='Likes (Week Ago)')
        axes[1, 0].set_title('Likes')
    
        sns.lineplot(ax = axes[1, 1], data = df_feed, x = 'event_date', y = 'CTR', color='green', label='CTR')
        sns.lineplot(ax=axes[1, 1], data=df_feed_two, x='event_date', y='CTR', color='red', label='CTR (Week Ago)')
        axes[1, 1].set_title('CTR')
    
        for ax in axes.flat:
            ax.set(xlabel = None, ylabel = None)
        
        fig.suptitle('Отчет по ключевым метрикам новостной ленты за прошедшую неделю (зеленый) и неделю до нее (красный)')
        plt.tight_layout()
    
        img_report = io.BytesIO()
        plt.savefig(img_report, format = 'png')
        img_report.seek(0)
        return img_report

    
    @task
    def send_tg_msg(app_report):
        chat_id = -938659451
        my_token = '6791782762:AAG4Uh9oaaK1T1FmYxAB8PMMNdHex4gW0So'
        bot = telegram.Bot(token = my_token)
        bot.send_message(chat_id = chat_id, text = app_report)

    @task
    def send_tg_aud(aud_img):
        chat_id = -938659451
        my_token = '6791782762:AAG4Uh9oaaK1T1FmYxAB8PMMNdHex4gW0So'
        bot = telegram.Bot(token = my_token)
        bot.send_photo(chat_id = chat_id, photo = aud_img)  
    
    
    @task
    def send_tg_img(img_report):
        chat_id = -938659451
        my_token = '6791782762:AAG4Uh9oaaK1T1FmYxAB8PMMNdHex4gW0So'
        bot = telegram.Bot(token = my_token)
        bot.send_photo(chat_id = chat_id, photo = img_report)    
        
    
    df_onlyfeed = extract_onlyfeed()
    df_onlymes = extract_onlymes()
    df_feed = extract_feed()
    df_feed_two = extract_feed_two()
    df_mes = extract_mes()
    df_audience = extract_audience()
    

    report = make_report(df_onlyfeed, df_onlymes, df_feed, df_mes)
    audience_img = make_audience_report(df_audience)
    image = make_image_report(df_feed, df_feed_two)

    send_tg_msg(report)
    send_tg_aud(audience_img)
    send_tg_img(image)

    
m_vasilev_app_bot = m_vasilev_app_bot()  
 