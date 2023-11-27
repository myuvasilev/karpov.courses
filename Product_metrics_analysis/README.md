# Задания по теме "Анализ продуктовых метрик"

### В данной директории хранятся задания по теме "Анализ продуктовых метрик" из симулятора аналитика Karpov.Courses:
1. Нужно проанализировать данные использования ленты новостей для двух типов пользователей: тех, кто попал на сайт через платные рекламные источники ('ads'), и тех, кто пришел через органические каналы ('organic').
Задача заключается в сравнении Retention в этих двух группах. Главная цель — понять, различается ли способ использования приложения у этих двух типов пользователей.
- [Анализ Retention Rate для двух групп пользователей](https://github.com/myuvasilev/karpov.courses/blob/main/Product_metrics_analysis/News_Feed_%20Retention.png)
Был проведен анализ Retention Rate двух типов трафика для 20 когорт пользователей, которые впервые запустили приложение в один из двадцати последних дней (считая с даты выполнения задания 26.10.2023)

2. Мы провели обширную рекламную кампанию, что привело к значительному увеличению числа новых пользователей в приложении, отображенному на графике активности аудитории.
Тем не менее, у нас есть сомнения относительно качества этого трафика. Необходимо изучить, каково поведение этих пользователей, привлеченных рекламой, в долгосрочной перспективе: насколько часто они возвращаются для использования приложения?
- [Retention Rate рекламной кампании](https://github.com/myuvasilev/karpov.courses/blob/main/Product_metrics_analysis/Marketing_Campaign_Retention.png)

3. Мы столкнулись с резким снижением активных пользователей! Требуется выяснить, какие группы пользователей испытывают проблемы с доступом к приложению и что объединяет их.
- [Исследование возможных причин снижения](https://github.com/myuvasilev/karpov.courses/blob/main/Product_metrics_analysis/dau_drop_1.png)
- [Обнаружение причины](https://github.com/myuvasilev/karpov.courses/blob/main/Product_metrics_analysis/dau_drop_2.png)
- [Наглядное доказательство и вывод](https://github.com/myuvasilev/karpov.courses/blob/main/Product_metrics_analysis/dau_drop_3.png)

4. Необходимо построить график, отображающий активную аудиторию по неделям, для каждой недели выделим три типа пользователей.
- Новые — первая активность в ленте была на этой неделе.
- Старые — активность была и на этой, и на прошлой неделе.
- Ушедшие — активность была на прошлой неделе, на этой не было.
[Активная аудитория по неделям](https://github.com/myuvasilev/karpov.courses/blob/main/Product_metrics_analysis/audience_by_week.png)