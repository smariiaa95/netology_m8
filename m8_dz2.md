# netology_m8
Spark
# Импорт необходимых библиотек
from pyspark import SparkFiles
from pyspark.sql import functions as F
from pyspark.sql.functions import lag

# Чтение из файла
df = spark.read.option("header", "true").csv("/mnt/c/Обучение_Нетология/Модуль_8/covid-data.csv")

# №1. Выберите 15 стран с наибольшим процентом переболевших на 31 марта (в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших).
df_1 = df.select('iso_code', F.col('location').alias('Страна'), (F.col('total_cases')*100/F.col('population')).alias('Процент переболевших')).where(F.col('date')=='2020-03-31').sort(F.col('Процент переболевших').desc()).limit(15)
# Запись результата в файл
df_1.write.csv("/mnt/c/Обучение_Нетология/Модуль_8/df_1.csv

![Снимок экрана 2025-05-13 010812](https://github.com/user-attachments/assets/677e870f-e488-44bc-b546-5d8706e34c26)


# №2. Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021 в отсортированном порядке по убыванию
# (в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)
# Последняя неделя марта с 29 по 31 число.
df_2 = df.select('date', 'location', 'new_cases').filter((F.col('date') >= '2021-03-29') & (F.col('date') <= '2021-03-31') & (F.col('continent') !=
'')).groupBy('location', 'date').agg(F.sum('new_cases').alias('new_cases')).sort(F.col('new_cases').desc()).limit(10)
# Запись результата в файл
df_2.write.csv("/mnt/c/Обучение_Нетология/Модуль_8/df_2.csv")

![Снимок экрана 2025-05-14 003402](https://github.com/user-attachments/assets/dd2c43cf-c39f-4bbd-973c-e90daf17b81b)


# №3. Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. (например: в россии вчера было 9150 , сегодня 8763, итог: 
# -387) (в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)
df_3 = df.select('date', 'location', 'new_cases').filter((F.col('date') >= '2021-03-29') & (F.col('date') <= '2021-03-31') & (F.col('iso_code') =='RUS')).withColumn('new_cases_prev', lag('new_cases').over(w)).withColumn('delta', F.col('new_cases')-F.col('new_cases_prev'))
# Запись результата в файл
df_3.write.csv("/mnt/c/Обучение_Нетология/Модуль_8/df_3.csv")

![Снимок экрана 2025-05-14 010014](https://github.com/user-attachments/assets/b5dab4d3-45ef-447e-95a7-36d1aac9c493)


