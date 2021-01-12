## Урок 2. Операции с данными: агрегаты, джойны. Оптимизация SQL-запросов

### Задание 1
Для упражнений сгрененирован большой набор синтетических данных в таблице `hw2.events_full`. Из этого набора данных созданы маленькие (относительно исходного набора) таблицы разного размера `kotelnikov.sample_[small, big, very_big]`. 

Ответить на вопросы:
 * какова структура таблиц
 * сколько в них записей 
 * сколько места занимают данные


    %pyspark

    events_full = spark.table("hw2.events_full")
    sample = spark.table("hw2.sample")
    sample_small = spark.table("hw2.sample_small")
    sample_big = spark.table("hw2.sample_big")
    sample_very_big = spark.table("hw2.sample_very_big")

<hr>

    %pyspark
    from pyspark.sql.functions import col
    
    print("Table \"sample\"")
    
    print("Schema:")
    sample.printSchema()
    
    print("Rows count:")
    print(sample.count())
    
    print("\nData size:")
    spark.sql("ANALYZE TABLE hw2.sample COMPUTE STATISTICS NOSCAN")
    spark\
        .sql("DESCRIBE EXTENDED hw2.sample")\
        .filter(col("col_name") == "Statistics")\
        .show()
        
    '''
    Table "sample"
    Schema:
    root
     |-- event_id: string (nullable = true)
    
    Rows count:
    104592
    
    Data size:
    +----------+-------------+-------+
    |  col_name|    data_type|comment|
    +----------+-------------+-------+
    |Statistics|6663726 bytes|       |
    +----------+-------------+-------+
    '''

<hr>

    %pyspark
    from pyspark.sql.functions import col
    
    print("Table \"sample_small\"")
    
    print("Schema:")
    sample_small.printSchema()
    
    print("Rows count:")
    print(sample_small.count())
    
    print("\nData size:")
    spark.sql("ANALYZE TABLE hw2.sample_small COMPUTE STATISTICS NOSCAN")
    spark\
        .sql("DESCRIBE EXTENDED hw2.sample_small")\
        .filter(col("col_name") == "Statistics")\
        .show()
        
    '''
    Table "sample_small"
    Schema:
    root
     |-- event_id: string (nullable = true)
    
    Rows count:
    100
    
    Data size:
    +----------+----------+-------+
    |  col_name| data_type|comment|
    +----------+----------+-------+
    |Statistics|7247 bytes|       |
    +----------+----------+-------+
    '''
    
<hr>

    %pyspark
    from pyspark.sql.functions import col
    
    print("Table \"sample_big\"")
    
    print("Schema:")
    sample_big.printSchema()
    
    print("Rows count:")
    print(sample_big.count())
    
    print("\nData size:")
    spark.sql("ANALYZE TABLE hw2.sample_big COMPUTE STATISTICS NOSCAN")
    spark\
        .sql("DESCRIBE EXTENDED hw2.sample_big")\
        .filter(col("col_name") == "Statistics")\
        .show()
    
    '''
    Table "sample_big"
    Schema:
    root
     |-- event_id: string (nullable = true)
    
    Rows count:
    449166
    
    Data size:
    +----------+--------------+-------+
    |  col_name|     data_type|comment|
    +----------+--------------+-------+
    |Statistics|28608600 bytes|       |
    +----------+--------------+-------+
    '''
    
<hr>

    %pyspark
    from pyspark.sql.functions import col
    
    print("Table \"sample_very_big\"")
    
    print("Schema:")
    sample_very_big.printSchema()
    
    print("Rows count:")
    print(sample_very_big.count())
    
    print("\nData size:")
    spark.sql("ANALYZE TABLE hw2.sample_very_big COMPUTE STATISTICS NOSCAN")
    spark\
        .sql("DESCRIBE EXTENDED hw2.sample_very_big")\
        .filter(col("col_name") == "Statistics")\
        .show()
        
    '''
    Table "sample_very_big"
    Schema:
    root
     |-- event_id: string (nullable = true)
    
    Rows count:
    2248079
    
    Data size:
    +----------+---------------+-------+
    |  col_name|      data_type|comment|
    +----------+---------------+-------+
    |Statistics|143186369 bytes|       |
    +----------+---------------+-------+
    '''

### Задание 2
Получить планы запросов для джойна большой таблицы `hw2.events_full` с каждой из таблиц `hw2.sample`, `hw2.sample_big`, `hw2.sample_very_big` по полю `event_id`. 
В каких случаях используется **BroadcastHashJoin**? 

**BroadcastHashJoin** автоматически выполняется для джойна с таблицами, размером меньше параметра `spark.sql.autoBroadcastJoinThreshold`. 
Узнать его значение можно командой `spark.conf.get("spark.sql.autoBroadcastJoinThreshold")`.

    %pyspark
    spark.conf.get('spark.sql.autoBroadcastJoinThreshold')
    
    '''
    u'26214400'
    '''
    
<hr>

    %pyspark
    events_full.join(sample, "event_id").explain()
    
    '''
    == Physical Plan ==
    *(2) Project [event_id#1072, city#1073, skew_key#1074, date#1075]
    +- *(2) BroadcastHashJoin [event_id#1072], [event_id#0], Inner, BuildRight
       :- *(2) Project [event_id#1072, city#1073, skew_key#1074, date#1075]
       :  +- *(2) Filter isnotnull(event_id#1072)
       :     +- *(2) FileScan parquet hw2.events_full[event_id#1072,city#1073,skew_key#1074,date#1075] Batched: true, Format: Parquet, Location: CatalogFileIndex[hdfs://bigdataanalytics-head-0.novalocal:8020/apps/spark/warehouse/hw2.db/events..., PartitionCount: 15, PartitionFilters: [], PushedFilters: [IsNotNull(event_id)], ReadSchema: struct<event_id:string,city:string,skew_key:string>
       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
          +- *(1) Project [event_id#0]
             +- *(1) Filter isnotnull(event_id#0)
                +- *(1) FileScan parquet hw2.sample[event_id#0] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://bigdataanalytics-head-0.novalocal:8020/apps/spark/warehouse/hw2.db/sample], PartitionFilters: [], PushedFilters: [IsNotNull(event_id)], ReadSchema: struct<event_id:string>
    '''

<hr>

    %pyspark
    
    events_full.join(sample_small, "event_id").explain()
    
    == Physical Plan ==
    *(2) Project [event_id#1072, city#1073, skew_key#1074, date#1075]
    +- *(2) BroadcastHashJoin [event_id#1072], [event_id#2], Inner, BuildRight
       :- *(2) Project [event_id#1072, city#1073, skew_key#1074, date#1075]
       :  +- *(2) Filter isnotnull(event_id#1072)
       :     +- *(2) FileScan parquet hw2.events_full[event_id#1072,city#1073,skew_key#1074,date#1075] Batched: true, Format: Parquet, Location: CatalogFileIndex[hdfs://bigdataanalytics-head-0.novalocal:8020/apps/spark/warehouse/hw2.db/events..., PartitionCount: 15, PartitionFilters: [], PushedFilters: [IsNotNull(event_id)], ReadSchema: struct<event_id:string,city:string,skew_key:string>
       +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
          +- *(1) Project [event_id#2]
             +- *(1) Filter isnotnull(event_id#2)
                +- *(1) FileScan parquet hw2.sample_small[event_id#2] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://bigdataanalytics-head-0.novalocal:8020/apps/spark/warehouse/hw2.db/sampl..., PartitionFilters: [], PushedFilters: [IsNotNull(event_id)], ReadSchema: struct<event_id:string>

<hr>

    %pyspark
    events_full.join(sample_big, "event_id").explain()
    
    == Physical Plan ==
    *(5) Project [event_id#1072, city#1073, skew_key#1074, date#1075]
    +- *(5) SortMergeJoin [event_id#1072], [event_id#4], Inner
       :- *(2) Sort [event_id#1072 ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(event_id#1072, 200)
       :     +- *(1) Project [event_id#1072, city#1073, skew_key#1074, date#1075]
       :        +- *(1) Filter isnotnull(event_id#1072)
       :           +- *(1) FileScan parquet hw2.events_full[event_id#1072,city#1073,skew_key#1074,date#1075] Batched: true, Format: Parquet, Location: CatalogFileIndex[hdfs://bigdataanalytics-head-0.novalocal:8020/apps/spark/warehouse/hw2.db/events..., PartitionCount: 15, PartitionFilters: [], PushedFilters: [IsNotNull(event_id)], ReadSchema: struct<event_id:string,city:string,skew_key:string>
       +- *(4) Sort [event_id#4 ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(event_id#4, 200)
             +- *(3) Project [event_id#4]
                +- *(3) Filter isnotnull(event_id#4)
                   +- *(3) FileScan parquet hw2.sample_big[event_id#4] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://bigdataanalytics-head-0.novalocal:8020/apps/spark/warehouse/hw2.db/sampl..., PartitionFilters: [], PushedFilters: [IsNotNull(event_id)], ReadSchema: struct<event_id:string>
  
<hr> 

    %pyspark
    events_full.join(sample_very_big, "event_id").explain()
    
    == Physical Plan ==
    *(5) Project [event_id#1072, city#1073, skew_key#1074, date#1075]
    +- *(5) SortMergeJoin [event_id#1072], [event_id#6], Inner
       :- *(2) Sort [event_id#1072 ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(event_id#1072, 200)
       :     +- *(1) Project [event_id#1072, city#1073, skew_key#1074, date#1075]
       :        +- *(1) Filter isnotnull(event_id#1072)
       :           +- *(1) FileScan parquet hw2.events_full[event_id#1072,city#1073,skew_key#1074,date#1075] Batched: true, Format: Parquet, Location: CatalogFileIndex[hdfs://bigdataanalytics-head-0.novalocal:8020/apps/spark/warehouse/hw2.db/events..., PartitionCount: 15, PartitionFilters: [], PushedFilters: [IsNotNull(event_id)], ReadSchema: struct<event_id:string,city:string,skew_key:string>
       +- *(4) Sort [event_id#6 ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(event_id#6, 200)
             +- *(3) Project [event_id#6]
                +- *(3) Filter isnotnull(event_id#6)
                   +- *(3) FileScan parquet hw2.sample_very_big[event_id#6] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://bigdataanalytics-head-0.novalocal:8020/apps/spark/warehouse/hw2.db/sampl..., PartitionFilters: [], PushedFilters: [IsNotNull(event_id)], ReadSchema: struct<event_id:string>


### Задание 3
Выполнить джойны с таблицами hw2.sample, hw2.sample_big в отдельных параграфах, чтобы узнать время выполнения запросов (например, вызвать .count() для результатов запросов). Время выполнения параграфа считается автоматически и указывается в нижней части по завершении

Зайти в spark ui (ссылку сгенерировать в следующем папраграфе). Сколько tasks создано на каждую операцию? Почему именно столько? Каков DAG вычислений?

    %pyspark
    
    events_full.join(sample, "event_id").count()
    104592

<hr>

    %pyspark
    events_full.join(sample_big, "event_id").count()
    
    Py4JJavaError: An error occurred while calling o332.count.

<hr>

    println("185.241.193.174:8088/proxy/" + sc.applicationId + "/jobs/")

* Для джойна с таблицей `hw2.sample` было создано 195 tasks. Время выполнения 2 минуты.
* Для джойна с таблицей `hw2.sample_big` было создано 397 tasks. Время выполнения - больше 10 минут. До конца так и не получилось дождаться - большая часть tasks фэйлится.
* Посмотрел DAG-визуализацию


***Насильный broadcast***

Оптимизировать джойн с таблицами hw2.sample_big, hw2.sample_very_big с помощью broadcast(df). Выполнить запрос, посмотреть в UI, как поменялся план запроса, DAG, количество тасков. Второй запрос не выполнится 

    %pyspark
    
    from pyspark.sql.functions import broadcast
    
    events_full.join(broadcast(sample), "event_id").count()
    
    104592


джоин с насильным бродкастом с таблицей `sample_big` выпадает с ошибкой, проходит только с таблицей `sample`


***Отключение auto broadcast***

Отключить автоматический броадкаст командой `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`. Сделать джойн с семплом `hw2.sample`, сравнить время выполнения запроса.

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

<hr>

    %pyspark
    
    events_full.join(sample, "event_id").count()
    
    104592

После отключения автоматического броадкаста джоин с таблицей hw22.sample занял 2мин 22 сек - на 22 секунды больше


***Вернуть настройку к исходной***

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "26214400")
    spark.sql("clear cache")


### Задание 4
В процессе обработки данных может возникнуть перекос объёма партиций по количеству данных (data skew). В таком случае время выполнения запроса может существенно увеличиться, так как данные распределятся по исполнителям неравномерно. В следующем параграфе происходит инициализация датафрейма, этот параграф нужно выполнить, изменять код нельзя. В задании нужно работать с инициализированным датафреймом.

Датафрейм разделен на 30 партиций по ключу `city`, который имеет сильно  неравномерное распределение.

    %pyspark 
    from pyspark.sql.functions import col
    
    skew_df = spark.table("hw2.events_full")\
    .where("date = '2020-11-01'")\
    .repartition(30, col("city"))\
    .cache()
    
    skew_df.count()
    
    40000000


#### 4.1. Наблюдение проблемы
Посчитать количество event_count различных событий event_id , содержащихся в skew_df с группировкой по городам. Результат упорядочить по event_count.

В spark ui в разделе jobs выбрать последнюю, в ней зайти в stage, состоящую из 30 тасков (из такого количества партиций состоит skew_df). На странице стейджа нажать кнопку Event Timeline и увидеть время выполнения тасков по экзекьюторам. Одному из них выпала партиция с существенно большим количеством данных. Остальные экзекьюторы в это время бездействуют – это и является проблемой, которую предлагается решить далее.

    %pyspark
    
    skew_df.show()

    +--------------------+-------------+--------------------+----------+
    |            event_id|         city|            skew_key|      date|
    +--------------------+-------------+--------------------+----------+
    |f729fb12f3c864d35...|SMALL_CITY_40|f729fb12f3c864d35...|2020-11-01|
    |e2a9a6132ed63b7ac...|SMALL_CITY_40|e2a9a6132ed63b7ac...|2020-11-01|
    |aa15104b3bc9301fb...|SMALL_CITY_40|aa15104b3bc9301fb...|2020-11-01|
    |ba0b0ef54600130f6...|SMALL_CITY_40|ba0b0ef54600130f6...|2020-11-01|
    |c0393d198d655c0ea...|SMALL_CITY_66|c0393d198d655c0ea...|2020-11-01|
    |d7aa3dae05b4a8ddb...|SMALL_CITY_40|d7aa3dae05b4a8ddb...|2020-11-01|
    |56654782e52f598ce...|SMALL_CITY_66|56654782e52f598ce...|2020-11-01|
    |c9f14144fa9b4fef8...|SMALL_CITY_40|c9f14144fa9b4fef8...|2020-11-01|
    |22bde9930551ea754...|SMALL_CITY_66|22bde9930551ea754...|2020-11-01|
    |9eace23b1377dcbaa...|SMALL_CITY_66|9eace23b1377dcbaa...|2020-11-01|
    |58424405d696031a8...|SMALL_CITY_40|58424405d696031a8...|2020-11-01|
    |d1ef798262289b351...|SMALL_CITY_40|d1ef798262289b351...|2020-11-01|
    |057803683b05f35a5...|SMALL_CITY_40|057803683b05f35a5...|2020-11-01|
    |9af732de6701ce5d4...|SMALL_CITY_40|9af732de6701ce5d4...|2020-11-01|
    |3239c2353c3fdebda...|SMALL_CITY_40|3239c2353c3fdebda...|2020-11-01|
    |1b80326b78af437fb...|SMALL_CITY_66|1b80326b78af437fb...|2020-11-01|
    |7c7685b2aa78ceb8c...|SMALL_CITY_40|7c7685b2aa78ceb8c...|2020-11-01|
    |388dc838d96cd9316...|SMALL_CITY_40|388dc838d96cd9316...|2020-11-01|
    |7022db21149e35b8a...|SMALL_CITY_40|7022db21149e35b8a...|2020-11-01|
    |56c949e85f57aa294...|SMALL_CITY_40|56c949e85f57aa294...|2020-11-01|
    +--------------------+-------------+--------------------+----------+
    only showing top 20 rows

<hr>

    %pyspark
    
    skew_df\
        .groupBy("city")\
        .count()\
        .orderBy(col("count"))\
        .show()
    
    +--------------+-----+
    |          city|count|
    +--------------+-----+
    |  SMALL_CITY_0|19762|
    |SMALL_CITY_100|20121|
    | SMALL_CITY_10|39613|
    | SMALL_CITY_19|39653|
    | SMALL_CITY_68|39669|
    | SMALL_CITY_27|39678|
    | SMALL_CITY_97|39704|
    | SMALL_CITY_40|39709|
    | SMALL_CITY_65|39735|
    | SMALL_CITY_78|39741|
    | SMALL_CITY_77|39757|
    | SMALL_CITY_44|39759|
    | SMALL_CITY_52|39767|
    | SMALL_CITY_99|39772|
    | SMALL_CITY_73|39773|
    | SMALL_CITY_37|39775|
    | SMALL_CITY_83|39802|
    | SMALL_CITY_70|39804|
    | SMALL_CITY_29|39808|
    |  SMALL_CITY_2|39814|
    +--------------+-----+
    only showing top 20 rows

<hr>

    185.241.193.174:8088/proxy/application_1607849830911_0172/jobs/
    
Зашел в Details for Stage 51, открыл Event Timeline и действительно увидел, 
что у Task 27 время выполнения 2,9 минут, в то время как у остальных Tasks не превышает 2 секунд.


#### 4.2. repartition
Один из способов решения проблемы агрегации по неравномерно распределенному ключу является 
предварительное перемешивание данных. Его можно сделать с помощью метода `repartition(p_num)`, 
где `p_num` -- количество партиций, на которые будет перемешан исходный датафрейм

    %pyspark
    
    skew_df_r = skew_df.repartition(30).cache()

<hr>    

    %pyspark
    
    skew_df_r\
        .groupBy("city")\
        .count()\
        .orderBy(col("count"))\
        .show()
    
    +--------------+-----+
    |          city|count|
    +--------------+-----+
    |  SMALL_CITY_0|19762|
    |SMALL_CITY_100|20121|
    | SMALL_CITY_10|39613|
    | SMALL_CITY_19|39653|
    | SMALL_CITY_68|39669|
    | SMALL_CITY_27|39678|
    | SMALL_CITY_97|39704|
    | SMALL_CITY_40|39709|
    | SMALL_CITY_65|39735|
    | SMALL_CITY_78|39741|
    | SMALL_CITY_77|39757|
    | SMALL_CITY_44|39759|
    | SMALL_CITY_52|39767|
    | SMALL_CITY_99|39772|
    | SMALL_CITY_73|39773|
    | SMALL_CITY_37|39775|
    | SMALL_CITY_83|39802|
    | SMALL_CITY_70|39804|
    | SMALL_CITY_29|39808|
    |  SMALL_CITY_2|39814|
    +--------------+-----+
    only showing top 20 rows

после перемешивания время распределилось более равномерно


#### 4.3. Key Salting
Другой способ исправить неравномерность по ключу -- создание синтетического ключа с равномерным распределением. 
В нашем случае неравномерность исходит от единственного значения city='BIG_CITY', 
которое часто повторяется в данных и при группировке попадает к одному экзекьютору. 
В таком случае лучше провести группировку в два этапа по синтетическому ключу CITY_SALT, 
который принимает значение BIG_CITY_rand (rand -- случайное целое число) для популярного значения 
BIG_CITY и CITY для остальных значений. На втором этапе восстанавливаем значения CITY и проводим повторную агрегацию, 
которая не занимает времени, потому что проводится по существенно меньшего размера данным. 

Такая же техника применима и к джойнам по неравномерному ключу, 
см, например https://itnext.io/handling-data-skew-in-apache-spark-9f56343e58e8

Что нужно реализовать:
* добавить синтетический ключ
* группировка по синтетическому ключу
* восстановление исходного значения
* группировка по исходной колонке

<hr>

пока не очень разобрался в этом задании
                    