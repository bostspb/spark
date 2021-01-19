## Урок 4. Машинное обучение на pySpark на примере линейной регрессии
* построить распределение статей в датасете по `rating` с `bin_size = 10`
* написать функцию `ratingToClass(rating: Int): String`, которая определяет категорию статьи (A, B, C, D) на основе рейтинга. Границы для классов подобрать самостоятельно.
* добавить к датасету категориальную фичу `rating_class`. При добавлении колонки использовать `udf` из функции в предыдущем пункте
* построить модель логистической регрессии `one vs all` для классификации статей по рассчитанным классам.
* получить `F1 score` для получившейся модели

<hr>

    %pyspark
    
    from pyspark.sql.functions import col
    from pyspark.sql.types import IntegerType
    
    habrData = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv("/user/admin/habr_data.csv")\
    .withColumn("rating", col("rating").cast(IntegerType()))\
    .cache()
    
    habrData.printSchema()
    
    root
     |-- link: string (nullable = true)
     |-- title: string (nullable = true)
     |-- published_date: string (nullable = true)
     |-- published_time: string (nullable = true)
     |-- modified_date: string (nullable = true)
     |-- modified_time: string (nullable = true)
     |-- author_type: string (nullable = true)
     |-- author_name: string (nullable = true)
     |-- description: string (nullable = true)
     |-- image: string (nullable = true)
     |-- article_categories: string (nullable = true)
     |-- href_count: integer (nullable = true)
     |-- img_count: integer (nullable = true)
     |-- tags: string (nullable = true)
     |-- h3_count: integer (nullable = true)
     |-- i_count: integer (nullable = true)
     |-- spoiler_count: integer (nullable = true)
     |-- positive_votes: integer (nullable = true)
     |-- negative_votes: string (nullable = true)
     |-- rating: integer (nullable = true)
     |-- bookmarks: integer (nullable = true)
     |-- views: integer (nullable = true)
     |-- comments: double (nullable = true)
     |-- text_len: double (nullable = true)
     |-- lines_count: integer (nullable = true)
     |-- sentences_count: integer (nullable = true)
     |-- first_5_sentences: string (nullable = true)
     |-- last_5_sentences: string (nullable = true)
     |-- max_sentence_len: string (nullable = true)
     |-- min_sentence_len: string (nullable = true)
     |-- mean_sentence_len: string (nullable = true)
     |-- median_sentence_len: string (nullable = true)
     |-- tokens_count: string (nullable = true)
     |-- max_token_len: string (nullable = true)
     |-- mean_token_len: string (nullable = true)
     |-- median_token_len: string (nullable = true)
     |-- alphabetic_tokens_count: string (nullable = true)
     |-- words_count: string (nullable = true)
     |-- words_mean: string (nullable = true)
     |-- ten_most_common_words: string (nullable = true)

<hr>

    %pyspark
    z.show(habrData)

<hr>

    %pyspark
    
    z.show(
        habrData.orderBy(col("rating")).groupBy("rating").count().select("rating", "count")
    )

#### Распределение статей по rating с bin_size = 10
    
    %pyspark
    z.show(
        habrData\
            .withColumn("rating_10", round(col("rating") / 10)*10)\
            .orderBy("rating_10")\
            .groupBy("rating_10")\
            .count()\
            .select("rating_10", "count")
    )

#### Добавляем к датасету категориальную фичу rating_class

    %pyspark
    
    def ratingToClass(rating):
        article_class = '1'
        if rating > 100: 
            article_class = '4'
        elif rating > 50:
            article_class = '3'
        elif rating > 0:
            article_class = '2'
        elif rating == 0:
            article_class = '1'
            
        return article_class
        
    rating_class = udf(ratingToClass)
    
    habrDataWithClass = habrData.withColumn("class", rating_class(col("rating")))
    
    habrDataWithClass.select("title", "rating", "class").show(20, False)

    +------------------------------------------------------------------------------------------------------+------+-----+
    |title                                                                                                 |rating|class|
    +------------------------------------------------------------------------------------------------------+------+-----+
    |Кодовым названием Ubuntu 12.04 будет Precise Pangolin                                                 |54    |4    |
    |Unreal Engine: QuickStart в Qt Creator под Arch Linux                                                 |17    |2    |
    |Денис Литвинов (COO FunCorp): продуктовые метрики для мобильных приложений в США                      |13    |2    |
    |ONLYOFFICE против Collabora: почему мы уверены, что наше решение лучше                                |20    |2    |
    |Parallels Desktop 13 — семь советов для эффективной работы                                            |22    |3    |
    |Полное руководство по написанию утилиты для Go                                                        |35    |3    |
    |Простой плагин для локализации приложений на Unity3D                                                  |5     |1    |
    |69 признаков того, что не вы трахаете проект, а он вас                                                |54    |4    |
    |О нехороших контекстных объявлениях                                                                   |5     |1    |
    |PHP Composer: фиксим зависимости без боли                                                             |76    |4    |
    |Переезд в Европу: приключение и выводы                                                                |54    |4    |
    |Сертификация систем фото/видеофиксации правонарушений и систем сегмента транспортной безопасности     |5     |1    |
    |Властелин прода в царстве legacy-кода (сказочка с открытым концом)                                    |13    |2    |
    |Intel oneAPI — один за всех, теперь — и для вас                                                       |10    |2    |
    |Quality pipelines в мобильной разработке, часть 1: Android                                            |21    |3    |
    |Превращая FunC в FunCtional с помощью Haskell: как Serokell победили в Telegram Blockchain Competition|15    |2    |
    |Официальный сайт JetBrains теперь доступен на русском языке                                           |14    |2    |
    |Интересные новинки Vue 3                                                                              |37    |3    |
    |Сервис для случайных встреч с незнакомцами, но не дейтинг. История стартапа Random Coffee             |22    |3    |
    |Хранилище key-value, или как наши приложения стали удобнее                                            |24    |3    |
    +------------------------------------------------------------------------------------------------------+------+-----+
    only showing top 20 rows    
<hr>

    %pyspark

    z.show(
        habrDataWithClass \
            .groupBy("class").count() \
            .select("class", "count")
            .orderBy("class")
    )
    
    
#### Разделим датасет на обучающую и тестовую выборки

    %pyspark
    trainDF, testDF = habrDataWithClass.randomSplit([.8, .2], seed=42)
    
    trainDF.coalesce(2).write.mode("overwrite").saveAsTable("habr.train")
    testDF.coalesce(2).write.mode("overwrite").saveAsTable("habr.test")
    
    print("В обучающей выборке " + str(trainDF.count()) + " строк, в тестовой " + str(testDF.count()) + " строк")
    
    В обучающей выборке 8463 строк, в тестовой 2094 строк
    
    
#### Построим модель логистической регрессии one vs all для классификации статей по рассчитанным классам

    %pyspark
    from pyspark.ml.feature import RegexTokenizer, HashingTF, IDF
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression, OneVsRest
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    
    train = spark.table("habr.train").selectExpr("description", "cast(class as Long) class").na.drop("any")
    test = spark.table("habr.test").selectExpr("description", "cast(class as Long) class").na.drop("any")
    
    regexTokenizer = RegexTokenizer(inputCol="description", outputCol="description_words", pattern="[^a-zа-яё]", gaps=True).setMinTokenLength(3)
    regexTokenized = regexTokenizer.transform(train)
    
    hashingTF = HashingTF(inputCol="description_words", outputCol="rawFeatures", numFeatures=200000)
    featurizedData = hashingTF.transform(regexTokenized)
    
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    
    lr = LogisticRegression(labelCol="class", featuresCol="features")
    ovr = OneVsRest(classifier=lr, labelCol="class", featuresCol="features")
    
    pipeline = Pipeline(stages=[regexTokenizer, hashingTF, idf, ovr])
    model = pipeline.fit(train)

<hr>
    
    %pyspark
    predictions  = model.transform(test)
    result.select("prediction", "class").show(20, True)


#### Получим F1 score для построенной модели

    %pyspark
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    
    predictions  = model.transform(test)
    
    evaluator = MulticlassClassificationEvaluator(metricName="f1", labelCol="class")
    f1Score = evaluator.evaluate(predictions)
    print("F1 Score: ")
    print(f1Score)

    F1 Score: 
    0.293333736818