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

#### Функция ratingToClass

    %pyspark
    
    def ratingToClass(rating):
        article_class = 'A'
        if rating > 100: 
            article_class = 'D'
        elif rating > 50:
            article_class = 'C'
        elif rating > 0:
            article_class = 'B'
        elif rating == 0:
            article_class = 'A'
            
        return article_class

#### Добавляем к датасету категориальную фичу rating_class

    %pyspark
    
    def ratingToClass(rating):
        article_class = 'A'
        if rating > 100: 
            article_class = 'D'
        elif rating > 50:
            article_class = 'C'
        elif rating > 0:
            article_class = 'B'
        elif rating == 0:
            article_class = 'A'
            
        return article_class
        
    rating_class = udf(ratingToClass)
    
    habrDataWithClass = habrData.withColumn("class", rating_class(col("rating")))
    
    habrDataWithClass.select("title", "rating", "class").show(20, False)

    +------------------------------------------------------------------------------------------------------+------+-----+
    |title                                                                                                 |rating|class|
    +------------------------------------------------------------------------------------------------------+------+-----+
    |Кодовым названием Ubuntu 12.04 будет Precise Pangolin                                                 |54    |C    |
    |Unreal Engine: QuickStart в Qt Creator под Arch Linux                                                 |17    |B    |
    |Денис Литвинов (COO FunCorp): продуктовые метрики для мобильных приложений в США                      |13    |B    |
    |ONLYOFFICE против Collabora: почему мы уверены, что наше решение лучше                                |20    |B    |
    |Parallels Desktop 13 — семь советов для эффективной работы                                            |22    |B    |
    |Полное руководство по написанию утилиты для Go                                                        |35    |B    |
    |Простой плагин для локализации приложений на Unity3D                                                  |5     |B    |
    |69 признаков того, что не вы трахаете проект, а он вас                                                |54    |C    |
    |О нехороших контекстных объявлениях                                                                   |5     |B    |
    |PHP Composer: фиксим зависимости без боли                                                             |76    |C    |
    |Переезд в Европу: приключение и выводы                                                                |54    |C    |
    |Сертификация систем фото/видеофиксации правонарушений и систем сегмента транспортной безопасности     |5     |B    |
    |Властелин прода в царстве legacy-кода (сказочка с открытым концом)                                    |13    |B    |
    |Intel oneAPI — один за всех, теперь — и для вас                                                       |10    |B    |
    |Quality pipelines в мобильной разработке, часть 1: Android                                            |21    |B    |
    |Превращая FunC в FunCtional с помощью Haskell: как Serokell победили в Telegram Blockchain Competition|15    |B    |
    |Официальный сайт JetBrains теперь доступен на русском языке                                           |14    |B    |
    |Интересные новинки Vue 3                                                                              |37    |B    |
    |Сервис для случайных встреч с незнакомцами, но не дейтинг. История стартапа Random Coffee             |22    |B    |
    |Хранилище key-value, или как наши приложения стали удобнее                                            |24    |B    |
    +------------------------------------------------------------------------------------------------------+------+-----+
    only showing top 20 rows
    
<hr>

    %pyspark
    
    trainDF, testDF = habrDataWithClass.randomSplit([.8, .2], seed=42)
    
    trainDF.coalesce(2).write.mode("overwrite").saveAsTable("habr.train")
    testDF.coalesce(2).write.mode("overwrite").saveAsTable("habr.test")
    
    print("There are " + str(trainDF.count()) + " rows in the training set, and " + str(testDF.count()) + " in the test set")
    
На этом месте Zeppelin'у уже перестали помогать перезагрузки и так и не получилось доделать этот итоговый проект.
Ниже приведен кусок кода из вебинара для линейной регрессии вместо логистической
    
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.feature import Tokenizer, RegexTokenizer, HashingTF, IDF
    from pyspark.sql.types import IntegerType
    from pyspark.ml import Pipeline
    from pyspark.sql.functions import udf
    
    # Prepare training data from a list of (label, features) tuples.
    train = spark.table("habr.train")\
    .selectExpr("title", "cast(rating as Long) rating")\
    .na.drop("any")
    
    # Prepare test data
    test = spark.table("habr.test")\
    .selectExpr(" title", "cast(rating as Long) rating")\
    .na.drop("any")
    
    tokenizer = Tokenizer(inputCol="title", outputCol="title_words")
    
    regexTokenizer = RegexTokenizer(inputCol="title", outputCol="title_words", pattern="[^a-zа-яё]", gaps=True)\
    .setMinTokenLength(3)
    
    # alternatively, pattern="\\w+", gaps(False)
    
    
    # tokenized = tokenizer.transform(train)
    # tokenized.select("title", "title_words")\
    #     .withColumn("tokens", countTokens(col("title_words"))).show(truncate=False)
    
    regexTokenized = regexTokenizer.transform(train)
    
    # regexTokenized.select("title", "title_words").withColumn("tokens", countTokens(col("title_words"))).show(truncate=False)
        
    
    hashingTF = HashingTF(inputCol="title_words", outputCol="rawFeatures", numFeatures=200000)
    featurizedData = hashingTF.transform(regexTokenized)
    # alternatively, CountVectorizer can also be used to get term frequency vectors
    
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    
    # rescaledData.select("rating", "features").show()
    
    
    lr = LinearRegression(maxIter=10, regParam=0.1, featuresCol='features', labelCol='rating', predictionCol='prediction')
    
    p = Pipeline(stages=[])
    
    pipeline = Pipeline(stages=[regexTokenizer, hashingTF, idf, lr])
     
    
    model = pipeline.fit(train)
    prediction = model.transform(test)
    
    from pyspark.ml.evaluation import RegressionEvaluator
    
    regressionEvaluator = RegressionEvaluator(
        predictionCol="prediction",
        labelCol="rating",
        metricName="rmse")
        
    rmse = regressionEvaluator.evaluate(prediction)
    print("RMSE is " + str(rmse))
    
    RMSE is 46.9776816861
