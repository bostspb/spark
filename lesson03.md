## Урок 3. Типы данных в Spark. Коллекции как объекты DataFrame. User-Defined Functions
* По данным habr_data получить таблицу с названиями топ-3 статей (по rating) для каждого автора
* По данным habr_data получить топ (по встречаемости) английских слов из заголовков.<br> 

_Возможное решение:_ <br>
1) выделение слов с помощью регулярных выражений, 
2) разделение на массивы слов 
3) explode массивовов 
4) группировка с подсчетом встречаемости

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

#### Топ-3 статей по rating для каждого автора

    %pyspark
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window
    
    windowSpec  = Window.partitionBy("author_name").orderBy(col("rating").desc())
    
    z.show(
        habrData\
            .withColumn("row_number", row_number().over(windowSpec)) \
            .where(col("row_number") <= 3)\
            .select("title","author_name","rating")
    )

<hr>
    
    %pyspark
    
    from pyspark.sql.functions import udf
    import re
    
    def extract_english_words(title):
        clean_title = re.sub('\W+', ' ', title.lower())
        words = clean_title.split()
        return words
    
    extract_english_words_udf = udf(extract_english_words)
    
    z.show(
        habrData\
            .withColumn("words", extract_english_words_udf("title"))
            .select("words")
    )

#### Топ по встречаемости английских слов в заголовках

    %pyspark
    
    from pyspark.sql.functions import split, explode, col
    
    z.show(
        habrData\
            .select(explode(split(col("title"), "\W+")).alias("word"))\
            .where((col("word") != '') & (col("word").rlike("[^0-9]")))
            .groupBy("word")\
            .count()\
            .orderBy(col("count").desc())
    )

<hr>

    %pyspark
    # а это неудачная попытка решить задачу через UDF - не получилось скрестить explode с UDF ((
    
    from pyspark.sql.functions import udf
    import re
    
    def extract_english_words(title):
        clean_title = re.sub('\W+', ' ', title.lower())
        words = clean_title.split()
        return words
    
    extract_english_words_udf = udf(extract_english_words)
    
    z.show(
        habrData\
            .withColumn("words", extract_english_words_udf("title"))
            .select("words")
    )