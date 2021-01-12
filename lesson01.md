## Урок 1. Архитектура Spark. Принципы исполнения запросов. Сохранение и чтение данных

Построить распределения клиентов по возрастам `spark.table("homework.bank")`:
1. Распределение по возрасту с динамическим численным параметром `max_age`
2. Распределение по возрасту с динамическим параметром `marital`


#### Содержимое таблицы homework.bank
    %pyspark

    bank_df = spark.table("homework.bank")
    z.show(bank_df)

#### Распределение по возрасту с динамическим численным параметром max_age
    %pyspark

    age_filter = bank_df.age <= int(z.input("Введите максимальный возраст:", 45))

    z.show(
        bank_df.filter(age_filter).groupBy(bank_df.age).count().orderBy(bank_df.age)
    )

#### Распределение по возрасту с динамическим параметром marital
    %pyspark

    marital_filter = bank_df.marital == z.select("Семейное положение:", [("single","Не замужем/не женат"), ("married","Замужем/женат"), ("divorced","В разводе")], "married")
    
    z.show(
        bank_df.filter(marital_filter).groupBy(bank_df.age).count().orderBy(bank_df.age)
    )