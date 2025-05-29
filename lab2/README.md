**Как запустить**
1. docker-compose up -d

2. docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /jars/postgresql-42.6.0.jar /app/p.py

3. docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /jars/postgresql-42.6.0.jar,/jars/clickhouse-jdbc-0.4.6-all.jar /app/pp.py

4. Потом можно войти в контейнер clickhouse и поделать запросы анализа витрин из файла "Запросы_для_анализа_витрин.sql"