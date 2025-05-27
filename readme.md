```
docker compose run airflow airflow db init
```

```
docker compose run airflow airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```       

```
docker compose up -d --build
```
