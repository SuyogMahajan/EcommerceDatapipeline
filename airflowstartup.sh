docker compose down 
docker compose up -d postgres 
docker compose run --rm airflow-webserver airflow db migrate
docker compose run --rm airflow-webserver airflow users create --username admin --password admin --firstname suyog --lastname mahajan --role Admin --email suyogmahajan2111@gmail.com
docker compose up --build