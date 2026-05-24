docker exec -it raffinerie-iot-spark-master-1 bash -c "/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.1 /app/traitement_kpi.py"
lancement job

docker exec -it raffinerie-iot-grafana-1 bash -c \
  "echo '[security]' >> /etc/grafana/grafana.ini && \
   echo 'allow_embedding = true' >> /etc/grafana/grafana.ini"
docker compose restart grafana
au cas ou grafana bloc le iframe
docker exec -it raffinerie-iot-grafana-1 bash -c "echo -e '[security]\nallow_embedding = true' >> /etc/grafana/grafana.ini"
docker compose restart grafana