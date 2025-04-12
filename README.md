# kafka_sent_analysis
análisis de sentimiento usando mensajes en un topic de kafka y modelo roberta.



## Construir e iniciar los servicios
docker-compose up -d --build

## Ejecutar el productor en una terminal
docker exec -it runner python3 /app/producer/comment_producer.py

## Ejecutar el consumidor en otra terminal
docker exec -it runner python3 /app/consumer/sentiment_analyzer.py
