#!/usr/bin/env python3
"""
Consumer de Kafka que analiza el sentimiento de los comentarios usando exclusivamente
un modelo de Hugging Face.
"""

import json
import time
import torch
from kafka import KafkaConsumer
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# Modelo de Hugging Face para análisis de sentimiento
MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"

def create_consumer(topic_name):
    """Crea y retorna un consumidor de Kafka."""
    # Intentar conectarse a diferentes brokers en caso de fallo
    bootstrap_servers = ['kafka1:19092', 'kafka2:19093', 'kafka3:19094']
    
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='sentiment-analyzer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        key_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    return consumer

class HuggingFaceSentimentAnalyzer:
    """Clase para analizar sentimiento utilizando un modelo de Hugging Face."""
    
    def __init__(self, model_name):
        """Inicializa el analizador con un modelo específico."""
        print(f"Cargando modelo de Hugging Face: {model_name}")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.label_mapping = ["Negativo", "Neutro", "Positivo"]
        
    def analyze(self, text):
        """
        Analiza el sentimiento del texto usando el modelo de Hugging Face.
        Retorna un diccionario con puntuaciones y clasificación.
        """
        # Tokenizar texto
        inputs = self.tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
        
        # Obtener predicciones
        with torch.no_grad():
            outputs = self.model(**inputs)
            scores = torch.nn.functional.softmax(outputs.logits, dim=1)[0].tolist()
            
        # Obtener etiqueta con mayor score
        predicted_class = scores.index(max(scores))
        classification = self.label_mapping[predicted_class]
        
        # Crear diccionario de resultados
        results = {
            "classification": classification,
            "scores": {
                "negative": scores[0],
                "neutral": scores[1],
                "positive": scores[2]
            }
        }
        
        return results

def main():
    """Función principal que ejecuta el consumidor."""
    topic_name = "comments"
    consumer = create_consumer(topic_name)
    
    print("Inicializando modelo de análisis de sentimiento...")
    hf_analyzer = HuggingFaceSentimentAnalyzer(MODEL_NAME)
    
    print(f"Iniciando consumidor y análisis de sentimiento en el tema '{topic_name}'...")
    
    try:
        for message in consumer:
            # Obtener datos del mensaje
            comment_data = message.value
            comment_text = comment_data.get("comment", "")
            timestamp = comment_data.get("timestamp", "")
            
            # Analizar sentimiento usando el modelo de Hugging Face
            analysis = hf_analyzer.analyze(comment_text)
            
            # Crear resultado
            result = {
                "timestamp": timestamp,
                "comment": comment_text,
                "sentiment": analysis["classification"],
                "scores": analysis["scores"]
            }
            
            # Imprimir resultado de manera formateada
            print("\n" + "="*80)
            print(f"COMENTARIO: {comment_text}")
            print(f"SENTIMIENTO: {analysis['classification']}")
            print(f"PROBABILIDADES: ")
            print(f"  - Positivo: {analysis['scores']['positive']:.4f}")
            print(f"  - Neutro: {analysis['scores']['neutral']:.4f}")
            print(f"  - Negativo: {analysis['scores']['negative']:.4f}")
            print("="*80 + "\n")
            
    except KeyboardInterrupt:
        print("Consumidor detenido por el usuario")
    except Exception as e:
        print(f"Error en el consumidor: {e}")
        raise  # Re-lanzar la excepción para ver el error completo
    finally:
        consumer.close()
        print("Consumidor cerrado")

if __name__ == "__main__":
    # Esperar un tiempo para que los servicios de Kafka estén completamente iniciados
    print("Esperando a que los servicios de Kafka estén disponibles...")
    time.sleep(15)
    main()