#!/usr/bin/env python3
"""
Producer de Kafka que genera comentarios aleatorios para análisis de sentimiento.
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Lista de comentarios predefinidos con diferentes sentimientos
COMMENTS = [
    # Comentarios positivos
    "Me encanta este producto, es fantástico!",
    "La experiencia de usuario es excelente, muy intuitiva.",
    "El servicio al cliente fue rápido y eficiente.",
    "Estoy muy satisfecho con mi compra, recomendaré este producto a mis amigos.",
    "La calidad es excepcional, vale cada centavo.",
    "Este restaurante ofrece una experiencia culinaria única y maravillosa.",
    "La película me dejó una sensación de felicidad y optimismo.",
    "El concierto fue increíble, la mejor experiencia musical de mi vida.",
    
    # Comentarios neutros
    "El producto funciona como se esperaba.",
    "La entrega llegó a tiempo.",
    "Es un producto estándar, nada especial pero funciona.",
    "El diseño es normal, cumple su función.",
    "No tengo una opinión fuerte sobre este producto.",
    "El servicio fue adecuado, ni bueno ni malo.",
    "La película tiene sus momentos, pero en general es bastante común.",
    "El hotel cumplió con los servicios básicos.",
    
    # Comentarios negativos
    "No estoy satisfecho con la calidad del producto.",
    "El servicio al cliente fue terrible, nunca respondieron a mis preguntas.",
    "El producto se rompió después de una semana, qué decepción.",
    "No recomiendo este producto, hay mejores opciones en el mercado.",
    "La relación calidad-precio es muy mala, me siento estafado.",
    "La comida estaba fría y sin sabor, una experiencia horrible.",
    "La película fue una pérdida total de tiempo y dinero.",
    "El hotel estaba sucio y el personal fue muy descortés."
]

def create_producer():
    """Crea y retorna un productor de Kafka."""
    # Intentar conectarse a diferentes brokers en caso de fallo
    bootstrap_servers = ['kafka1:19092', 'kafka2:19093', 'kafka3:19094']
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    return producer

def generate_comment():
    """Genera un comentario aleatorio de la lista predefinida."""
    return random.choice(COMMENTS)

def main():
    """Función principal que ejecuta el productor."""
    producer = create_producer()
    topic_name = "comments"
    
    print(f"Iniciando productor de comentarios en el tema '{topic_name}'...")
    
    try:
        while True:
            # Generar un comentario
            comment = generate_comment()
            
            # Crear mensaje con tiempo actual
            message = {
                "timestamp": datetime.now().isoformat(),
                "comment": comment
            }
            
            # Clave aleatoria para distribución equitativa en particiones
            key = f"comment-{random.randint(1, 1000)}"
            
            # Enviar mensaje
            future = producer.send(topic_name, key=key, value=message)
            
            # Imprimir confirmación
            result = future.get(timeout=60)
            print(f"Mensaje enviado: {message}")
            print(f"Partición: {result.partition}, Offset: {result.offset}")
            
            # Esperar entre 2 y 5 segundos antes de enviar el siguiente comentario
            time.sleep(random.uniform(2, 5))
            
    except KeyboardInterrupt:
        print("Productor detenido por el usuario")
    except Exception as e:
        print(f"Error en el productor: {e}")
    finally:
        producer.close()
        print("Productor cerrado")

if __name__ == "__main__":
    # Esperar un tiempo para que los servicios de Kafka estén completamente iniciados
    print("Esperando a que los servicios de Kafka estén disponibles...")
    time.sleep(10)
    main()