import socket
import time
import json
import threading
from datetime import datetime
from confluent_kafka import Producer, Consumer
import psycopg2
import os
import pytz


# Configuración del productor de Kafka
producer_conf = {
    'bootstrap.servers': '3.133.39.161:9092',  # Cambia según tu configuración
    'client.id': socket.gethostname(),
}
producer = Producer(producer_conf)

# Configuración del consumidor de Kafka
consumer_conf = {
    'bootstrap.servers': '3.133.39.161:9092',
    'group.id': 'weather-station-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)

# Función para enviar un mensaje a Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Mensaje fallido: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# Conexión a PostgreSQL
def connect_postgres():
    try:
        conn = psycopg2.connect(
            host= '',
            database='kafka',
            user='',
            password='',
            port='14004'
        )
        return conn
    except Exception as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None

# Eliminar e insertar datos en PostgreSQL
def delete_and_insert_postgres(conn, data):
    # Solo insertar si la variable está en la lista deseada
    valid_variables = ['Sm', 'Ta', 'Pa', 'Ua', 'Rc', 'Hc']
    if data['Variable'] not in valid_variables:
        print(f"Variable {data['Variable']} no es válida para insertar.")
        return

    try:
        with conn.cursor() as cursor:
            query_delete = "DELETE FROM stream_data WHERE \"Variable\" = %s"
            cursor.execute(query_delete, (data['Variable'],))
            print(f"Registro eliminado para la variable {data['Variable']}")

            query_insert = """
                INSERT INTO stream_data ("Timestamp", "Variable", "Measurement")
                VALUES (%s, %s, %s)
            """
            cursor.execute(query_insert, (data['Timestamp'], data['Variable'], data['Measurement']))
            print(f"Datos insertados correctamente para {data['Variable']}")
        conn.commit()
    except Exception as e:
        print(f"Error al eliminar e insertar en PostgreSQL: {e}")
        conn.rollback()

# Función para procesar los mensajes del consumidor
def process_kafka_messages():
    conn = connect_postgres()
    if not conn:
        print("No se pudo conectar a PostgreSQL.")
        return

    consumer.subscribe(['weather-station'])
    try:
        print("Esperando mensajes del tópico 'weather-station'...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error al consumir mensaje: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                print(f"Mensaje recibido: {data}")
                if 'Timestamp' in data and 'Variable' in data and 'Measurement' in data:
                    delete_and_insert_postgres(conn, data)
                else:
                    print("El mensaje no tiene el formato esperado")
            except json.JSONDecodeError as e:
                print(f"Error al decodificar JSON: {e}")
            except Exception as e:
                print(f"Error procesando mensaje: {e}")
    finally:
        consumer.close()
        if conn:
            conn.close()

# Función para enviar datos a Kafka
def send_data_to_kafka():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(('rtd.hpwren.ucsd.edu', 12020))
    except Exception as e:
        print(f"Error al conectar al socket: {e}")
        s.close()
        return

    tiempo_entre_mensajes = 6  # segundos
    while True:
        data = s.recv(1024).decode()  # Decodificar los datos binarios en un string legible

        for line in data.splitlines():
            parts = line.split("\t")
            if len(parts) == 4:
                ip, id, tstamp, values = parts
                try:
                    timestamp_seconds = int(tstamp)
                    # tstamp_date = datetime.fromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')
                    # Crear un objeto datetime en UTC
                    utc_time = datetime.fromtimestamp(timestamp_seconds, pytz.UTC)

                    # Si necesitamos convertirlo a una zona horaria local, por ejemplo, Buenos Aires (UTC-3)
                    local_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
                    local_time = utc_time.astimezone(local_timezone)

                    # Formatear la fecha en el formato deseado
                    tstamp_date = local_time.strftime('%Y-%m-%d %H:%M:%S')

                    for val in values.split(",")[1:]:
                        variable, measurement = val.strip().split("=")
                        data_dict = {
                            "IP": ip,
                            "ID": id,
                            "Variable": variable,
                            "Measurement": measurement,
                            "Timestamp": tstamp_date
                        }
                        data_json = json.dumps(data_dict)

                        # Solo enviar datos de variables válidas
                        if variable in ['Sm', 'Ta', 'Pa', 'Ua', 'Rc', 'Hc']:
                            producer.produce(
                                topic='weather-station',
                                value=data_json,
                                callback=delivery_report
                            )
                            producer.poll(0)
                            print(f"Mensaje enviado: {data_json}")
                        else:
                            print(f"Variable {variable} no es válida para Kafka.")

                        time.sleep(tiempo_entre_mensajes)
                except Exception as e:
                    print(f"Error procesando la línea: {line}")
                    print(f"Error: {e}")
        time.sleep(tiempo_entre_mensajes)

# Crear hilos para el productor y consumidor
producer_thread = threading.Thread(target=send_data_to_kafka)
consumer_thread = threading.Thread(target=process_kafka_messages)

# Iniciar los hilos
producer_thread.start()
consumer_thread.start()

# Esperar que ambos hilos finalicen
producer_thread.join()
consumer_thread.join()
