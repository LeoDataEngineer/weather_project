from twilio.rest import Client
from datetime import datetime
import pandas as pd
import json
import os

# Configuración de Twilio
TWILIO_ACCOUNT_SID = ''
TWILIO_AUTH_TOKEN = ''
PHONE_NUMBER = ''  # Número de teléfono de Twilio

# Lista de números de teléfono a los que enviar las alertas
TO_PHONE_NUMBERS = ['+541198498431']

# Función para obtener la fecha actual
def get_date():
    return datetime.now().strftime("%Y-%m-%d")

# Diccionario de umbrales para cada variable
THRESHOLDS = {
    'Wind speed average (m/s)': 25,
    'Air temperature (°C)': {'low': -10, 'high': 35},
    'Air pressure (hPa)': {'low': 880, 'high': 1050},
    'Relative humidity (%RH)': {'low': 5, 'high': 95},
    'Rain accumulation (mm)': 50,
    'Hail accumulation (hits/cm²)': 10,
}

# Función para enviar un mensaje usando Twilio
def send_message(account_sid, auth_token, message_body, to):
    
    try:
      client = Client(account_sid, auth_token)
      print("Conexión exitosa con Twilio")
    except Exception as e:
      print(f"Error al conectar con Twilio: {e}")
    
    # client = Client(account_sid, auth_token)
    message = client.messages.create(
        body=message_body,
        from_=PHONE_NUMBER,
        to=to
    )
    return message.sid


# Función para cargar el estado de las alertas enviadas
def load_alerts_state():
    if os.path.exists("sent_alerts.json"):
        with open("sent_alerts.json", "r") as f:
            try:
                sent_alerts = json.load(f)
                # Convertir las fechas de vuelta a pd.Timestamp si es necesario
                def convert_strings_to_timestamps(data):
                    if isinstance(data, str):  # Si es una cadena, intentar convertirla a Timestamp
                        try:
                            return pd.to_datetime(data)  # Convertir a Timestamp
                        except ValueError:
                            return data  # Si no es una fecha válida, devolver la cadena tal cual
                    elif isinstance(data, dict):  # Si es un diccionario, recorrer sus valores
                        return {key: convert_strings_to_timestamps(value) for key, value in data.items()}
                    elif isinstance(data, list):  # Si es una lista, recorrer sus elementos
                        return [convert_strings_to_timestamps(item) for item in data]
                    return data  # Si no es una cadena, devolver el dato tal como está

                # Convertir todas las cadenas de fecha de vuelta a Timestamp
                sent_alerts = convert_strings_to_timestamps(sent_alerts)
                return sent_alerts
            except json.JSONDecodeError:
                print("Advertencia: El archivo sent_alerts.json está vacío o malformado.")
                return {}
    else:
        return {}

# Función para guardar el estado de las alertas enviadas
def save_alerts_state(sent_alerts):
    # Convertir las fechas (Timestamp) a cadenas de texto antes de guardarlas
    def convert_timestamps_to_strings(data):
        if isinstance(data, pd.Timestamp):  # Si es un Timestamp, lo convertimos a cadena
            return data.strftime("%Y-%m-%d %H:%M:%S")  # Formato de fecha y hora
        elif isinstance(data, dict):  # Si es un diccionario, recorrer sus valores
            return {key: convert_timestamps_to_strings(value) for key, value in data.items()}
        elif isinstance(data, list):  # Si es una lista, recorrer sus elementos
            return [convert_timestamps_to_strings(item) for item in data]
        return data  # Si no es un Timestamp, devolver el dato tal como está

    # Convertir todas las fechas de Timestamp en sent_alerts a cadenas
    sent_alerts = convert_timestamps_to_strings(sent_alerts)

    # Guardar el diccionario convertido a un archivo JSON
    with open("sent_alerts.json", "w") as f:
        json.dump(sent_alerts, f, indent=4)

# Función para generar alertas según los umbrales y comparando los valores actuales con los anteriores
def generate_alerts(df, current_date, sent_alerts):
    alerts = []
    alerted_variables = set()  # Conjunto para almacenar variables ya alertadas

    for variable, threshold in THRESHOLDS.items():
        if isinstance(threshold, dict):  # Variables con límites superior e inferior
            condition = (
                ((df['Variable'] == variable) & (df['Measurement'].astype(float) < threshold['low'])) |
                ((df['Variable'] == variable) & (df['Measurement'].astype(float) > threshold['high']))
            )
        else:  # Variables con solo un límite superior
            condition = (df['Variable'] == variable) & (df['Measurement'].astype(float) > threshold)
        
        alert_data = df[condition]

        # Asegurarse de que la fecha de los datos coincida con la fecha actual
        alert_data['Timestamp'] = pd.to_datetime(alert_data['Timestamp'], errors='coerce')  # Convertir 'Timestamp' a datetime
        alert_data = alert_data[alert_data['Timestamp'].dt.date == datetime.strptime(current_date, "%Y-%m-%d").date()]

        # Solo agregar alertas para variables que no hayan sido alertadas antes
        if not alert_data.empty and variable not in alerted_variables:
            # Verificar si esta variable ya tiene alertas en el archivo para este día
            if current_date in sent_alerts and variable in sent_alerts[current_date]:
                prev_value = sent_alerts[current_date].get(variable, {}).get('value', None)
                # Si el valor anterior es mayor al valor actual (indica que la alerta ya pasó), no se genera otra alerta
                if prev_value is not None and prev_value > alert_data.iloc[0]['Measurement']:
                    # Borrar el registro del valor anterior (ya no hay alerta)
                    sent_alerts[current_date].pop(variable, None)
                    continue  # No enviar notificación, solo borrar el valor anterior

            alerts.append(alert_data)
            alerted_variables.add(variable)  # Añadir la variable al conjunto para evitar duplicados
    
    return alerts

# Función principal para el flujo de datos y notificaciones
def main_send_text(data):
    input_date = get_date()
    sent_alerts = load_alerts_state()  # Cargar el estado de alertas enviadas

    # Generar alertas según los umbrales y asegurarse de que los datos sean actuales
    alerts = generate_alerts(data, input_date, sent_alerts)

    # Evitar enviar alertas repetidas y enviar un solo mensaje por variable
    if alerts:
        for alert_df in alerts:
            for _, row in alert_df.iterrows():
                variable = row['Variable']
                date = row['Timestamp']
                
                # Verificar si ya se ha enviado una alerta para esta variable hoy
                if variable not in sent_alerts.get(input_date, {}):
                    # Crear el mensaje
                    message_body = f"\n¡Alerta de {variable}!\nFecha: {date}\n\nCondición detectada:\n{variable} : {row['Measurement']}\n"
                    
                    # Enviar el mensaje a todos los números de teléfono
                    for phone_number in TO_PHONE_NUMBERS:
                        message_id = send_message(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, message_body, phone_number)
                        print(f'Alerta enviada con éxito al número {phone_number}. ID del mensaje: {message_id}')
                    
                    # Actualizar el estado de la alerta enviada
                    if input_date not in sent_alerts:
                        sent_alerts[input_date] = {}
                    sent_alerts[input_date][variable] = {'value': row['Measurement'], 'date': date}

        # Guardar el estado de las alertas enviadas
        save_alerts_state(sent_alerts)
    else:
        print('No se detectaron condiciones extremas o la fecha de los datos no coincide con la fecha actual.')

