import streamlit as st
from streamlit.components.v1 import html
import pandas as pd
import psycopg2
import smtplib
import re
import base64
from PIL import Image
from io import BytesIO
from modelo_ML import main
from grafico import visualize_data
from twilio_script import main_send_text
from pronostico_api import pronostico_api

st.set_page_config(page_title="weather.nexting.click", layout="centered")


menu_main = st.sidebar.radio(
    "Navegación",
    ["Inicio", "Datos en tiempo real", "Gráficos históricos", "Pronostico", "Predicción","Videos","Notificaciones de alertas", "Contacto"]
)


##################################  Funciones ###########################################

def mostrar_prediccion(prediccion, string):
    """
    Muestra una predicción con un fondo dinámico basado en su nivel de probabilidad.

    Args:
    prediccion (str): El texto de la predicción (por ejemplo, 'Baja Probabilidad', 'Media Probabilidad', 'Alta Probabilidad').
    """
    # Determinar el color según la probabilidad
    if prediccion == "Baja Probabilidad":
        bg_color = "lightgreen"
    elif prediccion == "Media Probabilidad":
        bg_color = "orange"
    elif prediccion == "Alta Probabilidad":
        bg_color = "red"
    else:
        bg_color = "lightgray"  # Color predeterminado para valores desconocidos

    # Mostrar el texto con fondo coloreado
    st.markdown(
        f"""
        <div style="
            background-color: {bg_color};
            padding: 10px;
            border-radius: 5px;
            color: white;
            font-weight: bold;
            text-align: center;">
            {string}: {prediccion}
        </div>
        """,
        unsafe_allow_html=True
    )



def execute_query(query):
    conn = psycopg2.connect(
        host='',
        database='kafka',
        user='',
        password='',
        port='14004'
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df
    
# Datos en streaming
def get_data_from_postgres_stream(table):
 
 
    query = f'''
            SELECT "Timestamp", "Variable", "Measurement"
            FROM {table}
            WHERE "Timestamp" IS NOT NULL
            AND "Variable" IN ('Sm', 'Ta', 'Pa', 'Ua', 'Rc', 'Hc');
        '''
   
    df = execute_query(query)
  
    # Reemplazar los valores de la columna "Variable"
    replacements = {
        'Sm': 'Wind speed average (m/s)',
        'Ta': 'Air temperature (°C)',
        'Pa': 'Air pressure (hPa)',
        'Ua': 'Relative humidity (%RH)',
        'Rc': 'Rain accumulation (mm)',
        'Hc': 'Hail accumulation (hits/cm2)'
    }
    
    # Aplicar el reemplazo de "Variable"
    df['Variable'] = df['Variable'].map(replacements).fillna(df['Variable'])

    # Eliminar la última letra de cada valor en la columna "Measurement"
    df['Measurement'] = df['Measurement'].apply(lambda x: x[:-1] if isinstance(x, str) else x)

    return df  # Cargar datos en tiempo real
    

# Función para conectarse a PostgreSQL y obtener los datos Datos en Bach
def get_data_from_postgres(column_name, table):
    
      
    query = f'''
        SELECT "Timestamp", "created_at", "{column_name}"
        FROM {table}
        WHERE "Timestamp" IS NOT NULL 
        AND "{column_name}" IS NOT NULL;
    '''
   
    df = execute_query(query)
    
    return df

######################################### Texto principal  ############################################################
if menu_main == "Inicio":
    # Agregar la imagen en el header
    st.image("imagen/weather.jpg")
    # Configuración de Streamlit
    st.title("Monitoreo Meteorológico en Tiempo Real - Datos de la Estación HPWREN (UCSD) y WeatherAPI.")

    # Subtítulo
    #st.write("Accede a datos meteorológicos en tiempo real, datos historicos de tipo serie temporal y probalidad del tiempo con un modelo de Maching leearig proporcionados por el proyecto HPWREN de la Universidad de California, San Diego.")
               
    st.markdown("""
    Accede a datos meteorológicos en tiempo real, series temporales históricas y predicciones de 
    probabilidad del clima, basadas en un modelo de *Machine Learning*. 
    Los datos son proporcionado por el proyecto HPWREN de la Universidad de California, San Diego.
                       
    Nuestro portal te ofrece:
    
    **Datos en Tiempo Real**: condiciones climáticas actuales con actualizaciones en tiempo real de Temperatura, humedad, velocidad del viento y más.  
    **Gráficos Históricos**: explora series de datos pasados para analizar tendencias y patrones climáticos.  
    **Pronóstico**: Obtén pronosticos  para los próximos días.  
    **Predicción con Machine Learning**: Modelos que anticipan probabilidades del clima para ayudarte a planificar.  
    **Videos**: Visualiza imagenes desde las antenas de los sensores (hpwren).        
    **Notificaciones de Alertas**: Suscríbete para recibir avisos en tiempo real sobre alertas meteorológicas.


    **Acceso gratuito**: Datos respaldados por el proyecto HPWREN (High Performance Wireless Research and Education Network).
    Esta plataforma está pensada en la Gestión Agrícola en San Diego, pero cualquier persona interesada puede hacer uso de los datos.
    """)
    # Agregar el enlace
    st.markdown("##### Links de interes del proyecto:")
    st.markdown("Proyecto de la estación: *[HPWREN(UCSD)](https://hpwren.ucsd.edu/)*")
    st.markdown("API de clima: *[WeatherAPI](https://www.weatherapi.com/)*")
    st.markdown("Repositorio del codigo del proyecto: [GitHub](https://github.com/LeoDataEngineer/weather_project)")
    st.markdown("Documentos acerca del proyecto: [Documentación](https://drive.google.com/file/d/1wcoP0qdst_rZ11-1JELMx_iHulsiEI_x/view?usp=sharing)")
    st.write("")

###################################### Datos en tiempo Real ################################################################


elif menu_main == "Datos en tiempo real":
    # Datos en tiempo Real
    # Función para mostrar los datos en "cajitas" en Streamlit
    def display_metrics(dataframe):
        st.subheader("**Datos en tiempo real:**")
        # Divide el DataFrame en dos partes: las primeras 3 filas y las últimas 3
        half = len(dataframe) // 2
        df1 = dataframe.iloc[:half]
        df2 = dataframe.iloc[half:]
        
        # Crea dos columnas
        col1, col2 = st.columns(2)
        
        # Muestra las primeras 3 variables en la primera columna
        with col1:
            for index, row in df1.iterrows():
                st.markdown(
                    f"""
                    <div style="text-align: center; margin: 10px 0;">
                        <div style="font-size: 18px; font-weight: bold;">{row['Variable']}</div>
                        <div style="font-size: 24px; color: blue;">{row['Measurement']}</div>
                        <div style="font-size: 18px; color: balck;">{row['Timestamp']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
        
        # Muestra las últimas 3 variables en la segunda columna
        with col2:
            for index, row in df2.iterrows():
                st.markdown(
                    f"""
                    <div style="text-align: center; margin: 10px 0;">
                        <div style="font-size: 18px; font-weight: bold;">{row['Variable']}</div>
                        <div style="font-size: 24px; color: blue;">{row['Measurement']}</div>
                        <div style="font-size: 18px; color: black;">{row['Timestamp']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )  

    # # Llama a la función para obtener los datos
    stream_data = get_data_from_postgres_stream('stream_data')

    # ##### Twilion ####


    # Muestra los datos como "cajitas" con etiquetas y valores
    display_metrics(stream_data)


##########################################  Datos en Lote  ##################################################################

elif menu_main == "Gráficos históricos":
    # Menú principal
    st.subheader(f"**Datos históricos:**")
    # Crear un menú horizontal utilizando `st.radio`
    menu = st.radio(
        "**Selecciona el tipo de datos:**", 
        ["Lluvia", "Viento", "Temperatura y Presión", "Granizo", "Humedad"], 
        horizontal=True
    )


    # Mostrar contenido basado en la selección
    if menu == "Lluvia":
        st.write("Aquí puedes mostrar los datos relacionados con Lluvia.")
    elif menu == "Viento":
        st.write("Aquí puedes mostrar los datos relacionados con Viento.")
    elif menu == "Temperatura y Presión":
        st.write("Aquí puedes mostrar los datos de Temperatura y Presión.")
    elif menu == "Granizo":
        st.write("Aquí puedes mostrar los datos relacionados con Granizo.")
    elif menu == "Humedad":
        st.write("Aquí puedes mostrar los datos relacionados con Humedad.")



    if menu == "Lluvia":
        # Cargar los datos de lluvia
        rain_columns = ["Rain accumulation (mm)", "Rain duration (s)", "Rain intensity (mm/h)"]
        rain_data = {col: get_data_from_postgres(col, 'rain_data') for col in rain_columns}
            
        # Seleccionar la columna de lluvia
        selected_column_rain = st.selectbox("Selecciona la columna de lluvia a visualizar:", rain_columns)
        grouping_option_rain = st.selectbox("Selecciona el intervalo de agrupamiento para lluvia:", ["Minuto", "Hora", "Día", "Semana", "Mes", "Año"])
        
        
        visualize_data(rain_data[selected_column_rain], selected_column_rain, grouping_option_rain)
        
            

    elif menu == "Viento":
        # Cargar los datos de viento
        wind_columns = [
            "Wind speed maximum (m/s)", 
            "Wind speed minimum (m/s)", 
            "Wind speed average (m/s)", 
            "Wind direction maximum (degrees)", 
            "Wind direction minimum (degrees)", 
            "Wind direction average (degrees)"
        ]
        wind_data = {col: get_data_from_postgres(col, 'wind_data') for col in wind_columns}
        # Seleccionar la columna de viento
        selected_column_wind = st.selectbox("Selecciona la columna de viento a visualizar:", wind_columns)
        grouping_option_wind = st.selectbox("Selecciona el intervalo de agrupamiento para viento:", ["Minuto", "Hora", "Día", "Semana", "Mes", "Año"])
        
        
        # Visualizar datos de viento
        visualize_data(wind_data[selected_column_wind], selected_column_wind, grouping_option_wind)
        
        
    elif menu == "Temperatura y Presión":
        # Cargar los datos de temperatura y presión
        temp_pressure_columns = ["Air pressure (hPa)", "Air temperature (°C)"]
        temp_pressure_data = {col: get_data_from_postgres(col, 'temperature_pressure_data') for col in temp_pressure_columns}
        
        # Seleccionar la columna de temperatura o presión
        selected_column_temp_pressure = st.selectbox("Selecciona la columna de temperatura o presión a visualizar:", temp_pressure_columns)
        grouping_option_temp_pressure = st.selectbox("Selecciona el intervalo de agrupamiento para temperatura y presión:", ["Minuto", "Hora", "Día", "Semana", "Mes", "Año"])
            
        # Visualizar datos de temperatura o presión
        visualize_data(temp_pressure_data[selected_column_temp_pressure], selected_column_temp_pressure, grouping_option_temp_pressure)
        
            
    elif menu == "Granizo":
        # Cargar los datos de granizo
        hail_columns = ["Hail accumulation (hits/cm²)", "Hail duration (s)", "Hail intensity"]
        hail_data = {col: get_data_from_postgres(col, 'hail_data') for col in hail_columns}
        # Seleccionar la columna de granizo
        selected_column_hail = st.selectbox("Selecciona la columna de granizo a visualizar:", hail_columns)
        grouping_option_hail = st.selectbox("Selecciona el intervalo de agrupamiento para granizo:", ["Minuto", "Hora", "Día", "Semana", "Mes", "Año"])
        
            
        # Visualizar datos de granizo
        visualize_data(hail_data[selected_column_hail], selected_column_hail, grouping_option_hail)
        
            

    elif menu == "Humedad":
        # Cargar los datos de humedad
        humidity_columns = ["Relative humidity (%RH)"]  # Agrega las columnas de humedad según tu esquema de base de datos
        humidity_data = {col: get_data_from_postgres(col, 'humidity_data') for col in humidity_columns}
    
        # Seleccionar la columna de humedad
        selected_column_humidity = st.selectbox("Selecciona la columna de humedad a visualizar:", humidity_columns)
        grouping_option_humidity = st.selectbox("Selecciona el intervalo de agrupamiento para humedad:", ["Minuto", "Hora", "Día", "Semana", "Mes", "Año"])
        
        # Visualizar datos de humedad
        visualize_data(humidity_data[selected_column_humidity], selected_column_humidity, grouping_option_humidity)
        
####################################### Pronostico #############################################################
elif menu_main == "Pronostico":
    st.subheader("**Pronóstico del clima en:**")
    pronostico_api()    
# ##################################### Prediccion ############################################################

elif menu_main == "Predicción":
    def cargar_datos(columnas, tabla):
        """
        Función para cargar datos desde PostgreSQL.
        
        Args:
            columnas (list): Lista de nombres de columnas a cargar.
            tabla (str): Nombre de la tabla en la base de datos.
        
        Returns:
            dict: Diccionario con los datos cargados.
        """
        return {col: get_data_from_postgres(col, tabla) for col in columnas}

    def preparar_prediccion(config, datos_cargados):
        """
        Función para preparar datos y generar predicciones basado en una configuración.
        
        Args:
            config (dict): Diccionario con configuraciones de predicción.
            datos_cargados (dict): Datos cargados para la predicción.
        
        Returns:
            DataFrame: Resultado de la predicción procesada.
        """
        column_name = config["column_name"]
        umbrales = config["umbrales"]
        data = datos_cargados[column_name]
        df = data.copy()
        df["Timestamp"] = data['Timestamp']
        return main(df, 'Timestamp', column_name, umbrales)

    # Configuración de variables y tablas
    configuraciones = {
        "Lluvia": {
            "columnas": ["Rain accumulation (mm)", "Rain duration (s)", "Rain intensity (mm/h)"],
            "tabla": "rain_data",
            "column_name": "Rain accumulation (mm)",
            "umbrales": [30, 50],
            "nombre_personalizado": "Predicción de fuertes lluvias"
        },
        "Viento": {
            "columnas": [
                "Wind speed maximum (m/s)", 
                "Wind speed minimum (m/s)", 
                "Wind speed average (m/s)", 
                "Wind direction maximum (degrees)", 
                "Wind direction minimum (degrees)", 
                "Wind direction average (degrees)"
            ],
            "tabla": "wind_data",
            "column_name": "Wind speed average (m/s)",
            "umbrales": [10, 25],
            "nombre_personalizado": "Predicción de fuertes vientos"
        },
        "Temperatura": {
            "columnas": ["Air pressure (hPa)", "Air temperature (°C)"],
            "tabla": "temperature_pressure_data",
            "column_name": "Air temperature (°C)",
            "umbrales": [20, 30],
            "nombre_personalizado": "Predicción de altas temperaturas"
        },
        "Granizo": {
            "columnas": ["Hail accumulation (hits/cm²)", "Hail duration (s)", "Hail intensity"],
            "tabla": "hail_data",
            "column_name": "Hail accumulation (hits/cm²)",
            "umbrales": [5, 20],
            "nombre_personalizado": "Predicción de fuertes caídas de granizo"
        },
        "Humedad": {
            "columnas": ["Relative humidity (%RH)"],
            "tabla": "humidity_data",
            "column_name": "Relative humidity (%RH)",
            "umbrales": [50, 80],
            "nombre_personalizado": "Predicción de alta humedad"
        },
        "Presión del aire": {
            "columnas": ["Air pressure (hPa)", "Air temperature (°C)"],
            "tabla":"temperature_pressure_data",
            "column_name": "Air pressure (hPa)",
            "umbrales": [988, 1025],
            "nombre_personalizado": "Predicción de alta Presión"
        }
    }

    # Mostrar predicciones dinámicamente
    st.subheader("**Predicción de variables:**")
    selected_variable = st.selectbox("Selecciona la predicción a visualizar:", list(configuraciones.keys()))

    if selected_variable:
        config = configuraciones[selected_variable]
        datos_cargados = cargar_datos(config["columnas"], config["tabla"])  # Carga datos para la variable seleccionada
        prediccion = preparar_prediccion(config, datos_cargados)  # Genera predicción para la variable seleccionada
        nombre_personalizado = config["nombre_personalizado"]
        mostrar_prediccion(prediccion, nombre_personalizado)
    else:
        st.warning("No hay predicción seleccionada.")

    st.write("""""")
    st.write("""""")

###################################### Videos ###################################################


elif menu_main == "Videos":
    # Función para convertir imagen a base64
    def image_to_base64(image):
        buffered = BytesIO()
        image.save(buffered, format="JPEG")
        return base64.b64encode(buffered.getvalue()).decode()

    # Título
    st.subheader("**Algunas cámaras de videos en vivo:**")
    st.write("Antenas de los sensores meteorologicos en San Diego, California")
    # URL de las cámaras
    camera_urls = [
        "https://www.hpwren.ucsd.edu/alpha/cameras/#mode=realTime&cams=bi_bi-n-mobo-c&iifi=bi_bi-n-mobo-c",
        "https://www.hpwren.ucsd.edu/alpha/cameras/#mode=realTime&cams=bi_bi-w-axis&iifi=bi_bi-w-axis",
        "https://www.hpwren.ucsd.edu/alpha/cameras/#mode=realTime&cams=bl2_bl-n-mobo-c&iifi=bl2_bl-n-mobo-c",
        "https://www.hpwren.ucsd.edu/alpha/cameras/#mode=realTime&cams=stgo_stgo-n-mobo-c&iifi=stgo_stgo-n-mobo-c",
    ]

    # Rutas de las imágenes locales
    image_files = [
        "imagen/bi-n-mobo-c.jpg",
        "imagen/bi-w-axis.jpg",
        "imagen/bl-n-mobo-c.jpg",
        "imagen/stgo-n-mobo-c.jpg",
    ]

    # Convertir imágenes locales a base64
    images_base64 = [image_to_base64(Image.open(img)) for img in image_files]

    # Crear diseño de dos columnas
    cols = st.columns(2)
    for i, (url, img_base64) in enumerate(zip(camera_urls, images_base64)):
        with cols[i % 2]:
            st.markdown(
                f'<a href="{url}" target="_blank"><img src="data:image/jpeg;base64,{img_base64}" width="300" height="200"></a>',
                unsafe_allow_html=True,
            )



######################################## Subscripcion de Notificaciones de alertas ###################################


elif menu_main == "Notificaciones de alertas":
    # Función para validar el formato del teléfono
    def validar_telefono(phone):
        patron = r'^\+\d{10,15}$'
        return bool(re.match(patron, phone))

    # Función para insertar datos en la base de datos
    def insertar_usuario(name, phone):
        try:
            conn = psycopg2.connect(
                host='',
                database='',
                user='',
                password='',
                port='14004'
            )
            cursor = conn.cursor()
            
            # Verificar si el número ya existe
            check_query = "SELECT COUNT(*) FROM usuario WHERE phone = %s;"
            cursor.execute(check_query, (phone,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                return "El número ya está registrado."
            
            # Insertar nuevo usuario
            insert_query = "INSERT INTO usuario (name, phone) VALUES (%s, %s);"
            cursor.execute(insert_query, (name, phone))
            conn.commit()
            return "Usuario registrado exitosamente y suscripción activada."
        except Exception as e:
            return f"Error al insertar usuario: {e}"
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # Interfaz de usuario con Streamlit
    st.subheader(f"**Suscripción a SMS de Alertas Meteorológicas:**")
    st.write("Ingrese sus datos para recibir notificaciones de alertas meteorológicas.")

    # Formulario para entradas y envío
    with st.form(key="form_suscripcion", clear_on_submit=True):
        nombre = st.text_input("Nombre", max_chars=255)
        telefono = st.text_input("Teléfono (formato +541145408005)")
        submit_button = st.form_submit_button("Suscribirse")

        # Acciones al enviar el formulario
        if submit_button:
            if not nombre or not telefono:
                st.warning("Por favor, complete todos los campos.")
            elif not validar_telefono(telefono):
                st.warning("El número de teléfono no tiene un formato válido. Ejemplo: +541165498405")
            else:
                resultado = insertar_usuario(nombre, telefono)
                if "exitosamente" in resultado:
                    st.success(resultado)
                elif "registrado" in resultado:
                    st.warning(resultado)
                else:
                    st.error(resultado)


###################################### Contacto  #################################################
           

elif menu_main == "Contacto":
    def valida_email(email):
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        # Validar el correo electrónico con la expresión regular
        if re.match(pattern, email):
            return True
        else:
            return False
    # Título de la página
    st.subheader(f"**Formulario de Contacto:**")

    # Crear un formulario de contacto
    with st.form(key="contact_form", clear_on_submit=True):
        name = st.text_input("Nombre completo")
        email = st.text_input("Correo electrónico")
        message = st.text_area("Mensaje")

        submit_button = st.form_submit_button("Enviar")
        if submit_button:
            if name == "":
                
                st.warning("El nombre es obligatorio")
                
            elif email == "":
                
                st.warning("El email es obligatorio")  
                
            elif not valida_email(email):
                st.warning("El email no es valido")
                
            elif message == "":
                st.warning("Mensaje es obligatorio")     
            
    
            else:
                try:
                    # Detalles del correo
                    sender_email = email  # Email proporcionado por el usuario
                    receiver_email = ""  # Dirección donde recibirás el mensaje
                    smtp_server = "smtp.gmail.com"
                    smtp_port = 587
                    sender_password = ""  # Contraseña o App Password de tu cuenta configurada

                    # Cuerpo del correo
                    subject = f"Nuevo mensaje de {name}"
                    body = f"Nombre: {name}\nCorreo: {email}\n\nMensaje:\n{message}"
                    email_message = f"Subject: {subject}\n\n{body}"

                    # Enviar el correo
                    with smtplib.SMTP(smtp_server, smtp_port) as server:
                        server.starttls()
                        server.login(receiver_email, sender_password)
                        server.sendmail(receiver_email, receiver_email, email_message)

                    st.success("¡Gracias por tu mensaje! Nos pondremos en contacto pronto.")
                    
                    
                except Exception as e:
                    st.error(f"Error al enviar el mensaje: {e}")
                
             
######################################## Redes sociales  ##############################################
    # URLs de íconos (puedes usar imágenes locales si prefieres)
    twitter_icon = "https://cdn-icons-png.flaticon.com/512/733/733579.png"
    facebook_icon = "https://cdn-icons-png.flaticon.com/512/733/733547.png"
    instagram_icon = "https://cdn-icons-png.flaticon.com/512/2111/2111463.png"
    linkedin_icon = "https://cdn-icons-png.flaticon.com/512/733/733561.png"
    # Sección de enlaces a redes sociales
    st.markdown("#### Redes sociales:")

    # Crear columnas para los enlaces
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.image(twitter_icon, width=40)
        st.markdown("[Twitter](https://www.x.com/)")

    with col2:
        st.image(facebook_icon, width=40)
        st.markdown("[Facebook](https://www.facebook.com/)")

    with col3:
        st.image(instagram_icon, width=40)
        st.markdown("[Instagram](https://www.instagram.com/)")

    with col4:
        st.image(linkedin_icon, width=40)
        st.markdown("[LinkedIn](https://www.linkedin.com/)")
        
#################################### Envio de alertas twilion ################################################
 # ##### Twilion ####
  # # Llama a la función para obtener los datos

elif menu_main == "":
    stream_data = get_data_from_postgres_stream('stream_data')
    main_send_text(stream_data)