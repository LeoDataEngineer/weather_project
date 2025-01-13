import requests
import streamlit as st
import plotly.graph_objs as go

# Configuración de la API
API_KEY = ""  # Reemplaza esto con tu clave de API
BASE_URL = "http://api.weatherapi.com/v1/forecast.json"
CITY = "San Diego"

# Construye la URL de la solicitud
def build_url(city, api_key, days=3):
    return f"{BASE_URL}?key={api_key}&q={city}&days={days}&aqi=no&alerts=no"

# Función para obtener los datos del clima
def get_weather_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Función para crear el gráfico
def create_weather_chart(weather_data, selected_date, variable):
    forecast_days = weather_data.get("forecast", {}).get("forecastday", [])
    selected_data = next(day for day in forecast_days if day["date"] == selected_date)
    hourly_data = selected_data["hour"]

    hours = [hour["time"] for hour in hourly_data]

    # Obtener los valores de la variable seleccionada
    variable_map = {
        "Temperatura": ("temp_c", "Temperatura (°C)"),
        "Lluvia": ("precip_mm", "Precipitación (mm)"),
        "Presión": ("pressure_mb", "Presión (mb)"),
        "Humedad": ("humidity", "Humedad (%)"),
        "Viento": ("wind_kph", "Viento (kph)"),
        "Nublado": ("cloud", "Nublado (%)"),
        "Probabilidad de lluvia": ("chance_of_rain", "Probabilidad de lluvia (%)"),
        "Probabilidad de nieve": ("chance_of_snow", "Probabilidad de nieve (%)")
    }

    variable_key, yaxis_title = variable_map[variable]
    if variable == "Presión":
        values = [round(hour.get(variable_key, 0)) for hour in hourly_data]  # Redondear los valores de presión
    else:
        values = [hour.get(variable_key, 0) for hour in hourly_data]

    # Crear el gráfico de barras con Plotly
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=hours,
            y=values,
            text=[f"{v}" for v in values],
            textposition="outside",
            name=variable,
            marker=dict(color='blue'),
            textfont=dict(size=10, color='black', family='Arial, sans-serif')
        )
    )
    fig.update_layout(
        title=f"{variable} horaria para {selected_date}",
        xaxis_title="Hora",
        yaxis_title=yaxis_title,
        template="plotly_white",
        uniformtext_minsize=8,
        uniformtext_mode='show'
    )

    return fig

# Función principal
def pronostico_api():
    # Construir la URL
    url = build_url(CITY, API_KEY)

    try:
        # Obtener los datos del clima
        weather_data = get_weather_data(url)

        # Extraer las fechas disponibles
        forecast_days = weather_data.get("forecast", {}).get("forecastday", [])
        dates = [day["date"] for day in forecast_days]  # Lista de fechas disponibles

        # Interfaz de usuario con Streamlit
        # st.subheader("**Pronóstico del clima en:**")
        selected_date = st.selectbox("Selecciona una fecha:", dates)
        

        # Crear un segundo selectbox para elegir la variable a mostrar
        variable = st.selectbox(
            "Selecciona la variable:",
            ["Temperatura", "Lluvia", "Presión", "Humedad", "Viento", "Nublado", "Probabilidad de lluvia", "Probabilidad de nieve"]
        )

        # Crear un contenedor vacío para el gráfico
        graph_container = st.empty()

        # Actualizar el gráfico cuando se seleccione una variable
        with graph_container:
            fig = create_weather_chart(weather_data, selected_date, variable)
            st.plotly_chart(fig)

    except requests.exceptions.RequestException as e:
        st.error(f"Error al obtener datos del clima: {e}")



