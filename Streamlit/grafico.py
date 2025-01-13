import numpy as np
import streamlit as st
import pandas as pd
import plotly.graph_objs as go


def visualize_data(df, column_name, grouping_option):
    # Convertir 'Timestamp' a datetime y preparar el DataFrame
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.drop_duplicates(subset=['Timestamp'])
    df = df.sort_values(by='Timestamp')
    df.set_index('Timestamp', inplace=True)

    # Mapeo de agrupamiento
    resample_dict = {
        "Minuto": '1T',
        "Hora": '1H',
        "Día": '1D',
        "Semana": '1W',
        "Mes": '1M',
        "Año": '1Y'
    }

    # Agrupar datos
    df_resampled = df.resample(resample_dict[grouping_option]).mean().dropna()
    df_resampled[column_name] = df_resampled[column_name].round(2)  # Redondear valores
    promedio_general = df_resampled[column_name].mean()

    # Obtener la última fecha y hora
    ultima_fecha = df.index.max()
    interval_mapping = {
        "Minuto": ultima_fecha.floor('min'),
        "Hora": ultima_fecha.floor('h'),
        "Día": ultima_fecha.floor('D'),
        "Semana": ultima_fecha - pd.to_timedelta(ultima_fecha.weekday(), unit='D'),
        "Mes": ultima_fecha.replace(day=1),
        "Año": ultima_fecha.replace(month=1, day=1)
    }
    inicio_intervalo = interval_mapping[grouping_option]
    datos_ultimo_intervalo = df[inicio_intervalo:ultima_fecha]
    promedio_ultimo_intervalo = datos_ultimo_intervalo[column_name].mean() if not datos_ultimo_intervalo.empty else None

    # Mostrar resultados
    if promedio_ultimo_intervalo is not None:
        st.write(f"**Promedio de {column_name} en el último {grouping_option.lower()} cargado: {promedio_ultimo_intervalo:.2f}**")
    else:
        st.write(f"**Promedio de {column_name} en el último {grouping_option.lower()} cargado: Sin datos suficientes**")

    st.write(f"**Promedio general de {column_name} por {grouping_option.lower()}: {promedio_general:.2f}**")
    
     # Obtener la última fecha de de la columna created_at 
    ultima_fecha_created_at = df['created_at'].max()
    st.write(f"**Datos actualizados hasta: {ultima_fecha_created_at.strftime('%Y-%m-%d %H:%M')}**")
  

    # Calcular línea de tendencia (regresión lineal)
    x = np.arange(len(df_resampled))  # Eje x como números enteros
    y = df_resampled[column_name].values  # Valores de la columna seleccionada
    coef = np.polyfit(x, y, 1)  # Coeficientes de la regresión lineal
    trend_line = np.polyval(coef, x)  # Valores de la línea de tendencia

    # Crear gráfico interactivo con Plotly
    fig = go.Figure()

    # Agregar los datos originales
    fig.add_trace(go.Scatter(
        x=df_resampled.index,
        y=df_resampled[column_name],
        mode='lines+markers',
        name=f"{column_name} por {grouping_option.lower()}",
        line=dict(color='blue')  # Color azul para los datos originales
    ))

    # Agregar la línea de tendencia
    fig.add_trace(go.Scatter(
        x=df_resampled.index,
        y=trend_line,
        mode='lines',
        name='Línea de tendencia',
        line=dict(color='red', dash='dash')  # Línea roja discontinua para la tendencia
    ))
    
   
    
    fig.update_layout(
    title=dict(
        text=f"Visualización de {column_name} agrupado por {grouping_option}",
        x=0,  # Centrado
        font=dict(
            size=16,  # Tamaño de la fuente
            family="sans-serif",  # Familia tipográfica (opcional)
            color="black"  # Color de la fuente (opcional)
        )
    ),
    xaxis_title="Fecha",
    yaxis_title=column_name,
    template="plotly_white",
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5
    ),
     yaxis=dict(tickformat=".2f")  # Formato del eje Y
)

    # Mostrar el gráfico en Streamlit
    st.plotly_chart(fig)