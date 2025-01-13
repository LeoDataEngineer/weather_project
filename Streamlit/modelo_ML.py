import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

def clasificar_variable(valor, umbrales):
    """Clasifica un valor basado en los umbrales proporcionados."""
    if valor <= umbrales[0]:
        return 0  # Baja probabilidad
    elif valor <= umbrales[1]:
        return 1  # Media probabilidad
    else:
        return 2  # Alta probabilidad

def preprocesar_datos(df, fecha_col, variable_col, umbrales):
    """Preprocesa los datos eliminando filas con valores nulos, filtrando los últimos 30 días y creando variables adicionales."""
    df = df.copy()
    # Eliminar filas con valores nulos en cualquier columna
    df = df.dropna(subset=[variable_col])

    # Convertir la columna de fecha a tipo datetime
    df[fecha_col] = pd.to_datetime(df[fecha_col])

    # Filtrar los datos para los últimos 30 días
    hoy = df[fecha_col].max()
    fecha_30_dias = hoy - pd.Timedelta(days=30)
    df = df[df[fecha_col] >= fecha_30_dias]

    # Crear la variable objetivo usando la función generalizada
    df['prediccion'] = df[variable_col].apply(lambda x: clasificar_variable(x, umbrales))

    # Extraer características del Timestamp
    df['day_of_week'] = df[fecha_col].dt.dayofweek
    df['day'] = df[fecha_col].dt.day
    df['month'] = df[fecha_col].dt.month

    return df

def entrenar_modelo(X, y):
    """Entrena el modelo y devuelve las predicciones y las etiquetas verdaderas."""
    # Dividir los datos en conjunto de entrenamiento y prueba (60% entrenamiento, 40% prueba)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42)

    # Entrenar el modelo
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Hacer predicciones en el conjunto de prueba
    y_pred = model.predict(X_test)

    return y_pred, y_test

def evaluar_modelo(y_test, y_pred):
    """Evalúa el modelo y devuelve la precisión y el informe de clasificación."""
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    return accuracy, report

def main(df, fecha_col, variable_col, umbrales):
    
    df = preprocesar_datos(df, fecha_col, variable_col, umbrales)
    
    # Seleccionar características y variable objetivo
    X = df[['day_of_week', 'day', 'month', variable_col]]
    y = df['prediccion']

    # Entrenar el modelo
    y_pred, y_test = entrenar_modelo(X, y)

    # Mapeo de clases a descripciones
    clases_map = {0: 'Baja Probabilidad', 1: 'Media Probabilidad', 2: 'Alta Probabilidad'}

    # Calcular el promedio de las predicciones
    promedio_predicciones = np.mean(y_pred)

    # Mapear el promedio de las predicciones a su descripción
    promedio_mapeado = round(promedio_predicciones)  # Redondear el promedio al entero más cercano
    descripcion_promedio = clases_map.get(promedio_mapeado, "Sin Clasificación")  # Obtener descripción

    return descripcion_promedio

  


