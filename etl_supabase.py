# etl_supabase.py
import os
import calendar
from datetime import datetime
import cdsapi
import xarray as xr
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# --- CARGAR VARIABLES DE ENTORNO ---
load_dotenv()

# --- CONFIG DB desde ENV ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "6543")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise SystemExit("Faltan variables de base de datos en .env")

# --- CONEXIÓN A SUPABASE ---
conexion_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conexion_str, connect_args={"sslmode": "require"})

# --- VARIABLES Y TABLAS ---
variables_tablas = {
    "2m_temperature": "tabla1",
    "total_precipitation": "tabla2",
    "surface_pressure": "tabla3",
    "surface_solar_radiation_downwards": "tabla4",
    "soil_temperature_level_1": "tabla5",
    "soil_temperature_level_2": "tabla6",
    "snow_cover": "tabla7",
    "skin_temperature": "tabla8"
}

# --- FUNCIONES ---
def obtener_ultima_fecha(tabla):
    """Obtiene la última fecha registrada en la tabla para no duplicar datos"""
    try:
        query = f"SELECT MAX(time) AS ultima_fecha FROM {tabla};"
        ultima_fecha = pd.read_sql(query, engine).iloc[0]["ultima_fecha"]
        if ultima_fecha is None:
            ultima_fecha = datetime.utcnow() - timedelta(days=1)
        return ultima_fecha
    except Exception as e:
        print(f"No se pudo obtener la última fecha de {tabla}: {e}")
        return datetime.utcnow() - timedelta(days=1)

def descargar_datos(variable, start_date, end_date, archivo_salida, area=None):
    """Descarga datos de ERA5-Land usando cdsapi"""
    c = cdsapi.Client()
    year = year or datetime.utcnow().year
    month = month or datetime.utcnow().month

    dias_mes = [f"{d:02d}" for d in range(1, calendar.monthrange(year, month)[1] + 1)]

    request = {
        "variable": [variable],
        "year": [str(y) for y in years],
        "month": [f"{m:02d}" for m in months],
        "day": dias_mes,
        "time": times,
        "format": "netcdf",
    }

    if area:
        # area = [N, W, S, E] ejemplo: El Salvador aproximado [14.5, -90.0, 13.0, -88.0]
        request["area"] = area

    print(f"Descargando {variable} desde {start_date.date()} hasta {end_date.date()}...")
    c.retrieve("reanalysis-era5-land-timeseries", request, archivo_salida)
    return archivo_salida

def procesar_y_actualizar(archivo, tabla):
    """Procesa NetCDF y actualiza la tabla en Supabase"""
    try:
        print("Abriendo NetCDF con xarray...")
        ds = xr.open_dataset(archivo)
        df = ds.to_dataframe().reset_index()
        df.columns = [str(c).lower().strip().replace(" ", "_") for c in df.columns]
        df["carga_timestamp"] = datetime.utcnow()
        print(f"Registros a subir: {len(df)}")

        df.to_sql(tabla, engine, if_exists=if_exists, index=False)
        print("Carga finalizada ✅")
    except SQLAlchemyError as e:
        print("Error SQLAlchemy:", e)
    except Exception as e:
        print("Error general:", e)

# --- FLUJO PRINCIPAL ---
if __name__ == "__main__":
    archivo = descargar_datos()
    procesar_y_cargar(archivo)
    print("ETL finalizado.")

