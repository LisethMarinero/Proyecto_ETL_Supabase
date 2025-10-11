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

load_dotenv()

# --- CONFIG DB desde ENV ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "6543")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise SystemExit("Faltan variables de base de datos en .env")

conexion_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conexion_str, connect_args={"sslmode": "require"})

def descargar_datos(archivo_salida="era5_land_daily.nc", variables=None, year=None, month=None, times=["00:00"], area=None):
    if variables is None:
        variables = ["2m_temperature", "total_precipitation"]

    c = cdsapi.Client()
    year = year or datetime.utcnow().year
    month = month or datetime.utcnow().month

    dias_mes = [f"{d:02d}" for d in range(1, calendar.monthrange(year, month)[1] + 1)]

    request = {
        "variable": variables,
        "year": str(year),
        "month": f"{month:02d}",
        "day": dias_mes,
        "time": times,
        "format": "netcdf",
    }

    if area:
        # area = [N, W, S, E] ejemplo: El Salvador aproximado [14.5, -90.0, 13.0, -88.0]
        request["area"] = area

    print("Descargando datos con request:", {k: request[k] for k in ["variable","year","month","day","time","format", "area"] if k in request})
    c.retrieve("reanalysis-era5-land-timeseries", request, archivo_salida)
    return archivo_salida

def procesar_y_cargar(archivo, tabla="era5_land_data", if_exists="append"):
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

if __name__ == "__main__":
    archivo = descargar_datos()
    procesar_y_cargar(archivo)
    print("ETL finalizado.")

