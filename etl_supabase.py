import os
import pandas as clspd
import cdsapi
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from datetime import datetime

# --- CARGAR VARIABLES DE ENTORNO ---
load_dotenv()

DB_USER = os.getenv("postgres.gkzvbidocktfkwhvngpg")
DB_PASSWORD = os.getenv("Hipopotamo123456")
DB_HOST = os.getenv("aws-1-us-east-2.pooler.supabase.com")
DB_PORT = os.getenv("6543")
DB_NAME = os.getenv("postgres")

# --- CONFIGURACIÓN DE CONEXIÓN A SUPABASE ---
conexion_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conexion_str, connect_args={'sslmode': 'require'})

# --- DESCARGA DE DATOS DE COPERNICUS ERA5 LAND ---
def descargar_datos():
    print("Descargando datos desde Copernicus CDS...")
    c = cdsapi.Client()

    archivo_salida = "era5_land_daily.nc"

    c.retrieve(
        "reanalysis-era5-land-timeseries",
        {
            "variable": [
                "2m_temperature",
                "total_precipitation",
                "surface_pressure",
                "surface_solar_radiation_downwards",
            ],
            "year": datetime.now().year,
            "month": datetime.now().month,
            "day": [f"{d:02d}" for d in range(1, 32)],
            "time": ["00:00"],
            "format": "netcdf",
        },
        archivo_salida
    )
    print(f"Datos descargados en {archivo_salida}")
    return archivo_salida

# --- PROCESAR Y SUBIR A SUPABASE ---
def procesar_y_cargar(archivo):
    try:
        import xarray as xr
        print("Procesando archivo NetCDF...")
        ds = xr.open_dataset(archivo)
        df = ds.to_dataframe().reset_index()

        # Limpiar nombres de columnas
        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]

        # Guardar en Supabase
        nombre_tabla = "era5_land_data"
        df.to_sql(nombre_tabla, engine, if_exists="replace", index=False)
        print(f"✅ Datos cargados correctamente en la tabla '{nombre_tabla}'.")

    except SQLAlchemyError as e:
        print(f"❌ Error al subir datos a Supabase: {e}")
    except Exception as e:
        print(f"❌ Error procesando datos: {e}")

# --- FLUJO PRINCIPAL ---
if __name__ == "__main__":
    archivo = descargar_datos()
    procesar_y_cargar(archivo)
    print("🌍 ETL completado exitosamente.")