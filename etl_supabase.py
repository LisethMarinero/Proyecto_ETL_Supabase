# etl_supabase.py
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# Leer credenciales desde variables de entorno (se las pasará GitHub Actions como secrets)
USER = os.getenv("postgres.gkzvbidocktfkwhvngpg")
PASSWORD = os.getenv("Hipopotamo123456")
HOST = os.getenv("aws-1-us-east-2.pooler.supabase.com")
PORT = os.getenv("6543")
DBNAME = os.getenv("postgress")

# Ruta local del CSV (si lo subes al repo) o puedes descargar desde URL/Azure/GDrive si prefieres.
# Ejemplo: si tus CSV están en el repo en carpeta /data, pon "data/miarchivo.csv"
CARPETA = os.getenv("CSV_FOLDER", ".")  # por defecto la raíz del repo
# Si tienes archivos específicos, puedes listarlos o leer uno en particular.
# Aquí procesamos todos los .csv en CARPETA
def crear_engine():
    conexion_str = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
    return create_engine(conexion_str, connect_args={'sslmode': 'require'})

def procesar_csv_a_postgres(engine):
    archivos = [f for f in os.listdir(CARPETA) if f.endswith(".csv")]
    if not archivos:
        print("No se encontraron archivos CSV en la carpeta especificada.")
        return

    print(f"Archivos encontrados: {archivos}")
    for archivo in archivos:
        ruta_archivo = os.path.join(CARPETA, archivo)
        print(f"\nProcesando archivo: {ruta_archivo}")

        try:
            df = pd.read_csv(ruta_archivo, encoding='utf-8', on_bad_lines='skip')
            df.dropna(how="all", inplace=True)
            df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
            # Ejemplo: agrega columna con fecha de carga
            df["fecha_carga"] = datetime.utcnow()

            nombre_tabla = os.path.splitext(archivo)[0].lower().replace(" ", "_")
            # Usamos append para no borrar tablas existentes; puedes cambiar a 'replace' si prefieres.
            df.to_sql(nombre_tabla, engine, if_exists="append", index=False)
            print(f"Datos cargados correctamente en la tabla: {nombre_tabla}")

        except pd.errors.EmptyDataError:
            print(f"El archivo {archivo} está vacío o corrupto.")
        except SQLAlchemyError as e:
            print(f"Error al subir datos a Supabase para {archivo}: {e}")
        except Exception as e:
            print(f"Ocurrió un error inesperado con {archivo}: {e}")

    print("\nETL completado correctamente.")

if __name__ == "__main__":
    print("Iniciando ETL...")
    engine = crear_engine()
    procesar_csv_a_postgres(engine)
