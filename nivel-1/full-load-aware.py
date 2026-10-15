import os
import io
import requests
from time import time
import pyarrow.parquet as pq
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import boto3
PARQUET_URL = os.getenv("PARQUET_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
BRONZE_OBJECT_PATH = os.getenv("BRONZE_OBJECT_PATH", "yellow_taxi/yellow_tripdata_2024-01.parquet")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "ny_taxi")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "changeme1234")
TABLE_NAME = os.getenv("TABLE_NAME", "yellow_taxi_data")
missing = []
if not MINIO_ACCESS_KEY: missing.append("MINIO_ROOT_USER")
if not MINIO_SECRET_KEY: missing.append("MINIO_ROOT_PASSWORD")
if not POSTGRES_PASSWORD: missing.append("POSTGRES_PASSWORD")
if missing: raise RuntimeError(f"❌ Variables de entorno faltantes: {', '.join(missing)}")
s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)
existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if BRONZE_BUCKET not in existing_buckets:
    s3.create_bucket(Bucket=BRONZE_BUCKET)
    print(f"✔ Bucket '{BRONZE_BUCKET}' creado")
else:
    print(f"✔ Bucket '{BRONZE_BUCKET}' ya existe")
print("⬇ Descargando Parquet desde fuente...")
response = requests.get(PARQUET_URL)
response.raise_for_status()
parquet_bytes = io.BytesIO(response.content)
print("✔ Descarga completada")
s3.put_object(Bucket=BRONZE_BUCKET, Key=BRONZE_OBJECT_PATH, Body=parquet_bytes.getvalue())
print(f"✔ Archivo almacenado en Bronze → {BRONZE_BUCKET}/{BRONZE_OBJECT_PATH}")
obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=BRONZE_OBJECT_PATH)
pqfile = pq.ParquetFile(io.BytesIO(obj["Body"].read()))
num_groups = pqfile.num_row_groups
print(f"✔ Parquet con {num_groups} row groups")
postgres_conn_str = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(postgres_conn_str)
df_schema = pqfile.read_row_group(0).slice(0, 0).to_pandas()
df_schema.columns = [c.lower() for c in df_schema.columns]
df_schema.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
print("✔ Tabla creada (solo esquema)")
conn = psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
cursor = conn.cursor()
for i in range(num_groups):
    t_start = time()
    print(f"→ Procesando row group {i + 1}/{num_groups}")
    table = pqfile.read_row_group(i)
    df = table.to_pandas()
    for col in df.columns:
        if df[col].dtype == "float64":
            if ((df[col] % 1 == 0) | df[col].isnull()).all():
                df[col] = df[col].astype("Int64")
    for col in df.columns:
        if "datetime" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce")
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cursor.copy_expert(f"COPY {TABLE_NAME} FROM STDIN WITH CSV", buffer)
    conn.commit()
    t_end = time()
    print(f"✔ Row group {i + 1} cargado en {t_end - t_start:.2f}s")
cursor.close()
conn.close()
print("✔ Full Load compose-aware finalizado correctamente")