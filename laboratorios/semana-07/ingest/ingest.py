"""
Extract + Load de NYC Yellow Taxi hacia Snowflake (capa RAW).

Para cada mes del rango START_MONTH..END_MONTH:
  1. Consulta (HEAD) el archivo en el CDN de la TLC. Si todavía no está
     publicado (403/404) lo omite con un aviso; se cargará en una corrida futura.
  2. Si el archivo ya se cargó con el mismo ETag (misma versión), lo omite.
  3. Descarga el parquet (con caché local), lo sube a un stage interno (PUT) y,
     dentro de UNA transacción, borra las filas previas de ese archivo y hace
     COPY INTO. Así, re-ejecutar la tubería nunca duplica datos.
  4. Registra la carga en RAW.LOAD_AUDIT.

Metadata agregada a cada fila: _SOURCE_FILE, _SOURCE_PERIOD, _LOADED_AT, _INGESTION_ID.
"""
import argparse
import logging
import os
import sys
import uuid
from pathlib import Path

import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
FILE_TEMPLATE = "yellow_tripdata_{period}.parquet"

RAW_SCHEMA = "RAW"
RAW_TABLE = "YELLOW_TRIPDATA"
STAGE = "TLC_STAGE"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")

# Columnas de la fuente -> tipo en RAW. Se conservan los nombres originales
# (VendorID, RatecodeID, Airport_fee, ...) y los tipos del parquet.
# Las claves del parquet distinguen mayúsculas; se prueban variantes para
# tolerar cambios de nombre entre meses (p. ej. Airport_fee / airport_fee).
SOURCE_COLUMNS = [
    ("VENDORID", "NUMBER(38,0)", ["VendorID"]),
    ("TPEP_PICKUP_DATETIME", "TIMESTAMP_NTZ", ["tpep_pickup_datetime"]),
    ("TPEP_DROPOFF_DATETIME", "TIMESTAMP_NTZ", ["tpep_dropoff_datetime"]),
    ("PASSENGER_COUNT", "NUMBER(38,0)", ["passenger_count"]),
    ("TRIP_DISTANCE", "FLOAT", ["trip_distance"]),
    ("RATECODEID", "NUMBER(38,0)", ["RatecodeID"]),
    ("STORE_AND_FWD_FLAG", "VARCHAR", ["store_and_fwd_flag"]),
    ("PULOCATIONID", "NUMBER(38,0)", ["PULocationID"]),
    ("DOLOCATIONID", "NUMBER(38,0)", ["DOLocationID"]),
    ("PAYMENT_TYPE", "NUMBER(38,0)", ["payment_type"]),
    ("FARE_AMOUNT", "FLOAT", ["fare_amount"]),
    ("EXTRA", "FLOAT", ["extra"]),
    ("MTA_TAX", "FLOAT", ["mta_tax"]),
    ("TIP_AMOUNT", "FLOAT", ["tip_amount"]),
    ("TOLLS_AMOUNT", "FLOAT", ["tolls_amount"]),
    ("IMPROVEMENT_SURCHARGE", "FLOAT", ["improvement_surcharge"]),
    ("TOTAL_AMOUNT", "FLOAT", ["total_amount"]),
    ("CONGESTION_SURCHARGE", "FLOAT", ["congestion_surcharge"]),
    ("AIRPORT_FEE", "FLOAT", ["Airport_fee", "airport_fee"]),
    ("CBD_CONGESTION_FEE", "FLOAT", ["cbd_congestion_fee"]),
    # Columna nueva desde 2026; en meses anteriores queda NULL.
    ("REQUEST_SOURCE", "VARCHAR", ["request_source"]),
]
METADATA_COLUMNS = [
    ("_SOURCE_FILE", "VARCHAR"),
    ("_SOURCE_PERIOD", "DATE"),
    ("_LOADED_AT", "TIMESTAMP_LTZ"),
    ("_INGESTION_ID", "VARCHAR"),
]


def month_range(start: str, end: str):
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    while (y, m) <= (ey, em):
        yield f"{y:04d}-{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def connect():
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    with open(key_path, "rb") as fh:
        pkey = serialization.load_pem_private_key(fh.read(), password=None)
    pkb = pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkb,
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=RAW_SCHEMA,
        session_parameters={"QUERY_TAG": "lab07_ingest", "TIMEZONE": "America/Guayaquil"},
    )


def bootstrap(cur):
    db = os.environ["SNOWFLAKE_DATABASE"]
    cols = ",\n  ".join(f"{n} {t}" for n, t, _ in SOURCE_COLUMNS)
    meta = ",\n  ".join(f"{n} {t}" for n, t in METADATA_COLUMNS)
    statements = [
        f"create schema if not exists {db}.{RAW_SCHEMA}",
        f"use schema {db}.{RAW_SCHEMA}",
        # USE_LOGICAL_TYPE: lee timestamp[us] del parquet como TIMESTAMP y no como entero.
        "create file format if not exists PARQUET_FF type = parquet use_logical_type = true",
        f"create stage if not exists {STAGE} file_format = PARQUET_FF",
        f"create table if not exists {RAW_TABLE} (\n  {cols},\n  {meta}\n)",
        """create table if not exists LOAD_AUDIT (
             source_file varchar, source_period date, etag varchar, rows_loaded number,
             ingestion_id varchar, loaded_at timestamp_ltz, status varchar)""",
    ]
    for s in statements:
        cur.execute(s)
    # Evolución de esquema: si la fuente agrega columnas nuevas en el futuro,
    # se agregan aquí sin recrear la tabla.
    cur.execute(f"select column_name from information_schema.columns "
                f"where table_schema = '{RAW_SCHEMA}' and table_name = '{RAW_TABLE}'")
    existing = {r[0] for r in cur.fetchall()}
    for name, typ, _ in SOURCE_COLUMNS:
        if name not in existing:
            log.info("Agregando columna nueva %s a RAW.%s", name, RAW_TABLE)
            cur.execute(f"alter table {RAW_TABLE} add column {name} {typ}")


def remote_version(url):
    """Devuelve el ETag del archivo o None si aún no está publicado."""
    r = requests.head(url, timeout=30, allow_redirects=True)
    if r.status_code in (403, 404):
        return None
    r.raise_for_status()
    return (r.headers.get("ETag") or r.headers.get("Last-Modified") or "").strip('"')


def download(url, dest: Path, etag: str):
    marker = dest.with_suffix(".etag")
    if dest.exists() and marker.exists() and marker.read_text() == etag:
        log.info("  usando caché local %s", dest.name)
        return
    tmp = dest.with_suffix(".part")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                fh.write(chunk)
    tmp.replace(dest)
    marker.write_text(etag)


def already_loaded(cur, fname, etag):
    cur.execute(
        "select etag from LOAD_AUDIT where source_file = %s and status = 'LOADED' "
        "order by loaded_at desc limit 1", (fname,))
    row = cur.fetchone()
    return row is not None and row[0] == etag


def load_month(cur, period, local_file: Path, etag, ingestion_id):
    fname = local_file.name
    stage_path = f"@{STAGE}/{period}"
    cur.execute(f"put 'file://{local_file.as_posix()}' {stage_path} "
                f"auto_compress = false overwrite = true parallel = 8")

    def pick(keys):
        exprs = [f'$1:"{k}"' for k in keys]
        return exprs[0] if len(exprs) == 1 else f"coalesce({', '.join(exprs)})"

    target_cols = [n for n, _, _ in SOURCE_COLUMNS] + [n for n, _ in METADATA_COLUMNS]
    select_exprs = [f"{pick(keys)}::{typ}" for _, typ, keys in SOURCE_COLUMNS] + [
        "metadata$filename",
        f"'{period}-01'::date",
        "current_timestamp()",
        f"'{ingestion_id}'",
    ]
    copy_sql = f"""
        copy into {RAW_TABLE} ({", ".join(target_cols)})
        from (select {", ".join(select_exprs)} from {stage_path}/{fname})
        file_format = (format_name = 'PARQUET_FF')
        force = true
        on_error = abort_statement
    """
    # Borrado + carga atómicos: si algo falla, no queda el mes a medias.
    cur.execute("begin")
    try:
        cur.execute(f"delete from {RAW_TABLE} where _source_file like %s", (f"%{fname}",))
        deleted = cur.rowcount
        cur.execute(copy_sql)
        rows = sum(r[3] for r in cur.fetchall())  # rows_loaded
        cur.execute(
            "insert into LOAD_AUDIT select %s, %s, %s, %s, %s, current_timestamp(), 'LOADED'",
            (fname, f"{period}-01", etag, rows, ingestion_id))
        cur.execute("commit")
    except Exception:
        cur.execute("rollback")
        raise
    log.info("  %s: %s filas cargadas (%s filas previas reemplazadas)", fname, f"{rows:,}", f"{deleted:,}")
    cur.execute(f"remove {stage_path}/{fname}")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=os.environ.get("START_MONTH", "2025-01"))
    ap.add_argument("--end", default=os.environ.get("END_MONTH", "2026-08"))
    ap.add_argument("--force", action="store_true", help="recargar aunque el ETag no haya cambiado")
    args = ap.parse_args()

    data_dir = Path(os.environ.get("DATA_DIR", "./data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    ingestion_id = str(uuid.uuid4())
    log.info("Ingesta %s | meses %s..%s", ingestion_id, args.start, args.end)

    conn = connect()
    cur = conn.cursor()
    bootstrap(cur)

    summary = {"loaded": [], "skipped_unchanged": [], "not_published": []}
    for period in month_range(args.start, args.end):
        fname = FILE_TEMPLATE.format(period=period)
        url = f"{BASE_URL}/{fname}"
        etag = remote_version(url)
        if etag is None:
            log.warning("%s aún no está publicado por la TLC; se omite.", fname)
            summary["not_published"].append(period)
            continue
        if not args.force and already_loaded(cur, fname, etag):
            log.info("%s ya cargado (misma versión); se omite.", fname)
            summary["skipped_unchanged"].append(period)
            continue
        log.info("Procesando %s", fname)
        local = data_dir / fname
        download(url, local, etag)
        load_month(cur, period, local, etag, ingestion_id)
        summary["loaded"].append(period)

    cur.execute(f"select count(*), count(distinct _source_file) from {RAW_TABLE}")
    total, files = cur.fetchone()
    log.info("Resumen: %s", summary)
    log.info("RAW.%s: %s filas en %s archivos", RAW_TABLE, f"{total:,}", files)
    conn.close()

    if not summary["loaded"] and not summary["skipped_unchanged"]:
        log.error("No se cargó ningún archivo.")
        sys.exit(1)


if __name__ == "__main__":
    main()
