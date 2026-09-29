# Laboratorio Integrador I — Tubería ELT NYC Yellow Taxi

Tubería ELT reproducible que ingiere los viajes de NYC Yellow Taxi (enero 2025 – agosto 2026),
los carga en Snowflake y los transforma con dbt en una arquitectura **Bronze → Silver → Gold**.

```
TLC (parquet mensuales, CDN)
   │  ingest/ingest.py   (Extract + Load, orquestado por Kestra)
   ▼
Snowflake  NYC_TAXI.RAW.YELLOW_TRIPDATA      copia fiel + metadata de carga
   │  dbt build          (Transform + tests)
   ▼
BRONZE  brz_yellow_tripdata                  vista, columnas originales + linaje
SILVER  slv_yellow_trips                     limpio, tipado, deduplicado
        slv_yellow_trips_rejected            cuarentena con motivo de rechazo
GOLD    fct_trips + dim_date, dim_time, dim_location,
        dim_vendor, dim_payment_type, dim_rate_code   (esquema estrella)
```

## Estructura

| Ruta | Contenido |
|---|---|
| `snowflake/00_setup.sql` | Setup único (ACCOUNTADMIN): rol, warehouse XS, base `NYC_TAXI`, usuario de servicio con llave RSA |
| `docker-compose.yml`, `Dockerfile` | Kestra + Postgres; imagen de Kestra con dbt-snowflake y el conector de Python |
| `kestra/flows/main_usfq.lab07_nyc_taxi_elt.yml` | Flow (nombre `<tenant>_<namespace>_<id>.yml`, formato que exige la sincronización de Kestra): `ingest` → `dbt_deps` → `dbt_build` |
| `ingest/ingest.py` | Descarga y carga de los parquet a `RAW` |
| `scripts/run_pipeline.sh` | La misma tubería, ejecutable por CLI |
| `dbt/` | Proyecto dbt (seeds, modelos, tests) |
| `keys/` | Llave privada del usuario de servicio (**no se versiona**) |

## Cómo ejecutar

Requisitos: Docker Desktop y una cuenta de Snowflake con acceso a ACCOUNTADMIN (sirve una trial).

1. Generar un par de llaves RSA para el usuario de servicio (Git Bash / Linux / macOS):
   ```bash
   mkdir -p keys
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8 -nocrypt
   openssl rsa -in keys/rsa_key.p8 -pubout -out keys/rsa_key.pub
   ```
   Reemplazar el valor de `rsa_public_key` en `snowflake/00_setup.sql` por el contenido de
   `keys/rsa_key.pub`, sin las líneas `-----BEGIN/END PUBLIC KEY-----` y en una sola línea.
   (La llave pública del repo corresponde a una llave privada que no se versiona.)
2. En Snowsight, con rol ACCOUNTADMIN, ejecutar `snowflake/00_setup.sql` (Run All).
3. `cp .env.example .env` y copiar en `SNOWFLAKE_ACCOUNT` el identificador que devuelve la última
   consulta del setup.
4. Levantar la infraestructura:
   ```bash
   docker compose up -d --build
   ```
5. Ejecutar la tubería:
   - **Kestra:** http://localhost:8080 (crear el usuario admin la primera vez) → Flows →
     `usfq.lab07.nyc_taxi_elt` → *Execute*.
   - **CLI:** `docker compose run --rm pipeline`

## Ingesta (Extract + Load)

Para cada mes del rango, `ingest.py`:

1. Hace `HEAD` al archivo en el CDN de la TLC. Si todavía no está publicado (403/404), lo omite
   con un aviso. **Agosto 2026 aún no estaba publicado al 28-sep-2026**; se cargará automáticamente
   en una corrida futura (el flow tiene un trigger mensual).
2. Compara el `ETag` con `RAW.LOAD_AUDIT`: si esa versión del archivo ya se cargó, lo omite.
3. Descarga el parquet, lo sube a un stage interno (`PUT`) y, **en una sola transacción**, borra las
   filas previas de ese archivo y ejecuta `COPY INTO`. Si algo falla hay `ROLLBACK`: nunca queda un
   mes a medias ni duplicado.
4. Registra la carga en `RAW.LOAD_AUDIT` (archivo, período, ETag, filas, id de ingesta, fecha).

Evolución de esquema: desde 2026 la fuente agrega la columna `request_source`; el script agrega
columnas nuevas a `RAW` con `ALTER TABLE` y para meses anteriores queda en `NULL`. También tolera
variaciones de mayúsculas en el nombre (`Airport_fee` / `airport_fee`).

## Bronze

`bronze.brz_yellow_tripdata` es una vista con **las mismas columnas, nombres y tipos del parquet**,
sin limpieza, más metadata de linaje:

| Columna | Significado |
|---|---|
| `_source_file` | Archivo de origen (`yellow_tripdata_YYYY-MM.parquet`) |
| `_source_period` | Mes al que corresponde el archivo |
| `_loaded_at` | Fecha y hora de carga en Snowflake |
| `_ingestion_id` | Id de la ejecución de ingesta |

Se materializa como vista para no duplicar ~70 M de filas y reflejar siempre `RAW`.

## Silver — decisiones de limpieza

Las reglas se derivaron de perfilar los datos (ej. enero 2025 y julio 2026). La lógica vive en la
macro `classify_yellow_trips`, compartida por el modelo limpio y la cuarentena.

### Tipos de datos y formatos / nombres inconsistentes (consistencia)
| Decisión | Justificación |
|---|---|
| Columnas renombradas a `snake_case` con unidad (`trip_distance_miles`, `*_amount`, `pickup_location_id`) | La fuente mezcla estilos (`VendorID`, `RatecodeID`, `Airport_fee`, `tpep_*`). |
| Montos a `NUMBER(10,2)` redondeados | Vienen como `double` con errores de coma flotante; el dinero se expresa en centavos. |
| Fechas en `TIMESTAMP_NTZ` | La TLC publica hora local de NYC sin zona horaria; no se convierte. |
| `store_and_fwd_flag` Y/N → booleano `is_store_and_forward` | Tipo semántico correcto; se normaliza con `upper(trim())`. |
| `request_source` → `upper(trim())`, vacío/nulo → `NOT_REPORTED` | Columna nueva de 2026; en meses previos no existe. |

### Valores nulos (completitud)
Todos los nulos observados provienen de viajes **Flex Fare** (`payment_type = 0`): ~15 % de los
viajes en ene-2025 y ~27 % en jul-2026 no traen `passenger_count`, `RatecodeID`,
`store_and_fwd_flag`, `congestion_surcharge` ni `Airport_fee`.

| Columna | Tratamiento | Justificación |
|---|---|---|
| `passenger_count` | Se deja `NULL` | Imputar (p. ej. 1) inventaría datos y sesgaría promedios. |
| `rate_code_id` | `NULL` → `99` (Unknown) | El diccionario de la TLC define 99 como "Null/unknown"; permite la FK. |
| `is_store_and_forward` | Se deja `NULL` | No hay forma de inferirlo. |
| `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee` | `NULL` → `0` | Son métricas aditivas; un recargo no reportado no se cobró por separado y `total_amount` ya incluye lo cobrado. Evita que `SUM` ignore filas de forma inconsistente. |

### Valores fuera de dominio (validez)
| Caso | Tratamiento | Justificación |
|---|---|---|
| `passenger_count` = 0 o > 6 | → `NULL` | Un viaje completado tiene ≥ 1 pasajero; un taxi amarillo admite como máximo 5 (+1 niño). El viaje es válido, solo el atributo no. |
| Vendor, rate code o payment type fuera del catálogo | → miembro desconocido (−1, 99, 5) | Conserva el viaje y garantiza integridad referencial. |
| Location ID fuera de las 265 zonas | → 264 (Unknown) | Igual que arriba, usando el propio código "Unknown" de la TLC. |

### Registros inválidos (exactitud) → se envían a `slv_yellow_trips_rejected`
Se evalúan en orden; el primero que falla es el `rejection_reason`:

| Motivo | Regla | Justificación |
|---|---|---|
| `missing_timestamp` | pickup o dropoff nulo | Sin tiempos no hay viaje analizable. |
| `pickup_outside_file_period` | mes del pickup ≠ mes del archivo | Los archivos traen registros con fechas de 2008, 2024, etc. (errores de reloj del taxímetro). |
| `negative_duration` | dropoff < pickup | Imposible físicamente. |
| `duration_too_long` | duración > 24 h | Taxímetro no cerrado; distorsiona duraciones. |
| `invalid_distance` | distancia < 0 o > 500 millas | Se observaron valores de hasta 276 000 millas. |
| `negative_amount` | `fare_amount` < 0 o `total_amount` < 0 | Son reversos/anulaciones contables, no viajes. |
| `amount_outlier` | `total_amount` > 1000 USD | Se observaron totales de hasta 863 000 USD. |
| `duplicate` | mismo `trip_key` dentro del período | Deduplicación (ver abajo). |

Umbrales configurables en `dbt_project.yml` (`vars`). Los viajes de distancia 0 **se conservan**:
pueden ser tarifas negociadas o cargos mínimos legítimos.

### Duplicados (unicidad)
La fuente no tiene id de viaje. `trip_key` = hash MD5 (`dbt_utils.generate_surrogate_key`) de todos
los atributos ya estandarizados. Filas idénticas comparten `trip_key`; se conserva una
(`row_number()`), el resto va a cuarentena como `duplicate`.

### Conciliación
El test `assert_bronze_reconciles_with_silver` verifica por mes que
**filas bronze = filas silver + filas rechazadas**: nada se pierde ni se duplica.

## Gold — esquema estrella

```
                 dim_date (x2: pickup / dropoff)
                        │
 dim_vendor ──┐         │         ┌── dim_location (x2: origen / destino)
              ├──── fct_trips ────┤
 dim_rate_code┘      │     │      └── dim_time
             dim_payment_type
```

**Grano de `fct_trips`:** un viaje de taxi amarillo válido (una fila por `trip_key`).

| Tabla | PK | Atributos principales |
|---|---|---|
| `fct_trips` | `trip_key` | FKs + métricas + atributos degenerados |
| `dim_date` | `date_key` (YYYYMMDD) | fecha, año, trimestre, mes, nombre de mes, día de semana, fin de semana |
| `dim_time` | `hour_key` (0–23) | etiqueta, franja del día, hora pico |
| `dim_location` | `location_id` | borough, zona, service zone, es aeropuerto |
| `dim_vendor` | `vendor_id` | nombre del proveedor TPEP |
| `dim_payment_type` | `payment_type_id` | forma de pago, es viaje pagado |
| `dim_rate_code` | `rate_code_id` | tarifa, es tarifa de aeropuerto |

**Foreign keys de `fct_trips`:** `pickup_date_key`, `dropoff_date_key` → `dim_date`;
`pickup_hour_key` → `dim_time`; `pickup_location_id`, `dropoff_location_id` → `dim_location`
(role-playing); `vendor_id` → `dim_vendor`; `payment_type_id` → `dim_payment_type`;
`rate_code_id` → `dim_rate_code`.

**Métricas (aditivas):** `passenger_count`, `trip_distance_miles`, `trip_duration_minutes`,
`fare_amount`, `extra_amount`, `mta_tax_amount`, `tip_amount`, `tolls_amount`,
`improvement_surcharge_amount`, `congestion_surcharge_amount`, `airport_fee_amount`,
`cbd_congestion_fee_amount`, `total_amount`.

**Atributos degenerados:** `pickup_datetime`, `dropoff_datetime`, `is_store_and_forward`,
`request_source`, `source_period`.

Los catálogos (zonas, vendors, formas de pago, tarifas) vienen del diccionario de datos oficial de la
TLC y se cargan como **seeds** en el schema `REFERENCE`.

## Validación (dbt tests)

`dbt build` ejecuta 90 nodos (seeds, modelos y tests):

- **`unique` + `not_null`** en todas las PK (`trip_key` y las PK de cada dimensión).
- **`relationships`** en las 8 FK de `fct_trips` y en los códigos de silver.
- **`accepted_values` / `dbt_utils.accepted_range`** en códigos, montos, distancias y duraciones.
- **Tests singulares** (`dbt/tests/`):
  - `assert_bronze_reconciles_with_silver`: bronze = silver + rechazados, por mes.
  - `assert_fact_matches_silver`: todas las filas de silver llegan al hecho.
  - `assert_dropoff_after_pickup`: consistencia temporal.
  - `assert_raw_file_loaded_once`: cada archivo proviene de una sola ingesta (no hay cargas duplicadas).

## Idempotencia (re-ejecución sin duplicados)

| Capa | Mecanismo |
|---|---|
| RAW | `DELETE` del archivo + `COPY INTO` en una transacción; los archivos sin cambios (mismo ETag) se omiten. |
| Bronze | Vista: siempre refleja RAW. |
| Silver y `fct_trips` | Incrementales `delete+insert` con `unique_key = source_period`: solo se re-procesan los meses cuyo upstream cambió, y se reemplazan completos. |
| Dimensiones | Tablas reconstruidas (`table`) en cada corrida. |

Re-ejecutar la tubería deja los mismos conteos y todos los tests en verde.

## Resultados (corrida del 28-sep-2026, enero 2025 – julio 2026)

| Capa | Filas |
|---|---:|
| RAW / Bronze (19 archivos) | 75 089 241 |
| Silver — válidos | 72 082 739 |
| Silver — rechazados | 3 006 502 (4,0 %) |
| Gold — `fct_trips` | 72 082 739 |

Conciliación: 72 082 739 + 3 006 502 = 75 089 241. `dbt build`: **PASS=90, ERROR=0**.

| Motivo de rechazo | Filas | % de rechazos |
|---|---:|---:|
| `negative_amount` | 3 000 589 | 99,80 |
| `invalid_distance` | 2 640 | 0,09 |
| `negative_duration` | 2 242 | 0,07 |
| `duration_too_long` | 573 | 0,02 |
| `pickup_outside_file_period` | 345 | 0,01 |
| `amount_outlier` | 112 | 0,00 |
| `duplicate` | 1 | 0,00 |

**Segunda ejecución (prueba de idempotencia):** los 19 archivos se omitieron por tener el mismo
ETag, los modelos incrementales procesaron 0 filas, los conteos quedaron idénticos y los 90 tests
pasaron (24 s).
