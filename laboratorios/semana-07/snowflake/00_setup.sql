-- =====================================================================
-- Laboratorio Integrador I - Setup de Snowflake (ejecutar UNA vez)
-- Ejecutar en Snowsight > Projects > Worksheets con rol ACCOUNTADMIN
-- (seleccionar todo y "Run All").
-- Crea: rol, warehouse XS, base de datos y un usuario de servicio que
-- se autentica con llave RSA (sin contraseña ni MFA).
-- =====================================================================
use role accountadmin;

create role if not exists LAB07_ROLE;
grant role LAB07_ROLE to role SYSADMIN;

-- Para que tú también puedas ver las tablas desde Snowsight
set my_user = current_user();
grant role LAB07_ROLE to user identifier($my_user);

create warehouse if not exists LAB07_WH
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;
grant usage, operate on warehouse LAB07_WH to role LAB07_ROLE;

create database if not exists NYC_TAXI;
grant ownership on database NYC_TAXI to role LAB07_ROLE copy current grants;

create user if not exists LAB07_SVC
  type = SERVICE
  default_role = LAB07_ROLE
  default_warehouse = LAB07_WH
  default_namespace = NYC_TAXI
  rsa_public_key = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1izbH/bYbFzYySSrRgUZK7lHqDjNNzZGEqqt0TdtoxbjQ8xuhpCtG6yfTt2YLoXZf58siKJq1GY448rDFpuaVjP7dQPKL+f8n0ieyn1F7lJDh2MQLZiz03dZqxt88Wk/HVzMAWknCyDuUL31TGHqZiQtwtGYDqhzTKOvpXBz7pTrvW30euwXmL8scrhPY9tRlavBs0oNOMiODZPsLaSDutrSA+dPNPS+x/Ft1omtpfRCBxt716zrJZAY+rxWL1OIws3IYSD7yCXk7jb+izDcCIcXulcbqgMJIdyTG2QtlA9HnHLllU5krbNFitt7ox7h6/Gomf165xO3irDwUp8XDwIDAQAB';
grant role LAB07_ROLE to user LAB07_SVC;

-- Copia el resultado de esta consulta (ORGANIZACION-CUENTA) en el .env
select current_organization_name() || '-' || current_account_name() as SNOWFLAKE_ACCOUNT;
