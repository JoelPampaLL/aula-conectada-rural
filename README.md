# Aula Conectada Rural — Plataforma BI

Plataforma integrada de Business Intelligence (7 capas) en **Streamlit + PostgreSQL (Neon)** para analizar la permanencia escolar en comunidades rurales del Perú.

## Requisitos previos

- Python 3.11 o superior
- Una base de datos PostgreSQL en Neon (cadena de conexión `DATABASE_URL`)

## Instalación y ejecución local

1. Instalar dependencias:

```bash
pip install -r requirements.txt
```

2. Configurar la cadena de conexión en `.streamlit/secrets.toml` (este archivo no debe subirse al repositorio):

```toml
DATABASE_URL = "postgresql://usuario:clave@host.neon.tech/neondb?sslmode=require"
```

También es posible definir `DATABASE_URL` como variable de entorno del sistema.

3. Ejecutar la aplicación:

```bash
streamlit run app.py
```

## Estructura de la Etapa 1

- `app.py`: Interfaz principal, indicador de estado del proceso, pestañas 1 (Fuentes de datos) y 2 (Staging Area), y marcadores de las pestañas 3 a 7.
- `db.py`: Conexión cacheada a Neon, creación automática de tablas `stg_*` y tablas de control (`log_carga`, `log_etl`, `log_staging_validacion`), carga masiva y consultas paginadas.
- `etl.py`: Perfilado inicial de columnas y motor genérico de control de calidad sobre datos crudos de staging.
- `kpi.py`: Definiciones base de KPIs para la capa semántica.
- `data/ejemplo/`: Los 5 archivos CSV de ejemplo de solo lectura.
- `modelo_aula_conectada.py`: Librería analítica (sin modificar).
