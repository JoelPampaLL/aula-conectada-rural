"""Capa de acceso a datos PostgreSQL (Neon), DDL de staging/control/DW, vistas y carga masiva."""

from __future__ import annotations

import io
import json
import os
import time
from datetime import datetime
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
import psycopg2.extras
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

FUENTES_CONFIG: dict[str, dict[str, Any]] = {
    "T1": {
        "codigo": "T1",
        "nombre": "Matrícula y deserción escolar (SIAGIE)",
        "tipo_origen": "Sistema transaccional",
        "tabla": "stg_t1_matricula_desercion",
        "archivo_ejemplo": "ACR_T1_siagie_matricula_desercion.csv",
        "clave_natural": ["cod_modular", "anio"],
        "columnas": [
            "id_registro",
            "cod_modular",
            "anio",
            "ubigeo",
            "nivel",
            "matricula_inicial",
            "matricula_final",
            "retirados",
            "desertores",
            "tasa_desercion_pct",
            "asistencia_prom_pct",
            "docentes_total",
            "docentes_titulados",
            "estado_registro",
            "fecha_corte",
        ],
        "tipos": {
            "id_registro": "Texto",
            "cod_modular": "Código (texto 7)",
            "anio": "Entero",
            "ubigeo": "Código (texto 6)",
            "nivel": "Texto",
            "matricula_inicial": "Entero",
            "matricula_final": "Entero",
            "retirados": "Entero",
            "desertores": "Entero",
            "tasa_desercion_pct": "Decimal",
            "asistencia_prom_pct": "Decimal",
            "docentes_total": "Entero",
            "docentes_titulados": "Entero",
            "estado_registro": "Texto",
            "fecha_corte": "Fecha",
        },
        "columnas_numericas": [
            "anio",
            "matricula_inicial",
            "matricula_final",
            "retirados",
            "desertores",
            "tasa_desercion_pct",
            "asistencia_prom_pct",
            "docentes_total",
            "docentes_titulados",
        ],
        "longitudes_codigo": {"cod_modular": 7, "ubigeo": 6},
    },
    "T2": {
        "codigo": "T2",
        "nombre": "Padrón de instituciones y geolocalización",
        "tipo_origen": "Geolocalización y padrón",
        "tabla": "stg_t2_padron_ie",
        "archivo_ejemplo": "ACR_T2_geo_padron_ie_rural.csv",
        "clave_natural": ["cod_modular"],
        "columnas": [
            "cod_modular",
            "cod_local",
            "nombre_ie",
            "nivel",
            "ubigeo",
            "departamento",
            "provincia",
            "distrito",
            "ugel",
            "gradiente_ruralidad",
            "latitud",
            "longitud",
            "altitud_msnm",
            "distancia_capital_distrital_km",
            "tiempo_acceso_horas",
            "tipo_acceso",
            "lengua_predominante",
            "kit_aula_conectada",
        ],
        "tipos": {
            "cod_modular": "Código (texto 7)",
            "cod_local": "Código (texto 6)",
            "nombre_ie": "Texto",
            "nivel": "Texto",
            "ubigeo": "Código (texto 6)",
            "departamento": "Texto",
            "provincia": "Texto",
            "distrito": "Texto",
            "ugel": "Texto",
            "gradiente_ruralidad": "Texto",
            "latitud": "Decimal",
            "longitud": "Decimal",
            "altitud_msnm": "Entero",
            "distancia_capital_distrital_km": "Decimal",
            "tiempo_acceso_horas": "Decimal",
            "tipo_acceso": "Texto",
            "lengua_predominante": "Texto",
            "kit_aula_conectada": "Texto",
        },
        "columnas_numericas": [
            "latitud",
            "longitud",
            "altitud_msnm",
            "distancia_capital_distrital_km",
            "tiempo_acceso_horas",
        ],
        "longitudes_codigo": {"cod_modular": 7, "cod_local": 6, "ubigeo": 6},
    },
    "T3": {
        "codigo": "T3",
        "nombre": "Telemetría IoT de conectividad y energía",
        "tipo_origen": "Sensores IoT",
        "tabla": "stg_t3_iot",
        "archivo_ejemplo": "ACR_T3_iot_conectividad_aula.csv",
        "clave_natural": ["id_lectura"],
        "columnas": [
            "id_lectura",
            "id_sensor",
            "cod_modular",
            "mes",
            "tipo_enlace",
            "ancho_banda_prom_mbps",
            "latencia_prom_ms",
            "disponibilidad_internet_pct",
            "horas_corte_energia",
            "voltaje_prom_v",
            "carga_bateria_solar_pct",
            "temperatura_aula_c",
            "dispositivos_conectados_prom",
            "estado_sensor",
        ],
        "tipos": {
            "id_lectura": "Texto",
            "id_sensor": "Texto",
            "cod_modular": "Código (texto 7)",
            "mes": "Texto (AAAA-MM)",
            "tipo_enlace": "Texto",
            "ancho_banda_prom_mbps": "Decimal",
            "latencia_prom_ms": "Decimal",
            "disponibilidad_internet_pct": "Decimal",
            "horas_corte_energia": "Decimal",
            "voltaje_prom_v": "Decimal",
            "carga_bateria_solar_pct": "Decimal",
            "temperatura_aula_c": "Decimal",
            "dispositivos_conectados_prom": "Entero",
            "estado_sensor": "Texto",
        },
        "columnas_numericas": [
            "ancho_banda_prom_mbps",
            "latencia_prom_ms",
            "disponibilidad_internet_pct",
            "horas_corte_energia",
            "voltaje_prom_v",
            "carga_bateria_solar_pct",
            "temperatura_aula_c",
            "dispositivos_conectados_prom",
        ],
        "longitudes_codigo": {"cod_modular": 7},
    },
    "T4": {
        "codigo": "T4",
        "nombre": "Censo de equipamiento por local escolar",
        "tipo_origen": "Archivos planos históricos",
        "tabla": "stg_t4_censo_local",
        "archivo_ejemplo": "ACR_T4_censo_equipamiento_local.csv",
        "clave_natural": ["cod_local", "anio_censo"],
        "columnas": [
            "id_censo",
            "cod_local",
            "ubigeo",
            "anio_censo",
            "n_ie_en_local",
            "declaro_computo",
            "cpu_total",
            "cpu_operativos",
            "cpu_en_reparacion",
            "cpu_mas_3_anios",
            "laptops_xo",
            "tabletas",
            "n_equipos_red",
            "tiene_internet",
            "tipo_internet",
            "fuente_energia",
            "tiene_ups_estabilizador",
            "tiene_pozo_tierra",
        ],
        "tipos": {
            "id_censo": "Texto",
            "cod_local": "Código (texto 6)",
            "ubigeo": "Código (texto 6)",
            "anio_censo": "Entero",
            "n_ie_en_local": "Entero",
            "declaro_computo": "Texto",
            "cpu_total": "Entero",
            "cpu_operativos": "Entero",
            "cpu_en_reparacion": "Entero",
            "cpu_mas_3_anios": "Entero",
            "laptops_xo": "Entero",
            "tabletas": "Entero",
            "n_equipos_red": "Entero",
            "tiene_internet": "Texto",
            "tipo_internet": "Texto",
            "fuente_energia": "Texto",
            "tiene_ups_estabilizador": "Texto",
            "tiene_pozo_tierra": "Texto",
        },
        "columnas_numericas": [
            "anio_censo",
            "n_ie_en_local",
            "cpu_total",
            "cpu_operativos",
            "cpu_en_reparacion",
            "cpu_mas_3_anios",
            "laptops_xo",
            "tabletas",
            "n_equipos_red",
        ],
        "longitudes_codigo": {"cod_local": 6, "ubigeo": 6},
    },
    "T5": {
        "codigo": "T5",
        "nombre": "Clima e indicadores socioeconómicos distritales",
        "tipo_origen": "APIs externas",
        "tabla": "stg_t5_clima_api",
        "archivo_ejemplo": "ACR_T5_api_clima_socioeconomico.csv",
        "clave_natural": ["ubigeo", "anio", "mes"],
        "columnas": [
            "id_registro",
            "ubigeo",
            "anio",
            "mes",
            "precipitacion_mm",
            "temp_media_c",
            "temp_min_c",
            "evento_climatico",
            "dias_sin_clases_clima",
            "pobreza_monetaria_pct",
            "hogares_con_internet_pct",
            "cobertura_movil",
        ],
        "tipos": {
            "id_registro": "Texto",
            "ubigeo": "Código (texto 6)",
            "anio": "Entero",
            "mes": "Entero",
            "precipitacion_mm": "Decimal",
            "temp_media_c": "Decimal",
            "temp_min_c": "Decimal",
            "evento_climatico": "Texto",
            "dias_sin_clases_clima": "Entero",
            "pobreza_monetaria_pct": "Decimal",
            "hogares_con_internet_pct": "Decimal",
            "cobertura_movil": "Texto",
        },
        "columnas_numericas": [
            "anio",
            "mes",
            "precipitacion_mm",
            "temp_media_c",
            "temp_min_c",
            "dias_sin_clases_clima",
            "pobreza_monetaria_pct",
            "hogares_con_internet_pct",
        ],
        "longitudes_codigo": {"ubigeo": 6},
    },
}

DW_TABLAS_META: list[dict[str, str]] = [
    {
        "tabla": "dim_departamento",
        "tipo": "Dimensión (jerarquía territorial)",
        "pk": "id_departamento",
        "fk": "—",
        "descripcion": "Departamentos presentes en el padrón.",
    },
    {
        "tabla": "dim_provincia",
        "tipo": "Dimensión (jerarquía territorial)",
        "pk": "id_provincia",
        "fk": "id_departamento → dim_departamento",
        "descripcion": "Provincias vinculadas a su departamento.",
    },
    {
        "tabla": "dim_distrito",
        "tipo": "Dimensión (jerarquía territorial)",
        "pk": "ubigeo",
        "fk": "id_provincia → dim_provincia",
        "descripcion": "Distritos identificados por ubigeo de 6 dígitos.",
    },
    {
        "tabla": "dim_ugel",
        "tipo": "Dimensión",
        "pk": "id_ugel",
        "fk": "—",
        "descripcion": "Unidades de Gestión Educativa Local.",
    },
    {
        "tabla": "dim_ruralidad",
        "tipo": "Dimensión (jerarquía institución)",
        "pk": "id_ruralidad",
        "fk": "—",
        "descripcion": "Gradiente de ruralidad (Rural 1, Rural 2, Rural 3).",
    },
    {
        "tabla": "dim_nivel",
        "tipo": "Dimensión (jerarquía institución)",
        "pk": "id_nivel",
        "fk": "—",
        "descripcion": "Nivel educativo (Primaria, Secundaria).",
    },
    {
        "tabla": "dim_tiempo",
        "tipo": "Dimensión",
        "pk": "id_tiempo",
        "fk": "—",
        "descripcion": "Calendario anual (mes=0) y mensual generado según los años cargados.",
    },
    {
        "tabla": "dim_local",
        "tipo": "Dimensión (jerarquía territorial)",
        "pk": "cod_local",
        "fk": "ubigeo → dim_distrito, id_ugel → dim_ugel",
        "descripcion": "Locales escolares físicos (pueden albergar más de una IE).",
    },
    {
        "tabla": "dim_ie",
        "tipo": "Dimensión (jerarquía institución)",
        "pk": "cod_modular",
        "fk": "id_nivel → dim_nivel, id_ruralidad → dim_ruralidad, cod_local → dim_local",
        "descripcion": "Instituciones educativas (sin lengua_predominante).",
    },
    {
        "tabla": "fact_permanencia",
        "tipo": "Tabla de hechos",
        "pk": "id_fact (UNIQUE: cod_modular, id_tiempo)",
        "fk": "cod_modular → dim_ie, id_tiempo → dim_tiempo",
        "descripcion": "Matrícula, deserción, equipamiento del local y clima anual por IE y año.",
    },
    {
        "tabla": "fact_conectividad",
        "tipo": "Tabla de hechos",
        "pk": "id_fact (UNIQUE: id_sensor, id_tiempo)",
        "fk": "cod_modular → dim_ie, id_tiempo → dim_tiempo",
        "descripcion": "Telemetría mensual IoT enriquecida con clima del distrito.",
    },
]

VISTAS_PREDEFINIDAS: dict[str, dict[str, Any]] = {
    "vw_kpi_departamento_anio": {
        "nombre": "KPIs de permanencia por departamento, año y nivel (vw_kpi_departamento_anio)",
        "filtros": ["departamento", "nivel"],
        "orden": "anio DESC, departamento ASC, nivel ASC",
    },
    "vw_kpi_distrito_anio": {
        "nombre": "KPIs de permanencia por distrito, año y nivel (vw_kpi_distrito_anio)",
        "filtros": ["departamento", "distrito", "nivel"],
        "orden": "anio DESC, departamento ASC, distrito ASC, nivel ASC",
    },
    "vw_kpi_local_base": {
        "nombre": "Equipamiento y alumnos por PC a nivel de local sin doble conteo (vw_kpi_local_base)",
        "filtros": ["departamento", "distrito", "nivel"],
        "orden": "departamento ASC, distrito ASC, cod_local ASC",
    },
    "vw_conectividad_ie": {
        "nombre": "Conectividad IoT por institución excluyendo lecturas Sin señal (vw_conectividad_ie)",
        "filtros": ["departamento", "distrito", "nivel"],
        "orden": "departamento ASC, distrito ASC, cod_modular ASC",
    },
    "vw_ml_t1": {
        "nombre": "Vista ML T1 — Matrícula y deserción anual (vw_ml_t1)",
        "filtros": [],
        "orden": "anio ASC, cod_modular ASC",
    },
    "vw_ml_t2": {
        "nombre": "Vista ML T2 — Padrón y geolocalización de IE (vw_ml_t2)",
        "filtros": ["departamento", "distrito", "nivel"],
        "orden": "departamento ASC, provincia ASC, distrito ASC, cod_modular ASC",
    },
    "vw_ml_t3": {
        "nombre": "Vista ML T3 — Telemetría IoT mensual (vw_ml_t3)",
        "filtros": [],
        "orden": "id_lectura ASC",
    },
    "vw_ml_t4": {
        "nombre": "Vista ML T4 — Censo de equipamiento por local (vw_ml_t4)",
        "filtros": [],
        "orden": "anio_censo ASC, cod_local ASC",
    },
    "vw_ml_t5": {
        "nombre": "Vista ML T5 — Clima y entorno agregado por distrito (vw_ml_t5)",
        "filtros": [],
        "orden": "ubigeo ASC",
    },
}

PARAMETROS_INICIALES: list[dict[str, Any]] = [
    {
        "clave": "promedio_nacional_pct",
        "valor": 2.08,
        "unidad": "%",
        "descripcion": "Tasa de deserción nacional de referencia (MINEDU 2023-2024).",
    },
    {
        "clave": "multiplo_riesgo_alto",
        "valor": 1.5,
        "unidad": "múltiplo",
        "descripcion": "Factor sobre el promedio nacional para definir riesgo alto de deserción.",
    },
    {
        "clave": "semaforo_rojo",
        "valor": 0.60,
        "unidad": "probabilidad",
        "descripcion": "Umbral de probabilidad >= para clasificar institución en semáforo Rojo.",
    },
    {
        "clave": "semaforo_amarillo",
        "valor": 0.35,
        "unidad": "probabilidad",
        "descripcion": "Umbral de probabilidad >= para clasificar institución en semáforo Amarillo.",
    },
    {
        "clave": "alumnos_por_pc_meta",
        "valor": 20.0,
        "unidad": "alumnos/PC",
        "descripcion": "Meta máxima de estudiantes por computadora operativa en el local.",
    },
    {
        "clave": "costo_reparar",
        "valor": 250.0,
        "unidad": "S/",
        "descripcion": "Costo unitario referencial para reparar una computadora.",
    },
    {
        "clave": "costo_reponer",
        "valor": 2200.0,
        "unidad": "S/",
        "descripcion": "Costo unitario referencial para reponer una computadora inoperativa.",
    },
    {
        "clave": "costo_dotar",
        "valor": 2500.0,
        "unidad": "S/",
        "descripcion": "Costo unitario referencial para dotar una computadora nueva.",
    },
    {
        "clave": "costo_kit_conectividad",
        "valor": 6000.0,
        "unidad": "S/",
        "descripcion": "Costo referencial del kit de conectividad por local sin Internet.",
    },
    {
        "clave": "presupuesto_soles",
        "valor": 250000.0,
        "unidad": "S/",
        "descripcion": "Presupuesto base para priorización de intervención y escenario optimista.",
    },
    {
        "clave": "baja_anual_base",
        "valor": 0.05,
        "unidad": "proporción",
        "descripcion": "Tasa anual base de baja operativa de computadoras.",
    },
    {
        "clave": "baja_anual_extra_obsolescencia",
        "valor": 0.15,
        "unidad": "proporción",
        "descripcion": "Tasa adicional proporcional a equipos con más de 3 años.",
    },
    {
        "clave": "efecto_intervencion",
        "valor": 0.20,
        "unidad": "proporción",
        "descripcion": "Reducción esperada de deserción en locales financiados (escenario optimista).",
    },
    {
        "clave": "shock_pesimista",
        "valor": 0.10,
        "unidad": "proporción",
        "descripcion": "Incremento de tasa de deserción ante deterioro climático/conectividad (escenario pesimista).",
    },
    {
        "clave": "contaminacion_anomalias",
        "valor": 0.03,
        "unidad": "proporción",
        "descripcion": "Proporción esperada de lecturas IoT anómalas para Isolation Forest.",
    },
]

IA_TABLAS_RESULTADOS: tuple[str, ...] = (
    "ia_pred_riesgo_ie",
    "ia_proyeccion_desertores",
    "ia_inversion_local",
    "ia_priorizacion",
    "ia_anomalias_iot",
    "ia_alertas_cronicas_iot",
    "ia_anomalias_acceso",
    "ia_importancia_variables",
)


def _obtener_database_url(quitar_channel_binding: bool = False) -> str:
    """Obtiene DATABASE_URL desde st.secrets o variable de entorno sin exponer credenciales."""
    url = ""
    try:
        if "DATABASE_URL" in st.secrets:
            url = str(st.secrets["DATABASE_URL"]).strip()
    except Exception:
        url = ""

    if not url:
        url = os.environ.get("DATABASE_URL", "").strip()

    if not url:
        raise RuntimeError(
            "No se encontró DATABASE_URL en st.secrets ni en las variables de entorno."
        )

    url = url.strip().strip('"').strip("'")
    if url.startswith("postgres://"):
        url = "postgresql+psycopg2://" + url[len("postgres://") :]
    elif url.startswith("postgresql://"):
        url = "postgresql+psycopg2://" + url[len("postgresql://") :]

    if quitar_channel_binding:
        parsed = urlparse(url)
        query_filtrada = [
            (k, v)
            for k, v in parse_qsl(parsed.query, keep_blank_values=True)
            if k.lower() != "channel_binding"
        ]
        url = urlunparse(parsed._replace(query=urlencode(query_filtrada)))

    return url


@st.cache_resource
def get_engine() -> Engine:
    """Crea y cachea el Engine SQLAlchemy hacia Neon con manejo de channel_binding."""
    url = _obtener_database_url(quitar_channel_binding=False)
    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 20},
    )
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as exc:
        msg = str(exc).lower()
        if "channel_binding" in msg or "channel_binding" in url.lower():
            engine.dispose()
            url_sin_cb = _obtener_database_url(quitar_channel_binding=True)
            engine_reintento = create_engine(
                url_sin_cb,
                pool_pre_ping=True,
                connect_args={"connect_timeout": 20},
            )
            try:
                with engine_reintento.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return engine_reintento
            except Exception as exc2:
                engine_reintento.dispose()
                raise RuntimeError(
                    "No se pudo conectar a la base de datos PostgreSQL en Neon tras omitir channel_binding."
                ) from exc2
        engine.dispose()
        raise RuntimeError(
            "No se pudo establecer conexión con la base de datos PostgreSQL en Neon."
        ) from exc


@st.cache_resource
def inicializar_esquema() -> bool:
    """Crea tablas staging, control, Data Warehouse (copo de nieve) y vistas vw_*."""
    engine = get_engine()
    sentencias: list[str] = []

    # 1. Tablas de Staging
    for cfg in FUENTES_CONFIG.values():
        cols_sql = ",\n    ".join(f'"{col}" TEXT' for col in cfg["columnas"])
        sentencias.append(
            f"""
            CREATE TABLE IF NOT EXISTS {cfg["tabla"]} (
                _fila SERIAL PRIMARY KEY,
                _lote INT NOT NULL,
                {cols_sql}
            );
            """
        )

    # 2. Tablas de Control
    sentencias.extend(
        [
            """
            CREATE TABLE IF NOT EXISTS log_carga (
                id SERIAL PRIMARY KEY,
                lote INT NOT NULL,
                fuente TEXT NOT NULL,
                nombre_archivo TEXT,
                filas INT NOT NULL,
                columnas INT NOT NULL,
                duracion_ms INT NOT NULL,
                estado TEXT NOT NULL,
                mensaje TEXT,
                fecha TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS log_etl (
                id SERIAL PRIMARY KEY,
                lote INT NOT NULL,
                regla TEXT NOT NULL,
                fuente TEXT NOT NULL,
                filas_afectadas INT NOT NULL DEFAULT 0,
                filas_cargadas INT NOT NULL DEFAULT 0,
                estado TEXT NOT NULL,
                fecha TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS log_staging_validacion (
                id SERIAL PRIMARY KEY,
                lote_validacion INT NOT NULL,
                estado TEXT NOT NULL,
                total_hallazgos INT NOT NULL,
                resumen_json TEXT NOT NULL,
                duracion_ms INT NOT NULL,
                fecha TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS log_etl_reporte (
                id SERIAL PRIMARY KEY,
                lote INT NOT NULL,
                reporte_json TEXT NOT NULL,
                duracion_ms INT NOT NULL,
                fecha TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
        ]
    )

    # 3. Data Warehouse (esquema copo de nieve exacto de la sección 7)
    sentencias.extend(
        [
            "CREATE TABLE IF NOT EXISTS dim_departamento (id_departamento SERIAL PRIMARY KEY, nombre TEXT UNIQUE NOT NULL);",
            """
            CREATE TABLE IF NOT EXISTS dim_provincia (
                id_provincia SERIAL PRIMARY KEY,
                nombre TEXT NOT NULL,
                id_departamento INT NOT NULL REFERENCES dim_departamento,
                UNIQUE (nombre, id_departamento)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_distrito (
                ubigeo CHAR(6) PRIMARY KEY,
                nombre TEXT NOT NULL,
                id_provincia INT NOT NULL REFERENCES dim_provincia
            );
            """,
            "CREATE TABLE IF NOT EXISTS dim_ugel (id_ugel SERIAL PRIMARY KEY, nombre TEXT UNIQUE NOT NULL);",
            "CREATE TABLE IF NOT EXISTS dim_ruralidad (id_ruralidad SMALLINT PRIMARY KEY, gradiente TEXT NOT NULL);",
            "CREATE TABLE IF NOT EXISTS dim_nivel (id_nivel SMALLINT PRIMARY KEY, nivel TEXT NOT NULL);",
            """
            CREATE TABLE IF NOT EXISTS dim_tiempo (
                id_tiempo INT PRIMARY KEY,
                anio SMALLINT,
                mes SMALLINT,
                trimestre SMALLINT,
                nombre_mes TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_local (
                cod_local CHAR(6) PRIMARY KEY,
                ubigeo CHAR(6) REFERENCES dim_distrito,
                id_ugel INT REFERENCES dim_ugel,
                latitud NUMERIC(9,5),
                longitud NUMERIC(9,5),
                altitud_msnm INT,
                distancia_capital_km NUMERIC(6,1),
                tiempo_acceso_h NUMERIC(5,1),
                tipo_acceso TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_ie (
                cod_modular CHAR(7) PRIMARY KEY,
                nombre TEXT,
                id_nivel SMALLINT REFERENCES dim_nivel,
                id_ruralidad SMALLINT REFERENCES dim_ruralidad,
                cod_local CHAR(6) REFERENCES dim_local,
                kit_aula_conectada BOOLEAN
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_permanencia (
                id_fact SERIAL PRIMARY KEY,
                cod_modular CHAR(7) NOT NULL REFERENCES dim_ie,
                id_tiempo INT NOT NULL REFERENCES dim_tiempo,
                matricula_inicial INT,
                matricula_final INT,
                retirados INT,
                desertores INT,
                tasa_desercion_pct NUMERIC(6,2),
                asistencia_prom_pct NUMERIC(5,1),
                docentes_total INT,
                docentes_titulados INT,
                declaro_computo BOOLEAN,
                cpu_total INT,
                cpu_operativos INT,
                cpu_en_reparacion INT,
                cpu_mas_3_anios INT,
                tiene_internet BOOLEAN,
                tipo_internet TEXT,
                fuente_energia TEXT,
                tiene_ups BOOLEAN,
                tiene_pozo_tierra BOOLEAN,
                n_equipos_red INT,
                precipitacion_anual_mm NUMERIC(7,1),
                dias_sin_clases_clima INT,
                temp_min_anual_c NUMERIC(5,1),
                pobreza_pct NUMERIC(4,1),
                hogares_internet_pct NUMERIC(4,1),
                cobertura_movil TEXT,
                estado_registro TEXT,
                UNIQUE (cod_modular, id_tiempo)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_conectividad (
                id_fact SERIAL PRIMARY KEY,
                id_lectura TEXT,
                cod_modular CHAR(7) NOT NULL REFERENCES dim_ie,
                id_tiempo INT NOT NULL REFERENCES dim_tiempo,
                id_sensor TEXT NOT NULL,
                tipo_enlace TEXT,
                ancho_banda_mbps NUMERIC(6,1),
                latencia_ms NUMERIC(6,0),
                disponibilidad_pct NUMERIC(5,1),
                horas_corte_energia NUMERIC(6,1),
                voltaje_v NUMERIC(5,1),
                carga_bateria_pct NUMERIC(5,1),
                temperatura_aula_c NUMERIC(4,1),
                dispositivos_conectados INT,
                estado_sensor TEXT,
                precipitacion_mm NUMERIC(7,1),
                evento_climatico TEXT,
                UNIQUE (id_sensor, id_tiempo)
            );
            """,
        ]
    )

    # 4. Vistas vw_ml_t1 .. vw_ml_t5 (sección 7) y vistas analíticas vw_kpi_*
    sentencias.extend(
        [
            """
            CREATE OR REPLACE VIEW vw_ml_t1 AS
            SELECT
                TRIM(fp.cod_modular)::TEXT AS cod_modular,
                dt.anio::INT AS anio,
                TRIM(dl.ubigeo)::TEXT AS ubigeo,
                dn.nivel::TEXT AS nivel,
                fp.matricula_inicial,
                fp.matricula_final,
                fp.retirados,
                fp.desertores,
                fp.asistencia_prom_pct::DOUBLE PRECISION AS asistencia_prom_pct,
                fp.docentes_total,
                fp.docentes_titulados
            FROM fact_permanencia fp
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
            JOIN dim_local dl ON dl.cod_local = ie.cod_local
            JOIN dim_nivel dn ON dn.id_nivel = ie.id_nivel
            ORDER BY dt.anio, fp.cod_modular;
            """,
            """
            CREATE OR REPLACE VIEW vw_ml_t2 AS
            SELECT
                TRIM(ie.cod_modular)::TEXT AS cod_modular,
                TRIM(dl.cod_local)::TEXT AS cod_local,
                TRIM(dl.ubigeo)::TEXT AS ubigeo,
                dep.nombre::TEXT AS departamento,
                prov.nombre::TEXT AS provincia,
                dist.nombre::TEXT AS distrito,
                ug.nombre::TEXT AS ugel,
                dn.nivel::TEXT AS nivel,
                dr.gradiente::TEXT AS gradiente_ruralidad,
                dl.latitud::DOUBLE PRECISION AS latitud,
                dl.longitud::DOUBLE PRECISION AS longitud,
                dl.altitud_msnm,
                dl.distancia_capital_km::DOUBLE PRECISION AS distancia_capital_distrital_km,
                dl.tiempo_acceso_h::DOUBLE PRECISION AS tiempo_acceso_horas,
                dl.tipo_acceso::TEXT AS tipo_acceso
            FROM dim_ie ie
            JOIN dim_local dl ON dl.cod_local = ie.cod_local
            JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
            JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
            JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
            JOIN dim_ugel ug ON ug.id_ugel = dl.id_ugel
            JOIN dim_nivel dn ON dn.id_nivel = ie.id_nivel
            JOIN dim_ruralidad dr ON dr.id_ruralidad = ie.id_ruralidad
            ORDER BY dep.nombre, prov.nombre, dist.nombre, ie.cod_modular;
            """,
            """
            CREATE OR REPLACE VIEW vw_ml_t3 AS
            SELECT
                fc.id_lectura::TEXT AS id_lectura,
                fc.id_sensor::TEXT AS id_sensor,
                TRIM(fc.cod_modular)::TEXT AS cod_modular,
                CONCAT(dt.anio::TEXT, '-', LPAD(dt.mes::TEXT, 2, '0'))::TEXT AS mes,
                fc.tipo_enlace::TEXT AS tipo_enlace,
                fc.ancho_banda_mbps::DOUBLE PRECISION AS ancho_banda_prom_mbps,
                fc.latencia_ms::DOUBLE PRECISION AS latencia_prom_ms,
                fc.disponibilidad_pct::DOUBLE PRECISION AS disponibilidad_internet_pct,
                fc.horas_corte_energia::DOUBLE PRECISION AS horas_corte_energia,
                fc.voltaje_v::DOUBLE PRECISION AS voltaje_prom_v,
                fc.carga_bateria_pct::DOUBLE PRECISION AS carga_bateria_solar_pct,
                fc.temperatura_aula_c::DOUBLE PRECISION AS temperatura_aula_c,
                fc.dispositivos_conectados AS dispositivos_conectados_prom,
                fc.estado_sensor::TEXT AS estado_sensor
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            ORDER BY fc.id_lectura;
            """,
            """
            CREATE OR REPLACE VIEW vw_ml_t4 AS
            SELECT
                cod_local,
                ubigeo,
                anio_censo,
                declaro_computo,
                cpu_total,
                cpu_operativos,
                cpu_en_reparacion,
                cpu_mas_3_anios,
                n_equipos_red,
                tiene_internet,
                tipo_internet,
                fuente_energia,
                tiene_ups_estabilizador,
                tiene_pozo_tierra
            FROM (
                SELECT DISTINCT ON (dl.cod_local, dt.anio)
                    TRIM(dl.cod_local)::TEXT AS cod_local,
                    TRIM(dl.ubigeo)::TEXT AS ubigeo,
                    dt.anio::INT AS anio_censo,
                    CASE WHEN fp.declaro_computo THEN 'Sí' ELSE 'No' END::TEXT AS declaro_computo,
                    fp.cpu_total,
                    fp.cpu_operativos,
                    fp.cpu_en_reparacion,
                    fp.cpu_mas_3_anios,
                    fp.n_equipos_red,
                    CASE
                        WHEN fp.tiene_internet IS TRUE THEN 'Sí'
                        WHEN fp.tiene_internet IS FALSE THEN 'No'
                        ELSE NULL
                    END::TEXT AS tiene_internet,
                    fp.tipo_internet::TEXT AS tipo_internet,
                    fp.fuente_energia::TEXT AS fuente_energia,
                    CASE
                        WHEN fp.tiene_ups IS TRUE THEN 'Sí'
                        WHEN fp.tiene_ups IS FALSE THEN 'No'
                        ELSE NULL
                    END::TEXT AS tiene_ups_estabilizador,
                    CASE
                        WHEN fp.tiene_pozo_tierra IS TRUE THEN 'Sí'
                        WHEN fp.tiene_pozo_tierra IS FALSE THEN 'No'
                        ELSE NULL
                    END::TEXT AS tiene_pozo_tierra
                FROM fact_permanencia fp
                JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
                JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
                JOIN dim_local dl ON dl.cod_local = ie.cod_local
                WHERE fp.declaro_computo IS NOT NULL
                ORDER BY dl.cod_local, dt.anio, fp.cod_modular
            ) sub
            ORDER BY anio_censo, cod_local;
            """,
            """
            CREATE OR REPLACE VIEW vw_ml_t5 AS
            SELECT
                ubigeo,
                precip_anual,
                dias_sin_clases,
                temp_min_anual,
                pobreza_pct,
                hogares_internet_pct,
                cobertura_movil
            FROM (
                SELECT DISTINCT ON (dl.ubigeo)
                    TRIM(dl.ubigeo)::TEXT AS ubigeo,
                    fp.precipitacion_anual_mm::DOUBLE PRECISION AS precip_anual,
                    fp.dias_sin_clases_clima::DOUBLE PRECISION AS dias_sin_clases,
                    fp.temp_min_anual_c::DOUBLE PRECISION AS temp_min_anual,
                    fp.pobreza_pct::DOUBLE PRECISION AS pobreza_pct,
                    fp.hogares_internet_pct::DOUBLE PRECISION AS hogares_internet_pct,
                    fp.cobertura_movil::TEXT AS cobertura_movil,
                    dt.anio
                FROM fact_permanencia fp
                JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
                JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
                JOIN dim_local dl ON dl.cod_local = ie.cod_local
                WHERE fp.precipitacion_anual_mm IS NOT NULL
                   OR fp.pobreza_pct IS NOT NULL
                ORDER BY dl.ubigeo, dt.anio DESC, fp.cod_modular
            ) sub
            ORDER BY ubigeo;
            """,
            """
            CREATE OR REPLACE VIEW vw_kpi_departamento_anio AS
            SELECT
                dep.nombre::TEXT AS departamento,
                dt.anio::INT AS anio,
                dn.nivel::TEXT AS nivel,
                COUNT(DISTINCT fp.cod_modular)::INT AS instituciones,
                COALESCE(SUM(fp.matricula_inicial), 0)::INT AS matricula_inicial,
                COALESCE(SUM(fp.desertores), 0)::INT AS desertores,
                ROUND(COALESCE(SUM(fp.desertores), 0)::NUMERIC / NULLIF(SUM(fp.matricula_inicial), 0) * 100, 2) AS tasa_desercion_pct,
                ROUND(AVG(fp.asistencia_prom_pct), 1) AS asistencia_prom_pct
            FROM fact_permanencia fp
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
            JOIN dim_nivel dn ON dn.id_nivel = ie.id_nivel
            JOIN dim_local dl ON dl.cod_local = ie.cod_local
            JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
            JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
            JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
            GROUP BY dep.nombre, dt.anio, dn.nivel;
            """,
            """
            CREATE OR REPLACE VIEW vw_kpi_distrito_anio AS
            SELECT
                dep.nombre::TEXT AS departamento,
                prov.nombre::TEXT AS provincia,
                dist.nombre::TEXT AS distrito,
                TRIM(dist.ubigeo)::TEXT AS ubigeo,
                dt.anio::INT AS anio,
                dn.nivel::TEXT AS nivel,
                COUNT(DISTINCT fp.cod_modular)::INT AS instituciones,
                COALESCE(SUM(fp.matricula_inicial), 0)::INT AS matricula_inicial,
                COALESCE(SUM(fp.desertores), 0)::INT AS desertores,
                ROUND(COALESCE(SUM(fp.desertores), 0)::NUMERIC / NULLIF(SUM(fp.matricula_inicial), 0) * 100, 2) AS tasa_desercion_pct,
                ROUND(AVG(fp.asistencia_prom_pct), 1) AS asistencia_prom_pct,
                MAX(fp.pobreza_pct) AS pobreza_pct,
                MAX(fp.hogares_internet_pct) AS hogares_internet_pct
            FROM fact_permanencia fp
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
            JOIN dim_nivel dn ON dn.id_nivel = ie.id_nivel
            JOIN dim_local dl ON dl.cod_local = ie.cod_local
            JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
            JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
            JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
            GROUP BY dep.nombre, prov.nombre, dist.nombre, dist.ubigeo, dt.anio, dn.nivel;
            """,
            """
            CREATE OR REPLACE VIEW vw_kpi_local_base AS
            WITH ultimo_censo AS (
                SELECT
                    ie.cod_local,
                    dt.anio AS anio_censo,
                    BOOL_OR(fp.declaro_computo) AS declaro_computo,
                    MAX(fp.cpu_total) AS cpu_total,
                    MAX(fp.cpu_operativos) AS cpu_operativos,
                    MAX(fp.cpu_en_reparacion) AS cpu_en_reparacion,
                    MAX(fp.cpu_mas_3_anios) AS cpu_mas_3_anios,
                    BOOL_OR(fp.tiene_internet) AS tiene_internet,
                    MAX(fp.fuente_energia) AS fuente_energia,
                    SUM(fp.matricula_inicial)::INT AS matricula_local,
                    STRING_AGG(DISTINCT dn.nivel, ' / ' ORDER BY dn.nivel) AS nivel,
                    ROW_NUMBER() OVER (PARTITION BY ie.cod_local ORDER BY dt.anio DESC) AS rn
                FROM fact_permanencia fp
                JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
                JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
                JOIN dim_nivel dn ON dn.id_nivel = ie.id_nivel
                WHERE fp.declaro_computo IS NOT NULL
                GROUP BY ie.cod_local, dt.anio
            )
            SELECT
                dep.nombre::TEXT AS departamento,
                prov.nombre::TEXT AS provincia,
                dist.nombre::TEXT AS distrito,
                TRIM(dist.ubigeo)::TEXT AS ubigeo,
                ug.nombre::TEXT AS ugel,
                TRIM(dl.cod_local)::TEXT AS cod_local,
                uc.anio_censo::INT AS anio_censo,
                uc.nivel::TEXT AS nivel,
                uc.matricula_local,
                CASE WHEN uc.declaro_computo THEN 'Sí' ELSE 'No' END::TEXT AS declaro_computo,
                uc.cpu_total,
                uc.cpu_operativos,
                uc.cpu_en_reparacion,
                uc.cpu_mas_3_anios,
                ROUND(uc.cpu_operativos::NUMERIC / NULLIF(uc.cpu_total, 0) * 100, 1) AS equipos_operativos_pct,
                ROUND(uc.matricula_local::NUMERIC / NULLIF(uc.cpu_operativos, 0), 1) AS alumnos_por_pc_operativa,
                CASE
                    WHEN uc.tiene_internet IS TRUE THEN 'Sí'
                    WHEN uc.tiene_internet IS FALSE THEN 'No'
                    ELSE NULL
                END::TEXT AS tiene_internet,
                uc.fuente_energia::TEXT AS fuente_energia
            FROM ultimo_censo uc
            JOIN dim_local dl ON dl.cod_local = uc.cod_local
            JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
            JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
            JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
            JOIN dim_ugel ug ON ug.id_ugel = dl.id_ugel
            WHERE uc.rn = 1;
            """,
            """
            CREATE OR REPLACE VIEW vw_conectividad_ie AS
            SELECT
                TRIM(ie.cod_modular)::TEXT AS cod_modular,
                ie.nombre::TEXT AS nombre_ie,
                dn.nivel::TEXT AS nivel,
                dep.nombre::TEXT AS departamento,
                prov.nombre::TEXT AS provincia,
                dist.nombre::TEXT AS distrito,
                TRIM(dl.cod_local)::TEXT AS cod_local,
                fc.id_sensor::TEXT AS id_sensor,
                MAX(fc.tipo_enlace)::TEXT AS tipo_enlace,
                COUNT(*)::INT AS lecturas_totales,
                COUNT(*) FILTER (WHERE fc.estado_sensor = 'Sin señal')::INT AS lecturas_sin_senal,
                ROUND(AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal'), 1) AS disponibilidad_prom_pct,
                ROUND(AVG(fc.latencia_ms) FILTER (WHERE fc.estado_sensor <> 'Sin señal'), 1) AS latencia_prom_ms,
                ROUND(AVG(fc.ancho_banda_mbps) FILTER (WHERE fc.estado_sensor <> 'Sin señal'), 1) AS ancho_banda_prom_mbps,
                ROUND(AVG(fc.horas_corte_energia), 1) AS horas_corte_mensual_prom,
                ROUND(SUM(fc.horas_corte_energia), 1) AS horas_corte_total
            FROM fact_conectividad fc
            JOIN dim_ie ie ON ie.cod_modular = fc.cod_modular
            JOIN dim_nivel dn ON dn.id_nivel = ie.id_nivel
            JOIN dim_local dl ON dl.cod_local = ie.cod_local
            JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
            JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
            JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
            GROUP BY ie.cod_modular, ie.nombre, dn.nivel, dep.nombre, prov.nombre, dist.nombre, dl.cod_local, fc.id_sensor;
            """,
        ]
    )

    # 5. Capa semántica (parámetros) e Inteligencia artificial (sección 8)
    sentencias.extend(
        [
            """
            CREATE TABLE IF NOT EXISTS sem_parametro (
                clave TEXT PRIMARY KEY,
                valor DOUBLE PRECISION NOT NULL,
                valor_defecto DOUBLE PRECISION NOT NULL,
                unidad TEXT,
                descripcion TEXT,
                fecha_actualizacion TIMESTAMPTZ
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ia_ejecucion (
                id_ejecucion SERIAL PRIMARY KEY,
                fecha_inicio TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                fecha_fin TIMESTAMPTZ,
                estado TEXT NOT NULL,
                anio_base SMALLINT,
                anios_entrenamiento TEXT,
                filas_entrenamiento INT,
                auc DOUBLE PRECISION,
                auc_linea_base DOUBLE PRECISION,
                mae_pp DOUBLE PRECISION,
                mae_linea_base_pp DOUBLE PRECISION,
                duracion_ms INT,
                parametros_json TEXT,
                mensaje TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ia_pred_riesgo_ie (
                id SERIAL PRIMARY KEY,
                id_ejecucion INT NOT NULL REFERENCES ia_ejecucion(id_ejecucion) ON DELETE CASCADE,
                cod_modular TEXT NOT NULL,
                cod_local TEXT,
                ubigeo TEXT,
                departamento TEXT,
                distrito TEXT,
                ugel TEXT,
                nivel TEXT,
                anio_base INT,
                matricula_base INT,
                tasa_desercion_base_pct DOUBLE PRECISION,
                prob_riesgo DOUBLE PRECISION,
                semaforo TEXT,
                tasa_esperada_siguiente DOUBLE PRECISION,
                factores_principales TEXT,
                anio_prediccion INT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ia_proyeccion_desertores (
                id SERIAL PRIMARY KEY,
                id_ejecucion INT NOT NULL REFERENCES ia_ejecucion(id_ejecucion) ON DELETE CASCADE,
                departamento TEXT NOT NULL,
                anio INT NOT NULL,
                escenario TEXT NOT NULL,
                tipo TEXT NOT NULL,
                matricula INT,
                desertores INT,
                tasa_pct DOUBLE PRECISION
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ia_inversion_local (
                id SERIAL PRIMARY KEY,
                id_ejecucion INT NOT NULL REFERENCES ia_ejecucion(id_ejecucion) ON DELETE CASCADE,
                cod_local TEXT NOT NULL,
                ubigeo TEXT,
                departamento TEXT,
                distrito TEXT,
                ugel TEXT,
                anio_base INT,
                anio_censo INT,
                matricula_local_base INT,
                cpu_total INT,
                cpu_operativos INT,
                alumnos_por_pc_operativa DOUBLE PRECISION,
                tiene_internet TEXT,
                fuente_energia TEXT,
                equipos_a_reparar INT,
                equipos_a_reponer INT,
                equipos_a_dotar INT,
                kit_conectividad INT,
                accion_sugerida TEXT,
                costo_estimado_soles INT,
                operativos_proy_t1 DOUBLE PRECISION,
                operativos_proy_t2 DOUBLE PRECISION,
                alumnos_por_pc_proy_t2 DOUBLE PRECISION
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ia_priorizacion (
                id SERIAL PRIMARY KEY,
                id_ejecucion INT NOT NULL REFERENCES ia_ejecucion(id_ejecucion) ON DELETE CASCADE,
                ranking INT NOT NULL,
                cod_local TEXT NOT NULL,
                ubigeo TEXT,
                departamento TEXT,
                distrito TEXT,
                ugel TEXT,
                n_ie INT,
                semaforo_max TEXT,
                estudiantes_en_riesgo DOUBLE PRECISION,
                accion_sugerida TEXT,
                costo_estimado_soles INT,
                riesgo_por_mil_soles DOUBLE PRECISION,
                costo_acumulado_soles INT,
                dentro_presupuesto BOOLEAN
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ia_anomalias_iot (
                id SERIAL PRIMARY KEY,
                id_ejecucion INT NOT NULL REFERENCES ia_ejecucion(id_ejecucion) ON DELETE CASCADE,
                id_lectura TEXT,
                id_sensor TEXT,
                cod_modular TEXT,
                mes TEXT,
                tipo_enlace TEXT,
                disponibilidad_internet_pct DOUBLE PRECISION,
                horas_corte_energia DOUBLE PRECISION,
                voltaje_prom_v DOUBLE PRECISION,
                score_anomalia DOUBLE PRECISION,
                motivo TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ia_alertas_cronicas_iot (
                id SERIAL PRIMARY KEY,
                id_ejecucion INT NOT NULL REFERENCES ia_ejecucion(id_ejecucion) ON DELETE CASCADE,
                id_sensor TEXT,
                cod_modular TEXT,
                tipo_enlace TEXT,
                disponibilidad_mediana_pct DOUBLE PRECISION,
                horas_corte_mediana DOUBLE PRECISION,
                alerta TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ia_anomalias_acceso (
                id SERIAL PRIMARY KEY,
                id_ejecucion INT NOT NULL REFERENCES ia_ejecucion(id_ejecucion) ON DELETE CASCADE,
                cod_modular TEXT,
                distrito TEXT,
                tipo_acceso TEXT,
                distancia_capital_distrital_km DOUBLE PRECISION,
                tiempo_acceso_horas DOUBLE PRECISION,
                z_tiempo DOUBLE PRECISION,
                motivo TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS ia_importancia_variables (
                id SERIAL PRIMARY KEY,
                id_ejecucion INT NOT NULL REFERENCES ia_ejecucion(id_ejecucion) ON DELETE CASCADE,
                variable TEXT NOT NULL,
                importancia DOUBLE PRECISION,
                descripcion TEXT
            );
            """,
        ]
    )

    with engine.begin() as conn:
        for ddl in sentencias:
            conn.execute(text(ddl))
        for p in PARAMETROS_INICIALES:
            conn.execute(
                text(
                    """
                    INSERT INTO sem_parametro (clave, valor, valor_defecto, unidad, descripcion, fecha_actualizacion)
                    VALUES (:clave, :valor, :valor, :unidad, :descripcion, NULL)
                    ON CONFLICT (clave) DO NOTHING
                    """
                ),
                p,
            )

    return True


def validar_estructura_csv(df: pd.DataFrame, fuente_id: str) -> tuple[bool, list[str], list[str]]:
    """Valida columnas requeridas sin depender del orden. Devuelve (valido, faltantes, sobrantes)."""
    cfg = FUENTES_CONFIG[fuente_id]
    esperadas = cfg["columnas"]
    presentes = [str(c) for c in df.columns]
    set_presentes = set(presentes)
    set_esperadas = set(esperadas)

    faltantes = [c for c in esperadas if c not in set_presentes]
    sobrantes = [c for c in presentes if c not in set_esperadas]
    return (len(faltantes) == 0, faltantes, sobrantes)


def generar_plantilla_csv(fuente_id: str) -> bytes:
    """Genera un CSV UTF-8 con solo los encabezados canónicos de la fuente."""
    cols = FUENTES_CONFIG[fuente_id]["columnas"]
    buffer = io.StringIO()
    buffer.write(",".join(cols) + "\n")
    return buffer.getvalue().encode("utf-8")


def leer_csv_crudo(origen: Any) -> pd.DataFrame:
    """Lee un CSV conservando todas las columnas como texto y preservando ceros iniciales."""
    if hasattr(origen, "getvalue"):
        contenido = io.BytesIO(origen.getvalue())
        return pd.read_csv(contenido, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    return pd.read_csv(origen, dtype=str, keep_default_na=False, encoding="utf-8-sig")


def obtener_siguiente_lote() -> int:
    """Calcula el siguiente identificador de lote de carga desde PostgreSQL."""
    engine = get_engine()
    with engine.connect() as conn:
        res = conn.execute(text("SELECT COALESCE(MAX(lote), 0) + 1 FROM log_carga")).scalar()
    return int(res or 1)


def obtener_siguiente_lote_etl() -> int:
    """Calcula el siguiente identificador de lote ETL desde PostgreSQL."""
    engine = get_engine()
    with engine.connect() as conn:
        res = conn.execute(text("SELECT COALESCE(MAX(lote), 0) + 1 FROM log_etl")).scalar()
    return int(res or 1)


def _normalizar_celda_staging(valor: Any) -> str | None:
    """Convierte únicamente celdas vacías en NULL; conserva textos y ceros iniciales intactos."""
    if valor is None:
        return None
    texto = str(valor)
    if texto.strip() == "":
        return None
    return texto


def cargar_fuente_staging(
    fuente_id: str,
    df: pd.DataFrame,
    nombre_archivo: str,
    lote: int | None = None,
) -> dict[str, Any]:
    """Reemplaza la tabla stg_* de la fuente mediante inserción masiva en una sola transacción."""
    cfg = FUENTES_CONFIG[fuente_id]
    es_valido, faltantes, sobrantes = validar_estructura_csv(df, fuente_id)
    if not es_valido:
        raise ValueError(
            f"El archivo para {fuente_id} no tiene todas las columnas requeridas. Faltan: {', '.join(faltantes)}"
        )

    columnas_canon = cfg["columnas"]
    tabla = cfg["tabla"]
    df_ordenado = df[columnas_canon].copy()

    inicio = time.perf_counter()
    if lote is None:
        lote = obtener_siguiente_lote()

    registros = [
        (lote, *[_normalizar_celda_staging(v) for v in fila])
        for fila in df_ordenado.itertuples(index=False, name=None)
    ]

    cols_insert = ", ".join(["_lote", *[f'"{c}"' for c in columnas_canon]])
    insert_sql = f"INSERT INTO {tabla} ({cols_insert}) VALUES %s"

    engine = get_engine()
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {tabla} RESTART IDENTITY")
            if registros:
                psycopg2.extras.execute_values(
                    cur,
                    insert_sql,
                    registros,
                    page_size=1000,
                )
            duracion_ms = int((time.perf_counter() - inicio) * 1000)
            mensaje = (
                f"Columnas ignoradas: {', '.join(sobrantes)}"
                if sobrantes
                else "Carga completa sin columnas sobrantes"
            )
            cur.execute(
                """
                INSERT INTO log_carga (lote, fuente, nombre_archivo, filas, columnas, duracion_ms, estado, mensaje)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    lote,
                    fuente_id,
                    nombre_archivo,
                    len(registros),
                    len(columnas_canon),
                    duracion_ms,
                    "ok",
                    mensaje,
                ),
            )
        raw_conn.commit()
    except Exception as exc:
        raw_conn.rollback()
        duracion_ms = int((time.perf_counter() - inicio) * 1000)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO log_carga (lote, fuente, nombre_archivo, filas, columnas, duracion_ms, estado, mensaje)
                        VALUES (:lote, :fuente, :archivo, :filas, :cols, :dur, 'error', :msg)
                        """
                    ),
                    {
                        "lote": lote,
                        "fuente": fuente_id,
                        "archivo": nombre_archivo,
                        "filas": len(df_ordenado),
                        "cols": len(columnas_canon),
                        "dur": duracion_ms,
                        "msg": str(exc)[:300],
                    },
                )
        except Exception:
            pass
        raise
    finally:
        raw_conn.close()

    st.cache_data.clear()
    return {
        "fuente": fuente_id,
        "lote": lote,
        "filas": len(registros),
        "columnas": len(columnas_canon),
        "duracion_ms": duracion_ms,
        "sobrantes": sobrantes,
    }


def cargar_datos_ejemplo(
    ruta_base: str = "data/ejemplo",
    callback_progreso: Callable[[float, str], None] | None = None,
) -> list[dict[str, Any]]:
    """Carga los 5 archivos CSV de ejemplo hacia sus tablas stg_* compartiendo un mismo lote."""
    lote = obtener_siguiente_lote()
    resultados: list[dict[str, Any]] = []
    fuentes = list(FUENTES_CONFIG.keys())
    total = len(fuentes)

    for idx, fuente_id in enumerate(fuentes):
        cfg = FUENTES_CONFIG[fuente_id]
        ruta_archivo = os.path.join(ruta_base, cfg["archivo_ejemplo"])
        if not os.path.exists(ruta_archivo):
            raise FileNotFoundError(f"No se encontró el archivo de ejemplo: {ruta_archivo}")

        if callback_progreso:
            callback_progreso(idx / total, f"Cargando {fuente_id}: {cfg['nombre']}...")

        df = leer_csv_crudo(ruta_archivo)
        res = cargar_fuente_staging(
            fuente_id=fuente_id,
            df=df,
            nombre_archivo=cfg["archivo_ejemplo"],
            lote=lote,
        )
        resultados.append(res)

    if callback_progreso:
        callback_progreso(1.0, "Carga de datos de ejemplo completada.")

    return resultados


def cargar_dw_masivo(
    datos_dw: dict[str, list[tuple[Any, ...]]],
    logs_etl: list[dict[str, Any]],
    reporte_etl: dict[str, Any],
    lote: int,
    duracion_ms: int,
) -> dict[str, int]:
    """Carga el Data Warehouse completo de forma atómica e idempotente mediante inserción masiva."""
    engine = get_engine()
    raw_conn = engine.raw_connection()
    conteos: dict[str, int] = {}

    try:
        with raw_conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE TABLE
                    fact_conectividad,
                    fact_permanencia,
                    dim_ie,
                    dim_local,
                    dim_tiempo,
                    dim_ugel,
                    dim_distrito,
                    dim_provincia,
                    dim_departamento
                RESTART IDENTITY CASCADE
                """
            )

            # Dimensiones estáticas / extensibles con idempotencia
            for id_rur, grad in datos_dw["dim_ruralidad"]:
                cur.execute(
                    """
                    INSERT INTO dim_ruralidad (id_ruralidad, gradiente)
                    VALUES (%s, %s)
                    ON CONFLICT (id_ruralidad) DO UPDATE SET gradiente = EXCLUDED.gradiente
                    """,
                    (id_rur, grad),
                )
            conteos["dim_ruralidad"] = len(datos_dw["dim_ruralidad"])

            for id_niv, niv in datos_dw["dim_nivel"]:
                cur.execute(
                    """
                    INSERT INTO dim_nivel (id_nivel, nivel)
                    VALUES (%s, %s)
                    ON CONFLICT (id_nivel) DO UPDATE SET nivel = EXCLUDED.nivel
                    """,
                    (id_niv, niv),
                )
            conteos["dim_nivel"] = len(datos_dw["dim_nivel"])

            # 1. dim_departamento -> mapa nombre -> id_departamento
            if datos_dw["dim_departamento"]:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO dim_departamento (nombre) VALUES %s",
                    datos_dw["dim_departamento"],
                    page_size=1000,
                )
            cur.execute("SELECT id_departamento, nombre FROM dim_departamento")
            map_dep = {nombre: id_d for id_d, nombre in cur.fetchall()}
            conteos["dim_departamento"] = len(map_dep)

            # 2. dim_provincia -> mapa (nombre_prov, nombre_dep) -> id_provincia
            filas_prov = [
                (prov, map_dep[dep])
                for prov, dep in datos_dw["dim_provincia"]
                if dep in map_dep
            ]
            if filas_prov:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO dim_provincia (nombre, id_departamento) VALUES %s",
                    filas_prov,
                    page_size=1000,
                )
            cur.execute(
                """
                SELECT p.id_provincia, p.nombre, d.nombre
                FROM dim_provincia p
                JOIN dim_departamento d ON d.id_departamento = p.id_departamento
                """
            )
            map_prov = {(nombre_p, nombre_d): id_p for id_p, nombre_p, nombre_d in cur.fetchall()}
            conteos["dim_provincia"] = len(map_prov)

            # 3. dim_distrito
            filas_dist = [
                (ubigeo, nombre_dist, map_prov[(nombre_prov, nombre_dep)])
                for ubigeo, nombre_dist, nombre_prov, nombre_dep in datos_dw["dim_distrito"]
                if (nombre_prov, nombre_dep) in map_prov
            ]
            if filas_dist:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO dim_distrito (ubigeo, nombre, id_provincia) VALUES %s",
                    filas_dist,
                    page_size=1000,
                )
            conteos["dim_distrito"] = len(filas_dist)

            # 4. dim_ugel
            if datos_dw["dim_ugel"]:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO dim_ugel (nombre) VALUES %s",
                    datos_dw["dim_ugel"],
                    page_size=1000,
                )
            cur.execute("SELECT id_ugel, nombre FROM dim_ugel")
            map_ugel = {nombre: id_u for id_u, nombre in cur.fetchall()}
            conteos["dim_ugel"] = len(map_ugel)

            # 5. dim_tiempo
            if datos_dw["dim_tiempo"]:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO dim_tiempo (id_tiempo, anio, mes, trimestre, nombre_mes) VALUES %s",
                    datos_dw["dim_tiempo"],
                    page_size=1000,
                )
            conteos["dim_tiempo"] = len(datos_dw["dim_tiempo"])

            # 6. dim_local
            filas_local = [
                (
                    cod_local,
                    ubigeo,
                    map_ugel.get(ugel_nombre),
                    latitud,
                    longitud,
                    altitud_msnm,
                    distancia_km,
                    tiempo_h,
                    tipo_acceso,
                )
                for (
                    cod_local,
                    ubigeo,
                    ugel_nombre,
                    latitud,
                    longitud,
                    altitud_msnm,
                    distancia_km,
                    tiempo_h,
                    tipo_acceso,
                ) in datos_dw["dim_local"]
            ]
            if filas_local:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dim_local (
                        cod_local, ubigeo, id_ugel, latitud, longitud,
                        altitud_msnm, distancia_capital_km, tiempo_acceso_h, tipo_acceso
                    ) VALUES %s
                    """,
                    filas_local,
                    page_size=1000,
                )
            conteos["dim_local"] = len(filas_local)

            # 7. dim_ie
            if datos_dw["dim_ie"]:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dim_ie (
                        cod_modular, nombre, id_nivel, id_ruralidad, cod_local, kit_aula_conectada
                    ) VALUES %s
                    """,
                    datos_dw["dim_ie"],
                    page_size=1000,
                )
            conteos["dim_ie"] = len(datos_dw["dim_ie"])

            # 8. fact_permanencia
            if datos_dw["fact_permanencia"]:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO fact_permanencia (
                        cod_modular, id_tiempo, matricula_inicial, matricula_final, retirados, desertores,
                        tasa_desercion_pct, asistencia_prom_pct, docentes_total, docentes_titulados,
                        declaro_computo, cpu_total, cpu_operativos, cpu_en_reparacion, cpu_mas_3_anios,
                        tiene_internet, tipo_internet, fuente_energia, tiene_ups, tiene_pozo_tierra, n_equipos_red,
                        precipitacion_anual_mm, dias_sin_clases_clima, temp_min_anual_c,
                        pobreza_pct, hogares_internet_pct, cobertura_movil, estado_registro
                    ) VALUES %s
                    """,
                    datos_dw["fact_permanencia"],
                    page_size=1000,
                )
            conteos["fact_permanencia"] = len(datos_dw["fact_permanencia"])

            # 9. fact_conectividad
            if datos_dw["fact_conectividad"]:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO fact_conectividad (
                        id_lectura, cod_modular, id_tiempo, id_sensor, tipo_enlace,
                        ancho_banda_mbps, latencia_ms, disponibilidad_pct, horas_corte_energia,
                        voltaje_v, carga_bateria_pct, temperatura_aula_c, dispositivos_conectados,
                        estado_sensor, precipitacion_mm, evento_climatico
                    ) VALUES %s
                    """,
                    datos_dw["fact_conectividad"],
                    page_size=1000,
                )
            conteos["fact_conectividad"] = len(datos_dw["fact_conectividad"])

            # 10. Registro en log_etl y log_etl_reporte
            filas_log = [
                (
                    lote,
                    item["regla"],
                    item["fuente"],
                    int(item["filas_afectadas"]),
                    int(item["filas_cargadas"]),
                    item["estado"],
                )
                for item in logs_etl
            ]
            if filas_log:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO log_etl (lote, regla, fuente, filas_afectadas, filas_cargadas, estado)
                    VALUES %s
                    """,
                    filas_log,
                    page_size=1000,
                )

            cur.execute(
                """
                INSERT INTO log_etl_reporte (lote, reporte_json, duracion_ms)
                VALUES (%s, %s, %s)
                """,
                (lote, json.dumps(reporte_etl, ensure_ascii=False), int(duracion_ms)),
            )

        raw_conn.commit()
    except Exception as exc:
        raw_conn.rollback()
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO log_etl (lote, regla, fuente, filas_afectadas, filas_cargadas, estado)
                        VALUES (:lote, :regla, 'DW', 0, 0, 'error')
                        """
                    ),
                    {"lote": lote, "regla": f"Error en ETL: {str(exc)[:200]}"},
                )
        except Exception:
            pass
        raise
    finally:
        raw_conn.close()

    st.cache_data.clear()
    return conteos


@st.cache_data(ttl=300, show_spinner=False)
def obtener_estado_fuentes() -> dict[str, dict[str, Any]]:
    """Obtiene conteo de filas, lote y última fecha de carga para las 5 fuentes desde Neon."""
    engine = get_engine()
    partes_union = [
        f"SELECT '{fid}' AS fuente, COUNT(*)::INT AS filas, MAX(_lote)::INT AS lote FROM {cfg['tabla']}"
        for fid, cfg in FUENTES_CONFIG.items()
    ]
    sql_conteos = " UNION ALL ".join(partes_union)

    sql_logs = """
        SELECT DISTINCT ON (fuente)
            fuente,
            lote,
            nombre_archivo,
            filas,
            duracion_ms,
            estado,
            fecha
        FROM log_carga
        WHERE estado = 'ok'
        ORDER BY fuente, fecha DESC, id DESC
    """

    with engine.connect() as conn:
        conteos_rows = conn.execute(text(sql_conteos)).mappings().all()
        logs_rows = conn.execute(text(sql_logs)).mappings().all()

    logs_por_fuente = {row["fuente"]: dict(row) for row in logs_rows}
    conteos_por_fuente = {row["fuente"]: dict(row) for row in conteos_rows}

    estado: dict[str, dict[str, Any]] = {}
    for fid, cfg in FUENTES_CONFIG.items():
        c = conteos_por_fuente.get(fid, {"filas": 0, "lote": None})
        lg = logs_por_fuente.get(fid, {})
        filas = int(c.get("filas") or 0)
        estado[fid] = {
            "codigo": fid,
            "nombre": cfg["nombre"],
            "tipo_origen": cfg["tipo_origen"],
            "tabla": cfg["tabla"],
            "filas": filas,
            "columnas": len(cfg["columnas"]),
            "lote": c.get("lote") or lg.get("lote"),
            "nombre_archivo": lg.get("nombre_archivo"),
            "fecha_carga": lg.get("fecha"),
            "cargada": filas > 0,
        }
    return estado


@st.cache_data(ttl=60, show_spinner=False)
def obtener_estado_proceso() -> dict[str, Any]:
    """Calcula el estado global del flujo de 7 capas desde PostgreSQL."""
    estado_fuentes = obtener_estado_fuentes()
    cargadas = sum(1 for item in estado_fuentes.values() if item["cargada"])
    fechas_carga = [
        item["fecha_carga"]
        for item in estado_fuentes.values()
        if item["cargada"] and item["fecha_carga"] is not None
    ]
    ultima_carga: datetime | None = max(fechas_carga) if fechas_carga else None

    engine = get_engine()
    with engine.connect() as conn:
        val_row = conn.execute(
            text(
                """
                SELECT fecha, estado, total_hallazgos
                FROM log_staging_validacion
                ORDER BY fecha DESC, id DESC
                LIMIT 1
                """
            )
        ).mappings().first()

        etl_row = conn.execute(
            text(
                """
                SELECT fecha
                FROM log_etl
                WHERE estado = 'ok'
                ORDER BY fecha DESC, id DESC
                LIMIT 1
                """
            )
        ).mappings().first()

        ia_existe = conn.execute(text("SELECT to_regclass('public.ia_ejecucion')")).scalar()
        ia_row = None
        if ia_existe is not None:
            ia_row = conn.execute(
                text(
                    """
                    SELECT fecha_fin AS fecha
                    FROM ia_ejecucion
                    WHERE estado = 'ok'
                    ORDER BY fecha_fin DESC, id_ejecucion DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()

        param_fecha = None
        sem_existe = conn.execute(text("SELECT to_regclass('public.sem_parametro')")).scalar()
        if sem_existe is not None:
            param_fecha = conn.execute(
                text("SELECT MAX(fecha_actualizacion) FROM sem_parametro")
            ).scalar()

    # Estado Staging
    if cargadas == 0 or val_row is None:
        st_staging = {"icono": "⚪", "texto": "Pendiente", "fecha": None}
    elif ultima_carga is not None and val_row["fecha"] < ultima_carga:
        st_staging = {"icono": "🟠", "texto": "Desactualizado", "fecha": val_row["fecha"]}
    else:
        st_staging = {"icono": "🟢", "texto": "Validado", "fecha": val_row["fecha"]}

    # Estado ETL
    if etl_row is None:
        st_etl = {"icono": "⚪", "texto": "Pendiente", "fecha": None}
    elif ultima_carga is not None and etl_row["fecha"] < ultima_carga:
        st_etl = {"icono": "🟠", "texto": "Desactualizado", "fecha": etl_row["fecha"]}
    else:
        st_etl = {"icono": "🟢", "texto": "Ejecutado", "fecha": etl_row["fecha"]}

    # Estado IA
    if ia_row is None or ia_row.get("fecha") is None:
        st_ia = {"icono": "⚪", "texto": "Pendiente", "fecha": None}
    elif (
        (ultima_carga is not None and ia_row["fecha"] < ultima_carga)
        or (etl_row is not None and ia_row["fecha"] < etl_row["fecha"])
        or (param_fecha is not None and ia_row["fecha"] < param_fecha)
    ):
        st_ia = {"icono": "🟠", "texto": "Desactualizado", "fecha": ia_row["fecha"]}
    else:
        st_ia = {"icono": "🟢", "texto": "Ejecutado", "fecha": ia_row["fecha"]}

    return {
        "fuentes_cargadas": cargadas,
        "total_fuentes": len(FUENTES_CONFIG),
        "ultima_carga": ultima_carga,
        "staging": st_staging,
        "etl": st_etl,
        "ia": st_ia,
    }


@st.cache_data(ttl=300, show_spinner=False)
def obtener_vista_previa_staging(
    fuente_id: str,
    busqueda: str = "",
    pagina: int = 1,
    tam_pagina: int = 25,
) -> tuple[pd.DataFrame, int]:
    """Consulta paginada (25 filas por defecto) con filtro de texto sobre stg_*."""
    cfg = FUENTES_CONFIG[fuente_id]
    tabla = cfg["tabla"]
    columnas = cfg["columnas"]
    cols_select = ", ".join(["_fila", "_lote", *[f'"{c}"' for c in columnas]])

    termino = busqueda.strip()
    where_sql = ""
    params: dict[str, Any] = {
        "limit": int(tam_pagina),
        "offset": max(0, (int(pagina) - 1) * int(tam_pagina)),
    }

    if termino:
        cols_busqueda = ", ".join(f'COALESCE("{c}", \'\')' for c in columnas)
        where_sql = f"WHERE CONCAT_WS(' ', {cols_busqueda}) ILIKE :patron"
        params["patron"] = f"%{termino}%"

    sql_total = f"SELECT COUNT(*)::INT FROM {tabla} {where_sql}"
    sql_datos = (
        f"SELECT {cols_select} FROM {tabla} {where_sql} "
        "ORDER BY _fila ASC LIMIT :limit OFFSET :offset"
    )

    engine = get_engine()
    with engine.connect() as conn:
        total = int(conn.execute(text(sql_total), params).scalar() or 0)
        filas = conn.execute(text(sql_datos), params).mappings().all()

    if not filas:
        return pd.DataFrame(columns=["_fila", "_lote", *columnas]), total

    return pd.DataFrame([dict(r) for r in filas]), total


@st.cache_data(ttl=300, show_spinner=False)
def obtener_logs_carga(limite: int = 100) -> pd.DataFrame:
    """Devuelve los registros recientes de log_carga desde PostgreSQL."""
    engine = get_engine()
    sql = """
        SELECT id, lote, fuente, nombre_archivo, filas, columnas, duracion_ms, estado, mensaje, fecha
        FROM log_carga
        ORDER BY fecha DESC, id DESC
        LIMIT :limite
    """
    with engine.connect() as conn:
        filas = conn.execute(text(sql), {"limite": int(limite)}).mappings().all()

    if not filas:
        return pd.DataFrame(
            columns=[
                "id",
                "lote",
                "fuente",
                "nombre_archivo",
                "filas",
                "columnas",
                "duracion_ms",
                "estado",
                "mensaje",
                "fecha",
            ]
        )
    return pd.DataFrame([dict(r) for r in filas])


@st.cache_data(ttl=300, show_spinner=False)
def obtener_logs_etl(limite: int = 150) -> pd.DataFrame:
    """Devuelve los registros recientes de log_etl desde PostgreSQL."""
    engine = get_engine()
    sql = """
        SELECT id, lote, regla, fuente, filas_afectadas, filas_cargadas, estado, fecha
        FROM log_etl
        ORDER BY fecha DESC, id DESC
        LIMIT :limite
    """
    with engine.connect() as conn:
        filas = conn.execute(text(sql), {"limite": int(limite)}).mappings().all()

    if not filas:
        return pd.DataFrame(
            columns=["id", "lote", "regla", "fuente", "filas_afectadas", "filas_cargadas", "estado", "fecha"]
        )
    return pd.DataFrame([dict(r) for r in filas])


@st.cache_data(ttl=300, show_spinner=False)
def obtener_ultimo_reporte_etl() -> dict[str, Any] | None:
    """Recupera el último reporte detallado (antes/después, ejemplos, huérfanas) del ETL."""
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, lote, reporte_json, duracion_ms, fecha
                FROM log_etl_reporte
                ORDER BY fecha DESC, id DESC
                LIMIT 1
                """
            )
        ).mappings().first()

    if not row:
        return None

    res = dict(row)
    res["reporte"] = json.loads(res["reporte_json"])
    return res


@st.cache_data(ttl=300, show_spinner=False)
def leer_tabla_staging(fuente_id: str) -> pd.DataFrame:
    """Lee la tabla cruda completa de staging conservando cadenas vacías."""
    cfg = FUENTES_CONFIG[fuente_id]
    tabla = cfg["tabla"]
    columnas = cfg["columnas"]
    cols_select = ", ".join(f'"{c}"' for c in columnas)

    engine = get_engine()
    with engine.connect() as conn:
        filas = conn.execute(text(f"SELECT {cols_select} FROM {tabla} ORDER BY _fila ASC")).mappings().all()

    if not filas:
        return pd.DataFrame(columns=columnas)

    df = pd.DataFrame([dict(r) for r in filas])
    return df.fillna("")


def guardar_validacion_staging(resumen: dict[str, Any], total_hallazgos: int, duracion_ms: int) -> int:
    """Registra la ejecución de validación de staging en PostgreSQL y limpia caché."""
    engine = get_engine()
    with engine.begin() as conn:
        lote_actual = int(
            conn.execute(text("SELECT COALESCE(MAX(lote), 0) FROM log_carga")).scalar() or 0
        )
        conn.execute(
            text(
                """
                INSERT INTO log_staging_validacion (lote_validacion, estado, total_hallazgos, resumen_json, duracion_ms)
                VALUES (:lote, 'ok', :hallazgos, :resumen, :duracion)
                """
            ),
            {
                "lote": lote_actual,
                "hallazgos": int(total_hallazgos),
                "resumen": json.dumps(resumen, ensure_ascii=False),
                "duracion": int(duracion_ms),
            },
        )
    st.cache_data.clear()
    return lote_actual


@st.cache_data(ttl=300, show_spinner=False)
def obtener_ultima_validacion_staging() -> dict[str, Any] | None:
    """Recupera el último reporte persistido de validación de staging."""
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, lote_validacion, estado, total_hallazgos, resumen_json, duracion_ms, fecha
                FROM log_staging_validacion
                ORDER BY fecha DESC, id DESC
                LIMIT 1
                """
            )
        ).mappings().first()

    if not row:
        return None

    resultado = dict(row)
    resultado["resumen"] = json.loads(resultado["resumen_json"])
    return resultado


@st.cache_data(ttl=300, show_spinner=False)
def obtener_resumen_tablas_dw() -> pd.DataFrame:
    """Consulta el conteo actual de filas de todas las tablas del Data Warehouse."""
    engine = get_engine()
    partes = [
        f"SELECT '{m['tabla']}' AS tabla, COUNT(*)::INT AS filas FROM {m['tabla']}"
        for m in DW_TABLAS_META
    ]
    sql = " UNION ALL ".join(partes)
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()

    conteos = {r["tabla"]: int(r["filas"]) for r in rows}
    salida = []
    for meta in DW_TABLAS_META:
        salida.append(
            {
                "Tabla": meta["tabla"],
                "Tipo": meta["tipo"],
                "Clave primaria": meta["pk"],
                "Claves foráneas": meta["fk"],
                "Filas": conteos.get(meta["tabla"], 0),
                "Descripción": meta["descripcion"],
            }
        )
    return pd.DataFrame(salida)


@st.cache_data(ttl=300, show_spinner=False)
def obtener_columnas_tabla_dw(nombre_tabla: str) -> pd.DataFrame:
    """Lee las columnas y tipos de una tabla del DW desde information_schema."""
    validas = {m["tabla"] for m in DW_TABLAS_META}
    if nombre_tabla not in validas:
        raise ValueError(f"Tabla no permitida: {nombre_tabla}")

    engine = get_engine()
    sql = """
        SELECT
            column_name AS columna,
            data_type AS tipo_dato,
            is_nullable AS admite_nulo
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :tabla
        ORDER BY ordinal_position
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"tabla": nombre_tabla}).mappings().all()

    return pd.DataFrame([dict(r) for r in rows])


@st.cache_data(ttl=300, show_spinner=False)
def obtener_opciones_filtros_dw() -> dict[str, Any]:
    """Obtiene opciones dinámicas (departamento, distritos por dep, nivel) desde el DW."""
    engine = get_engine()
    with engine.connect() as conn:
        deps = [
            r[0]
            for r in conn.execute(
                text("SELECT nombre FROM dim_departamento ORDER BY nombre")
            ).fetchall()
        ]
        dist_rows = conn.execute(
            text(
                """
                SELECT DISTINCT dep.nombre AS departamento, dist.nombre AS distrito
                FROM dim_distrito dist
                JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
                JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
                ORDER BY dep.nombre, dist.nombre
                """
            )
        ).mappings().all()
        niveles = [
            r[0]
            for r in conn.execute(text("SELECT nivel FROM dim_nivel ORDER BY id_nivel")).fetchall()
        ]

    return {
        "departamentos": deps,
        "distritos": [dict(r) for r in dist_rows],
        "niveles": niveles,
    }


@st.cache_data(ttl=300, show_spinner=False)
def consultar_vista_dw(
    nombre_vista: str,
    departamento: str = "Todos",
    distrito: str = "Todos",
    nivel: str = "Todos",
    limite: int = 500,
) -> pd.DataFrame:
    """Consulta una vista predefinida vw_* con filtros opcionales seguros."""
    if nombre_vista not in VISTAS_PREDEFINIDAS:
        raise ValueError(f"Vista no permitida: {nombre_vista}")

    cfg = VISTAS_PREDEFINIDAS[nombre_vista]
    filtros_soportados = set(cfg["filtros"])
    condiciones: list[str] = []
    params: dict[str, Any] = {"limite": int(limite)}

    if "departamento" in filtros_soportados and departamento != "Todos":
        condiciones.append("departamento = :departamento")
        params["departamento"] = departamento

    if "distrito" in filtros_soportados and distrito != "Todos":
        condiciones.append("distrito = :distrito")
        params["distrito"] = distrito

    if "nivel" in filtros_soportados and nivel != "Todos":
        if nombre_vista == "vw_kpi_local_base":
            condiciones.append("nivel ILIKE :nivel_like")
            params["nivel_like"] = f"%{nivel}%"
        else:
            condiciones.append("nivel = :nivel")
            params["nivel"] = nivel

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""
    sql = f"SELECT * FROM {nombre_vista} {where_sql} ORDER BY {cfg['orden']} LIMIT :limite"

    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    if not rows:
        with engine.connect() as conn:
            cols_rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :vista
                    ORDER BY ordinal_position
                    """
                ),
                {"vista": nombre_vista},
            ).fetchall()
        return pd.DataFrame(columns=[r[0] for r in cols_rows])

    return pd.DataFrame([dict(r) for r in rows])


def _limpiar_valor_db(valor: Any) -> Any:
    """Convierte valores nulos de pandas/numpy a None y tipos numpy a nativos Python."""
    if valor is None:
        return None
    try:
        if pd.isna(valor):
            return None
    except Exception:
        pass
    if hasattr(valor, "item"):
        return valor.item()
    return valor


@st.cache_data(ttl=300, show_spinner=False)
def obtener_parametros_semantica() -> pd.DataFrame:
    """Lee la tabla sem_parametro completa desde PostgreSQL."""
    engine = get_engine()
    sql = """
        SELECT clave, valor, valor_defecto, unidad, descripcion, fecha_actualizacion
        FROM sem_parametro
        ORDER BY clave
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    return pd.DataFrame([dict(r) for r in rows])


def construir_config_ia() -> dict[str, Any]:
    """Lee sem_parametro desde PostgreSQL y arma el diccionario config para ejecutar_ia."""
    df_params = obtener_parametros_semantica()
    planos = {row["clave"]: float(row["valor"]) for _, row in df_params.iterrows()}

    claves_directas = [
        "promedio_nacional_pct",
        "multiplo_riesgo_alto",
        "semaforo_rojo",
        "semaforo_amarillo",
        "alumnos_por_pc_meta",
        "presupuesto_soles",
        "baja_anual_base",
        "baja_anual_extra_obsolescencia",
        "efecto_intervencion",
        "shock_pesimista",
        "contaminacion_anomalias",
    ]
    config: dict[str, Any] = {k: planos[k] for k in claves_directas if k in planos}
    if "alumnos_por_pc_meta" in config:
        config["alumnos_por_pc_meta"] = int(round(config["alumnos_por_pc_meta"]))

    config["costos_soles"] = {
        "reparar": int(round(planos.get("costo_reparar", 250))),
        "reponer": int(round(planos.get("costo_reponer", 2200))),
        "dotar": int(round(planos.get("costo_dotar", 2500))),
        "kit_conectividad": int(round(planos.get("costo_kit_conectividad", 6000))),
    }
    return config


def leer_vistas_ml_dw() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Lee en vivo las cinco vistas vw_ml_t1 .. vw_ml_t5 desde el Data Warehouse con pd.read_sql."""
    engine = get_engine()
    with engine.connect() as conn:
        v1 = pd.read_sql(text("SELECT * FROM vw_ml_t1"), conn)
        v2 = pd.read_sql(text("SELECT * FROM vw_ml_t2"), conn)
        v3 = pd.read_sql(text("SELECT * FROM vw_ml_t3"), conn)
        v4 = pd.read_sql(text("SELECT * FROM vw_ml_t4"), conn)
        v5 = pd.read_sql(text("SELECT * FROM vw_ml_t5"), conn)
    return v1, v2, v3, v4, v5


def iniciar_ejecucion_ia(config_ia: dict[str, Any]) -> int:
    """Registra una fila en_curso en ia_ejecucion evitando ejecuciones simultáneas (< 10 min)."""
    engine = get_engine()
    param_json = json.dumps({"supuestos": config_ia}, ensure_ascii=False, default=str)

    with engine.begin() as conn:
        activa = conn.execute(
            text(
                """
                SELECT id_ejecucion, fecha_inicio
                FROM ia_ejecucion
                WHERE estado = 'en_curso'
                  AND fecha_inicio > NOW() - INTERVAL '10 minutes'
                ORDER BY fecha_inicio DESC
                LIMIT 1
                """
            )
        ).mappings().first()

        if activa is not None:
            raise RuntimeError(
                "Ya existe una ejecución de modelos en curso iniciada recientemente "
                f"(ejecución #{activa['id_ejecucion']}). Espera a que termine antes de lanzar otra."
            )

        conn.execute(
            text(
                """
                UPDATE ia_ejecucion
                SET estado = 'error',
                    fecha_fin = NOW(),
                    mensaje = 'Ejecución previa expirada por superar los 10 minutos.'
                WHERE estado = 'en_curso'
                  AND fecha_inicio <= NOW() - INTERVAL '10 minutes'
                """
            )
        )

        nuevo_id = conn.execute(
            text(
                """
                INSERT INTO ia_ejecucion (estado, parametros_json, mensaje)
                VALUES ('en_curso', :param_json, 'Ejecución en curso...')
                RETURNING id_ejecucion
                """
            ),
            {"param_json": param_json},
        ).scalar_one()

    return int(nuevo_id)


def finalizar_ejecucion_ia_error(id_ejecucion: int, mensaje: str, duracion_ms: int) -> None:
    """Marca una ejecución de IA como error en ia_ejecucion y limpia caché."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE ia_ejecucion
                SET estado = 'error',
                    fecha_fin = NOW(),
                    duracion_ms = :duracion_ms,
                    mensaje = :mensaje
                WHERE id_ejecucion = :id_ejecucion
                """
            ),
            {
                "id_ejecucion": int(id_ejecucion),
                "duracion_ms": int(duracion_ms),
                "mensaje": str(mensaje)[:1000],
            },
        )
    st.cache_data.clear()


def _preparar_filas_ia(
    id_ejecucion: int,
    df: pd.DataFrame,
    columnas: list[str],
) -> list[tuple[Any, ...]]:
    if df is None or df.empty:
        return []
    sub = df[columnas]
    return [
        (id_ejecucion, *[_limpiar_valor_db(v) for v in fila])
        for fila in sub.itertuples(index=False, name=None)
    ]


def guardar_resultados_ia_masivo(
    id_ejecucion: int,
    resultado: dict[str, Any],
    config_ia: dict[str, Any],
    duracion_ms: int,
) -> None:
    """Reemplaza masivamente las 8 tablas ia_* y actualiza ia_ejecucion en una sola transacción."""
    especificaciones: list[tuple[str, str, list[str]]] = [
        (
            "ia_pred_riesgo_ie",
            "pred_riesgo_ie",
            [
                "cod_modular",
                "cod_local",
                "ubigeo",
                "departamento",
                "distrito",
                "ugel",
                "nivel",
                "anio_base",
                "matricula_base",
                "tasa_desercion_base_pct",
                "prob_riesgo",
                "semaforo",
                "tasa_esperada_siguiente",
                "factores_principales",
                "anio_prediccion",
            ],
        ),
        (
            "ia_proyeccion_desertores",
            "proyeccion_desertores",
            ["departamento", "anio", "escenario", "tipo", "matricula", "desertores", "tasa_pct"],
        ),
        (
            "ia_inversion_local",
            "inversion_local",
            [
                "cod_local",
                "ubigeo",
                "departamento",
                "distrito",
                "ugel",
                "anio_base",
                "anio_censo",
                "matricula_local_base",
                "cpu_total",
                "cpu_operativos",
                "alumnos_por_pc_operativa",
                "tiene_internet",
                "fuente_energia",
                "equipos_a_reparar",
                "equipos_a_reponer",
                "equipos_a_dotar",
                "kit_conectividad",
                "accion_sugerida",
                "costo_estimado_soles",
                "operativos_proy_t1",
                "operativos_proy_t2",
                "alumnos_por_pc_proy_t2",
            ],
        ),
        (
            "ia_priorizacion",
            "priorizacion",
            [
                "ranking",
                "cod_local",
                "ubigeo",
                "departamento",
                "distrito",
                "ugel",
                "n_ie",
                "semaforo_max",
                "estudiantes_en_riesgo",
                "accion_sugerida",
                "costo_estimado_soles",
                "riesgo_por_mil_soles",
                "costo_acumulado_soles",
                "dentro_presupuesto",
            ],
        ),
        (
            "ia_anomalias_iot",
            "anomalias_iot",
            [
                "id_lectura",
                "id_sensor",
                "cod_modular",
                "mes",
                "tipo_enlace",
                "disponibilidad_internet_pct",
                "horas_corte_energia",
                "voltaje_prom_v",
                "score_anomalia",
                "motivo",
            ],
        ),
        (
            "ia_alertas_cronicas_iot",
            "alertas_cronicas_iot",
            [
                "id_sensor",
                "cod_modular",
                "tipo_enlace",
                "disponibilidad_mediana_pct",
                "horas_corte_mediana",
                "alerta",
            ],
        ),
        (
            "ia_anomalias_acceso",
            "anomalias_acceso",
            [
                "cod_modular",
                "distrito",
                "tipo_acceso",
                "distancia_capital_distrital_km",
                "tiempo_acceso_horas",
                "z_tiempo",
                "motivo",
            ],
        ),
        (
            "ia_importancia_variables",
            "importancia_variables",
            ["variable", "importancia", "descripcion"],
        ),
    ]

    resumen = resultado["metricas"]
    met_modelo = resumen["metricas"]
    clasif = met_modelo["clasificacion"]
    reg = met_modelo["regresion_tasa"]
    anios_ent_txt = ", ".join(str(a) for a in met_modelo.get("anios_entrenamiento", []))
    param_payload = json.dumps(
        {"supuestos": config_ia, "resumen": resumen},
        ensure_ascii=False,
        default=str,
    )

    engine = get_engine()
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(
                f"TRUNCATE TABLE {', '.join(IA_TABLAS_RESULTADOS)} RESTART IDENTITY"
            )
            for tabla_db, clave_res, columnas in especificaciones:
                filas = _preparar_filas_ia(id_ejecucion, resultado[clave_res], columnas)
                if filas:
                    cols_sql = ", ".join(["id_ejecucion", *columnas])
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {tabla_db} ({cols_sql}) VALUES %s",
                        filas,
                        page_size=1000,
                    )

            cur.execute(
                """
                UPDATE ia_ejecucion
                SET fecha_fin = NOW(),
                    estado = 'ok',
                    anio_base = %s,
                    anios_entrenamiento = %s,
                    filas_entrenamiento = %s,
                    auc = %s,
                    auc_linea_base = %s,
                    mae_pp = %s,
                    mae_linea_base_pp = %s,
                    duracion_ms = %s,
                    parametros_json = %s,
                    mensaje = %s
                WHERE id_ejecucion = %s
                """,
                (
                    int(met_modelo["anio_base"]),
                    anios_ent_txt,
                    int(met_modelo["filas_entrenamiento"]),
                    float(clasif["auc_modelo"]),
                    float(clasif["auc_linea_base_persistencia"]),
                    float(reg["mae_modelo_pp"]),
                    float(reg["mae_linea_base_persistencia_pp"]),
                    int(duracion_ms),
                    param_payload,
                    "Modelos entrenados y resultados actualizados correctamente.",
                    int(id_ejecucion),
                ),
            )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    st.cache_data.clear()


@st.cache_data(ttl=300, show_spinner=False)
def obtener_ultima_ejecucion_ia() -> dict[str, Any] | None:
    """Recupera los metadatos y métricas de la última ejecución exitosa de IA."""
    engine = get_engine()
    sql = """
        SELECT id_ejecucion, fecha_inicio, fecha_fin, estado, anio_base,
               anios_entrenamiento, filas_entrenamiento, auc, auc_linea_base,
               mae_pp, mae_linea_base_pp, duracion_ms, parametros_json, mensaje
        FROM ia_ejecucion
        WHERE estado = 'ok'
        ORDER BY fecha_fin DESC, id_ejecucion DESC
        LIMIT 1
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().first()

    if not row:
        return None

    res = dict(row)
    try:
        res["detalle_json"] = json.loads(res.get("parametros_json") or "{}")
    except Exception:
        res["detalle_json"] = {}
    return res


@st.cache_data(ttl=300, show_spinner=False)
def obtener_tabla_ia(nombre_tabla: str) -> pd.DataFrame:
    """Lee una tabla ia_* desde PostgreSQL (Neon)."""
    if nombre_tabla not in IA_TABLAS_RESULTADOS:
        raise ValueError(f"Tabla de IA no permitida: {nombre_tabla}")

    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT * FROM {nombre_tabla} ORDER BY id ASC")).mappings().all()

    if not rows:
        with engine.connect() as conn:
            cols_rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :tabla
                    ORDER BY ordinal_position
                    """
                ),
                {"tabla": nombre_tabla},
            ).fetchall()
        return pd.DataFrame(columns=[r[0] for r in cols_rows])

    return pd.DataFrame([dict(r) for r in rows])


@st.cache_data(ttl=300, show_spinner=False)
def obtener_serie_sensor_dw(id_sensor: str) -> pd.DataFrame:
    """Obtiene la serie mensual completa de un sensor desde vw_ml_t3 para graficar anomalías."""
    engine = get_engine()
    sql = """
        SELECT id_lectura, id_sensor, cod_modular, mes, tipo_enlace,
               ancho_banda_prom_mbps, latencia_prom_ms,
               disponibilidad_internet_pct, horas_corte_energia,
               voltaje_prom_v, estado_sensor
        FROM vw_ml_t3
        WHERE id_sensor = :id_sensor
        ORDER BY mes ASC
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"id_sensor": id_sensor}).mappings().all()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])
