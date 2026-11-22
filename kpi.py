"""Capa semántica (catálogo de 9 KPIs, sem_parametro, sem_glosario, jerarquías) y consultas SQL para BI."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import text

from db import PARAMETROS_INICIALES, get_engine, obtener_parametros_semantica

COMPONENTES_INDICE_CONECTIVIDAD: list[dict[str, Any]] = [
    {
        "componente": "Equipamiento",
        "peso": "40 % (0.40)",
        "formula": "100 × (0.5 × mín(1, % operativos) + 0.5 × mín(1, meta_alumnos_pc ÷ alumnos por PC operativa))",
        "nota": "Calculado a nivel de local escolar (sin doble conteo de equipos). Si no hay PCs operativas, aporta 0.",
    },
    {
        "componente": "Internet",
        "peso": "30 % (0.30)",
        "formula": "Disponibilidad media del sensor IoT (excluyendo 'Sin señal'); sin sensor: 60 si el local tiene Internet, 0 si no",
        "nota": "Combina telemetría real en tiempo de aula con declaración censal del local cuando no existe sensor.",
    },
    {
        "componente": "Energía",
        "peso": "30 % (0.30)",
        "formula": "100 × (1 − mín(1, horas de corte al mes ÷ 100)); sin sensor: Red pública 90, Panel solar 70, Grupo electrógeno 50, Sin energía 0",
        "nota": "Penaliza cortes eléctricos prolongados observados por el sensor o falta de red continua en el local.",
    },
]

JERARQUIAS_SEMANTICAS: list[dict[str, str]] = [
    {
        "jerarquia": "Territorial (navegación drill-down)",
        "niveles": "Departamento → Provincia → Distrito (ubigeo) → Local escolar (cod_local) → Institución (cod_modular)",
        "tablas_dw": "dim_departamento → dim_provincia → dim_distrito → dim_local → dim_ie",
        "uso": "Agregaciones geográficas, comparativos regionales y exploración jerárquica en Visualización BI.",
    },
    {
        "jerarquia": "Administrativa (gestión educativa)",
        "niveles": "UGEL → Local escolar (cod_local) → Institución Educativa (cod_modular)",
        "tablas_dw": "dim_ugel → dim_local → dim_ie",
        "uso": "Seguimiento operativo por Unidad de Gestión Educativa Local y priorización de intervenciones.",
    },
    {
        "jerarquia": "Caracterización de institución",
        "niveles": "Nivel educativo (Primaria / Secundaria) · Gradiente de ruralidad (Rural 1 / Rural 2 / Rural 3)",
        "tablas_dw": "dim_nivel · dim_ruralidad → dim_ie",
        "uso": "Segmentación por etapa escolar y nivel de aislamiento geográfico.",
    },
    {
        "jerarquia": "Temporal",
        "niveles": "Año → Trimestre → Mes (id_tiempo = año×100 + mes; mes = 0 para cierre anual)",
        "tablas_dw": "dim_tiempo → fact_permanencia / fact_conectividad",
        "uso": "Series interanuales de matrícula/deserción y series mensuales de telemetría IoT.",
    },
]

GLOSARIO_INICIAL: list[dict[str, str]] = [
    {
        "termino": "ubigeo",
        "categoria": "Territorio",
        "definicion": "Código oficial del INEI de 6 dígitos (texto con cero inicial significativo) que identifica de forma única a un distrito del Perú (2 departamento + 2 provincia + 2 distrito).",
        "fuente_referencia": "T1, T2, T4, T5 · dim_distrito",
    },
    {
        "termino": "COD_MOD (cod_modular)",
        "categoria": "Institución",
        "definicion": "Código modular de 7 dígitos (texto con ceros iniciales) que identifica a cada institución educativa o servicio educativo en el padrón del MINEDU.",
        "fuente_referencia": "T1, T2, T3 · dim_ie",
    },
    {
        "termino": "CODLOCAL (cod_local)",
        "categoria": "Infraestructura",
        "definicion": "Código de 6 dígitos (texto con ceros iniciales) que identifica la infraestructura física del local escolar. Un mismo local puede albergar hasta dos instituciones (ej. Primaria y Secundaria).",
        "fuente_referencia": "T2, T4 · dim_local",
    },
    {
        "termino": "UGEL",
        "categoria": "Gestión",
        "definicion": "Unidad de Gestión Educativa Local: instancia operativa descentralizada encargada de supervisar, acompañar y gestionar recursos para las instituciones educativas de su jurisdicción.",
        "fuente_referencia": "T2 · dim_ugel",
    },
    {
        "termino": "DRE",
        "categoria": "Gestión",
        "definicion": "Dirección Regional de Educación (o Gerencia Regional de Educación): órgano técnico normativo de alcance departamental que coordina a las UGEL de su región.",
        "fuente_referencia": "Contexto MINEDU / Departamental",
    },
    {
        "termino": "Gradiente de ruralidad",
        "categoria": "Territorio",
        "definicion": "Clasificación de aislamiento del servicio educativo rural en Rural 1, Rural 2 o Rural 3; a mayor número, mayor distancia o dificultad de acceso respecto a centros urbanos.",
        "fuente_referencia": "T2 · dim_ruralidad",
    },
    {
        "termino": "Deserción escolar (tasa interanual)",
        "categoria": "Permanencia",
        "definicion": "Porcentaje de estudiantes matriculados al inicio del año lectivo que dejaron de asistir de forma interanual: (Σ desertores ÷ Σ matrícula inicial) × 100.",
        "fuente_referencia": "T1 · fact_permanencia",
    },
    {
        "termino": "Matrícula inicial",
        "categoria": "Permanencia",
        "definicion": "Número total de estudiantes registrados formalmente al inicio del año escolar en una institución educativa.",
        "fuente_referencia": "T1 · fact_permanencia",
    },
    {
        "termino": "Matrícula final y retirados",
        "categoria": "Permanencia",
        "definicion": "Matrícula final refleja los estudiantes que concluyen el año; retirados corresponde a quienes formalizaron su retiro durante el periodo lectivo.",
        "fuente_referencia": "T1 · fact_permanencia",
    },
    {
        "termino": "Semáforo de riesgo",
        "categoria": "Inteligencia artificial",
        "definicion": "Clasificación de prioridad preventiva según probabilidad de riesgo alto (prob_riesgo): 🔴 Rojo (≥ semaforo_rojo), 🟡 Amarillo (entre semaforo_amarillo y semaforo_rojo) y 🟢 Verde (< semaforo_amarillo).",
        "fuente_referencia": "ia_pred_riesgo_ie · sem_parametro",
    },
    {
        "termino": "Alumnos por PC operativa",
        "categoria": "Equipamiento",
        "definicion": "Indicador calculado a nivel de local escolar (suma de matrícula de todas las IEs del local dividida entre computadoras operativas del local) para evitar el doble conteo de equipos compartidos.",
        "fuente_referencia": "T1 + T4 · vw_kpi_local_base",
    },
    {
        "termino": "Kit Aula Conectada",
        "categoria": "Conectividad",
        "definicion": "Paquete tecnológico de conectividad, energía de respaldo o sensores IoT asignado a una institución o recomendado para locales sin acceso a Internet.",
        "fuente_referencia": "T2, T3 · dim_ie",
    },
    {
        "termino": "Estado «Sin señal» (IoT)",
        "categoria": "Conectividad",
        "definicion": "Lectura mensual en la que el sensor IoT no reportó métricas de red válidas; se conserva para contabilizar caídas críticas, pero se excluye del promedio de disponibilidad de Internet.",
        "fuente_referencia": "T3 · fact_conectividad",
    },
    {
        "termino": "Año base",
        "categoria": "Inteligencia artificial",
        "definicion": "Último año lectivo presente en T1; se determina dinámicamente a partir de los datos cargados y sirve de punto de partida para predecir año base + 1 y + 2.",
        "fuente_referencia": "ia_ejecucion · vw_ml_t1",
    },
    {
        "termino": "Escenarios de proyección",
        "categoria": "Inteligencia artificial",
        "definicion": "Simulación departamental de desertores para año base + 1 y + 2: Base (tendencia sin nuevas intervenciones), Optimista (reducción por locales financiados) y Pesimista (shock por deterioro climático/conectividad).",
        "fuente_referencia": "ia_proyeccion_desertores",
    },
]

CATALOGO_KPIS: list[dict[str, Any]] = [
    {
        "codigo": "tasa_desercion_pct",
        "nombre": "Tasa de deserción (%)",
        "categoria": "Permanencia escolar",
        "formula": "Σ desertores ÷ Σ matrícula inicial × 100",
        "unidad": "%",
        "meta_defecto": "< 2.08 % (promedio nacional 2023-2024)",
        "param_meta": "promedio_nacional_pct",
        "sql": """SELECT
    dt.anio,
    dep.nombre AS departamento,
    SUM(fp.matricula_inicial) AS matricula_inicial,
    SUM(fp.desertores) AS desertores,
    ROUND(SUM(fp.desertores)::NUMERIC / NULLIF(SUM(fp.matricula_inicial), 0) * 100, 2) AS tasa_desercion_pct
FROM fact_permanencia fp
JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
JOIN dim_local dl ON dl.cod_local = ie.cod_local
JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
GROUP BY dt.anio, dep.nombre
ORDER BY dt.anio, dep.nombre;""",
    },
    {
        "codigo": "alumnos_por_pc_operativa",
        "nombre": "Alumnos por PC operativa",
        "categoria": "Equipamiento (local)",
        "formula": "Σ matrícula de las instituciones del local ÷ cpu_operativos del local (a nivel de local, sin doble conteo)",
        "unidad": "estudiantes/PC",
        "meta_defecto": "≤ 20 estudiantes/PC",
        "param_meta": "alumnos_por_pc_meta",
        "sql": """WITH censo_local AS (
    SELECT DISTINCT ON (ie.cod_local)
        ie.cod_local,
        MAX(fp.cpu_operativos) AS cpu_operativos,
        SUM(fp.matricula_inicial) AS matricula_local
    FROM fact_permanencia fp
    JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
    JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
    WHERE fp.declaro_computo IS NOT NULL
    GROUP BY ie.cod_local, dt.anio, ar.anio
    ORDER BY ie.cod_local, dt.anio DESC
)
SELECT
    ROUND(SUM(matricula_local)::NUMERIC / NULLIF(SUM(cpu_operativos), 0), 1) AS alumnos_por_pc_operativa
FROM censo_local;""",
    },
    {
        "codigo": "equipos_operativos_pct",
        "nombre": "Equipos operativos (%)",
        "categoria": "Equipamiento (local)",
        "formula": "cpu_operativos ÷ cpu_total × 100 (agregado por local escolar, sin doble conteo)",
        "unidad": "%",
        "meta_defecto": "> 80.9 %",
        "param_meta": None,
        "sql": """WITH censo_local AS (
    SELECT DISTINCT ON (ie.cod_local)
        ie.cod_local,
        MAX(fp.cpu_operativos) AS cpu_operativos,
        MAX(fp.cpu_total) AS cpu_total
    FROM fact_permanencia fp
    JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
    JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
    WHERE fp.declaro_computo IS NOT NULL
    GROUP BY ie.cod_local, dt.anio, ar.anio
    ORDER BY ie.cod_local, dt.anio DESC
)
SELECT
    ROUND(SUM(cpu_operativos)::NUMERIC / NULLIF(SUM(cpu_total), 0) * 100, 1) AS equipos_operativos_pct
FROM censo_local;""",
    },
    {
        "codigo": "disponibilidad_internet_pct",
        "nombre": "Disponibilidad de Internet (%)",
        "categoria": "Conectividad IoT",
        "formula": "Promedio mensual de disponibilidad_pct en sensores IoT (excluye lecturas 'Sin señal')",
        "unidad": "%",
        "meta_defecto": "≥ 90 %",
        "param_meta": None,
        "sql": """SELECT
    ROUND(AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal'), 1) AS disponibilidad_internet_pct,
    COUNT(*) FILTER (WHERE fc.estado_sensor <> 'Sin señal') AS lecturas_validas,
    COUNT(*) FILTER (WHERE fc.estado_sensor = 'Sin señal') AS lecturas_sin_senal
FROM fact_conectividad fc;""",
    },
    {
        "codigo": "horas_corte_energia",
        "nombre": "Horas de corte de energía",
        "categoria": "Conectividad IoT",
        "formula": "Promedio mensual (y suma acumulada) de horas_corte_energia registradas por sensor y mes",
        "unidad": "horas/mes",
        "meta_defecto": "≤ 24 h por mes",
        "param_meta": None,
        "sql": """SELECT
    ROUND(AVG(fc.horas_corte_energia), 1) AS horas_corte_mensual_prom,
    ROUND(SUM(fc.horas_corte_energia), 1) AS horas_corte_total
FROM fact_conectividad fc;""",
    },
    {
        "codigo": "indice_conectividad_rural",
        "nombre": "Índice de conectividad rural (0-100)",
        "categoria": "Índice compuesto",
        "formula": "0.40 × Equipamiento + 0.30 × Internet + 0.30 × Energía (ver descomposición de componentes)",
        "unidad": "puntos (0-100)",
        "meta_defecto": "≥ 70 puntos",
        "param_meta": "alumnos_por_pc_meta",
        "sql": """-- Calculado por institución usando equipamiento del local (sin doble conteo),
-- telemetría IoT de la IE y respaldos censales de Internet/energía del local:
SELECT
    ROUND(AVG(
        0.40 * comp_equipamiento + 0.30 * comp_internet + 0.30 * comp_energia
    )::NUMERIC, 1) AS indice_conectividad_rural
FROM vw_indice_conectividad_ie;  -- Equivalente en CTE dinámica por filtros activos""",
    },
    {
        "codigo": "prob_riesgo_desercion",
        "nombre": "Probabilidad de riesgo de deserción",
        "categoria": "Inteligencia artificial",
        "formula": "prob_riesgo del Módulo 1 (Random Forest sobre 34 variables del año base)",
        "unidad": "probabilidad (0-1)",
        "meta_defecto": "Rojo ≥ 0.60 · Amarillo 0.35–0.59 · Verde < 0.35",
        "param_meta": "semaforo_umbral",
        "sql": """SELECT
    ROUND(AVG(prob_riesgo)::NUMERIC, 3) AS prob_riesgo_promedio,
    COUNT(*) FILTER (WHERE semaforo = 'Rojo') AS ie_rojo,
    COUNT(*) FILTER (WHERE semaforo = 'Amarillo') AS ie_amarillo,
    COUNT(*) FILTER (WHERE semaforo = 'Verde') AS ie_verde
FROM ia_pred_riesgo_ie;""",
    },
    {
        "codigo": "desertores_proyectados_t2",
        "nombre": "Desertores proyectados (año base + 2)",
        "categoria": "Inteligencia artificial",
        "formula": "Σ desertores proyectados en ia_proyeccion_desertores para año base + 2 por escenario",
        "unidad": "estudiantes",
        "meta_defecto": "Menor que el escenario base",
        "param_meta": "efecto_intervencion",
        "sql": """SELECT
    anio,
    escenario,
    SUM(desertores) AS desertores_proyectados,
    ROUND(SUM(desertores)::NUMERIC / NULLIF(SUM(matricula), 0) * 100, 2) AS tasa_pct
FROM ia_proyeccion_desertores
WHERE tipo = 'proyeccion'
  AND anio = (SELECT MAX(anio) FROM ia_proyeccion_desertores)
GROUP BY anio, escenario
ORDER BY escenario;""",
    },
    {
        "codigo": "presupuesto_intervencion_soles",
        "nombre": "Presupuesto estimado de intervención (S/)",
        "categoria": "Inteligencia artificial",
        "formula": "Σ costo_estimado_soles de los locales seleccionados dentro del tope presupuestal (ia_priorizacion)",
        "unidad": "S/",
        "meta_defecto": "Dentro del presupuesto asignado (S/ 250,000)",
        "param_meta": "presupuesto_soles",
        "sql": """SELECT
    COUNT(*) FILTER (WHERE dentro_presupuesto) AS locales_financiados,
    COALESCE(SUM(costo_estimado_soles) FILTER (WHERE dentro_presupuesto), 0) AS inversion_asignada_soles,
    COALESCE(SUM(costo_estimado_soles), 0) AS demanda_total_soles
FROM ia_priorizacion;""",
    },
]

DEFINICIONES_KPI: list[dict[str, str]] = [
    {
        "codigo": k["codigo"],
        "nombre": k["nombre"],
        "unidad": k["unidad"],
        "meta": k["meta_defecto"],
    }
    for k in CATALOGO_KPIS
]


@st.cache_resource
def inicializar_capa_semantica() -> bool:
    """Crea la tabla sem_glosario y garantiza la carga inicial de sem_parametro y sem_glosario."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sem_glosario (
                    termino TEXT PRIMARY KEY,
                    categoria TEXT NOT NULL,
                    definicion TEXT NOT NULL,
                    fuente_referencia TEXT,
                    fecha_actualizacion TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )
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
        for g in GLOSARIO_INICIAL:
            conn.execute(
                text(
                    """
                    INSERT INTO sem_glosario (termino, categoria, definicion, fuente_referencia)
                    VALUES (:termino, :categoria, :definicion, :fuente_referencia)
                    ON CONFLICT (termino) DO NOTHING
                    """
                ),
                g,
            )
    return True


def obtener_mapa_parametros() -> dict[str, float]:
    """Devuelve un diccionario clave -> valor actual desde sem_parametro."""
    df = obtener_parametros_semantica()
    if df.empty:
        return {p["clave"]: float(p["valor"]) for p in PARAMETROS_INICIALES}
    return {str(r["clave"]): float(r["valor"]) for _, r in df.iterrows()}


def obtener_catalogo_kpis_actualizado(params: dict[str, float] | None = None) -> list[dict[str, Any]]:
    """Retorna los 9 KPIs del catálogo con sus metas reflejando sem_parametro."""
    p = params if params is not None else obtener_mapa_parametros()
    prom_nac = p.get("promedio_nacional_pct", 2.08)
    meta_apc = int(round(p.get("alumnos_por_pc_meta", 20)))
    rojo = p.get("semaforo_rojo", 0.60)
    amarillo = p.get("semaforo_amarillo", 0.35)
    pres = int(round(p.get("presupuesto_soles", 250000)))
    efecto = p.get("efecto_intervencion", 0.20) * 100.0

    salida: list[dict[str, Any]] = []
    for k in CATALOGO_KPIS:
        item = dict(k)
        cod = k["codigo"]
        if cod == "tasa_desercion_pct":
            item["meta"] = f"< {prom_nac:.2f} %"
        elif cod == "alumnos_por_pc_operativa":
            item["meta"] = f"≤ {meta_apc:,} estudiantes/PC"
        elif cod == "prob_riesgo_desercion":
            item["meta"] = f"🔴 Rojo ≥ {rojo:.2f} · 🟡 Amarillo {amarillo:.2f}–{max(amarillo, rojo - 0.01):.2f} · 🟢 Verde < {amarillo:.2f}"
        elif cod == "desertores_proyectados_t2":
            item["meta"] = f"Menor que escenario base (efecto intervención: -{efecto:.0f} %)"
        elif cod == "presupuesto_intervencion_soles":
            item["meta"] = f"Dentro del presupuesto asignado (S/ {pres:,})"
        else:
            item["meta"] = k["meta_defecto"]
        salida.append(item)
    return salida


def actualizar_parametros_semantica(nuevos_valores: dict[str, float]) -> int:
    """Actualiza parámetros editados en sem_parametro y marca fecha_actualizacion=NOW() para invalidar IA."""
    df_actual = obtener_parametros_semantica()
    actuales = {str(r["clave"]): float(r["valor"]) for _, r in df_actual.iterrows()}

    modificados = [
        (clave, float(val))
        for clave, val in nuevos_valores.items()
        if clave in actuales and abs(float(val) - actuales[clave]) > 1e-9
    ]
    if not modificados:
        return 0

    engine = get_engine()
    with engine.begin() as conn:
        for clave, val in modificados:
            conn.execute(
                text(
                    """
                    UPDATE sem_parametro
                    SET valor = :valor,
                        fecha_actualizacion = NOW()
                    WHERE clave = :clave
                    """
                ),
                {"clave": clave, "valor": val},
            )

    st.cache_data.clear()
    return len(modificados)


def restablecer_parametros_semantica() -> int:
    """Restablece sem_parametro a valor_defecto; actualiza fecha_actualizacion si hubo cambios."""
    engine = get_engine()
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                UPDATE sem_parametro
                SET valor = valor_defecto,
                    fecha_actualizacion = NOW()
                WHERE ABS(valor - valor_defecto) > 1e-9
                """
            )
        )
        afectados = int(res.rowcount or 0)

    if afectados > 0:
        st.cache_data.clear()
    return afectados


@st.cache_data(ttl=300, show_spinner=False)
def obtener_glosario_semantica() -> pd.DataFrame:
    """Lee todos los términos de sem_glosario desde PostgreSQL."""
    engine = get_engine()
    sql = """
        SELECT termino, categoria, definicion, fuente_referencia, fecha_actualizacion
        FROM sem_glosario
        ORDER BY categoria ASC, termino ASC
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    if not rows:
        return pd.DataFrame(columns=["termino", "categoria", "definicion", "fuente_referencia", "fecha_actualizacion"])
    return pd.DataFrame([dict(r) for r in rows])


def guardar_termino_glosario(
    termino: str,
    categoria: str,
    definicion: str,
    fuente_referencia: str,
) -> None:
    """Inserta o actualiza un término en sem_glosario y limpia caché."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO sem_glosario (termino, categoria, definicion, fuente_referencia, fecha_actualizacion)
                VALUES (:termino, :categoria, :definicion, :fuente_referencia, NOW())
                ON CONFLICT (termino) DO UPDATE
                SET categoria = EXCLUDED.categoria,
                    definicion = EXCLUDED.definicion,
                    fuente_referencia = EXCLUDED.fuente_referencia,
                    fecha_actualizacion = NOW()
                """
            ),
            {
                "termino": termino.strip(),
                "categoria": categoria.strip(),
                "definicion": definicion.strip(),
                "fuente_referencia": fuente_referencia.strip() or "Capa semántica",
            },
        )
    st.cache_data.clear()


@st.cache_data(ttl=300, show_spinner=False)
def obtener_resumen_jerarquias_dw() -> dict[str, int]:
    """Devuelve conteos vivos de cada nivel de las jerarquías en el Data Warehouse."""
    engine = get_engine()
    sql = """
        SELECT
            (SELECT COUNT(*)::INT FROM dim_departamento) AS departamentos,
            (SELECT COUNT(*)::INT FROM dim_provincia) AS provincias,
            (SELECT COUNT(*)::INT FROM dim_distrito) AS distritos,
            (SELECT COUNT(*)::INT FROM dim_local) AS locales,
            (SELECT COUNT(*)::INT FROM dim_ie) AS instituciones,
            (SELECT COUNT(*)::INT FROM dim_ugel) AS ugeles,
            (SELECT COUNT(*)::INT FROM dim_nivel) AS niveles,
            (SELECT COUNT(*)::INT FROM dim_ruralidad) AS ruralidades,
            (SELECT COUNT(DISTINCT anio)::INT FROM dim_tiempo WHERE mes = 0) AS anios
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().first()
    return dict(row) if row else {}


# --------------------------------------------------------------------------------------
# Consultas SQL para Pestaña 7 (Visualización BI) — Todas cacheadas con st.cache_data
# --------------------------------------------------------------------------------------

def _construir_filtros_sql(
    anio: int | None,
    departamento: str,
    ugel: str,
    nivel: str,
    ruralidad: str,
    semaforo: str,
) -> tuple[str, dict[str, Any]]:
    """Construye cláusulas WHERE seguras y parametrizadas para dimensiones e IA."""
    condiciones: list[str] = ["1=1"]
    params: dict[str, Any] = {"anio_sel": anio}

    if departamento and departamento != "Todos":
        condiciones.append("dep.nombre = :departamento")
        params["departamento"] = departamento
    if ugel and ugel != "Todas":
        condiciones.append("ug.nombre = :ugel")
        params["ugel"] = ugel
    if nivel and nivel != "Todos":
        condiciones.append("dn.nivel = :nivel")
        params["nivel"] = nivel
    if ruralidad and ruralidad != "Todas":
        condiciones.append("dr.gradiente = :ruralidad")
        params["ruralidad"] = ruralidad
    if semaforo and semaforo != "Todos":
        condiciones.append("ia.semaforo = :semaforo")
        params["semaforo"] = semaforo

    return " AND ".join(condiciones), params


def _cte_ie_base_y_locales(where_ie: str, anio: int | None) -> str:
    """Devuelve bloque CTE común que filtra instituciones, determina año ref y desduplica locales."""
    filtro_anio_exists = "AND dt0.anio = :anio_sel" if anio is not None else ""
    filtro_anio_ref = "WHERE dt.anio = :anio_sel" if anio is not None else ""
    return f"""
        ie_base AS (
            SELECT DISTINCT
                ie.cod_modular,
                ie.nombre AS nombre_ie,
                ie.cod_local,
                ie.kit_aula_conectada,
                dl.ubigeo,
                dl.latitud,
                dl.longitud,
                dl.tipo_acceso,
                dl.tiempo_acceso_h,
                dep.nombre AS departamento,
                prov.nombre AS provincia,
                dist.nombre AS distrito,
                ug.nombre AS ugel,
                dn.nivel AS nivel,
                dr.gradiente AS gradiente_ruralidad,
                ia.prob_riesgo,
                ia.semaforo,
                ia.tasa_esperada_siguiente,
                ia.factores_principales
            FROM dim_ie ie
            JOIN dim_local dl ON dl.cod_local = ie.cod_local
            JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
            JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
            JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
            JOIN dim_ugel ug ON ug.id_ugel = dl.id_ugel
            JOIN dim_nivel dn ON dn.id_nivel = ie.id_nivel
            JOIN dim_ruralidad dr ON dr.id_ruralidad = ie.id_ruralidad
            LEFT JOIN ia_pred_riesgo_ie ia ON ia.cod_modular = ie.cod_modular
            WHERE {where_ie}
              AND EXISTS (
                  SELECT 1
                  FROM fact_permanencia fp0
                  JOIN dim_tiempo dt0 ON dt0.id_tiempo = fp0.id_tiempo
                  WHERE fp0.cod_modular = ie.cod_modular
                    {filtro_anio_exists}
              )
        ),
        anio_ref AS (
            SELECT MAX(dt.anio) AS anio
            FROM fact_permanencia fp
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fp.cod_modular
            {filtro_anio_ref}
        ),
        -- Matrícula total de TODAS las IEs del local en el año de referencia (regla local vs servicio)
        mat_local_ref AS (
            SELECT
                ie.cod_local,
                COALESCE(SUM(fp.matricula_inicial), 0)::BIGINT AS matricula_local
            FROM fact_permanencia fp
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
            CROSS JOIN anio_ref ar
            WHERE dt.anio = ar.anio
              AND ie.cod_local IN (SELECT DISTINCT cod_local FROM ie_base)
            GROUP BY ie.cod_local
        ),
        -- Equipamiento del local deduplicado (1 fila por cod_local, censo más reciente <= anio_ref)
        censo_local AS (
            SELECT DISTINCT ON (ie.cod_local)
                ie.cod_local,
                dt.anio AS anio_censo,
                BOOL_OR(fp.declaro_computo) AS declaro_computo,
                MAX(fp.cpu_total) AS cpu_total,
                MAX(fp.cpu_operativos) AS cpu_operativos,
                MAX(fp.cpu_en_reparacion) AS cpu_en_reparacion,
                MAX(fp.cpu_mas_3_anios) AS cpu_mas_3_anios,
                BOOL_OR(fp.tiene_internet) AS tiene_internet,
                MAX(fp.fuente_energia) AS fuente_energia
            FROM fact_permanencia fp
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
            CROSS JOIN anio_ref ar
            WHERE fp.declaro_computo IS NOT NULL
              AND ie.cod_local IN (SELECT DISTINCT cod_local FROM ie_base)
            GROUP BY ie.cod_local, dt.anio, ar.anio
            ORDER BY ie.cod_local,
                     CASE WHEN dt.anio <= ar.anio THEN 0 ELSE 1 END ASC,
                     ABS(dt.anio - ar.anio) ASC,
                     dt.anio DESC
        ),
        local_equip AS (
            SELECT
                cl.cod_local,
                cl.anio_censo,
                COALESCE(ml.matricula_local, 0)::BIGINT AS matricula_local,
                cl.declaro_computo,
                CASE WHEN cl.declaro_computo THEN COALESCE(cl.cpu_total, 0) ELSE 0 END AS cpu_total,
                CASE WHEN cl.declaro_computo THEN COALESCE(cl.cpu_operativos, 0) ELSE 0 END AS cpu_operativos,
                cl.tiene_internet,
                cl.fuente_energia,
                CASE
                    WHEN cl.declaro_computo AND COALESCE(cl.cpu_operativos, 0) > 0
                    THEN ml.matricula_local::DOUBLE PRECISION / cl.cpu_operativos::DOUBLE PRECISION
                    ELSE NULL
                END AS alumnos_por_pc_operativa,
                CASE
                    WHEN cl.declaro_computo AND COALESCE(cl.cpu_total, 0) > 0
                    THEN cl.cpu_operativos::DOUBLE PRECISION / cl.cpu_total::DOUBLE PRECISION * 100.0
                    ELSE NULL
                END AS equipos_operativos_pct
            FROM censo_local cl
            LEFT JOIN mat_local_ref ml ON ml.cod_local = cl.cod_local
        )
    """


@st.cache_data(ttl=300, show_spinner=False)
def obtener_opciones_filtros_bi() -> dict[str, Any]:
    """Lee dinámicamente todas las opciones de filtros para Visualización BI desde PostgreSQL."""
    engine = get_engine()
    with engine.connect() as conn:
        anios = [
            int(r[0])
            for r in conn.execute(
                text(
                    """
                    SELECT DISTINCT dt.anio
                    FROM fact_permanencia fp
                    JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
                    ORDER BY dt.anio DESC
                    """
                )
            ).fetchall()
        ]
        departamentos = [
            r[0]
            for r in conn.execute(text("SELECT nombre FROM dim_departamento ORDER BY nombre")).fetchall()
        ]
        ugeles_dep = [
            dict(r)
            for r in conn.execute(
                text(
                    """
                    SELECT DISTINCT dep.nombre AS departamento, ug.nombre AS ugel
                    FROM dim_local dl
                    JOIN dim_ugel ug ON ug.id_ugel = dl.id_ugel
                    JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
                    JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
                    JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
                    ORDER BY dep.nombre, ug.nombre
                    """
                )
            ).mappings().all()
        ]
        niveles = [
            r[0]
            for r in conn.execute(text("SELECT nivel FROM dim_nivel ORDER BY id_nivel")).fetchall()
        ]
        ruralidades = [
            r[0]
            for r in conn.execute(text("SELECT gradiente FROM dim_ruralidad ORDER BY id_ruralidad")).fetchall()
        ]
        semaforos_raw = [
            r[0]
            for r in conn.execute(
                text("SELECT DISTINCT semaforo FROM ia_pred_riesgo_ie WHERE semaforo IS NOT NULL")
            ).fetchall()
        ]

    orden_sem = {"Rojo": 0, "Amarillo": 1, "Verde": 2}
    semaforos = sorted(semaforos_raw, key=lambda s: orden_sem.get(s, 99))
    return {
        "anios": anios,
        "departamentos": departamentos,
        "ugeles_por_dep": ugeles_dep,
        "niveles": niveles,
        "ruralidades": ruralidades,
        "semaforos": semaforos,
    }


@st.cache_data(ttl=300, show_spinner=False)
def obtener_kpis_ejecutivos_bi(
    anio: int | None,
    departamento: str,
    ugel: str,
    nivel: str,
    ruralidad: str,
    semaforo: str,
    meta_alumnos_pc: float = 20.0,
) -> dict[str, Any]:
    """Calcula los 9 KPIs ejecutivos en SQL respetando filtros y evitando doble conteo de local."""
    where_ie, params = _construir_filtros_sql(anio, departamento, ugel, nivel, ruralidad, semaforo)
    params["meta_apc"] = float(meta_alumnos_pc)
    cte = _cte_ie_base_y_locales(where_ie, anio)
    filtro_anio_fp = "WHERE dt.anio = :anio_sel" if anio is not None else ""
    filtro_anio_fc = "WHERE dt.anio = :anio_sel" if anio is not None else ""

    sql_dw = f"""
        WITH {cte},
        perm_agg AS (
            SELECT
                COUNT(DISTINCT fp.cod_modular)::INT AS ie_con_permanencia,
                COALESCE(SUM(fp.matricula_inicial), 0)::BIGINT AS total_matricula,
                COALESCE(SUM(fp.desertores), 0)::BIGINT AS total_desertores,
                ROUND(COALESCE(SUM(fp.desertores), 0)::NUMERIC / NULLIF(SUM(fp.matricula_inicial), 0) * 100, 2) AS tasa_desercion_pct,
                ROUND(AVG(fp.asistencia_prom_pct), 1) AS asistencia_prom_pct
            FROM fact_permanencia fp
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fp.cod_modular
            {filtro_anio_fp}
        ),
        equip_agg AS (
            SELECT
                COUNT(DISTINCT le.cod_local)::INT AS locales_con_censo,
                COALESCE(SUM(le.matricula_local), 0)::BIGINT AS matricula_locales_censo,
                COALESCE(SUM(le.cpu_total), 0)::BIGINT AS cpu_total,
                COALESCE(SUM(le.cpu_operativos), 0)::BIGINT AS cpu_operativos,
                ROUND(COALESCE(SUM(le.matricula_local), 0)::NUMERIC / NULLIF(SUM(le.cpu_operativos), 0), 1) AS alumnos_por_pc_operativa,
                ROUND(COALESCE(SUM(le.cpu_operativos), 0)::NUMERIC / NULLIF(SUM(le.cpu_total), 0) * 100, 1) AS equipos_operativos_pct
            FROM local_equip le
        ),
        iot_por_ie AS (
            SELECT
                fc.cod_modular,
                COUNT(*)::INT AS lecturas,
                COUNT(*) FILTER (WHERE fc.estado_sensor = 'Sin señal')::INT AS lecturas_sin_senal,
                AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal') AS disp_prom_ie,
                AVG(fc.horas_corte_energia) AS cortes_prom_ie
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fc.cod_modular
            {filtro_anio_fc}
            GROUP BY fc.cod_modular
        ),
        iot_agg AS (
            SELECT
                COUNT(DISTINCT fc.id_sensor)::INT AS sensores_activos,
                COUNT(*)::INT AS lecturas_totales,
                COUNT(*) FILTER (WHERE fc.estado_sensor = 'Sin señal')::INT AS lecturas_sin_senal,
                ROUND(AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal'), 1) AS disponibilidad_internet_pct,
                ROUND(AVG(fc.horas_corte_energia), 1) AS horas_corte_energia_prom,
                ROUND(SUM(fc.horas_corte_energia), 1) AS horas_corte_energia_total
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fc.cod_modular
            {filtro_anio_fc}
        ),
        indice_por_ie AS (
            SELECT
                ib.cod_modular,
                100.0 * (
                    0.5 * LEAST(1.0, COALESCE(le.equipos_operativos_pct, 0.0) / 100.0)
                    + 0.5 * CASE
                        WHEN le.alumnos_por_pc_operativa IS NOT NULL AND le.alumnos_por_pc_operativa > 0
                        THEN LEAST(1.0, :meta_apc / le.alumnos_por_pc_operativa)
                        ELSE 0.0
                    END
                ) AS comp_equipamiento,
                CASE
                    WHEN iot.lecturas IS NOT NULL AND iot.lecturas > 0
                    THEN COALESCE(iot.disp_prom_ie, 0.0)
                    WHEN le.tiene_internet IS TRUE THEN 60.0
                    ELSE 0.0
                END AS comp_internet,
                CASE
                    WHEN iot.cortes_prom_ie IS NOT NULL
                    THEN 100.0 * (1.0 - LEAST(1.0, GREATEST(0.0, iot.cortes_prom_ie) / 100.0))
                    WHEN le.fuente_energia = 'Red pública' THEN 90.0
                    WHEN le.fuente_energia = 'Panel solar' THEN 70.0
                    WHEN le.fuente_energia = 'Grupo electrógeno' THEN 50.0
                    ELSE 0.0
                END AS comp_energia
            FROM ie_base ib
            LEFT JOIN local_equip le ON le.cod_local = ib.cod_local
            LEFT JOIN iot_por_ie iot ON iot.cod_modular = ib.cod_modular
        ),
        indice_agg AS (
            SELECT
                ROUND(AVG(comp_equipamiento)::NUMERIC, 1) AS comp_equipamiento_prom,
                ROUND(AVG(comp_internet)::NUMERIC, 1) AS comp_internet_prom,
                ROUND(AVG(comp_energia)::NUMERIC, 1) AS comp_energia_prom,
                ROUND(AVG(0.40 * comp_equipamiento + 0.30 * comp_internet + 0.30 * comp_energia)::NUMERIC, 1) AS indice_conectividad_rural
            FROM indice_por_ie
        ),
        ia_agg AS (
            SELECT
                COUNT(prob_riesgo)::INT AS ies_evaluadas_ia,
                ROUND(AVG(prob_riesgo)::NUMERIC, 3) AS prob_riesgo_prom,
                COUNT(*) FILTER (WHERE semaforo = 'Rojo')::INT AS ies_rojo,
                COUNT(*) FILTER (WHERE semaforo = 'Amarillo')::INT AS ies_amarillo,
                COUNT(*) FILTER (WHERE semaforo = 'Verde')::INT AS ies_verde
            FROM ie_base
        )
        SELECT
            (SELECT COUNT(*)::INT FROM ie_base) AS total_ies,
            (SELECT COUNT(DISTINCT cod_local)::INT FROM ie_base) AS total_locales,
            (SELECT anio FROM anio_ref) AS anio_referencia,
            pa.total_matricula,
            pa.total_desertores,
            pa.tasa_desercion_pct,
            pa.asistencia_prom_pct,
            ea.locales_con_censo,
            ea.cpu_total,
            ea.cpu_operativos,
            ea.alumnos_por_pc_operativa,
            ea.equipos_operativos_pct,
            ia_iot.sensores_activos,
            ia_iot.lecturas_totales,
            ia_iot.lecturas_sin_senal,
            ia_iot.disponibilidad_internet_pct,
            ia_iot.horas_corte_energia_prom,
            ia_iot.horas_corte_energia_total,
            ind.comp_equipamiento_prom,
            ind.comp_internet_prom,
            ind.comp_energia_prom,
            ind.indice_conectividad_rural,
            ia_a.ies_evaluadas_ia,
            ia_a.prob_riesgo_prom,
            ia_a.ies_rojo,
            ia_a.ies_amarillo,
            ia_a.ies_verde
        FROM perm_agg pa
        CROSS JOIN equip_agg ea
        CROSS JOIN iot_agg ia_iot
        CROSS JOIN indice_agg ind
        CROSS JOIN ia_agg ia_a
    """

    cond_proy = ["tipo = 'proyeccion'"]
    params_proy: dict[str, Any] = {}
    if departamento and departamento != "Todos":
        cond_proy.append("departamento = :departamento")
        params_proy["departamento"] = departamento

    sql_proy = f"""
        WITH ult AS (SELECT MAX(anio) AS anio_t2 FROM ia_proyeccion_desertores)
        SELECT
            p.anio,
            p.escenario,
            COALESCE(SUM(p.desertores), 0)::INT AS desertores,
            COALESCE(SUM(p.matricula), 0)::INT AS matricula
        FROM ia_proyeccion_desertores p
        CROSS JOIN ult u
        WHERE {' AND '.join(cond_proy)}
          AND p.anio = u.anio_t2
        GROUP BY p.anio, p.escenario
    """

    cond_prio = ["1=1"]
    params_prio: dict[str, Any] = {}
    if departamento and departamento != "Todos":
        cond_prio.append("departamento = :departamento")
        params_prio["departamento"] = departamento
    if ugel and ugel != "Todas":
        cond_prio.append("ugel = :ugel")
        params_prio["ugel"] = ugel

    sql_prio = f"""
        SELECT
            COUNT(*) FILTER (WHERE dentro_presupuesto)::INT AS locales_financiados,
            COUNT(*)::INT AS locales_con_deficit,
            COALESCE(SUM(costo_estimado_soles) FILTER (WHERE dentro_presupuesto), 0)::BIGINT AS inversion_asignada,
            COALESCE(SUM(costo_estimado_soles), 0)::BIGINT AS inversion_total_requerida
        FROM ia_priorizacion
        WHERE {' AND '.join(cond_prio)}
    """

    engine = get_engine()
    with engine.connect() as conn:
        row_dw = conn.execute(text(sql_dw), params).mappings().first()
        rows_proy = conn.execute(text(sql_proy), params_proy).mappings().all()
        row_prio = conn.execute(text(sql_prio), params_prio).mappings().first()

    res = dict(row_dw) if row_dw else {}
    proy_map = {r["escenario"]: int(r["desertores"]) for r in rows_proy}
    anio_t2 = rows_proy[0]["anio"] if rows_proy else None
    res["anio_proy_t2"] = anio_t2
    res["desertores_proy_t2_base"] = proy_map.get("base")
    res["desertores_proy_t2_optimista"] = proy_map.get("optimista")
    res["desertores_proy_t2_pesimista"] = proy_map.get("pesimista")

    if row_prio:
        res.update(dict(row_prio))
    return res


@st.cache_data(ttl=300, show_spinner=False)
def obtener_desercion_departamento_anio(
    anio: int | None,
    departamento: str,
    ugel: str,
    nivel: str,
    ruralidad: str,
    semaforo: str,
) -> pd.DataFrame:
    """Serie de tasa de deserción por departamento y año desde el DW (afectada por los filtros)."""
    where_ie, params = _construir_filtros_sql(anio, departamento, ugel, nivel, ruralidad, semaforo)
    filtro_anio = "AND dt.anio = :anio_sel" if anio is not None else ""
    sql = f"""
        SELECT
            dep.nombre AS departamento,
            dt.anio::INT AS anio,
            COUNT(DISTINCT fp.cod_modular)::INT AS instituciones,
            COALESCE(SUM(fp.matricula_inicial), 0)::BIGINT AS matricula_inicial,
            COALESCE(SUM(fp.desertores), 0)::BIGINT AS desertores,
            ROUND(COALESCE(SUM(fp.desertores), 0)::NUMERIC / NULLIF(SUM(fp.matricula_inicial), 0) * 100, 2) AS tasa_desercion_pct,
            ROUND(AVG(fp.asistencia_prom_pct), 1) AS asistencia_prom_pct
        FROM fact_permanencia fp
        JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
        JOIN dim_ie ie ON ie.cod_modular = fp.cod_modular
        JOIN dim_local dl ON dl.cod_local = ie.cod_local
        JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
        JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
        JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
        JOIN dim_ugel ug ON ug.id_ugel = dl.id_ugel
        JOIN dim_nivel dn ON dn.id_nivel = ie.id_nivel
        JOIN dim_ruralidad dr ON dr.id_ruralidad = ie.id_ruralidad
        LEFT JOIN ia_pred_riesgo_ie ia ON ia.cod_modular = ie.cod_modular
        WHERE {where_ie}
          {filtro_anio}
        GROUP BY dep.nombre, dt.anio
        ORDER BY dt.anio ASC, dep.nombre ASC
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


@st.cache_data(ttl=300, show_spinner=False)
def obtener_reparto_semaforo_bi(
    anio: int | None,
    departamento: str,
    ugel: str,
    nivel: str,
    ruralidad: str,
    semaforo: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Devuelve reparto global del semáforo y reparto por departamento (afectado por filtros)."""
    where_ie, params = _construir_filtros_sql(anio, departamento, ugel, nivel, ruralidad, semaforo)
    cte = _cte_ie_base_y_locales(where_ie, anio)
    sql = f"""
        WITH {cte}
        SELECT
            departamento,
            COALESCE(semaforo, 'Sin evaluar') AS semaforo,
            COUNT(*)::INT AS instituciones,
            ROUND(AVG(prob_riesgo)::NUMERIC, 3) AS prob_media
        FROM ie_base
        GROUP BY departamento, COALESCE(semaforo, 'Sin evaluar')
        ORDER BY departamento ASC
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    df_dep = pd.DataFrame([dict(r) for r in rows])
    df_global = (
        df_dep.groupby("semaforo", as_index=False)["instituciones"]
        .sum()
        .sort_values("instituciones", ascending=False)
    )
    return df_global, df_dep


@st.cache_data(ttl=300, show_spinner=False)
def obtener_indice_conectividad_bi(
    anio: int | None,
    departamento: str,
    ugel: str,
    nivel: str,
    ruralidad: str,
    semaforo: str,
    meta_alumnos_pc: float = 20.0,
) -> pd.DataFrame:
    """Calcula los 3 componentes del índice de conectividad rural agrupados por departamento y ruralidad."""
    where_ie, params = _construir_filtros_sql(anio, departamento, ugel, nivel, ruralidad, semaforo)
    params["meta_apc"] = float(meta_alumnos_pc)
    cte = _cte_ie_base_y_locales(where_ie, anio)
    filtro_anio_fc = "WHERE dt.anio = :anio_sel" if anio is not None else ""

    sql = f"""
        WITH {cte},
        iot_por_ie AS (
            SELECT
                fc.cod_modular,
                COUNT(*)::INT AS lecturas,
                AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal') AS disp_prom_ie,
                AVG(fc.horas_corte_energia) AS cortes_prom_ie
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fc.cod_modular
            {filtro_anio_fc}
            GROUP BY fc.cod_modular
        ),
        ie_ind AS (
            SELECT
                ib.departamento,
                ib.gradiente_ruralidad,
                100.0 * (
                    0.5 * LEAST(1.0, COALESCE(le.equipos_operativos_pct, 0.0) / 100.0)
                    + 0.5 * CASE
                        WHEN le.alumnos_por_pc_operativa IS NOT NULL AND le.alumnos_por_pc_operativa > 0
                        THEN LEAST(1.0, :meta_apc / le.alumnos_por_pc_operativa)
                        ELSE 0.0
                    END
                ) AS comp_equipamiento,
                CASE
                    WHEN iot.lecturas IS NOT NULL AND iot.lecturas > 0
                    THEN COALESCE(iot.disp_prom_ie, 0.0)
                    WHEN le.tiene_internet IS TRUE THEN 60.0
                    ELSE 0.0
                END AS comp_internet,
                CASE
                    WHEN iot.cortes_prom_ie IS NOT NULL
                    THEN 100.0 * (1.0 - LEAST(1.0, GREATEST(0.0, iot.cortes_prom_ie) / 100.0))
                    WHEN le.fuente_energia = 'Red pública' THEN 90.0
                    WHEN le.fuente_energia = 'Panel solar' THEN 70.0
                    WHEN le.fuente_energia = 'Grupo electrógeno' THEN 50.0
                    ELSE 0.0
                END AS comp_energia
            FROM ie_base ib
            LEFT JOIN local_equip le ON le.cod_local = ib.cod_local
            LEFT JOIN iot_por_ie iot ON iot.cod_modular = ib.cod_modular
        )
        SELECT
            departamento,
            gradiente_ruralidad,
            COUNT(*)::INT AS instituciones,
            ROUND(AVG(comp_equipamiento)::NUMERIC, 1) AS equipamiento,
            ROUND(AVG(comp_internet)::NUMERIC, 1) AS internet,
            ROUND(AVG(comp_energia)::NUMERIC, 1) AS energia,
            ROUND(AVG(0.40 * comp_equipamiento + 0.30 * comp_internet + 0.30 * comp_energia)::NUMERIC, 1) AS indice_conectividad_rural
        FROM ie_ind
        GROUP BY departamento, gradiente_ruralidad
        ORDER BY departamento, gradiente_ruralidad
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


@st.cache_data(ttl=300, show_spinner=False)
def obtener_alertas_activas_bi(
    anio: int | None,
    departamento: str,
    ugel: str,
    nivel: str,
    ruralidad: str,
    semaforo: str,
    meta_desercion: float = 2.08,
    meta_alumnos_pc: float = 20.0,
) -> tuple[dict[str, int], pd.DataFrame]:
    """Evalúa las 6 reglas de alerta (§9) en SQL y devuelve resumen y detalle por IE."""
    where_ie, params = _construir_filtros_sql(anio, departamento, ugel, nivel, ruralidad, semaforo)
    params["meta_des"] = float(meta_desercion)
    params["meta_apc"] = float(meta_alumnos_pc)
    cte = _cte_ie_base_y_locales(where_ie, anio)
    filtro_anio_fc = "WHERE dt.anio = :anio_sel" if anio is not None else ""

    sql = f"""
        WITH {cte},
        perm_ie_ref AS (
            SELECT
                fp.cod_modular,
                fp.matricula_inicial,
                fp.desertores,
                fp.tasa_desercion_pct
            FROM fact_permanencia fp
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            CROSS JOIN anio_ref ar
            WHERE dt.anio = ar.anio
        ),
        iot_ie AS (
            SELECT
                fc.cod_modular,
                ROUND(AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal'), 1) AS disp_prom,
                ROUND(AVG(fc.horas_corte_energia), 1) AS cortes_prom
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fc.cod_modular
            {filtro_anio_fc}
            GROUP BY fc.cod_modular
        ),
        anom_ie AS (
            SELECT DISTINCT cod_modular, TRUE AS tiene_anomalia_iot
            FROM (
                SELECT cod_modular FROM ia_anomalias_iot
                UNION ALL
                SELECT cod_modular FROM ia_alertas_cronicas_iot
            ) sub
        ),
        eval_ie AS (
            SELECT
                ib.cod_modular,
                ib.nombre_ie,
                ib.cod_local,
                ib.departamento,
                ib.distrito,
                ib.ugel,
                ib.nivel,
                ib.gradiente_ruralidad,
                COALESCE(ib.semaforo, 'Sin evaluar') AS semaforo,
                ib.prob_riesgo,
                pr.tasa_desercion_pct,
                ROUND(le.alumnos_por_pc_operativa::NUMERIC, 1) AS alumnos_por_pc_local,
                le.cpu_operativos,
                le.declaro_computo,
                iot.disp_prom,
                iot.cortes_prom,
                (COALESCE(pr.tasa_desercion_pct, 0) > :meta_des) AS al_desercion,
                (iot.disp_prom IS NOT NULL AND iot.disp_prom < 90.0) AS al_internet,
                (iot.cortes_prom IS NOT NULL AND iot.cortes_prom > 24.0) AS al_energia,
                (
                    (le.alumnos_por_pc_operativa IS NOT NULL AND le.alumnos_por_pc_operativa > :meta_apc)
                    OR (le.cod_local IS NOT NULL AND COALESCE(le.cpu_operativos, 0) = 0 AND COALESCE(le.matricula_local, 0) > 0)
                ) AS al_alumnos_pc,
                (ib.semaforo = 'Rojo') AS al_rojo,
                COALESCE(an.tiene_anomalia_iot, FALSE) AS al_iot
            FROM ie_base ib
            LEFT JOIN perm_ie_ref pr ON pr.cod_modular = ib.cod_modular
            LEFT JOIN local_equip le ON le.cod_local = ib.cod_local
            LEFT JOIN iot_ie iot ON iot.cod_modular = ib.cod_modular
            LEFT JOIN anom_ie an ON an.cod_modular = ib.cod_modular
        )
        SELECT *
        FROM eval_ie
        WHERE al_desercion OR al_internet OR al_energia OR al_alumnos_pc OR al_rojo OR al_iot
        ORDER BY
            (al_rojo::INT + al_desercion::INT + al_internet::INT + al_energia::INT + al_alumnos_pc::INT + al_iot::INT) DESC,
            prob_riesgo DESC NULLS LAST,
            tasa_desercion_pct DESC NULLS LAST
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    if not rows:
        resumen_vacio = {
            "desercion_sobre_meta": 0,
            "internet_bajo_90": 0,
            "cortes_sobre_24": 0,
            "alumnos_pc_sobre_meta": 0,
            "semaforo_rojo": 0,
            "anomalia_iot": 0,
            "total_ie_alertadas": 0,
        }
        return resumen_vacio, pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    resumen = {
        "desercion_sobre_meta": int(df["al_desercion"].sum()),
        "internet_bajo_90": int(df["al_internet"].sum()),
        "cortes_sobre_24": int(df["al_energia"].sum()),
        "alumnos_pc_sobre_meta": int(df["al_alumnos_pc"].sum()),
        "semaforo_rojo": int(df["al_rojo"].sum()),
        "anomalia_iot": int(df["al_iot"].sum()),
        "total_ie_alertadas": int(len(df)),
    }

    def _armar_etiquetas(r: pd.Series) -> str:
        etiquetas = []
        if r["al_rojo"]:
            etiquetas.append("🔴 Semáforo Rojo")
        if r["al_desercion"]:
            etiquetas.append(f"Deserción > {meta_desercion:.2f} %")
        if r["al_internet"]:
            etiquetas.append("Internet < 90 %")
        if r["al_energia"]:
            etiquetas.append("Cortes > 24 h/mes")
        if r["al_alumnos_pc"]:
            etiquetas.append(f"Alum/PC > {int(round(meta_alumnos_pc))}")
        if r["al_iot"]:
            etiquetas.append("Anomalía IoT")
        return " · ".join(etiquetas)

    df["cantidad_alertas"] = (
        df["al_rojo"].astype(int)
        + df["al_desercion"].astype(int)
        + df["al_internet"].astype(int)
        + df["al_energia"].astype(int)
        + df["al_alumnos_pc"].astype(int)
        + df["al_iot"].astype(int)
    )
    df["alertas_activas"] = df.apply(_armar_etiquetas, axis=1)
    df["prioridad_alerta"] = df["cantidad_alertas"].map(
        lambda c: "🔴 Crítica (≥3)" if c >= 3 else ("🟡 Media (2)" if c == 2 else "🟠 Preventiva (1)")
    )
    return resumen, df


@st.cache_data(ttl=300, show_spinner=False)
def obtener_drilldown_departamentos_bi(
    anio: int | None,
    departamento: str,
    ugel: str,
    nivel: str,
    ruralidad: str,
    semaforo: str,
    meta_alumnos_pc: float = 20.0,
) -> pd.DataFrame:
    """Nivel 1 del drill-down: indicadores agregados por departamento."""
    where_ie, params = _construir_filtros_sql(anio, departamento, ugel, nivel, ruralidad, semaforo)
    params["meta_apc"] = float(meta_alumnos_pc)
    cte = _cte_ie_base_y_locales(where_ie, anio)
    filtro_anio_fp = "WHERE dt.anio = :anio_sel" if anio is not None else ""
    filtro_anio_fc = "WHERE dt.anio = :anio_sel" if anio is not None else ""

    sql = f"""
        WITH {cte},
        dep_perm AS (
            SELECT
                ib.departamento,
                COUNT(DISTINCT ib.cod_modular)::INT AS instituciones,
                COUNT(DISTINCT ib.cod_local)::INT AS locales,
                COALESCE(SUM(fp.matricula_inicial), 0)::BIGINT AS matricula_inicial,
                COALESCE(SUM(fp.desertores), 0)::BIGINT AS desertores,
                ROUND(COALESCE(SUM(fp.desertores), 0)::NUMERIC / NULLIF(SUM(fp.matricula_inicial), 0) * 100, 2) AS tasa_desercion_pct,
                ROUND(AVG(fp.asistencia_prom_pct), 1) AS asistencia_prom_pct
            FROM ie_base ib
            JOIN fact_permanencia fp ON fp.cod_modular = ib.cod_modular
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            {filtro_anio_fp}
            GROUP BY ib.departamento
        ),
        dep_loc AS (
            SELECT
                loc_dep.departamento,
                ROUND(SUM(le.matricula_local)::NUMERIC / NULLIF(SUM(le.cpu_operativos), 0), 1) AS alumnos_por_pc_operativa,
                ROUND(SUM(le.cpu_operativos)::NUMERIC / NULLIF(SUM(le.cpu_total), 0) * 100, 1) AS equipos_operativos_pct
            FROM (SELECT DISTINCT cod_local, departamento FROM ie_base) loc_dep
            JOIN local_equip le ON le.cod_local = loc_dep.cod_local
            GROUP BY loc_dep.departamento
        ),
        iot_por_ie AS (
            SELECT
                fc.cod_modular,
                COUNT(*)::INT AS lecturas,
                AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal') AS disp_prom_ie,
                AVG(fc.horas_corte_energia) AS cortes_prom_ie
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fc.cod_modular
            {filtro_anio_fc}
            GROUP BY fc.cod_modular
        ),
        dep_iot AS (
            SELECT
                ib.departamento,
                ROUND(AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal'), 1) AS disponibilidad_internet_pct,
                ROUND(AVG(fc.horas_corte_energia), 1) AS horas_corte_mensual_prom
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fc.cod_modular
            {filtro_anio_fc}
            GROUP BY ib.departamento
        ),
        dep_ind AS (
            SELECT
                ib.departamento,
                COUNT(*) FILTER (WHERE ib.semaforo = 'Rojo')::INT AS ies_rojo,
                ROUND(AVG(
                    0.40 * (100.0 * (
                        0.5 * LEAST(1.0, COALESCE(le.equipos_operativos_pct, 0.0) / 100.0)
                        + 0.5 * CASE WHEN le.alumnos_por_pc_operativa > 0 THEN LEAST(1.0, :meta_apc / le.alumnos_por_pc_operativa) ELSE 0.0 END
                    ))
                    + 0.30 * (CASE WHEN iot.lecturas > 0 THEN COALESCE(iot.disp_prom_ie, 0.0) WHEN le.tiene_internet IS TRUE THEN 60.0 ELSE 0.0 END)
                    + 0.30 * (CASE WHEN iot.cortes_prom_ie IS NOT NULL THEN 100.0 * (1.0 - LEAST(1.0, GREATEST(0.0, iot.cortes_prom_ie) / 100.0))
                                   WHEN le.fuente_energia = 'Red pública' THEN 90.0
                                   WHEN le.fuente_energia = 'Panel solar' THEN 70.0
                                   WHEN le.fuente_energia = 'Grupo electrógeno' THEN 50.0 ELSE 0.0 END)
                )::NUMERIC, 1) AS indice_conectividad_rural
            FROM ie_base ib
            LEFT JOIN local_equip le ON le.cod_local = ib.cod_local
            LEFT JOIN iot_por_ie iot ON iot.cod_modular = ib.cod_modular
            GROUP BY ib.departamento
        )
        SELECT
            dp.departamento,
            dp.instituciones,
            dp.locales,
            dp.matricula_inicial,
            dp.desertores,
            dp.tasa_desercion_pct,
            dp.asistencia_prom_pct,
            dl.alumnos_por_pc_operativa,
            dl.equipos_operativos_pct,
            diot.disponibilidad_internet_pct,
            diot.horas_corte_mensual_prom,
            dind.indice_conectividad_rural,
            dind.ies_rojo
        FROM dep_perm dp
        LEFT JOIN dep_loc dl ON dl.departamento = dp.departamento
        LEFT JOIN dep_iot diot ON diot.departamento = dp.departamento
        LEFT JOIN dep_ind dind ON dind.departamento = dp.departamento
        ORDER BY dp.tasa_desercion_pct DESC NULLS LAST, dp.departamento ASC
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def obtener_reporte_distritos_bi(
    anio: int | None,
    departamento: str,
    ugel: str,
    nivel: str,
    ruralidad: str,
    semaforo: str,
    meta_alumnos_pc: float = 20.0,
) -> pd.DataFrame:
    """Reporte y Nivel 2 del drill-down: indicadores agregados por distrito (ubigeo)."""
    where_ie, params = _construir_filtros_sql(anio, departamento, ugel, nivel, ruralidad, semaforo)
    params["meta_apc"] = float(meta_alumnos_pc)
    cte = _cte_ie_base_y_locales(where_ie, anio)
    filtro_anio_fp = "WHERE dt.anio = :anio_sel" if anio is not None else ""
    filtro_anio_fc = "WHERE dt.anio = :anio_sel" if anio is not None else ""

    sql = f"""
        WITH {cte},
        dist_perm AS (
            SELECT
                ib.departamento,
                ib.provincia,
                ib.distrito,
                TRIM(ib.ubigeo)::TEXT AS ubigeo,
                COUNT(DISTINCT ib.cod_modular)::INT AS instituciones,
                COUNT(DISTINCT ib.cod_local)::INT AS locales,
                COALESCE(SUM(fp.matricula_inicial), 0)::BIGINT AS matricula_inicial,
                COALESCE(SUM(fp.desertores), 0)::BIGINT AS desertores,
                ROUND(COALESCE(SUM(fp.desertores), 0)::NUMERIC / NULLIF(SUM(fp.matricula_inicial), 0) * 100, 2) AS tasa_desercion_pct,
                ROUND(AVG(fp.asistencia_prom_pct), 1) AS asistencia_prom_pct,
                ROUND(MAX(fp.pobreza_pct)::NUMERIC, 1) AS pobreza_pct,
                ROUND(MAX(fp.hogares_internet_pct)::NUMERIC, 1) AS hogares_internet_pct
            FROM ie_base ib
            JOIN fact_permanencia fp ON fp.cod_modular = ib.cod_modular
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            {filtro_anio_fp}
            GROUP BY ib.departamento, ib.provincia, ib.distrito, ib.ubigeo
        ),
        dist_loc AS (
            SELECT
                TRIM(loc_dist.ubigeo)::TEXT AS ubigeo,
                ROUND(SUM(le.matricula_local)::NUMERIC / NULLIF(SUM(le.cpu_operativos), 0), 1) AS alumnos_por_pc_operativa,
                ROUND(SUM(le.cpu_operativos)::NUMERIC / NULLIF(SUM(le.cpu_total), 0) * 100, 1) AS equipos_operativos_pct
            FROM (SELECT DISTINCT cod_local, ubigeo FROM ie_base) loc_dist
            JOIN local_equip le ON le.cod_local = loc_dist.cod_local
            GROUP BY loc_dist.ubigeo
        ),
        iot_por_ie AS (
            SELECT
                fc.cod_modular,
                COUNT(*)::INT AS lecturas,
                AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal') AS disp_prom_ie,
                AVG(fc.horas_corte_energia) AS cortes_prom_ie
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fc.cod_modular
            {filtro_anio_fc}
            GROUP BY fc.cod_modular
        ),
        dist_iot AS (
            SELECT
                TRIM(ib.ubigeo)::TEXT AS ubigeo,
                ROUND(AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal'), 1) AS disponibilidad_internet_pct,
                ROUND(AVG(fc.horas_corte_energia), 1) AS horas_corte_mensual_prom
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fc.cod_modular
            {filtro_anio_fc}
            GROUP BY ib.ubigeo
        ),
        dist_ind AS (
            SELECT
                TRIM(ib.ubigeo)::TEXT AS ubigeo,
                COUNT(*) FILTER (WHERE ib.semaforo = 'Rojo')::INT AS ies_rojo,
                ROUND(AVG(
                    0.40 * (100.0 * (
                        0.5 * LEAST(1.0, COALESCE(le.equipos_operativos_pct, 0.0) / 100.0)
                        + 0.5 * CASE WHEN le.alumnos_por_pc_operativa > 0 THEN LEAST(1.0, :meta_apc / le.alumnos_por_pc_operativa) ELSE 0.0 END
                    ))
                    + 0.30 * (CASE WHEN iot.lecturas > 0 THEN COALESCE(iot.disp_prom_ie, 0.0) WHEN le.tiene_internet IS TRUE THEN 60.0 ELSE 0.0 END)
                    + 0.30 * (CASE WHEN iot.cortes_prom_ie IS NOT NULL THEN 100.0 * (1.0 - LEAST(1.0, GREATEST(0.0, iot.cortes_prom_ie) / 100.0))
                                   WHEN le.fuente_energia = 'Red pública' THEN 90.0
                                   WHEN le.fuente_energia = 'Panel solar' THEN 70.0
                                   WHEN le.fuente_energia = 'Grupo electrógeno' THEN 50.0 ELSE 0.0 END)
                )::NUMERIC, 1) AS indice_conectividad_rural
            FROM ie_base ib
            LEFT JOIN local_equip le ON le.cod_local = ib.cod_local
            LEFT JOIN iot_por_ie iot ON iot.cod_modular = ib.cod_modular
            GROUP BY ib.ubigeo
        )
        SELECT
            dp.departamento,
            dp.provincia,
            dp.distrito,
            dp.ubigeo,
            dp.instituciones,
            dp.locales,
            dp.matricula_inicial,
            dp.desertores,
            dp.tasa_desercion_pct,
            dp.asistencia_prom_pct,
            dl.alumnos_por_pc_operativa,
            dl.equipos_operativos_pct,
            diot.disponibilidad_internet_pct,
            diot.horas_corte_mensual_prom,
            dind.indice_conectividad_rural,
            dind.ies_rojo,
            dp.pobreza_pct,
            dp.hogares_internet_pct
        FROM dist_perm dp
        LEFT JOIN dist_loc dl ON dl.ubigeo = dp.ubigeo
        LEFT JOIN dist_iot diot ON diot.ubigeo = dp.ubigeo
        LEFT JOIN dist_ind dind ON dind.ubigeo = dp.ubigeo
        ORDER BY dp.tasa_desercion_pct DESC NULLS LAST, dp.departamento, dp.distrito
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def obtener_reporte_instituciones_bi(
    anio: int | None,
    departamento: str,
    ugel: str,
    nivel: str,
    ruralidad: str,
    semaforo: str,
    ubigeo_filtro: str | None = None,
    meta_alumnos_pc: float = 20.0,
) -> pd.DataFrame:
    """Reporte y Nivel 3/Mapa: indicadores completos a nivel de institución educativa."""
    where_ie, params = _construir_filtros_sql(anio, departamento, ugel, nivel, ruralidad, semaforo)
    if ubigeo_filtro:
        where_ie = f"{where_ie} AND TRIM(dl.ubigeo) = :ubigeo_filtro"
        params["ubigeo_filtro"] = ubigeo_filtro.strip()

    params["meta_apc"] = float(meta_alumnos_pc)
    cte = _cte_ie_base_y_locales(where_ie, anio)
    filtro_anio_fp = "WHERE dt.anio = :anio_sel" if anio is not None else ""
    filtro_anio_fc = "WHERE dt.anio = :anio_sel" if anio is not None else ""

    sql = f"""
        WITH {cte},
        ie_perm AS (
            SELECT
                fp.cod_modular,
                COALESCE(SUM(fp.matricula_inicial), 0)::BIGINT AS matricula_inicial,
                COALESCE(SUM(fp.desertores), 0)::BIGINT AS desertores,
                ROUND(COALESCE(SUM(fp.desertores), 0)::NUMERIC / NULLIF(SUM(fp.matricula_inicial), 0) * 100, 2) AS tasa_desercion_pct,
                ROUND(AVG(fp.asistencia_prom_pct), 1) AS asistencia_prom_pct
            FROM fact_permanencia fp
            JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fp.cod_modular
            {filtro_anio_fp}
            GROUP BY fp.cod_modular
        ),
        ie_iot AS (
            SELECT
                fc.cod_modular,
                COUNT(*)::INT AS lecturas_iot,
                ROUND(AVG(fc.disponibilidad_pct) FILTER (WHERE fc.estado_sensor <> 'Sin señal'), 1) AS disponibilidad_internet_pct,
                ROUND(AVG(fc.horas_corte_energia), 1) AS horas_corte_mensual_prom
            FROM fact_conectividad fc
            JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
            JOIN ie_base ib ON ib.cod_modular = fc.cod_modular
            {filtro_anio_fc}
            GROUP BY fc.cod_modular
        )
        SELECT
            TRIM(ib.cod_modular)::TEXT AS cod_modular,
            ib.nombre_ie,
            TRIM(ib.cod_local)::TEXT AS cod_local,
            TRIM(ib.ubigeo)::TEXT AS ubigeo,
            ib.departamento,
            ib.provincia,
            ib.distrito,
            ib.ugel,
            ib.nivel,
            ib.gradiente_ruralidad,
            ib.latitud::DOUBLE PRECISION AS latitud,
            ib.longitud::DOUBLE PRECISION AS longitud,
            ib.tipo_acceso,
            ib.tiempo_acceso_h::DOUBLE PRECISION AS tiempo_acceso_h,
            ib.kit_aula_conectada,
            COALESCE(ib.semaforo, 'Sin evaluar') AS semaforo,
            ib.prob_riesgo,
            ib.tasa_esperada_siguiente,
            ib.factores_principales,
            ip.matricula_inicial,
            ip.desertores,
            ip.tasa_desercion_pct,
            ip.asistencia_prom_pct,
            le.cpu_total,
            le.cpu_operativos,
            ROUND(le.alumnos_por_pc_operativa::NUMERIC, 1) AS alumnos_por_pc_local,
            ROUND(le.equipos_operativos_pct::NUMERIC, 1) AS equipos_operativos_pct,
            iot.disponibilidad_internet_pct,
            iot.horas_corte_mensual_prom,
            ROUND((
                0.40 * (100.0 * (
                    0.5 * LEAST(1.0, COALESCE(le.equipos_operativos_pct, 0.0) / 100.0)
                    + 0.5 * CASE WHEN le.alumnos_por_pc_operativa > 0 THEN LEAST(1.0, :meta_apc / le.alumnos_por_pc_operativa) ELSE 0.0 END
                ))
                + 0.30 * (CASE WHEN COALESCE(iot.lecturas_iot, 0) > 0 THEN COALESCE(iot.disponibilidad_internet_pct, 0.0)
                               WHEN le.tiene_internet IS TRUE THEN 60.0 ELSE 0.0 END)
                + 0.30 * (CASE WHEN iot.horas_corte_mensual_prom IS NOT NULL THEN 100.0 * (1.0 - LEAST(1.0, GREATEST(0.0, iot.horas_corte_mensual_prom) / 100.0))
                               WHEN le.fuente_energia = 'Red pública' THEN 90.0
                               WHEN le.fuente_energia = 'Panel solar' THEN 70.0
                               WHEN le.fuente_energia = 'Grupo electrógeno' THEN 50.0 ELSE 0.0 END)
            )::NUMERIC, 1) AS indice_conectividad_rural
        FROM ie_base ib
        LEFT JOIN ie_perm ip ON ip.cod_modular = ib.cod_modular
        LEFT JOIN local_equip le ON le.cod_local = ib.cod_local
        LEFT JOIN ie_iot iot ON iot.cod_modular = ib.cod_modular
        ORDER BY ib.prob_riesgo DESC NULLS LAST, ip.tasa_desercion_pct DESC NULLS LAST, ib.nombre_ie ASC
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def obtener_ficha_institucion_bi(cod_modular: str) -> dict[str, Any]:
    """Recupera el detalle completo (perfil, historial, local compartido, IoT e IA) de una IE."""
    engine = get_engine()
    cod = cod_modular.strip()

    sql_perfil = """
        SELECT
            TRIM(ie.cod_modular)::TEXT AS cod_modular,
            ie.nombre AS nombre_ie,
            TRIM(dl.cod_local)::TEXT AS cod_local,
            TRIM(dl.ubigeo)::TEXT AS ubigeo,
            dep.nombre AS departamento,
            prov.nombre AS provincia,
            dist.nombre AS distrito,
            ug.nombre AS ugel,
            dn.nivel AS nivel,
            dr.gradiente AS gradiente_ruralidad,
            dl.latitud::DOUBLE PRECISION AS latitud,
            dl.longitud::DOUBLE PRECISION AS longitud,
            dl.altitud_msnm,
            dl.distancia_capital_km::DOUBLE PRECISION AS distancia_capital_km,
            dl.tiempo_acceso_h::DOUBLE PRECISION AS tiempo_acceso_h,
            dl.tipo_acceso,
            ie.kit_aula_conectada,
            (SELECT COUNT(*)::INT FROM dim_ie ie2 WHERE ie2.cod_local = ie.cod_local) AS ie_en_local,
            ia.prob_riesgo,
            ia.semaforo,
            ia.tasa_esperada_siguiente,
            ia.factores_principales,
            ia.anio_base,
            ia.anio_prediccion,
            inv.accion_sugerida,
            inv.costo_estimado_soles,
            inv.alumnos_por_pc_operativa AS alumnos_por_pc_local_ia
        FROM dim_ie ie
        JOIN dim_local dl ON dl.cod_local = ie.cod_local
        JOIN dim_distrito dist ON dist.ubigeo = dl.ubigeo
        JOIN dim_provincia prov ON prov.id_provincia = dist.id_provincia
        JOIN dim_departamento dep ON dep.id_departamento = prov.id_departamento
        JOIN dim_ugel ug ON ug.id_ugel = dl.id_ugel
        JOIN dim_nivel dn ON dn.id_nivel = ie.id_nivel
        JOIN dim_ruralidad dr ON dr.id_ruralidad = ie.id_ruralidad
        LEFT JOIN ia_pred_riesgo_ie ia ON ia.cod_modular = ie.cod_modular
        LEFT JOIN ia_inversion_local inv ON TRIM(inv.cod_local) = TRIM(ie.cod_local)
        WHERE TRIM(ie.cod_modular) = :cod
    """

    sql_hist = """
        SELECT
            dt.anio::INT AS anio,
            fp.matricula_inicial,
            fp.matricula_final,
            fp.retirados,
            fp.desertores,
            fp.tasa_desercion_pct::DOUBLE PRECISION AS tasa_desercion_pct,
            fp.asistencia_prom_pct::DOUBLE PRECISION AS asistencia_prom_pct,
            fp.docentes_total,
            fp.docentes_titulados,
            fp.declaro_computo,
            fp.cpu_total,
            fp.cpu_operativos,
            fp.tiene_internet,
            fp.fuente_energia,
            fp.estado_registro
        FROM fact_permanencia fp
        JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
        WHERE TRIM(fp.cod_modular) = :cod
        ORDER BY dt.anio ASC
    """

    sql_iot = """
        SELECT
            fc.id_lectura,
            fc.id_sensor,
            CONCAT(dt.anio::TEXT, '-', LPAD(dt.mes::TEXT, 2, '0')) AS mes,
            fc.tipo_enlace,
            fc.ancho_banda_mbps::DOUBLE PRECISION AS ancho_banda_mbps,
            fc.latencia_ms::DOUBLE PRECISION AS latencia_ms,
            fc.disponibilidad_pct::DOUBLE PRECISION AS disponibilidad_pct,
            fc.horas_corte_energia::DOUBLE PRECISION AS horas_corte_energia,
            fc.voltaje_v::DOUBLE PRECISION AS voltaje_v,
            fc.estado_sensor,
            fc.evento_climatico
        FROM fact_conectividad fc
        JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
        WHERE TRIM(fc.cod_modular) = :cod
        ORDER BY dt.anio ASC, dt.mes ASC
    """

    sql_anom = """
        SELECT id_lectura, id_sensor, mes, score_anomalia, motivo
        FROM ia_anomalias_iot
        WHERE TRIM(cod_modular) = :cod
        ORDER BY mes ASC
    """

    with engine.connect() as conn:
        perfil_row = conn.execute(text(sql_perfil), {"cod": cod}).mappings().first()
        hist_rows = conn.execute(text(sql_hist), {"cod": cod}).mappings().all()
        iot_rows = conn.execute(text(sql_iot), {"cod": cod}).mappings().all()
        anom_rows = conn.execute(text(sql_anom), {"cod": cod}).mappings().all()

    return {
        "perfil": dict(perfil_row) if perfil_row else None,
        "historico": pd.DataFrame([dict(r) for r in hist_rows]) if hist_rows else pd.DataFrame(),
        "telemetria": pd.DataFrame([dict(r) for r in iot_rows]) if iot_rows else pd.DataFrame(),
        "anomalias": pd.DataFrame([dict(r) for r in anom_rows]) if anom_rows else pd.DataFrame(),
    }
