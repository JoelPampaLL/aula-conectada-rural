"""Aplicación web Streamlit «Aula Conectada Rural» — Plataforma BI de 7 capas."""

from __future__ import annotations

import math
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from db import (
    FUENTES_CONFIG,
    VISTAS_PREDEFINIDAS,
    cargar_datos_ejemplo,
    cargar_fuente_staging,
    consultar_vista_dw,
    generar_plantilla_csv,
    guardar_validacion_staging,
    inicializar_esquema,
    leer_csv_crudo,
    leer_tabla_staging,
    obtener_columnas_tabla_dw,
    obtener_estado_fuentes,
    obtener_estado_proceso,
    obtener_logs_carga,
    obtener_logs_etl,
    obtener_opciones_filtros_dw,
    obtener_parametros_semantica,
    obtener_resumen_tablas_dw,
    obtener_serie_sensor_dw,
    obtener_tabla_ia,
    obtener_ultima_ejecucion_ia,
    obtener_ultima_validacion_staging,
    obtener_ultimo_reporte_etl,
    obtener_vista_previa_staging,
    validar_estructura_csv,
)
from etl import (
    ejecutar_control_calidad_staging,
    ejecutar_etl_completo,
    perfilar_tabla_staging,
)
from ia import (
    ejecutar_modelos_ia_en_vivo,
    recalcular_priorizacion_presupuesto,
)
from kpi import (
    COMPONENTES_INDICE_CONECTIVIDAD,
    JERARQUIAS_SEMANTICAS,
    actualizar_parametros_semantica,
    guardar_termino_glosario,
    inicializar_capa_semantica,
    obtener_alertas_activas_bi,
    obtener_catalogo_kpis_actualizado,
    obtener_desercion_departamento_anio,
    obtener_drilldown_departamentos_bi,
    obtener_ficha_institucion_bi,
    obtener_glosario_semantica,
    obtener_indice_conectividad_bi,
    obtener_kpis_ejecutivos_bi,
    obtener_mapa_parametros,
    obtener_opciones_filtros_bi,
    obtener_reparto_semaforo_bi,
    obtener_reporte_distritos_bi,
    obtener_reporte_instituciones_bi,
    obtener_resumen_jerarquias_dw,
    restablecer_parametros_semantica,
)
from modelo_aula_conectada import ErrorIA

ZONA_LIMA = ZoneInfo("America/Lima")
DATASTUDIO_URL_ABRIR = "https://datastudio.google.com/reporting/ea0cac40-f6fb-4022-8e44-464c9e1a0fdd/page/F76DG"
DATASTUDIO_URL_EMBED = "https://datastudio.google.com:/embed/reporting/ea0cac40-f6fb-4022-8e44-464c9e1a0fdd/page/F76DG"

st.set_page_config(
    page_title="Aula Conectada Rural — Plataforma BI",
    page_icon="🏫",
    layout="wide",
)


def fmt_int(valor: int | float | None) -> str:
    if valor is None or pd.isna(valor):
        return "0"
    return f"{int(valor):,}"


def fmt_dec(valor: float | None, decimales: int = 1) -> str:
    if valor is None or pd.isna(valor):
        return "—"
    return f"{float(valor):,.{decimales}f}"


def fmt_soles(valor: int | float | None) -> str:
    if valor is None or pd.isna(valor):
        return "—"
    return f"S/ {int(round(float(valor))):,}"


def fmt_semaforo(valor: str | None) -> str:
    mapa = {
        "Rojo": "🔴 Rojo",
        "Amarillo": "🟡 Amarillo",
        "Verde": "🟢 Verde",
    }
    txt = str(valor or "").strip()
    return mapa.get(txt, txt or "—")


def fmt_fecha(dt: Any, con_hora: bool = True) -> str:
    if dt is None or pd.isna(dt):
        return "—"
    if isinstance(dt, str):
        return dt
    if isinstance(dt, datetime):
        if dt.tzinfo is not None:
            dt = dt.astimezone(ZONA_LIMA)
        return dt.strftime("%Y-%m-%d %H:%M" if con_hora else "%Y-%m-%d")
    return str(dt)


def _sincronizar_auto_ia(origen_key: str) -> None:
    st.session_state["ia_auto_despues_etl"] = bool(st.session_state.get(origen_key, True))


def _renderizar_checkbox_auto_ia(widget_key: str) -> bool:
    st.session_state.setdefault("ia_auto_despues_etl", True)
    return st.checkbox(
        "Ejecutar automáticamente después del ETL",
        value=bool(st.session_state["ia_auto_despues_etl"]),
        key=widget_key,
        on_change=_sincronizar_auto_ia,
        args=(widget_key,),
    )


def _ejecutar_ia_con_progreso() -> tuple[bool, str | None, str | None]:
    """Ejecuta los modelos de IA en vivo con barra st.progress. Devuelve (exito, error_ia, aviso)."""
    barra = st.progress(0, text="Iniciando ejecución de modelos de Inteligencia Artificial...")
    try:
        ejecutar_modelos_ia_en_vivo(
            callback_progreso=lambda pct, msg: barra.progress(min(max(pct, 0.0), 1.0), text=msg)
        )
        return True, None, None
    except ErrorIA as exc:
        barra.empty()
        return False, str(exc), None
    except RuntimeError as exc:
        barra.empty()
        return False, None, str(exc)
    except Exception as exc:
        barra.empty()
        return False, f"Error inesperado al ejecutar los modelos de IA: {exc}", None


def renderizar_indicador_proceso(estado_proceso: dict[str, Any]) -> None:
    """Muestra la barra superior con el estado persistido de las 4 macroetapas."""
    cargadas = estado_proceso["fuentes_cargadas"]
    total = estado_proceso["total_fuentes"]
    if cargadas == total:
        ico_fuentes, txt_fuentes = "🟢", f"{cargadas}/{total} cargadas"
    elif cargadas > 0:
        ico_fuentes, txt_fuentes = "🟡", f"{cargadas}/{total} cargadas"
    else:
        ico_fuentes, txt_fuentes = "⚪", f"0/{total} cargadas"

    stg = estado_proceso["staging"]
    etl = estado_proceso["etl"]
    ia = estado_proceso["ia"]

    stg_fecha = f" ({fmt_fecha(stg['fecha'])})" if stg.get("fecha") else ""
    etl_fecha = f" ({fmt_fecha(etl['fecha'])})" if etl.get("fecha") else ""
    ia_fecha = f" ({fmt_fecha(ia['fecha'])})" if ia.get("fecha") else ""

    with st.container(border=True):
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(f"**Fuentes cargadas**\n\n{ico_fuentes} {txt_fuentes}")
        c2.markdown(f"**Staging validado**\n\n{stg['icono']} {stg['texto']}{stg_fecha}")
        c3.markdown(f"**ETL**\n\n{etl['icono']} {etl['texto']}{etl_fecha}")
        c4.markdown(f"**IA**\n\n{ia['icono']} {ia['texto']}{ia_fecha}")


@st.dialog("Confirmar reemplazo de fuente")
def dialogo_confirmar_reemplazo(
    fuente_id: str,
    df_nuevo: pd.DataFrame,
    nombre_archivo: str,
    filas_actuales: int,
    sobrantes: list[str],
) -> None:
    """Modal de confirmación antes de sobrescribir una tabla stg_* ya cargada."""
    cfg = FUENTES_CONFIG[fuente_id]
    st.warning(
        f"La fuente **{fuente_id} — {cfg['nombre']}** ya tiene "
        f"**{fmt_int(filas_actuales)}** filas en la tabla `{cfg['tabla']}`."
    )
    st.write(
        f"Si continúas, se eliminarán los datos actuales de `{cfg['tabla']}` y se "
        f"cargarán **{fmt_int(len(df_nuevo))}** filas desde `{nombre_archivo}`. "
        "La validación de staging, el ETL y la IA pasarán a **Desactualizado**."
    )
    if sobrantes:
        st.info(f"Columnas sobrantes que serán ignoradas: {', '.join(sobrantes)}")

    col_ok, col_cancel = st.columns(2)
    if col_ok.button("Sí, reemplazar datos", type="primary", use_container_width=True):
        with st.status(f"Reemplazando {fuente_id} en PostgreSQL (Neon)...", expanded=True):
            res = cargar_fuente_staging(
                fuente_id=fuente_id,
                df=df_nuevo,
                nombre_archivo=nombre_archivo,
            )
            st.write(
                f"Cargadas {fmt_int(res['filas'])} filas (lote {res['lote']}) "
                f"en {fmt_int(res['duracion_ms'])} ms."
            )
        st.rerun()

    if col_cancel.button("Cancelar", use_container_width=True):
        st.rerun()


@st.dialog("Confirmar carga de datos de ejemplo")
def dialogo_confirmar_ejemplo() -> None:
    """Modal de confirmación antes de reemplazar fuentes ya cargadas con los 5 CSV de ejemplo."""
    st.warning(
        "Ya existen datos cargados en al menos una tabla de staging. "
        "Cargar los datos de ejemplo reemplazará las 5 tablas `stg_*` actuales."
    )
    col_ok, col_cancel = st.columns(2)
    if col_ok.button("Sí, cargar datos de ejemplo", type="primary", use_container_width=True):
        barra = st.progress(0, text="Iniciando carga de datos de ejemplo...")
        cargar_datos_ejemplo(
            callback_progreso=lambda pct, msg: barra.progress(min(max(pct, 0.0), 1.0), text=msg)
        )
        st.rerun()

    if col_cancel.button("Cancelar", use_container_width=True):
        st.rerun()


def _renderizar_diagrama_relaciones() -> None:
    """Muestra un diagrama simple y legible de las relaciones entre las 5 fuentes."""
    svg = """
    <div style="overflow-x:auto;">
      <svg viewBox="0 0 880 250" width="100%" height="230" xmlns="http://www.w3.org/2000/svg"
           style="font-family: system-ui, -apple-system, sans-serif;">
        <defs>
          <marker id="flecha" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 1 L 10 5 L 0 9 z" fill="#2563eb"/>
          </marker>
        </defs>

        <!-- T1 -->
        <rect x="20" y="20" width="230" height="68" rx="8" fill="#eff6ff" stroke="#2563eb" stroke-width="1.5"/>
        <text x="135" y="46" text-anchor="middle" font-size="13" font-weight="700" fill="#1e3a8a">T1 · SIAGIE Matrícula</text>
        <text x="135" y="68" text-anchor="middle" font-size="11" fill="#334155">Clave: cod_modular + anio</text>

        <!-- T3 -->
        <rect x="20" y="152" width="230" height="68" rx="8" fill="#eff6ff" stroke="#2563eb" stroke-width="1.5"/>
        <text x="135" y="178" text-anchor="middle" font-size="13" font-weight="700" fill="#1e3a8a">T3 · Sensores IoT</text>
        <text x="135" y="200" text-anchor="middle" font-size="11" fill="#334155">Clave: id_lectura (id_sensor + mes)</text>

        <!-- T2 Centro -->
        <rect x="330" y="74" width="220" height="94" rx="10" fill="#ecfdf5" stroke="#059669" stroke-width="2"/>
        <text x="440" y="103" text-anchor="middle" font-size="14" font-weight="700" fill="#065f46">T2 · Padrón IE Rural</text>
        <text x="440" y="125" text-anchor="middle" font-size="11" fill="#064e3b">PK: cod_modular (7 díg.)</text>
        <text x="440" y="144" text-anchor="middle" font-size="11" fill="#064e3b">cod_local (6) · ubigeo (6)</text>

        <!-- T4 -->
        <rect x="630" y="20" width="230" height="68" rx="8" fill="#fefce8" stroke="#ca8a04" stroke-width="1.5"/>
        <text x="745" y="46" text-anchor="middle" font-size="13" font-weight="700" fill="#854d0e">T4 · Censo Equipamiento</text>
        <text x="745" y="68" text-anchor="middle" font-size="11" fill="#334155">Clave: cod_local + anio_censo</text>

        <!-- T5 -->
        <rect x="630" y="152" width="230" height="68" rx="8" fill="#fefce8" stroke="#ca8a04" stroke-width="1.5"/>
        <text x="745" y="178" text-anchor="middle" font-size="13" font-weight="700" fill="#854d0e">T5 · API Clima y Entorno</text>
        <text x="745" y="200" text-anchor="middle" font-size="11" fill="#334155">Clave: ubigeo + anio + mes</text>

        <!-- Flechas hacia T2 -->
        <line x1="250" y1="54" x2="326" y2="104" stroke="#2563eb" stroke-width="1.8" marker-end="url(#flecha)"/>
        <text x="278" y="66" text-anchor="middle" font-size="10.5" font-weight="600" fill="#1d4ed8">cod_modular</text>

        <line x1="250" y1="186" x2="326" y2="142" stroke="#2563eb" stroke-width="1.8" marker-end="url(#flecha)"/>
        <text x="278" y="182" text-anchor="middle" font-size="10.5" font-weight="600" fill="#1d4ed8">cod_modular</text>

        <line x1="630" y1="54" x2="554" y2="104" stroke="#2563eb" stroke-width="1.8" marker-end="url(#flecha)"/>
        <text x="598" y="66" text-anchor="middle" font-size="10.5" font-weight="600" fill="#1d4ed8">cod_local</text>

        <line x1="630" y1="186" x2="554" y2="142" stroke="#2563eb" stroke-width="1.8" marker-end="url(#flecha)"/>
        <text x="598" y="182" text-anchor="middle" font-size="10.5" font-weight="600" fill="#1d4ed8">ubigeo</text>
      </svg>
    </div>
    """
    with st.container(border=True):
        st.markdown("#### Relaciones entre las 5 fuentes")
        st.markdown(svg, unsafe_allow_html=True)
        st.caption(
            "T1.cod_modular → T2.cod_modular  ·  T3.cod_modular → T2.cod_modular  ·  "
            "T4.cod_local → T2.cod_local  ·  T5.ubigeo → T2.ubigeo"
        )


def renderizar_tab_fuentes(estado_fuentes: dict[str, dict[str, Any]]) -> None:
    """Pestaña 1 completa: tarjetas de fuentes, carga CSV, plantillas, ejemplo y vista previa."""
    st.subheader("1. Fuentes de datos")
    st.write(
        "Carga los 5 archivos CSV de origen en formato crudo hacia la base PostgreSQL (Neon). "
        "Todas las columnas se almacenan como texto (`TEXT`) preservando los ceros iniciales y sin "
        "corregir defectos."
    )

    hay_alguna_cargada = any(info["cargada"] for info in estado_fuentes.values())
    col_btn, col_info = st.columns([1.4, 3.6])
    if col_btn.button("Cargar datos de ejemplo", type="primary", use_container_width=True):
        if hay_alguna_cargada:
            dialogo_confirmar_ejemplo()
        else:
            barra = st.progress(0, text="Iniciando carga de datos de ejemplo...")
            cargar_datos_ejemplo(
                callback_progreso=lambda pct, msg: barra.progress(min(max(pct, 0.0), 1.0), text=msg)
            )
            st.rerun()

    col_info.info(
        "Los archivos de ejemplo (`data/ejemplo/`) son solo una muestra de referencia. "
        "Puedes reemplazar cualquier fuente subiendo un CSV con las mismas columnas requeridas."
    )

    st.markdown("#### Estado y carga por fuente")
    codigos = list(FUENTES_CONFIG.keys())
    for idx in range(0, len(codigos), 2):
        cols_grid = st.columns(2)
        for j in range(2):
            if idx + j >= len(codigos):
                continue
            fid = codigos[idx + j]
            info = estado_fuentes[fid]
            cfg = FUENTES_CONFIG[fid]

            with cols_grid[j]:
                with st.container(border=True):
                    estado_txt = (
                        f"🟢 Cargada · {fmt_fecha(info['fecha_carga'])} (lote {info['lote']})"
                        if info["cargada"]
                        else "⚪ Sin cargar"
                    )
                    st.markdown(f"##### {fid} — {info['nombre']}")
                    st.caption(f"Origen: {info['tipo_origen']} · Tabla: `{info['tabla']}`")
                    st.markdown(f"**Estado:** {estado_txt}")

                    m1, m2 = st.columns(2)
                    m1.metric("Filas en staging", fmt_int(info["filas"]))
                    m2.metric("Columnas requeridas", fmt_int(info["columnas"]))

                    st.download_button(
                        label=f"Descargar plantilla {fid}",
                        data=generar_plantilla_csv(fid),
                        file_name=f"plantilla_{cfg['archivo_ejemplo']}",
                        mime="text/csv",
                        use_container_width=True,
                        key=f"dl_{fid}",
                    )

                    archivo = st.file_uploader(
                        f"Subir CSV para {fid}",
                        type=["csv"],
                        key=f"up_{fid}",
                        help="Se valida por nombres de columna (el orden no importa).",
                    )
                    if archivo is not None:
                        try:
                            df_subido = leer_csv_crudo(archivo)
                        except Exception as exc:
                            st.error(f"No se pudo leer el CSV `{archivo.name}`: {exc}")
                        else:
                            valido, faltantes, sobrantes = validar_estructura_csv(df_subido, fid)
                            if not valido:
                                lista_faltantes = "\n".join(f"- `{col}`" for col in faltantes)
                                st.error(
                                    f"Archivo rechazado. Faltan **{len(faltantes)}** columnas "
                                    f"obligatorias:\n{lista_faltantes}"
                                )
                            else:
                                if sobrantes:
                                    st.warning(
                                        f"Columnas sobrantes detectadas (serán ignoradas): "
                                        f"`{', '.join(sobrantes)}`"
                                    )
                                st.caption(
                                    f"Archivo válido: `{archivo.name}` · "
                                    f"{fmt_int(len(df_subido))} filas detectadas."
                                )
                                etiqueta_btn = (
                                    f"Reemplazar {fid} en Staging"
                                    if info["cargada"]
                                    else f"Cargar {fid} en Staging"
                                )
                                if st.button(
                                    etiqueta_btn,
                                    type="primary",
                                    use_container_width=True,
                                    key=f"btn_load_{fid}",
                                ):
                                    if info["cargada"]:
                                        dialogo_confirmar_reemplazo(
                                            fuente_id=fid,
                                            df_nuevo=df_subido,
                                            nombre_archivo=archivo.name,
                                            filas_actuales=info["filas"],
                                            sobrantes=sobrantes,
                                        )
                                    else:
                                        with st.status(
                                            f"Cargando {fid} en PostgreSQL (Neon)...", expanded=True
                                        ):
                                            res = cargar_fuente_staging(
                                                fuente_id=fid,
                                                df=df_subido,
                                                nombre_archivo=archivo.name,
                                            )
                                            st.write(
                                                f"Cargadas {fmt_int(res['filas'])} filas "
                                                f"en {fmt_int(res['duracion_ms'])} ms."
                                            )
                                        st.rerun()

    st.divider()
    st.markdown("#### Vista previa paginada (25 filas por página)")
    fuentes_cargadas = [fid for fid, info in estado_fuentes.items() if info["cargada"]]
    if not fuentes_cargadas:
        st.info("Carga al menos una fuente para explorar la vista previa desde PostgreSQL.")
    else:
        c_sel, c_busq = st.columns([1.5, 2.5])
        fuente_sel = c_sel.selectbox(
            "Fuente a inspeccionar",
            options=fuentes_cargadas,
            format_func=lambda fid: f"{fid} — {FUENTES_CONFIG[fid]['nombre']}",
            key="preview_fuente_sel",
        )
        busqueda = c_busq.text_input(
            "Buscar en cualquier columna",
            placeholder="Ej. código modular, ubigeo, distrito, Sin señal...",
            key="preview_busqueda",
        )

        clave_filtro = f"{fuente_sel}::{busqueda}"
        if st.session_state.get("_preview_filtro_anterior") != clave_filtro:
            st.session_state["_preview_filtro_anterior"] = clave_filtro
            st.session_state["preview_pagina"] = 1

        pagina_actual = int(st.session_state.get("preview_pagina", 1))
        tam_pagina = 25
        df_vista, total_coincidencias = obtener_vista_previa_staging(
            fuente_id=fuente_sel,
            busqueda=busqueda,
            pagina=pagina_actual,
            tam_pagina=tam_pagina,
        )
        total_paginas = max(1, math.ceil(total_coincidencias / tam_pagina))
        if pagina_actual > total_paginas:
            pagina_actual = total_paginas
            st.session_state["preview_pagina"] = pagina_actual
            df_vista, total_coincidencias = obtener_vista_previa_staging(
                fuente_id=fuente_sel,
                busqueda=busqueda,
                pagina=pagina_actual,
                tam_pagina=tam_pagina,
            )

        p1, p2, p3 = st.columns([1, 2, 1])
        if p1.button("⬅ Anterior", disabled=(pagina_actual <= 1), use_container_width=True):
            st.session_state["preview_pagina"] = pagina_actual - 1
            st.rerun()

        inicio_rango = ((pagina_actual - 1) * tam_pagina + 1) if total_coincidencias > 0 else 0
        fin_rango = min(pagina_actual * tam_pagina, total_coincidencias)
        p2.markdown(
            f"<div style='text-align:center; padding-top:6px;'>"
            f"Página <b>{pagina_actual}</b> de <b>{total_paginas}</b> · "
            f"Mostrando <b>{fmt_int(inicio_rango)}–{fmt_int(fin_rango)}</b> de "
            f"<b>{fmt_int(total_coincidencias)}</b> filas</div>",
            unsafe_allow_html=True,
        )
        if p3.button("Siguiente ➡", disabled=(pagina_actual >= total_paginas), use_container_width=True):
            st.session_state["preview_pagina"] = pagina_actual + 1
            st.rerun()

        st.dataframe(df_vista.fillna(""), use_container_width=True, hide_index=True)

    st.divider()
    _renderizar_diagrama_relaciones()


def _ejecutar_validacion_completa(estado_fuentes: dict[str, dict[str, Any]]) -> None:
    """Lee tablas cargadas de staging, ejecuta perfilado + calidad y guarda el reporte en Neon."""
    inicio = time.perf_counter()
    fuentes_cargadas = [fid for fid, info in estado_fuentes.items() if info["cargada"]]
    if not fuentes_cargadas:
        st.warning("No hay fuentes cargadas para validar.")
        return

    barra = st.progress(0, text="Leyendo tablas de staging desde Neon...")
    tablas: dict[str, pd.DataFrame] = {}
    perfilados: dict[str, list[dict[str, Any]]] = {}
    resumen_tablas: list[dict[str, Any]] = []

    total_pasos = len(fuentes_cargadas) + 2
    for idx, fid in enumerate(fuentes_cargadas):
        barra.progress((idx + 0.5) / total_pasos, text=f"Perfilando {fid} ({FUENTES_CONFIG[fid]['tabla']})...")
        df_raw = leer_tabla_staging(fid)
        tablas[fid] = df_raw
        df_perf = perfilar_tabla_staging(fid, df_raw)
        perfilados[fid] = df_perf.to_dict(orient="records")

        resumen_tablas.append(
            {
                "Fuente": fid,
                "Tabla": FUENTES_CONFIG[fid]["tabla"],
                "Filas": int(len(df_raw)),
                "Columnas": int(len(FUENTES_CONFIG[fid]["columnas"])),
                "Celdas vacías": int(df_perf["Vacíos"].sum()),
                "Textos en col. numérica": int(df_perf["Textos en col. numérica"].sum()),
            }
        )

    barra.progress((len(fuentes_cargadas) + 1) / total_pasos, text="Ejecutando reglas de control de calidad...")
    df_hallazgos, advertencias = ejecutar_control_calidad_staging(tablas)

    duracion_ms = int((time.perf_counter() - inicio) * 1000)
    resumen = {
        "resumen_tablas": resumen_tablas,
        "perfilados": perfilados,
        "hallazgos": df_hallazgos.to_dict(orient="records"),
        "advertencias": advertencias,
    }

    barra.progress((total_pasos - 0.1) / total_pasos, text="Guardando diagnóstico en PostgreSQL...")
    guardar_validacion_staging(
        resumen=resumen,
        total_hallazgos=int(len(df_hallazgos)),
        duracion_ms=duracion_ms,
    )
    barra.progress(1.0, text="Validación de staging completada.")


def renderizar_tab_staging(
    estado_fuentes: dict[str, dict[str, Any]],
    estado_proceso: dict[str, Any],
) -> None:
    """Pestaña 2 completa: validación inicial, control de calidad genérico y logs de carga."""
    st.subheader("2. Staging Area (zona de aterrizaje)")
    st.info(
        "Los datos crudos se mantienen intactos en `stg_*`. Ningún valor es corregido o eliminado en esta capa; "
        "los defectos detectados quedan visibles para su tratamiento en el ETL."
    )

    fuentes_cargadas = [fid for fid, info in estado_fuentes.items() if info["cargada"]]
    col_val, col_msg = st.columns([1.4, 3.6])
    if col_val.button(
        "Validar staging",
        type="primary",
        disabled=(len(fuentes_cargadas) == 0),
        use_container_width=True,
    ):
        _ejecutar_validacion_completa(estado_fuentes)
        st.rerun()

    if len(fuentes_cargadas) == 0:
        col_msg.warning("Carga al menos una fuente en la pestaña «Fuentes de datos» para validar el staging.")
    elif estado_proceso["staging"]["texto"] == "Desactualizado":
        col_msg.warning(
            "Se cargaron o reemplazaron datos después de la última validación. "
            "Haz clic en **Validar staging** para actualizar el reporte."
        )
    elif estado_proceso["staging"]["texto"] == "Validado":
        col_msg.success(
            f"Validación al día · Última ejecución: {fmt_fecha(estado_proceso['staging']['fecha'])}"
        )

    validacion = obtener_ultima_validacion_staging()
    if validacion is None:
        st.caption("Aún no se ha ejecutado ninguna validación de staging.")
    else:
        resumen = validacion["resumen"]
        st.caption(
            f"Reporte almacenado · Lote de referencia: {validacion['lote_validacion']} · "
            f"Fecha: {fmt_fecha(validacion['fecha'])} · Duración: {fmt_int(validacion['duracion_ms'])} ms"
        )

        st.markdown("#### Validación inicial por tabla (filas, columnas, tipos y completitud)")
        df_resumen = pd.DataFrame(resumen.get("resumen_tablas", []))
        if not df_resumen.empty:
            df_resumen_fmt = df_resumen.copy()
            for col in ["Filas", "Columnas", "Celdas vacías", "Textos en col. numérica"]:
                df_resumen_fmt[col] = df_resumen_fmt[col].map(fmt_int)
            st.dataframe(df_resumen_fmt, use_container_width=True, hide_index=True)

        perfilados = resumen.get("perfilados", {})
        fuentes_perfil = [f for f in FUENTES_CONFIG if f in perfilados]
        if fuentes_perfil:
            fuente_prof_sel = st.selectbox(
                "Detalle de tipos y % de vacíos por columna",
                options=fuentes_perfil,
                format_func=lambda fid: f"{fid} — {FUENTES_CONFIG[fid]['nombre']}",
                key="staging_perfil_sel",
            )
            df_perf = pd.DataFrame(perfilados[fuente_prof_sel])
            if not df_perf.empty:
                df_perf_fmt = df_perf.copy()
                df_perf_fmt["Filas"] = df_perf_fmt["Filas"].map(fmt_int)
                df_perf_fmt["Vacíos"] = df_perf_fmt["Vacíos"].map(fmt_int)
                df_perf_fmt["% Vacíos"] = df_perf_fmt["% Vacíos"].map(lambda v: f"{fmt_dec(v, 1)} %")
                df_perf_fmt["% Convertible a número"] = df_perf_fmt["% Convertible a número"].map(
                    lambda v: "No aplica (texto)" if v is None or pd.isna(v) else f"{fmt_dec(v, 1)} %"
                )
                df_perf_fmt["Textos en col. numérica"] = df_perf_fmt["Textos en col. numérica"].map(fmt_int)
                st.dataframe(df_perf_fmt, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### Control de calidad genérico")
        for adv in resumen.get("advertencias", []):
            st.warning(adv)

        df_hallazgos = pd.DataFrame(resumen.get("hallazgos", []))
        if df_hallazgos.empty:
            st.success("🟢 Sin hallazgos de calidad en las tablas evaluadas.")
        else:
            cnt_alta = int((df_hallazgos["severidad"] == "Alta").sum())
            cnt_media = int((df_hallazgos["severidad"] == "Media").sum())
            cnt_baja = int((df_hallazgos["severidad"] == "Baja").sum())

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Reglas con hallazgos", fmt_int(len(df_hallazgos)))
            k2.metric("🔴 Severidad Alta", fmt_int(cnt_alta))
            k3.metric("🟡 Severidad Media", fmt_int(cnt_media))
            k4.metric("🟢 Severidad Baja", fmt_int(cnt_baja))

            f1, f2 = st.columns(2)
            filtro_fuente = f1.selectbox(
                "Filtrar por fuente",
                options=["Todas", *sorted(df_hallazgos["fuente"].unique().tolist())],
                key="qc_filtro_fuente",
            )
            filtro_sev = f2.selectbox(
                "Filtrar por severidad",
                options=["Todas", "Alta", "Media", "Baja"],
                format_func=lambda s: (
                    "Todas"
                    if s == "Todas"
                    else {"Alta": "🔴 Alta", "Media": "🟡 Media", "Baja": "🟢 Baja"}[s]
                ),
                key="qc_filtro_sev",
            )

            df_filtrado = df_hallazgos.copy()
            if filtro_fuente != "Todas":
                df_filtrado = df_filtrado[df_filtrado["fuente"] == filtro_fuente]
            if filtro_sev != "Todas":
                df_filtrado = df_filtrado[df_filtrado["severidad"] == filtro_sev]

            df_mostrar = df_filtrado[
                ["severidad_etiqueta", "fuente", "regla", "columna_o_clave", "cantidad", "detalle"]
            ].rename(
                columns={
                    "severidad_etiqueta": "Severidad",
                    "fuente": "Fuente",
                    "regla": "Regla",
                    "columna_o_clave": "Columna / Clave",
                    "cantidad": "Cantidad",
                    "detalle": "Detalle",
                }
            )
            df_mostrar["Cantidad"] = df_mostrar["Cantidad"].map(fmt_int)
            st.dataframe(df_mostrar, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("#### Logs de carga (`log_carga`)")
    df_logs = obtener_logs_carga(limite=100)
    if df_logs.empty:
        st.caption("No hay cargas registradas en `log_carga`.")
    else:
        df_logs_fmt = df_logs.copy()
        df_logs_fmt["filas"] = df_logs_fmt["filas"].map(fmt_int)
        df_logs_fmt["columnas"] = df_logs_fmt["columnas"].map(fmt_int)
        df_logs_fmt["duracion_ms"] = df_logs_fmt["duracion_ms"].map(lambda ms: f"{fmt_int(ms)} ms")
        df_logs_fmt["estado"] = df_logs_fmt["estado"].map(
            lambda e: "🟢 ok" if e == "ok" else "🔴 error"
        )
        df_logs_fmt["fecha"] = df_logs_fmt["fecha"].map(lambda f: fmt_fecha(f, con_hora=True))
        df_logs_fmt = df_logs_fmt.rename(
            columns={
                "lote": "Lote",
                "fuente": "Fuente",
                "nombre_archivo": "Archivo",
                "filas": "Filas",
                "columnas": "Columnas",
                "duracion_ms": "Duración",
                "estado": "Estado",
                "mensaje": "Observación",
                "fecha": "Fecha",
            }
        )[["Lote", "Fuente", "Archivo", "Filas", "Columnas", "Duración", "Estado", "Observación", "Fecha"]]
        st.dataframe(df_logs_fmt, use_container_width=True, hide_index=True)


def renderizar_tab_etl(
    estado_fuentes: dict[str, dict[str, Any]],
    estado_proceso: dict[str, Any],
) -> None:
    """Pestaña 3 completa: bloques Extract, Transform (limpiar_tablas) y Load idempotente al DW."""
    st.subheader("3. Proceso ETL (Extract · Transform · Load)")
    st.write(
        "Extrae los datos crudos de `stg_*`, aplica las reglas de limpieza de `limpiar_tablas()` "
        "(sección 5), excluye y reporta filas con claves huérfanas, y carga de forma idempotente el "
        "Data Warehouse en esquema copo de nieve."
    )

    todas_cargadas = all(info["cargada"] for info in estado_fuentes.values())

    col_btn, col_auto, col_estado = st.columns([1.3, 1.5, 2.2])
    auto_ia_etl = False
    with col_auto:
        auto_ia_etl = _renderizar_checkbox_auto_ia("etl_chk_auto_ia")

    if col_btn.button(
        "Ejecutar ETL",
        type="primary",
        disabled=(not todas_cargadas),
        use_container_width=True,
    ):
        barra = st.progress(0, text="Iniciando proceso ETL...")
        try:
            ejecutar_etl_completo(
                callback_progreso=lambda pct, msg: barra.progress(min(max(pct, 0.0), 1.0), text=msg)
            )
        except Exception as exc:
            st.error(f"El proceso ETL falló: {exc}")
        else:
            if auto_ia_etl:
                ok_ia, err_ia, aviso_ia = _ejecutar_ia_con_progreso()
                if ok_ia:
                    st.rerun()
                elif err_ia:
                    st.success("ETL completado correctamente en el Data Warehouse.")
                    st.error(f"La ejecución automática de IA no pudo completarse: {err_ia}")
                elif aviso_ia:
                    st.success("ETL completado correctamente en el Data Warehouse.")
                    st.warning(aviso_ia)
            else:
                st.rerun()

    if not todas_cargadas:
        faltan = [fid for fid, info in estado_fuentes.items() if not info["cargada"]]
        col_estado.warning(
            f"Se requieren las 5 fuentes cargadas para ejecutar el ETL. Faltan: {', '.join(faltan)}."
        )
    elif estado_proceso["etl"]["texto"] == "Desactualizado":
        col_estado.warning(
            "Se reemplazaron fuentes en staging después del último ETL. "
            "Ejecuta nuevamente **Ejecutar ETL** para actualizar el Data Warehouse."
        )
    elif estado_proceso["etl"]["texto"] == "Ejecutado":
        col_estado.success(
            f"ETL ejecutado y actualizado · Último lote: {fmt_fecha(estado_proceso['etl']['fecha'])}"
        )
    else:
        col_estado.info("Las 5 fuentes están listas en staging. Haz clic en **Ejecutar ETL**.")

    # ---- Bloque 1: Extract ----
    with st.container(border=True):
        st.markdown("#### Bloque 1 · Extract (tablas de entrada en `stg_*`)")
        filas_ext = [
            {
                "Fuente": fid,
                "Tabla staging": info["tabla"],
                "Estado": "🟢 Lista" if info["cargada"] else "⚪ Sin cargar",
                "Filas en staging": fmt_int(info["filas"]),
                "Lote origen": info["lote"] if info["lote"] is not None else "—",
            }
            for fid, info in estado_fuentes.items()
        ]
        st.dataframe(pd.DataFrame(filas_ext), use_container_width=True, hide_index=True)

    ultimo_etl = obtener_ultimo_reporte_etl()
    if ultimo_etl is None:
        st.caption("Aún no se ha ejecutado el ETL sobre las tablas actuales.")
    else:
        rep = ultimo_etl["reporte"]
        st.caption(
            f"Última ejecución ETL · Lote **{ultimo_etl['lote']}** · "
            f"Fecha: {fmt_fecha(ultimo_etl['fecha'])} · Duración: {fmt_int(ultimo_etl['duracion_ms'])} ms"
        )

        # ---- Bloque 2: Transform ----
        with st.container(border=True):
            st.markdown("#### Bloque 2 · Transform (`limpiar_tablas` · Antes vs. Después)")

            log_limp = rep.get("log_limpiar_tablas", {})
            if log_limp:
                cols_m = st.columns(len(log_limp))
                for col_m, (k_log, val_log) in zip(cols_m, log_limp.items()):
                    etiqueta = k_log.replace("_", " ")
                    col_m.metric(etiqueta, fmt_int(val_log))

            df_reglas = pd.DataFrame(rep.get("reglas_comparativas", []))
            if not df_reglas.empty:
                df_reglas_fmt = df_reglas.copy()
                df_reglas_fmt["Antes (staging)"] = df_reglas_fmt["Antes (staging)"].map(fmt_int)
                df_reglas_fmt["Después (limpio)"] = df_reglas_fmt["Después (limpio)"].map(fmt_int)
                st.dataframe(df_reglas_fmt, use_container_width=True, hide_index=True)

            st.markdown("##### Antes y después de un registro de ejemplo por regla")
            df_ej = pd.DataFrame(rep.get("ejemplos_reglas", []))
            if df_ej.empty:
                st.info("No se registraron modificaciones en las reglas para el juego de datos actual.")
            else:
                st.dataframe(df_ej, use_container_width=True, hide_index=True)

            st.markdown("##### Claves huérfanas excluidas en el ETL")
            df_huerf = pd.DataFrame(rep.get("huerfanas", []))
            if df_huerf.empty:
                st.success("🟢 No se encontraron filas con claves huérfanas; todas las referencias con T2 son válidas.")
            else:
                df_huerf_fmt = df_huerf.copy()
                df_huerf_fmt["Filas excluidas"] = df_huerf_fmt["Filas excluidas"].map(fmt_int)
                st.warning("Se excluyeron filas sin clave válida en T2 para preservar la integridad del DW:")
                st.dataframe(df_huerf_fmt, use_container_width=True, hide_index=True)

        # ---- Bloque 3: Load ----
        with st.container(border=True):
            st.markdown("#### Bloque 3 · Load (Data Warehouse e historial `log_etl`)")
            conteos = rep.get("conteos_dw", {})
            if conteos:
                st.caption(
                    f"Años generados dinámicamente en `dim_tiempo`: "
                    f"{', '.join(str(a) for a in rep.get('anios_dw', [])) or '—'}"
                )
                orden_dw = [
                    "dim_departamento",
                    "dim_provincia",
                    "dim_distrito",
                    "dim_ugel",
                    "dim_local",
                    "dim_ie",
                    "dim_tiempo",
                    "fact_permanencia",
                    "fact_conectividad",
                ]
                filas_dw_load = [
                    {"Tabla DW": t, "Filas cargadas": fmt_int(conteos.get(t, 0))}
                    for t in orden_dw
                ]
                st.dataframe(pd.DataFrame(filas_dw_load), use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("#### Historial de ejecuciones ETL (`log_etl`)")
    df_log_etl = obtener_logs_etl(limite=150)
    if df_log_etl.empty:
        st.caption("No hay registros en `log_etl`.")
    else:
        df_le = df_log_etl.copy()
        df_le["filas_afectadas"] = df_le["filas_afectadas"].map(fmt_int)
        df_le["filas_cargadas"] = df_le["filas_cargadas"].map(fmt_int)
        df_le["estado"] = df_le["estado"].map(lambda e: "🟢 ok" if e == "ok" else "🔴 error")
        df_le["fecha"] = df_le["fecha"].map(lambda f: fmt_fecha(f, con_hora=True))
        df_le = df_le.rename(
            columns={
                "lote": "Lote",
                "fuente": "Fuente",
                "regla": "Regla / Paso",
                "filas_afectadas": "Filas afectadas (antes)",
                "filas_cargadas": "Filas resultantes (después)",
                "estado": "Estado",
                "fecha": "Fecha",
            }
        )[["Lote", "Fuente", "Regla / Paso", "Filas afectadas (antes)", "Filas resultantes (después)", "Estado", "Fecha"]]
        st.dataframe(df_le, use_container_width=True, hide_index=True)


def _renderizar_diagrama_dw() -> None:
    """Diagrama físico Entidad-Relación (Snowflake Schema) con tablas, campos reales y cardinalidad 1 a N."""

    def _esc(txt: str) -> str:
        return txt.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    paletas: dict[str, tuple[str, str]] = {
        "territorial": ("#065f46", "#10b981"),
        "educativa": ("#1e3a8a", "#3b82f6"),
        "apoyo": ("#334155", "#64748b"),
        "tiempo": ("#581c87", "#a855f7"),
        "hechos": ("#92400e", "#eab308"),
    }

    # Definición fiel a las tablas y columnas reales creadas en PostgreSQL (db.py)
    tablas: dict[str, dict[str, Any]] = {
        "dim_departamento": {
            "x": 18,
            "y": 14,
            "w": 230,
            "tipo": "territorial",
            "cols": 1,
            "campos": [
                ("id_departamento", "PK", "serial"),
                ("nombre", "", "text"),
            ],
        },
        "dim_provincia": {
            "x": 288,
            "y": 14,
            "w": 240,
            "tipo": "territorial",
            "cols": 1,
            "campos": [
                ("id_provincia", "PK", "serial"),
                ("id_departamento", "FK", "int"),
                ("nombre", "", "text"),
            ],
        },
        "dim_distrito": {
            "x": 568,
            "y": 14,
            "w": 230,
            "tipo": "territorial",
            "cols": 1,
            "campos": [
                ("ubigeo", "PK", "char(6)"),
                ("id_provincia", "FK", "int"),
                ("nombre", "", "text"),
            ],
        },
        "dim_ugel": {
            "x": 855,
            "y": 14,
            "w": 210,
            "tipo": "apoyo",
            "cols": 1,
            "campos": [
                ("id_ugel", "PK", "serial"),
                ("nombre", "", "text"),
            ],
        },
        "dim_local": {
            "x": 505,
            "y": 134,
            "w": 295,
            "tipo": "educativa",
            "cols": 1,
            "campos": [
                ("cod_local", "PK", "char(6)"),
                ("ubigeo", "FK", "char(6)"),
                ("id_ugel", "FK", "int"),
                ("latitud", "", "numeric"),
                ("longitud", "", "numeric"),
                ("altitud_msnm", "", "int"),
                ("distancia_capital_km", "", "numeric"),
                ("tiempo_acceso_h", "", "numeric"),
                ("tipo_acceso", "", "text"),
            ],
        },
        "dim_ie": {
            "x": 845,
            "y": 134,
            "w": 245,
            "tipo": "educativa",
            "cols": 1,
            "campos": [
                ("cod_modular", "PK", "char(7)"),
                ("cod_local", "FK", "char(6)"),
                ("id_nivel", "FK", "smallint"),
                ("id_ruralidad", "FK", "smallint"),
                ("nombre", "", "text"),
                ("kit_aula_conectada", "", "bool"),
            ],
        },
        "dim_nivel": {
            "x": 1135,
            "y": 120,
            "w": 167,
            "tipo": "apoyo",
            "cols": 1,
            "campos": [
                ("id_nivel", "PK", "smallint"),
                ("nivel", "", "text"),
            ],
        },
        "dim_ruralidad": {
            "x": 1135,
            "y": 214,
            "w": 167,
            "tipo": "apoyo",
            "cols": 1,
            "campos": [
                ("id_ruralidad", "PK", "smallint"),
                ("gradiente", "", "text"),
            ],
        },
        "dim_tiempo": {
            "x": 18,
            "y": 380,
            "w": 215,
            "tipo": "tiempo",
            "cols": 1,
            "campos": [
                ("id_tiempo", "PK", "int"),
                ("anio", "", "smallint"),
                ("mes", "", "smallint"),
                ("trimestre", "", "smallint"),
                ("nombre_mes", "", "text"),
            ],
        },
        "fact_permanencia": {
            "x": 266,
            "y": 358,
            "w": 502,
            "tipo": "hechos",
            "cols": 2,
            "campos": [
                ("id_fact", "PK", "serial"),
                ("cod_modular", "FK", "char(7)"),
                ("id_tiempo", "FK", "int"),
                ("matricula_inicial", "", "int"),
                ("matricula_final", "", "int"),
                ("retirados", "", "int"),
                ("desertores", "", "int"),
                ("tasa_desercion_pct", "", "numeric"),
                ("asistencia_prom_pct", "", "numeric"),
                ("docentes_total", "", "int"),
                ("docentes_titulados", "", "int"),
                ("declaro_computo", "", "bool"),
                ("cpu_total", "", "int"),
                ("cpu_operativos", "", "int"),
                ("cpu_en_reparacion", "", "int"),
                ("cpu_mas_3_anios", "", "int"),
                ("tiene_internet", "", "bool"),
                ("tipo_internet", "", "text"),
                ("fuente_energia", "", "text"),
                ("tiene_ups", "", "bool"),
                ("tiene_pozo_tierra", "", "bool"),
                ("n_equipos_red", "", "int"),
                ("precipitacion_anual_mm", "", "numeric"),
                ("dias_sin_clases_clima", "", "int"),
                ("temp_min_anual_c", "", "numeric"),
                ("pobreza_pct", "", "numeric"),
                ("hogares_internet_pct", "", "numeric"),
                ("cobertura_movil", "", "text"),
                ("estado_registro", "", "text"),
            ],
        },
        "fact_conectividad": {
            "x": 805,
            "y": 358,
            "w": 497,
            "tipo": "hechos",
            "cols": 2,
            "campos": [
                ("id_fact", "PK", "serial"),
                ("cod_modular", "FK", "char(7)"),
                ("id_tiempo", "FK", "int"),
                ("id_lectura", "", "text"),
                ("id_sensor", "", "text"),
                ("tipo_enlace", "", "text"),
                ("ancho_banda_mbps", "", "numeric"),
                ("latencia_ms", "", "numeric"),
                ("disponibilidad_pct", "", "numeric"),
                ("horas_corte_energia", "", "numeric"),
                ("voltaje_v", "", "numeric"),
                ("carga_bateria_pct", "", "numeric"),
                ("temperatura_aula_c", "", "numeric"),
                ("dispositivos_conectados", "", "int"),
                ("estado_sensor", "", "text"),
                ("precipitacion_mm", "", "numeric"),
                ("evento_climatico", "", "text"),
            ],
        },
    }

    partes_svg: list[str] = []

    # 1. Conectores ortogonales 1 a N (Crow's Foot) dibujados bajo las tarjetas
    # Cada relación define puntos (x, y) desde el lado padre (1) hacia el lado hijo (N)
    relaciones: list[tuple[list[tuple[float, float]], str]] = [
        # dim_departamento (1) -> dim_provincia (N)
        ([(248, 51), (288, 51)], "#10b981"),
        # dim_provincia (1) -> dim_distrito (N)
        ([(528, 59), (568, 59)], "#10b981"),
        # dim_distrito (1) -> dim_local (N)
        ([(652, 105), (652, 134)], "#38bdf8"),
        # dim_ugel (1) -> dim_local (N)
        ([(955, 88), (955, 112), (745, 112), (745, 134)], "#94a3b8"),
        # dim_local (1) -> dim_ie (N)
        ([(800, 205), (845, 205)], "#3b82f6"),
        # dim_nivel (1) -> dim_ie (N)
        ([(1135, 158), (1090, 158)], "#94a3b8"),
        # dim_ruralidad (1) -> dim_ie (N)
        ([(1135, 246), (1090, 246)], "#94a3b8"),
        # dim_ie (1) -> fact_permanencia (N)
        ([(910, 276), (910, 342), (540, 342), (540, 358)], "#eab308"),
        # dim_ie (1) -> fact_conectividad (N)
        ([(1035, 276), (1035, 358)], "#eab308"),
        # dim_tiempo (1) -> fact_permanencia (N)
        ([(233, 442), (266, 442)], "#a855f7"),
        # dim_tiempo (1) -> fact_conectividad (N)
        ([(125, 505), (125, 668), (925, 668), (925, 550)], "#a855f7"),
    ]

    for pts, color_lin in relaciones:
        pts_str = " ".join(f"{px:g},{py:g}" for px, py in pts)
        partes_svg.append(
            f'<polyline points="{pts_str}" fill="none" stroke="{color_lin}" stroke-width="1.6" stroke-linejoin="round"/>'
        )

        # Marcador "1" en el origen (lado PK)
        x0, y0 = pts[0]
        x1, y1 = pts[1]
        dx0 = 1 if x1 > x0 else (-1 if x1 < x0 else 0)
        dy0 = 1 if y1 > y0 else (-1 if y1 < y0 else 0)
        bx0, by0 = x0 + dx0 * 7, y0 + dy0 * 7
        if dx0 != 0:
            partes_svg.append(
                f'<line x1="{bx0:g}" y1="{by0 - 5:g}" x2="{bx0:g}" y2="{by0 + 5:g}" stroke="{color_lin}" stroke-width="1.8"/>'
                f'<text x="{x0 + dx0 * 13:g}" y="{by0 - 6:g}" text-anchor="middle" font-size="9" font-weight="700" fill="{color_lin}">1</text>'
            )
        else:
            partes_svg.append(
                f'<line x1="{bx0 - 5:g}" y1="{by0:g}" x2="{bx0 + 5:g}" y2="{by0:g}" stroke="{color_lin}" stroke-width="1.8"/>'
                f'<text x="{bx0 + 9:g}" y="{y0 + dy0 * 14:g}" text-anchor="middle" font-size="9" font-weight="700" fill="{color_lin}">1</text>'
            )

        # Marcador "N" (Crow's Foot / pata de gallo) en el destino (lado FK)
        px_prev, py_prev = pts[-2]
        px_end, py_end = pts[-1]
        dxn = 1 if px_end > px_prev else (-1 if px_end < px_prev else 0)
        dyn = 1 if py_end > py_prev else (-1 if py_end < py_prev else 0)
        cx, cy = px_end - dxn * 11, py_end - dyn * 11
        if dxn != 0:
            partes_svg.extend(
                [
                    f'<line x1="{cx:g}" y1="{cy - 5:g}" x2="{cx:g}" y2="{cy + 5:g}" stroke="{color_lin}" stroke-width="1.6"/>',
                    f'<line x1="{cx:g}" y1="{cy:g}" x2="{px_end:g}" y2="{py_end - 5.5:g}" stroke="{color_lin}" stroke-width="1.6"/>',
                    f'<line x1="{cx:g}" y1="{cy:g}" x2="{px_end:g}" y2="{py_end:g}" stroke="{color_lin}" stroke-width="1.6"/>',
                    f'<line x1="{cx:g}" y1="{cy:g}" x2="{px_end:g}" y2="{py_end + 5.5:g}" stroke="{color_lin}" stroke-width="1.6"/>',
                    f'<text x="{px_end - dxn * 17:g}" y="{cy - 7:g}" text-anchor="middle" font-size="9" font-weight="700" fill="{color_lin}">N</text>',
                ]
            )
        else:
            partes_svg.extend(
                [
                    f'<line x1="{cx - 5:g}" y1="{cy:g}" x2="{cx + 5:g}" y2="{cy:g}" stroke="{color_lin}" stroke-width="1.6"/>',
                    f'<line x1="{cx:g}" y1="{cy:g}" x2="{px_end - 5.5:g}" y2="{py_end:g}" stroke="{color_lin}" stroke-width="1.6"/>',
                    f'<line x1="{cx:g}" y1="{cy:g}" x2="{px_end:g}" y2="{py_end:g}" stroke="{color_lin}" stroke-width="1.6"/>',
                    f'<line x1="{cx:g}" y1="{cy:g}" x2="{px_end + 5.5:g}" y2="{py_end:g}" stroke="{color_lin}" stroke-width="1.6"/>',
                    f'<text x="{cx + 10:g}" y="{py_end - dyn * 14:g}" text-anchor="middle" font-size="9" font-weight="700" fill="{color_lin}">N</text>',
                ]
            )

    # 2. Tarjetas ERD de las 11 tablas
    header_h = 27
    pad_y = 7
    row_h = 16.8
    for nombre_tabla, cfg in tablas.items():
        tx = float(cfg["x"])
        ty = float(cfg["y"])
        tw = float(cfg["w"])
        n_cols = int(cfg["cols"])
        campos: list[tuple[str, str, str]] = cfg["campos"]
        n_rows = math.ceil(len(campos) / n_cols)
        th = header_h + (pad_y * 2) + (n_rows * row_h)
        bg_head, borde_head = paletas[cfg["tipo"]]

        clip_id = f"clip_{nombre_tabla}"
        partes_svg.extend(
            [
                f'<defs><clipPath id="{clip_id}"><rect x="{tx:g}" y="{ty:g}" width="{tw:g}" height="{th:g}" rx="7"/></clipPath></defs>',
                f'<rect x="{tx:g}" y="{ty:g}" width="{tw:g}" height="{th:g}" rx="7" fill="#161b22" stroke="#30363d" stroke-width="1.2"/>',
                f'<g clip-path="url(#{clip_id})">',
                f'<rect x="{tx:g}" y="{ty:g}" width="{tw:g}" height="{header_h}" fill="{bg_head}"/>',
                f'<line x1="{tx:g}" y1="{ty + header_h:g}" x2="{tx + tw:g}" y2="{ty + header_h:g}" stroke="{borde_head}" stroke-width="1.4"/>',
                f'<text x="{tx + 10:g}" y="{ty + 17.5:g}" font-size="11.5" font-weight="700" fill="#f8fafc">{_esc(nombre_tabla)}</text>',
            ]
        )

        if n_cols == 2:
            mid_x = tx + tw / 2.0
            partes_svg.append(
                f'<line x1="{mid_x:g}" y1="{ty + header_h + 4:g}" x2="{mid_x:g}" y2="{ty + th - 4:g}" stroke="#262c36" stroke-width="1"/>'
            )

        col_w = tw / n_cols
        for idx, (col_nom, rol, tipo_dato) in enumerate(campos):
            sub_c = idx // n_rows
            sub_r = idx % n_rows
            cx = tx + sub_c * col_w
            cy = ty + header_h + pad_y + (sub_r * row_h) + 11.5

            if rol == "PK":
                partes_svg.append(
                    f'<rect x="{cx + 7:g}" y="{cy - 8.5:g}" width="21" height="11.5" rx="2.5" fill="#422006" stroke="#eab308" stroke-width="0.8"/>'
                )
                partes_svg.append(
                    f'<text x="{cx + 17.5:g}" y="{cy:g}" text-anchor="middle" font-size="7.5" font-weight="700" fill="#fde047">PK</text>'
                )
                txt_x = cx + 33
                col_fill = "#f8fafc"
                col_weight = "600"
            elif rol == "FK":
                partes_svg.append(
                    f'<rect x="{cx + 7:g}" y="{cy - 8.5:g}" width="21" height="11.5" rx="2.5" fill="#172554" stroke="#60a5fa" stroke-width="0.8"/>'
                )
                partes_svg.append(
                    f'<text x="{cx + 17.5:g}" y="{cy:g}" text-anchor="middle" font-size="7.5" font-weight="700" fill="#93c5fd">FK</text>'
                )
                txt_x = cx + 33
                col_fill = "#dbeafe"
                col_weight = "600"
            else:
                partes_svg.append(
                    f'<circle cx="{cx + 16:g}" cy="{cy - 3:g}" r="2" fill="#475569"/>'
                )
                txt_x = cx + 24
                col_fill = "#c9d1d9"
                col_weight = "400"

            partes_svg.append(
                f'<text x="{txt_x:g}" y="{cy:g}" font-size="9.6" font-weight="{col_weight}" fill="{col_fill}">{_esc(col_nom)}</text>'
            )
            partes_svg.append(
                f'<text x="{cx + col_w - 8:g}" y="{cy:g}" text-anchor="end" font-size="8.6" fill="#8b949e">{_esc(tipo_dato)}</text>'
            )

        partes_svg.append("</g>")

    # 3. Leyenda compacta en esquina inferior derecha (libre de cruces)
    partes_svg.extend(
        [
            '<rect x="975" y="576" width="327" height="78" rx="7" fill="#161b22" stroke="#30363d" stroke-width="1.1"/>',
            '<text x="987" y="594" font-size="10" font-weight="700" fill="#e6edf3">Leyenda física · Copo de Nieve (1 : N)</text>',
            '<rect x="987" y="603" width="21" height="11.5" rx="2.5" fill="#422006" stroke="#eab308" stroke-width="0.8"/>',
            '<text x="997.5" y="611.5" text-anchor="middle" font-size="7.5" font-weight="700" fill="#fde047">PK</text>',
            '<text x="1014" y="612" font-size="9" fill="#c9d1d9">Clave primaria</text>',
            '<rect x="1095" y="603" width="21" height="11.5" rx="2.5" fill="#172554" stroke="#60a5fa" stroke-width="0.8"/>',
            '<text x="1105.5" y="611.5" text-anchor="middle" font-size="7.5" font-weight="700" fill="#93c5fd">FK</text>',
            '<text x="1122" y="612" font-size="9" fill="#c9d1d9">Clave foránea</text>',
            '<line x1="1205" y1="609" x2="1245" y2="609" stroke="#94a3b8" stroke-width="1.5"/>',
            '<line x1="1212" y1="604" x2="1212" y2="614" stroke="#94a3b8" stroke-width="1.6"/>',
            '<line x1="1236" y1="605" x2="1236" y2="613" stroke="#94a3b8" stroke-width="1.4"/>',
            '<line x1="1236" y1="609" x2="1245" y2="604" stroke="#94a3b8" stroke-width="1.4"/>',
            '<line x1="1236" y1="609" x2="1245" y2="614" stroke="#94a3b8" stroke-width="1.4"/>',
            '<text x="1251" y="612" font-size="9" fill="#c9d1d9">1 a N</text>',
            '<rect x="987" y="628" width="9" height="9" rx="2" fill="#065f46" stroke="#10b981"/>',
            '<text x="1000" y="636" font-size="8.6" fill="#94a3b8">Territorial</text>',
            '<rect x="1055" y="628" width="9" height="9" rx="2" fill="#1e3a8a" stroke="#3b82f6"/>',
            '<text x="1068" y="636" font-size="8.6" fill="#94a3b8">Local / IE</text>',
            '<rect x="1118" y="628" width="9" height="9" rx="2" fill="#334155" stroke="#64748b"/>',
            '<text x="1131" y="636" font-size="8.6" fill="#94a3b8">Apoyo</text>',
            '<rect x="1170" y="628" width="9" height="9" rx="2" fill="#581c87" stroke="#a855f7"/>',
            '<text x="1183" y="636" font-size="8.6" fill="#94a3b8">Tiempo</text>',
            '<rect x="1225" y="628" width="9" height="9" rx="2" fill="#92400e" stroke="#eab308"/>',
            '<text x="1238" y="636" font-size="8.6" fill="#94a3b8">Hechos</text>',
        ]
    )

    svg_body = "\n".join(partes_svg)
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8" />
<style>
  html, body {{
    margin: 0;
    padding: 0;
    overflow: hidden;
    background: #0d1117;
    border-radius: 8px;
  }}
  .dw-erd-wrap {{
    width: 100%;
    height: 680px;
    display: flex;
    justify-content: center;
    align-items: center;
    background: #0d1117;
  }}
</style>
</head>
<body>
<div class="dw-erd-wrap">
<svg viewBox="0 0 1320 680" width="100%" height="100%" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" style="max-width: 1320px; max-height: 680px; display: block; margin: 0 auto; font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;">
{svg_body}
</svg>
</div>
</body>
</html>"""
    with st.container(border=True):
        st.markdown("#### Esquema copo de nieve y jerarquía territorial")
        st.info(
            "**Jerarquía territorial:** `Departamento` → `Provincia` → `Distrito (ubigeo)` → "
            "`Local escolar (cod_local)` → `Institución Educativa (cod_modular)`"
        )
        components.html(html, height=680, scrolling=False)


def renderizar_tab_dw() -> None:
    """Pestaña 4 completa: esquema copo de nieve, catálogo de tablas y visor de vistas vw_*."""
    st.subheader("4. Data Warehouse (esquema copo de nieve)")
    _renderizar_diagrama_dw()

    st.markdown("#### Tablas del esquema, claves y conteo de filas")
    df_tablas = obtener_resumen_tablas_dw()
    df_tablas_fmt = df_tablas.copy()
    df_tablas_fmt["Filas"] = df_tablas_fmt["Filas"].map(fmt_int)
    st.dataframe(df_tablas_fmt, use_container_width=True, hide_index=True)

    tabla_sel = st.selectbox(
        "Inspeccionar columnas de una tabla del Data Warehouse",
        options=df_tablas["Tabla"].tolist(),
        key="dw_tabla_cols_sel",
    )
    df_cols = obtener_columnas_tabla_dw(tabla_sel)
    st.dataframe(
        df_cols.rename(
            columns={
                "columna": "Columna",
                "tipo_dato": "Tipo PostgreSQL",
                "admite_nulo": "Admite nulo",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.markdown("#### Visor de consultas predefinidas sobre vistas `vw_*`")
    total_hechos = int(df_tablas.loc[df_tablas["Tabla"] == "fact_permanencia", "Filas"].iloc[0])
    if total_hechos == 0:
        st.info("El Data Warehouse aún no tiene datos. Ejecuta el proceso ETL en la pestaña 3.")
        return

    opciones = obtener_opciones_filtros_dw()
    vista_sel = st.selectbox(
        "Consulta predefinida (vista SQL)",
        options=list(VISTAS_PREDEFINIDAS.keys()),
        format_func=lambda v: VISTAS_PREDEFINIDAS[v]["nombre"],
        key="dw_vista_sel",
    )

    filtros_vista = set(VISTAS_PREDEFINIDAS[vista_sel]["filtros"])
    c_dep, c_dist, c_niv, c_ref = st.columns([1.2, 1.2, 1.0, 0.8])

    deps_opts = ["Todos", *opciones["departamentos"]]
    dep_sel = c_dep.selectbox(
        "Departamento",
        options=deps_opts,
        disabled=("departamento" not in filtros_vista),
        key="dw_filtro_dep",
    )

    if dep_sel == "Todos":
        dists_filtrados = sorted({r["distrito"] for r in opciones["distritos"]})
    else:
        dists_filtrados = sorted(
            {r["distrito"] for r in opciones["distritos"] if r["departamento"] == dep_sel}
        )
    dist_sel = c_dist.selectbox(
        "Distrito",
        options=["Todos", *dists_filtrados],
        disabled=("distrito" not in filtros_vista),
        key="dw_filtro_dist",
    )

    niv_sel = c_niv.selectbox(
        "Nivel",
        options=["Todos", *opciones["niveles"]],
        disabled=("nivel" not in filtros_vista),
        key="dw_filtro_niv",
    )

    if c_ref.button("Actualizar vista", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    if not filtros_vista:
        st.caption("Esta vista de entrada para ML muestra la tabla completa sin filtros territoriales.")

    df_vista_dw = consultar_vista_dw(
        nombre_vista=vista_sel,
        departamento=dep_sel if "departamento" in filtros_vista else "Todos",
        distrito=dist_sel if "distrito" in filtros_vista else "Todos",
        nivel=niv_sel if "nivel" in filtros_vista else "Todos",
        limite=500,
    )

    st.caption(f"Mostrando **{fmt_int(len(df_vista_dw))}** filas desde `{vista_sel}` (máximo 500).")
    st.dataframe(df_vista_dw, use_container_width=True, hide_index=True)

    st.download_button(
        label=f"Exportar {vista_sel} a CSV",
        data=df_vista_dw.to_csv(index=False).encode("utf-8"),
        file_name=f"{vista_sel}.csv",
        mime="text/csv",
    )


def _renderizar_modulo1_riesgo(
    ultima_ejecucion: dict[str, Any],
    resumen_modelo: dict[str, Any],
) -> None:
    """Módulo 1: Riesgo de deserción, importancia de variables y proyección por escenarios."""
    df_riesgo = obtener_tabla_ia("ia_pred_riesgo_ie")
    df_imp = obtener_tabla_ia("ia_importancia_variables")
    df_proy = obtener_tabla_ia("ia_proyeccion_desertores")

    if df_riesgo.empty:
        st.info("No hay predicciones de riesgo registradas.")
        return

    conteo_sem = df_riesgo["semaforo"].value_counts().to_dict()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Instituciones evaluadas", fmt_int(len(df_riesgo)))
    c2.metric("🔴 Riesgo Rojo", fmt_int(conteo_sem.get("Rojo", 0)))
    c3.metric("🟡 Riesgo Amarillo", fmt_int(conteo_sem.get("Amarillo", 0)))
    c4.metric("🟢 Riesgo Verde", fmt_int(conteo_sem.get("Verde", 0)))

    st.markdown("##### Predicción de riesgo de deserción por institución")
    f1, f2, f3, f4 = st.columns([1.2, 1.0, 1.0, 1.4])
    deps = sorted(df_riesgo["departamento"].dropna().unique().tolist())
    niveles = sorted(df_riesgo["nivel"].dropna().unique().tolist())
    dep_sel = f1.selectbox("Departamento", ["Todos", *deps], key="ia_m1_dep")
    sem_sel = f2.selectbox(
        "Semáforo",
        ["Todos", "Rojo", "Amarillo", "Verde"],
        format_func=lambda s: "Todos" if s == "Todos" else fmt_semaforo(s),
        key="ia_m1_sem",
    )
    niv_sel = f3.selectbox("Nivel", ["Todos", *niveles], key="ia_m1_niv")
    busq = f4.text_input("Buscar código modular, local o distrito", key="ia_m1_busq").strip().lower()

    df_fil = df_riesgo.copy()
    if dep_sel != "Todos":
        df_fil = df_fil[df_fil["departamento"] == dep_sel]
    if sem_sel != "Todos":
        df_fil = df_fil[df_fil["semaforo"] == sem_sel]
    if niv_sel != "Todos":
        df_fil = df_fil[df_fil["nivel"] == niv_sel]
    if busq:
        mask = (
            df_fil["cod_modular"].fillna("").str.lower().str.contains(busq)
            | df_fil["cod_local"].fillna("").str.lower().str.contains(busq)
            | df_fil["distrito"].fillna("").str.lower().str.contains(busq)
        )
        df_fil = df_fil[mask]

    df_fil = df_fil.sort_values("prob_riesgo", ascending=False).reset_index(drop=True)
    df_show = pd.DataFrame(
        {
            "Semáforo": df_fil["semaforo"].map(fmt_semaforo),
            "Prob. riesgo": df_fil["prob_riesgo"].map(lambda v: fmt_dec(v, 3)),
            "Cód. modular": df_fil["cod_modular"],
            "Cód. local": df_fil["cod_local"],
            "Departamento": df_fil["departamento"],
            "Distrito": df_fil["distrito"],
            "UGEL": df_fil["ugel"],
            "Nivel": df_fil["nivel"],
            "Matrícula base": df_fil["matricula_base"].map(fmt_int),
            "Tasa base (%)": df_fil["tasa_desercion_base_pct"].map(lambda v: fmt_dec(v, 2)),
            f"Tasa esperada {int(ultima_ejecucion['anio_base']) + 1} (%)": df_fil[
                "tasa_esperada_siguiente"
            ].map(lambda v: fmt_dec(v, 2)),
            "Factores principales": df_fil["factores_principales"],
        }
    )
    st.caption(f"Mostrando **{fmt_int(len(df_show))}** instituciones ordenadas de mayor a menor riesgo.")
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    st.download_button(
        label="Exportar riesgo por institución (CSV)",
        data=df_fil.drop(columns=["id", "id_ejecucion"], errors="ignore").to_csv(index=False).encode("utf-8"),
        file_name="ia_pred_riesgo_ie.csv",
        mime="text/csv",
        key="dl_ia_m1_riesgo",
    )

    st.divider()
    col_imp, col_proy = st.columns([1.0, 1.35])

    with col_imp:
        st.markdown("##### Importancia de variables (Random Forest)")
        if df_imp.empty:
            st.caption("Sin datos de importancia de variables.")
        else:
            top_imp = df_imp.head(15).copy()
            top_imp["etiqueta"] = top_imp["descripcion"].fillna(top_imp["variable"])
            top_imp = top_imp.sort_values("importancia", ascending=True)
            fig_imp = px.bar(
                top_imp,
                x="importancia",
                y="etiqueta",
                orientation="h",
                labels={"importancia": "Importancia relativa", "etiqueta": "Variable"},
            )
            fig_imp.update_layout(
                height=440,
                margin=dict(l=10, r=15, t=20, b=20),
                yaxis_title="",
            )
            st.plotly_chart(fig_imp, use_container_width=True)

    with col_proy:
        st.markdown("##### Proyección de desertores por departamento (histórico + escenarios)")
        if df_proy.empty:
            st.caption("Sin datos de proyección.")
        else:
            c_d, c_m = st.columns(2)
            deps_proy = sorted(df_proy["departamento"].dropna().unique().tolist())
            dep_proy_sel = c_d.selectbox(
                "Departamento a proyectar",
                ["Todos (nacional)", *deps_proy],
                key="ia_m1_dep_proy",
            )
            metrica_proy = c_m.selectbox(
                "Indicador gráfico",
                ["desertores", "tasa_pct"],
                format_func=lambda m: "Desertores estimados" if m == "desertores" else "Tasa de deserción (%)",
                key="ia_m1_metrica_proy",
            )

            if dep_proy_sel == "Todos (nacional)":
                df_g = (
                    df_proy.groupby(["anio", "escenario", "tipo"], as_index=False)[["matricula", "desertores"]]
                    .sum()
                )
                df_g["tasa_pct"] = (df_g["desertores"] / df_g["matricula"].replace(0, pd.NA) * 100).round(2)
            else:
                df_g = df_proy[df_proy["departamento"] == dep_proy_sel].copy()

            # Conectar visualmente el último año histórico con el primer año de cada escenario
            df_real = df_g[df_g["escenario"] == "real"].sort_values("anio")
            anio_base_int = int(ultima_ejecucion["anio_base"])
            punto_base = df_real[df_real["anio"] == anio_base_int]

            etiquetas_esc = {
                "real": "Histórico real",
                "base": "Escenario base",
                "optimista": "Escenario optimista (con intervención)",
                "pesimista": "Escenario pesimista (shock)",
            }
            colores_esc = {
                "real": "#1e293b",
                "base": "#2563eb",
                "optimista": "#16a34a",
                "pesimista": "#dc2626",
            }
            trazos_dash = {
                "real": "solid",
                "base": "dash",
                "optimista": "dot",
                "pesimista": "dashdot",
            }

            fig_proy = go.Figure()
            for esc_key in ("real", "base", "optimista", "pesimista"):
                sub = df_g[df_g["escenario"] == esc_key].sort_values("anio")
                if sub.empty:
                    continue
                if esc_key != "real" and not punto_base.empty:
                    sub = pd.concat([punto_base, sub], ignore_index=True).sort_values("anio")
                fig_proy.add_trace(
                    go.Scatter(
                        x=sub["anio"].astype(int),
                        y=sub[metrica_proy],
                        mode="lines+markers",
                        name=etiquetas_esc[esc_key],
                        line=dict(color=colores_esc[esc_key], width=2.5, dash=trazos_dash[esc_key]),
                        marker=dict(size=7),
                    )
                )

            fig_proy.update_layout(
                height=380,
                margin=dict(l=10, r=15, t=20, b=20),
                xaxis=dict(title="Año", dtick=1),
                yaxis=dict(
                    title="Estudiantes" if metrica_proy == "desertores" else "Tasa de deserción (%)"
                ),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            )
            st.plotly_chart(fig_proy, use_container_width=True)

            df_g_show = df_g.sort_values(["anio", "escenario"]).copy()
            df_g_show["Escenario"] = df_g_show["escenario"].map(lambda e: etiquetas_esc.get(e, e))
            df_g_show["Año"] = df_g_show["anio"].astype(int)
            df_g_show["Matrícula"] = df_g_show["matricula"].map(fmt_int)
            df_g_show["Desertores"] = df_g_show["desertores"].map(fmt_int)
            df_g_show["Tasa (%)"] = df_g_show["tasa_pct"].map(lambda v: fmt_dec(v, 2))
            st.dataframe(
                df_g_show[["Año", "Escenario", "Matrícula", "Desertores", "Tasa (%)"]],
                use_container_width=True,
                hide_index=True,
            )


def _renderizar_modulo2_inversion(ultima_ejecucion: dict[str, Any]) -> None:
    """Módulo 2: Proyección determinista de inversión en equipamiento y conectividad por local."""
    df_inv = obtener_tabla_ia("ia_inversion_local")
    if df_inv.empty:
        st.info("No hay datos de proyección de inversión registrados.")
        return

    anio_base = int(ultima_ejecucion["anio_base"])
    locales_con_accion = int((df_inv["costo_estimado_soles"] > 0).sum())
    costo_total = int(df_inv["costo_estimado_soles"].fillna(0).sum())
    total_reparar = int(df_inv["equipos_a_reparar"].fillna(0).sum())
    total_reponer = int(df_inv["equipos_a_reponer"].fillna(0).sum())
    total_dotar = int(df_inv["equipos_a_dotar"].fillna(0).sum())
    total_kits = int(df_inv["kit_conectividad"].fillna(0).sum())

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Inversión total estimada", fmt_soles(costo_total))
    m2.metric("Locales con acción sugerida", f"{fmt_int(locales_con_accion)} / {fmt_int(len(df_inv))}")
    m3.metric(
        "Equipos (Reparar / Reponer / Dotar)",
        f"{fmt_int(total_reparar)} / {fmt_int(total_reponer)} / {fmt_int(total_dotar)}",
    )
    m4.metric("Kits de conectividad", fmt_int(total_kits))

    f1, f2, f3 = st.columns([1.2, 1.5, 1.0])
    deps = sorted(df_inv["departamento"].dropna().unique().tolist())
    acciones = sorted(df_inv["accion_sugerida"].dropna().unique().tolist())
    dep_sel = f1.selectbox("Departamento", ["Todos", *deps], key="ia_m2_dep")
    acc_sel = f2.selectbox("Acción sugerida", ["Todas", *acciones], key="ia_m2_acc")
    solo_con_costo = f3.checkbox("Solo locales con costo > S/ 0", value=True, key="ia_m2_solo")

    df_fil = df_inv.copy()
    if dep_sel != "Todos":
        df_fil = df_fil[df_fil["departamento"] == dep_sel]
    if acc_sel != "Todas":
        df_fil = df_fil[df_fil["accion_sugerida"] == acc_sel]
    if solo_con_costo:
        df_fil = df_fil[df_fil["costo_estimado_soles"] > 0]

    df_fil = df_fil.sort_values("costo_estimado_soles", ascending=False).reset_index(drop=True)

    df_show = pd.DataFrame(
        {
            "Cód. local": df_fil["cod_local"],
            "Departamento": df_fil["departamento"],
            "Distrito": df_fil["distrito"],
            "UGEL": df_fil["ugel"],
            "Matrícula local": df_fil["matricula_local_base"].map(fmt_int),
            "CPU op. / total": df_fil.apply(
                lambda r: f"{fmt_int(r['cpu_operativos'])} / {fmt_int(r['cpu_total'])}", axis=1
            ),
            "Alumnos/PC": df_fil["alumnos_por_pc_operativa"].map(lambda v: fmt_dec(v, 1)),
            "Internet": df_fil["tiene_internet"],
            "Reparar": df_fil["equipos_a_reparar"].map(fmt_int),
            "Reponer": df_fil["equipos_a_reponer"].map(fmt_int),
            "Dotar": df_fil["equipos_a_dotar"].map(fmt_int),
            "Kit": df_fil["kit_conectividad"].map(fmt_int),
            "Acción sugerida": df_fil["accion_sugerida"],
            "Costo estimado": df_fil["costo_estimado_soles"].map(fmt_soles),
            f"Op. {anio_base + 1}": df_fil["operativos_proy_t1"].map(lambda v: fmt_dec(v, 1)),
            f"Op. {anio_base + 2}": df_fil["operativos_proy_t2"].map(lambda v: fmt_dec(v, 1)),
            f"Alum/PC {anio_base + 2}": df_fil["alumnos_por_pc_proy_t2"].map(lambda v: fmt_dec(v, 1)),
        }
    )
    st.caption(
        f"Mostrando **{fmt_int(len(df_show))}** locales · Subtotal filtrado: "
        f"**{fmt_soles(df_fil['costo_estimado_soles'].sum())}**"
    )
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    st.download_button(
        label="Exportar proyección de inversión (CSV)",
        data=df_fil.drop(columns=["id", "id_ejecucion"], errors="ignore").to_csv(index=False).encode("utf-8"),
        file_name="ia_inversion_local.csv",
        mime="text/csv",
        key="dl_ia_m2_inv",
    )


def _renderizar_modulo3_priorizacion() -> None:
    """Módulo 3: Ranking costo-efectividad con control deslizante de presupuesto en vivo sin reentrenar."""
    df_prio_base = obtener_tabla_ia("ia_priorizacion")
    if df_prio_base.empty:
        st.info("No hay datos de priorización registrados.")
        return

    df_params = obtener_parametros_semantica()
    fila_pres = df_params[df_params["clave"] == "presupuesto_soles"]
    pres_inicial = int(round(float(fila_pres["valor"].iloc[0]))) if not fila_pres.empty else 250000
    pres_inicial = max(50000, min(1000000, (pres_inicial // 10000) * 10000))

    presupuesto_sel = st.slider(
        "Presupuesto disponible (S/)",
        min_value=50000,
        max_value=1000000,
        value=pres_inicial,
        step=10000,
        format="S/ %d",
        key="ia_m3_presupuesto_slider",
        help="Recalcula el costo acumulado y los locales cubiertos instantáneamente sin volver a entrenar.",
    )

    df_calc, resumen_pres = recalcular_priorizacion_presupuesto(df_prio_base, presupuesto_sel)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(
        "Locales financiados",
        f"{fmt_int(resumen_pres['locales_financiados'])} / {fmt_int(resumen_pres['locales_totales'])}",
    )
    c2.metric("Instituciones cubiertas", fmt_int(resumen_pres["ie_cubiertas"]))
    c3.metric("Inversión asignada", fmt_soles(resumen_pres["inversion_asignada"]))
    c4.metric("Saldo disponible", fmt_soles(resumen_pres["saldo_disponible"]))
    c5.metric("Est. en riesgo cubiertos", fmt_dec(resumen_pres["estudiantes_riesgo_cubiertos"], 1))

    f1, f2 = st.columns(2)
    deps = sorted(df_calc["departamento"].dropna().unique().tolist())
    dep_sel = f1.selectbox("Filtrar por departamento", ["Todos", *deps], key="ia_m3_dep")
    est_sel = f2.selectbox(
        "Cobertura presupuestal",
        ["Todos", "Financiados", "En espera"],
        key="ia_m3_estado",
    )

    df_fil = df_calc.copy()
    if dep_sel != "Todos":
        df_fil = df_fil[df_fil["departamento"] == dep_sel]
    if est_sel == "Financiados":
        df_fil = df_fil[df_fil["dentro_presupuesto"]]
    elif est_sel == "En espera":
        df_fil = df_fil[~df_fil["dentro_presupuesto"]]

    df_show = pd.DataFrame(
        {
            "Ranking": df_fil["ranking"].map(fmt_int),
            "Cód. local": df_fil["cod_local"],
            "Departamento": df_fil["departamento"],
            "Distrito": df_fil["distrito"],
            "IEs": df_fil["n_ie"].map(fmt_int),
            "Semáforo máx.": df_fil["semaforo_max"].map(fmt_semaforo),
            "Est. en riesgo": df_fil["estudiantes_en_riesgo"].map(lambda v: fmt_dec(v, 1)),
            "Riesgo / S/ 1,000": df_fil["riesgo_por_mil_soles"].map(lambda v: fmt_dec(v, 2)),
            "Acción sugerida": df_fil["accion_sugerida"],
            "Costo": df_fil["costo_estimado_soles"].map(fmt_soles),
            "Costo acumulado": df_fil["costo_acumulado_soles"].map(fmt_soles),
            "Presupuesto": df_fil["dentro_presupuesto"].map(
                lambda ok: "✅ Financiado" if bool(ok) else "⏳ En espera"
            ),
        }
    )
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    # Curva costo acumulado vs. tope presupuestal (primeros 60 locales del ranking)
    df_curva = df_calc.head(60).copy()
    fig_curva = go.Figure()
    fig_curva.add_trace(
        go.Scatter(
            x=df_curva["ranking"],
            y=df_curva["costo_acumulado_soles"],
            mode="lines+markers",
            name="Costo acumulado (S/)",
            line=dict(color="#2563eb", width=2.5),
            customdata=df_curva[["cod_local", "distrito", "riesgo_por_mil_soles"]],
            hovertemplate=(
                "Ranking #%{x} · Local %{customdata[0]} (%{customdata[1]})<br>"
                "Costo acumulado: S/ %{y:,.0f}<br>"
                "Riesgo/mil S/: %{customdata[2]:.2f}<extra></extra>"
            ),
        )
    )
    fig_curva.add_hline(
        y=presupuesto_sel,
        line_dash="dash",
        line_color="#dc2626",
        annotation_text=f"Tope: {fmt_soles(presupuesto_sel)}",
        annotation_position="top left",
    )
    fig_curva.update_layout(
        height=320,
        margin=dict(l=10, r=15, t=25, b=20),
        xaxis_title="Ranking de prioridad (top 60 locales)",
        yaxis_title="Costo acumulado (S/)",
    )
    st.plotly_chart(fig_curva, use_container_width=True)

    st.download_button(
        label="Exportar priorización recalculada (CSV)",
        data=df_fil.drop(columns=["id", "id_ejecucion"], errors="ignore").to_csv(index=False).encode("utf-8"),
        file_name="ia_priorizacion_presupuesto.csv",
        mime="text/csv",
        key="dl_ia_m3_prio",
    )


def _renderizar_modulo4_anomalias() -> None:
    """Módulo 4: Anomalías IoT (tabla + serie mensual por sensor), alertas crónicas y acceso."""
    df_iot = obtener_tabla_ia("ia_anomalias_iot")
    df_cron = obtener_tabla_ia("ia_alertas_cronicas_iot")
    df_acc = obtener_tabla_ia("ia_anomalias_acceso")

    k1, k2, k3 = st.columns(3)
    k1.metric("Lecturas IoT anómalas", fmt_int(len(df_iot)))
    k2.metric("Sensores con alerta crónica", fmt_int(len(df_cron)))
    k3.metric("Anomalías en tiempo de acceso", fmt_int(len(df_acc)))

    st.markdown("##### 1. Anomalías puntuales en telemetría IoT")
    if df_iot.empty:
        st.success("🟢 No se detectaron anomalías puntuales en las lecturas IoT.")
    else:
        f1, f2 = st.columns(2)
        enlaces = sorted(df_iot["tipo_enlace"].dropna().unique().tolist())
        enlace_sel = f1.selectbox("Filtrar por tipo de enlace", ["Todos", *enlaces], key="ia_m4_enlace")
        sensor_filtro = f2.text_input("Buscar sensor, lectura o código modular", key="ia_m4_busq").strip().lower()

        df_iot_fil = df_iot.copy()
        if enlace_sel != "Todos":
            df_iot_fil = df_iot_fil[df_iot_fil["tipo_enlace"] == enlace_sel]
        if sensor_filtro:
            mask = (
                df_iot_fil["id_sensor"].fillna("").str.lower().str.contains(sensor_filtro)
                | df_iot_fil["id_lectura"].fillna("").str.lower().str.contains(sensor_filtro)
                | df_iot_fil["cod_modular"].fillna("").str.lower().str.contains(sensor_filtro)
            )
            df_iot_fil = df_iot_fil[mask]

        df_iot_fil = df_iot_fil.sort_values("score_anomalia", ascending=False).reset_index(drop=True)
        df_iot_show = pd.DataFrame(
            {
                "Lectura": df_iot_fil["id_lectura"],
                "Sensor": df_iot_fil["id_sensor"],
                "Cód. modular": df_iot_fil["cod_modular"],
                "Mes": df_iot_fil["mes"],
                "Enlace": df_iot_fil["tipo_enlace"],
                "Disponibilidad (%)": df_iot_fil["disponibilidad_internet_pct"].map(lambda v: fmt_dec(v, 1)),
                "Cortes energía (h)": df_iot_fil["horas_corte_energia"].map(lambda v: fmt_dec(v, 1)),
                "Voltaje (V)": df_iot_fil["voltaje_prom_v"].map(lambda v: fmt_dec(v, 1)),
                "Score anomalía": df_iot_fil["score_anomalia"].map(lambda v: fmt_dec(v, 4)),
                "Motivo": df_iot_fil["motivo"],
            }
        )
        st.dataframe(df_iot_show, use_container_width=True, hide_index=True)

        sensores_anom = sorted(df_iot["id_sensor"].dropna().unique().tolist())
        if sensores_anom:
            st.markdown("##### Serie mensual del sensor seleccionado")
            c_sens, c_var = st.columns([1.4, 1.0])
            sensor_sel = c_sens.selectbox(
                "Elegir sensor con anomalías",
                options=sensores_anom,
                key="ia_m4_sensor_serie",
            )
            var_serie = c_var.selectbox(
                "Variable a inspeccionar",
                options=["disponibilidad_internet_pct", "horas_corte_energia", "latencia_prom_ms", "voltaje_prom_v"],
                format_func=lambda v: {
                    "disponibilidad_internet_pct": "Disponibilidad de Internet (%)",
                    "horas_corte_energia": "Horas de corte de energía",
                    "latencia_prom_ms": "Latencia promedio (ms)",
                    "voltaje_prom_v": "Voltaje promedio (V)",
                }[v],
                key="ia_m4_var_serie",
            )

            df_serie = obtener_serie_sensor_dw(sensor_sel)
            if df_serie.empty:
                st.caption("No se encontraron lecturas mensuales para este sensor.")
            else:
                meses_anom = set(df_iot.loc[df_iot["id_sensor"] == sensor_sel, "mes"].tolist())
                df_serie["es_anomalo"] = df_serie["mes"].isin(meses_anom)
                fig_sens = go.Figure()
                fig_sens.add_trace(
                    go.Scatter(
                        x=df_serie["mes"],
                        y=df_serie[var_serie],
                        mode="lines+markers",
                        name="Serie mensual",
                        line=dict(color="#2563eb", width=2),
                        marker=dict(size=7, color="#2563eb"),
                    )
                )
                sub_anom = df_serie[df_serie["es_anomalo"]]
                if not sub_anom.empty:
                    fig_sens.add_trace(
                        go.Scatter(
                            x=sub_anom["mes"],
                            y=sub_anom[var_serie],
                            mode="markers",
                            name="Mes anómalo detectado",
                            marker=dict(size=12, color="#dc2626", symbol="diamond"),
                        )
                    )
                fig_sens.update_layout(
                    height=320,
                    margin=dict(l=10, r=15, t=25, b=20),
                    xaxis_title="Mes",
                    yaxis_title=var_serie,
                )
                st.plotly_chart(fig_sens, use_container_width=True)

    st.divider()
    col_cron, col_acc = st.columns(2)
    with col_cron:
        st.markdown("##### 2. Alertas crónicas (servicio persistentemente deficiente)")
        st.caption("Sensores con disponibilidad mediana < 60 % o cortes medianos > 100 h/mes.")
        if df_cron.empty:
            st.success("🟢 Sin sensores en condición crónica crítica.")
        else:
            df_cron_show = pd.DataFrame(
                {
                    "Sensor": df_cron["id_sensor"],
                    "Cód. modular": df_cron["cod_modular"],
                    "Enlace": df_cron["tipo_enlace"],
                    "Disp. mediana (%)": df_cron["disponibilidad_mediana_pct"].map(lambda v: fmt_dec(v, 1)),
                    "Cortes mediana (h)": df_cron["horas_corte_mediana"].map(lambda v: fmt_dec(v, 1)),
                    "Alerta": df_cron["alerta"],
                }
            )
            st.dataframe(df_cron_show, use_container_width=True, hide_index=True)

    with col_acc:
        st.markdown("##### 3. Anomalías en tiempos de acceso geográfico")
        st.caption("Instituciones con |z_tiempo| > 2.5 respecto al tiempo esperado por tipo de acceso.")
        if df_acc.empty:
            st.success("🟢 Sin anomalías significativas de tiempo de acceso.")
        else:
            df_acc_show = pd.DataFrame(
                {
                    "Cód. modular": df_acc["cod_modular"],
                    "Distrito": df_acc["distrito"],
                    "Tipo de acceso": df_acc["tipo_acceso"],
                    "Distancia (km)": df_acc["distancia_capital_distrital_km"].map(lambda v: fmt_dec(v, 1)),
                    "Tiempo (h)": df_acc["tiempo_acceso_horas"].map(lambda v: fmt_dec(v, 1)),
                    "Z-tiempo": df_acc["z_tiempo"].map(lambda v: fmt_dec(v, 2)),
                    "Motivo": df_acc["motivo"],
                }
            )
            st.dataframe(df_acc_show, use_container_width=True, hide_index=True)


def renderizar_tab_ia(estado_proceso: dict[str, Any]) -> None:
    """Pestaña 5 completa: ejecución en vivo desde vw_ml_* y los 4 módulos de IA."""
    st.subheader("5. Inteligencia artificial (ejecución en vivo)")
    st.warning(
        "⚠️ **Resultados calculados en este momento sobre datos simulados; no son desempeño real.**"
    )

    dw_listo = estado_proceso["etl"]["texto"] in {"Ejecutado", "Desactualizado"} and (
        estado_proceso["etl"]["fecha"] is not None
    )

    col_btn, col_auto, col_msg = st.columns([1.3, 1.5, 2.2])
    with col_auto:
        _renderizar_checkbox_auto_ia("ia_chk_auto_etl")

    if col_btn.button(
        "Ejecutar modelos",
        type="primary",
        disabled=(not dw_listo),
        use_container_width=True,
        key="btn_ejecutar_ia_tab5",
    ):
        ok_ia, err_ia, aviso_ia = _ejecutar_ia_con_progreso()
        if ok_ia:
            st.rerun()
        elif err_ia:
            st.error(err_ia)
        elif aviso_ia:
            st.warning(aviso_ia)

    if not dw_listo:
        col_msg.warning("Ejecuta primero el proceso ETL en la pestaña 3 para poblar el Data Warehouse.")
    elif estado_proceso["ia"]["texto"] == "Desactualizado":
        col_msg.warning(
            "Los datos del Data Warehouse o los parámetros cambiaron. Haz clic en **Ejecutar modelos** "
            "para reentrenar y actualizar las predicciones."
        )
    elif estado_proceso["ia"]["texto"] == "Ejecutado":
        col_msg.success(f"Modelos al día · Última ejecución: {fmt_fecha(estado_proceso['ia']['fecha'])}")
    else:
        col_msg.info("El Data Warehouse está listo. Haz clic en **Ejecutar modelos** para entrenar en vivo.")

    ultima = obtener_ultima_ejecucion_ia()
    if ultima is None:
        st.caption("Aún no se han ejecutado los modelos de Inteligencia Artificial sobre el Data Warehouse.")
        return

    detalle = ultima.get("detalle_json", {})
    resumen_modelo = detalle.get("resumen", {})
    met_modelo = resumen_modelo.get("metricas", {})
    reg = met_modelo.get("regresion_tasa", {})
    clasif = met_modelo.get("clasificacion", {})

    with st.container(border=True):
        st.markdown("#### Resumen de la última ejecución")
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Fecha de ejecución", fmt_fecha(ultima.get("fecha_fin")))
        m2.metric("Año base", str(ultima.get("anio_base") or "—"))
        m3.metric("Años de entrenamiento", str(ultima.get("anios_entrenamiento") or "—"))
        m4.metric("Filas de entrenamiento", fmt_int(ultima.get("filas_entrenamiento")))
        dur_s = (ultima.get("duracion_ms") or 0) / 1000.0
        m5.metric("Duración", f"{fmt_dec(dur_s, 1)} s")

        k1, k2, k3, k4 = st.columns(4)
        auc_mod = ultima.get("auc")
        auc_base = ultima.get("auc_linea_base")
        mae_mod = ultima.get("mae_pp")
        mae_base = ultima.get("mae_linea_base_pp")
        k1.metric(
            "AUC clasificación",
            fmt_dec(auc_mod, 3),
            delta=f"{float(auc_mod) - float(auc_base):+.3f} vs. línea base"
            if auc_mod is not None and auc_base is not None
            else None,
        )
        k2.metric(
            "MAE regresión (pp)",
            fmt_dec(mae_mod, 3),
            delta=f"Línea base: {fmt_dec(mae_base, 3)} pp" if mae_base is not None else None,
            delta_color="off",
        )
        k3.metric("R² regresión · F1 clasif.", f"{fmt_dec(reg.get('r2_modelo'), 3)} · {fmt_dec(clasif.get('f1'), 3)}")
        sem_res = resumen_modelo.get("semaforo_instituciones", {})
        k4.metric(
            "Semáforo (🔴 / 🟡 / 🟢)",
            f"{fmt_int(sem_res.get('Rojo', 0))} / {fmt_int(sem_res.get('Amarillo', 0))} / {fmt_int(sem_res.get('Verde', 0))}",
        )
        if met_modelo.get("validacion"):
            st.caption(
                f"Validación: {met_modelo['validacion']} · "
                f"Umbral riesgo alto: {fmt_dec(met_modelo.get('umbral_riesgo_pct'), 2)} % · "
                f"Prevalencia: {fmt_dec((met_modelo.get('prevalencia_riesgo_alto') or 0) * 100, 1)} %"
            )

    modulo_sel = st.radio(
        "Módulo de Inteligencia Artificial",
        options=[
            "Módulo 1 · Riesgo y proyección",
            "Módulo 2 · Proyección de inversión",
            "Módulo 3 · Priorización de recursos",
            "Módulo 4 · Anomalías",
        ],
        horizontal=True,
        key="ia_modulo_selector",
    )

    if modulo_sel.startswith("Módulo 1"):
        _renderizar_modulo1_riesgo(ultima, resumen_modelo)
    elif modulo_sel.startswith("Módulo 2"):
        _renderizar_modulo2_inversion(ultima)
    elif modulo_sel.startswith("Módulo 3"):
        _renderizar_modulo3_priorizacion()
    else:
        _renderizar_modulo4_anomalias()


def renderizar_tab_semantica(estado_proceso: dict[str, Any]) -> None:
    """Pestaña 6 completa: catálogo de 9 KPIs, parámetros editables, glosario y jerarquías."""
    st.subheader("6. Capa semántica")
    st.write(
        "Definición única de indicadores de negocio (9 KPIs), parámetros de umbral y costos en "
        "`sem_parametro`, glosario institucional en `sem_glosario` y jerarquías analíticas."
    )

    params_map = obtener_mapa_parametros()
    seccion_sem = st.radio(
        "Sección de la capa semántica",
        options=[
            "Catálogo de KPIs (9)",
            "Parámetros editables (sem_parametro)",
            "Glosario (sem_glosario)",
            "Jerarquías disponibles",
        ],
        horizontal=True,
        key="sem_seccion_selector",
    )

    if seccion_sem == "Catálogo de KPIs (9)":
        catalogo = obtener_catalogo_kpis_actualizado(params_map)
        df_cat = pd.DataFrame(
            [
                {
                    "Código": k["codigo"],
                    "Nombre del KPI": k["nombre"],
                    "Categoría": k["categoria"],
                    "Fórmula (lenguaje de negocio)": k["formula"],
                    "Unidad": k["unidad"],
                    "Meta vigente": k["meta"],
                }
                for k in catalogo
            ]
        )
        st.dataframe(df_cat, use_container_width=True, hide_index=True)

        st.download_button(
            label="Exportar catálogo de KPIs (CSV)",
            data=df_cat.to_csv(index=False).encode("utf-8-sig"),
            file_name="catalogo_kpis_acr.csv",
            mime="text/csv",
            key="dl_sem_catalogo_kpi",
        )

        st.divider()
        st.markdown("#### Inspección de fórmula y SQL asociado")
        kpi_sel_cod = st.selectbox(
            "Seleccionar KPI",
            options=[k["codigo"] for k in catalogo],
            format_func=lambda c: next(k["nombre"] for k in catalogo if k["codigo"] == c),
            key="sem_kpi_inspector",
        )
        kpi_sel = next(k for k in catalogo if k["codigo"] == kpi_sel_cod)
        with st.container(border=True):
            c1, c2, c3 = st.columns(3)
            c1.markdown(f"**Categoría:** {kpi_sel['categoria']}")
            c2.markdown(f"**Unidad:** {kpi_sel['unidad']}")
            c3.markdown(f"**Meta vigente:** {kpi_sel['meta']}")
            st.markdown(f"**Fórmula de negocio:** {kpi_sel['formula']}")
            st.markdown("**SQL asociado sobre el Data Warehouse / tablas IA:**")
            st.code(kpi_sel["sql"], language="sql")

        with st.container(border=True):
            st.markdown("#### Descomposición del Índice de conectividad rural (0-100)")
            st.caption(
                "Índice = 0.40 × Equipamiento + 0.30 × Internet + 0.30 × Energía. "
                "Cada componente se escala de 0 a 100."
            )
            df_comp = pd.DataFrame(
                [
                    {
                        "Componente": c["componente"],
                        "Peso": c["peso"],
                        "Fórmula": c["formula"],
                        "Nota de cálculo": c["nota"],
                    }
                    for c in COMPONENTES_INDICE_CONECTIVIDAD
                ]
            )
            st.dataframe(df_comp, use_container_width=True, hide_index=True)

    elif seccion_sem == "Parámetros editables (sem_parametro)":
        ia_estado = estado_proceso["ia"]
        st.info(
            f"Estado actual de Inteligencia Artificial: **{ia_estado['icono']} {ia_estado['texto']}**. "
            "Al guardar cambios o restablecer valores en `sem_parametro`, la IA pasará a "
            "**🟠 Desactualizado** hasta volver a ejecutarse en la pestaña 5."
        )

        df_params = obtener_parametros_semantica()
        if df_params.empty:
            st.warning("No se encontraron parámetros en `sem_parametro`.")
            return

        col_reset, col_info_res = st.columns([1.3, 3.7])
        if col_reset.button("Restablecer", use_container_width=True, key="btn_sem_restablecer"):
            restaurados = restablecer_parametros_semantica()
            for clave_p in df_params["clave"].tolist():
                st.session_state.pop(f"sem_in_{clave_p}", None)
            if restaurados > 0:
                st.success(
                    f"Se restablecieron {fmt_int(restaurados)} parámetros a su valor por defecto. "
                    "La IA quedó marcada como Desactualizado."
                )
                st.rerun()
            else:
                st.info("Todos los parámetros ya se encuentran en su valor por defecto.")

        col_info_res.caption(
            "El botón **Restablecer** devuelve los 15 supuestos de `sem_parametro` a sus valores iniciales de la especificación."
        )

        claves_enteras = {"alumnos_por_pc_meta", "presupuesto_soles", "costo_reparar", "costo_reponer", "costo_dotar", "costo_kit_conectividad"}
        claves_prob = {"semaforo_rojo", "semaforo_amarillo", "baja_anual_base", "baja_anual_extra_obsolescencia", "efecto_intervencion", "shock_pesimista", "contaminacion_anomalias"}

        with st.form("form_sem_parametros"):
            st.markdown("##### Editar supuestos y umbrales (`sem_parametro`)")
            nuevos_valores: dict[str, float] = {}
            registros_p = df_params.to_dict(orient="records")
            for idx in range(0, len(registros_p), 2):
                cols_p = st.columns(2)
                for j in range(2):
                    if idx + j >= len(registros_p):
                        continue
                    reg = registros_p[idx + j]
                    clave = str(reg["clave"])
                    val_act = float(reg["valor"])
                    val_def = float(reg["valor_defecto"])
                    unidad = str(reg.get("unidad") or "")
                    desc = str(reg.get("descripcion") or "")

                    with cols_p[j]:
                        with st.container(border=True):
                            st.markdown(f"**`{clave}`** ({unidad})")
                            st.caption(f"{desc} · Por defecto: `{val_def:g}`")
                            if clave in claves_enteras:
                                nuevos_valores[clave] = float(
                                    st.number_input(
                                        f"Valor ({clave})",
                                        min_value=0.0,
                                        value=val_act,
                                        step=1.0 if clave == "alumnos_por_pc_meta" else 50.0,
                                        format="%.0f",
                                        key=f"sem_in_{clave}",
                                        label_visibility="collapsed",
                                    )
                                )
                            elif clave in claves_prob:
                                nuevos_valores[clave] = float(
                                    st.number_input(
                                        f"Valor ({clave})",
                                        min_value=0.0,
                                        max_value=1.0,
                                        value=min(1.0, max(0.0, val_act)),
                                        step=0.01,
                                        format="%.2f",
                                        key=f"sem_in_{clave}",
                                        label_visibility="collapsed",
                                    )
                                )
                            else:
                                nuevos_valores[clave] = float(
                                    st.number_input(
                                        f"Valor ({clave})",
                                        min_value=0.0,
                                        value=val_act,
                                        step=0.05,
                                        format="%.2f",
                                        key=f"sem_in_{clave}",
                                        label_visibility="collapsed",
                                    )
                                )

            enviado = st.form_submit_button("Guardar parámetros", type="primary", use_container_width=True)
            if enviado:
                if nuevos_valores.get("semaforo_rojo", 0.60) <= nuevos_valores.get("semaforo_amarillo", 0.35):
                    st.error("El umbral `semaforo_rojo` debe ser estrictamente mayor que `semaforo_amarillo`.")
                else:
                    modificados = actualizar_parametros_semantica(nuevos_valores)
                    if modificados > 0:
                        for clave_p in nuevos_valores:
                            st.session_state.pop(f"sem_in_{clave_p}", None)
                        st.success(
                            f"Se actualizaron {fmt_int(modificados)} parámetros en `sem_parametro`. "
                            "La IA pasó a Desactualizado."
                        )
                        st.rerun()
                    else:
                        st.info("No hubo cambios respecto a los valores ya guardados.")

        st.divider()
        st.markdown("##### Estado registrado en `sem_parametro`")
        df_p_show = pd.DataFrame(
            {
                "Clave": df_params["clave"],
                "Valor vigente": df_params["valor"],
                "Valor por defecto": df_params["valor_defecto"],
                "Unidad": df_params["unidad"],
                "Última modificación": df_params["fecha_actualizacion"].map(
                    lambda f: fmt_fecha(f, con_hora=True) if pd.notna(f) else "Valor inicial"
                ),
                "Descripción": df_params["descripcion"],
            }
        )
        st.dataframe(df_p_show, use_container_width=True, hide_index=True)
        st.download_button(
            label="Exportar parámetros (CSV)",
            data=df_params.to_csv(index=False).encode("utf-8-sig"),
            file_name="sem_parametro_acr.csv",
            mime="text/csv",
            key="dl_sem_parametros",
        )

    elif seccion_sem == "Glosario (sem_glosario)":
        df_glos = obtener_glosario_semantica()
        c_busq, c_cat = st.columns([2.2, 1.2])
        busq_glos = c_busq.text_input(
            "Buscar término o definición",
            placeholder="Ej. ubigeo, ruralidad, CODLOCAL, deserción...",
            key="sem_glos_busq",
        ).strip().lower()
        cats = ["Todas", *sorted(df_glos["categoria"].dropna().unique().tolist())] if not df_glos.empty else ["Todas"]
        cat_sel = c_cat.selectbox("Categoría", options=cats, key="sem_glos_cat")

        df_fil = df_glos.copy()
        if cat_sel != "Todas":
            df_fil = df_fil[df_fil["categoria"] == cat_sel]
        if busq_glos:
            mask = (
                df_fil["termino"].fillna("").str.lower().str.contains(busq_glos)
                | df_fil["definicion"].fillna("").str.lower().str.contains(busq_glos)
            )
            df_fil = df_fil[mask]

        df_g_show = pd.DataFrame(
            {
                "Término": df_fil["termino"],
                "Categoría": df_fil["categoria"],
                "Definición": df_fil["definicion"],
                "Fuente / Referencia": df_fil["fuente_referencia"],
            }
        )
        st.caption(f"Mostrando **{fmt_int(len(df_g_show))}** términos del glosario en `sem_glosario`.")
        st.dataframe(df_g_show, use_container_width=True, hide_index=True)

        st.download_button(
            label="Exportar glosario (CSV)",
            data=df_g_show.to_csv(index=False).encode("utf-8-sig"),
            file_name="sem_glosario_acr.csv",
            mime="text/csv",
            key="dl_sem_glosario",
        )

        with st.expander("Agregar o actualizar término en `sem_glosario`"):
            with st.form("form_sem_glosario"):
                g1, g2 = st.columns([1.4, 1.0])
                nuevo_termino = g1.text_input("Término", key="sem_glos_in_term")
                nueva_cat = g2.selectbox(
                    "Categoría",
                    options=["Territorio", "Institución", "Infraestructura", "Gestión", "Permanencia", "Conectividad", "Inteligencia artificial"],
                    key="sem_glos_in_cat",
                )
                nueva_def = st.text_area("Definición", key="sem_glos_in_def")
                nueva_ref = st.text_input("Fuente / Referencia", value="Capa semántica", key="sem_glos_in_ref")
                if st.form_submit_button("Guardar término", use_container_width=True):
                    if not nuevo_termino.strip() or not nueva_def.strip():
                        st.error("El término y la definición son obligatorios.")
                    else:
                        guardar_termino_glosario(nuevo_termino, nueva_cat, nueva_def, nueva_ref)
                        st.success(f"Término `{nuevo_termino.strip()}` guardado en `sem_glosario`.")
                        st.rerun()

    else:
        st.markdown("#### Jerarquías analíticas disponibles")
        conteos_jer = obtener_resumen_jerarquias_dw()
        if conteos_jer:
            j1, j2, j3, j4, j5 = st.columns(5)
            j1.metric("Departamentos", fmt_int(conteos_jer.get("departamentos")))
            j2.metric("Provincias", fmt_int(conteos_jer.get("provincias")))
            j3.metric("Distritos", fmt_int(conteos_jer.get("distritos")))
            j4.metric("Locales escolares", fmt_int(conteos_jer.get("locales")))
            j5.metric("Instituciones", fmt_int(conteos_jer.get("instituciones")))

            j6, j7, j8, j9 = st.columns(4)
            j6.metric("UGEL", fmt_int(conteos_jer.get("ugeles")))
            j7.metric("Niveles educativos", fmt_int(conteos_jer.get("niveles")))
            j8.metric("Gradientes ruralidad", fmt_int(conteos_jer.get("ruralidades")))
            j9.metric("Años en DW", fmt_int(conteos_jer.get("anios")))

        df_jer = pd.DataFrame(
            [
                {
                    "Jerarquía": j["jerarquia"],
                    "Niveles de navegación": j["niveles"],
                    "Tablas del DW": j["tablas_dw"],
                    "Uso analítico": j["uso"],
                }
                for j in JERARQUIAS_SEMANTICAS
            ]
        )
        st.dataframe(df_jer, use_container_width=True, hide_index=True)


def _limpiar_filtros_bi() -> None:
    st.session_state["bi_fil_anio"] = "Todos"
    st.session_state["bi_fil_dep"] = "Todos"
    st.session_state["bi_fil_ugel"] = "Todas"
    st.session_state["bi_fil_nivel"] = "Todos"
    st.session_state["bi_fil_rur"] = "Todas"
    st.session_state["bi_fil_sem"] = "Todos"
    st.session_state["bi_dd_departamento"] = None
    st.session_state["bi_dd_ubigeo"] = None
    st.session_state["bi_dd_distrito"] = None
    st.session_state["bi_dd_cod_modular"] = None


def _renderizar_ficha_institucion_bi(cod_modular: str) -> None:
    """Muestra ficha analítica completa de una institución al final del drill-down."""
    ficha = obtener_ficha_institucion_bi(cod_modular)
    perfil = ficha.get("perfil")
    if not perfil:
        st.warning(f"No se encontró información detallada para el código modular `{cod_modular}`.")
        return

    with st.container(border=True):
        st.markdown(f"##### 🏫 {perfil['nombre_ie']} (`{perfil['cod_modular']}`)")
        st.caption(
            f"Ubicación: **{perfil['departamento']} / {perfil['provincia']} / {perfil['distrito']}** "
            f"(Ubigeo `{perfil['ubigeo']}`) · UGEL: **{perfil['ugel']}** · Local: `{perfil['cod_local']}`"
        )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Semáforo de riesgo (IA)", fmt_semaforo(perfil.get("semaforo")))
        c2.metric("Probabilidad de riesgo", fmt_dec(perfil.get("prob_riesgo"), 3))
        c3.metric("Nivel / Ruralidad", f"{perfil['nivel']} · {perfil['gradiente_ruralidad']}")
        c4.metric(
            "Acceso al local",
            f"{perfil.get('tipo_acceso') or '—'} ({fmt_dec(perfil.get('tiempo_acceso_h'), 1)} h)",
        )

        if (perfil.get("ie_en_local") or 1) > 1:
            st.info(
                f"Este local escolar (`{perfil['cod_local']}`) es compartido por "
                f"**{fmt_int(perfil['ie_en_local'])} instituciones educativas**. "
                "Los indicadores de equipamiento y alumnos/PC se calculan sobre la matrícula total del local sin doble conteo."
            )

        if perfil.get("factores_principales"):
            st.markdown(f"**Factores principales de riesgo (IA):** {perfil['factores_principales']}")
        if perfil.get("accion_sugerida"):
            st.markdown(
                f"**Acción sugerida en el local (`{perfil['cod_local']}`):** "
                f"{perfil['accion_sugerida']} · Costo estimado: **{fmt_soles(perfil.get('costo_estimado_soles'))}**"
            )

    df_hist = ficha["historico"]
    df_iot = ficha["telemetria"]
    col_h, col_t = st.columns(2)

    with col_h:
        st.markdown("###### Evolución histórica de matrícula y deserción")
        if df_hist.empty:
            st.caption("Sin registros anuales en `fact_permanencia`.")
        else:
            fig_h = go.Figure()
            fig_h.add_trace(
                go.Bar(
                    x=df_hist["anio"].astype(int),
                    y=df_hist["matricula_inicial"],
                    name="Matrícula inicial",
                    marker_color="#93c5fd",
                )
            )
            fig_h.add_trace(
                go.Scatter(
                    x=df_hist["anio"].astype(int),
                    y=df_hist["tasa_desercion_pct"],
                    name="Tasa de deserción (%)",
                    yaxis="y2",
                    mode="lines+markers",
                    line=dict(color="#dc2626", width=2.5),
                )
            )
            fig_h.update_layout(
                height=300,
                margin=dict(l=10, r=10, t=25, b=20),
                xaxis=dict(title="Año", dtick=1),
                yaxis=dict(title="Estudiantes"),
                yaxis2=dict(title="Deserción (%)", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_h, use_container_width=True)
            st.dataframe(df_hist, use_container_width=True, hide_index=True)

    with col_t:
        st.markdown("###### Telemetría IoT mensual")
        if df_iot.empty:
            st.caption("Esta institución no cuenta con lecturas IoT en `fact_conectividad`.")
        else:
            fig_t = go.Figure()
            fig_t.add_trace(
                go.Scatter(
                    x=df_iot["mes"],
                    y=df_iot["disponibilidad_pct"],
                    mode="lines+markers",
                    name="Disponibilidad Internet (%)",
                    line=dict(color="#2563eb", width=2.5),
                )
            )
            fig_t.add_trace(
                go.Bar(
                    x=df_iot["mes"],
                    y=df_iot["horas_corte_energia"],
                    name="Cortes de energía (h)",
                    yaxis="y2",
                    marker_color="#f59e0b",
                    opacity=0.65,
                )
            )
            fig_t.update_layout(
                height=300,
                margin=dict(l=10, r=10, t=25, b=20),
                xaxis=dict(title="Mes"),
                yaxis=dict(title="Disponibilidad (%)", range=[0, 105]),
                yaxis2=dict(title="Horas corte", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_t, use_container_width=True)
            st.dataframe(df_iot, use_container_width=True, hide_index=True)


def renderizar_tab_bi(estado_proceso: dict[str, Any]) -> None:
    """Pestaña 7 completa: dashboard ejecutivo, filtros dinámicos, drill-down, mapa, alertas y reportes."""
    c_tit, c_act = st.columns([4.0, 1.0])
    c_tit.subheader("7. Visualización BI (Dashboard ejecutivo)")
    if c_act.button("Actualizar", use_container_width=True, key="btn_bi_actualizar"):
        st.cache_data.clear()
        st.rerun()

    df_tablas_dw = obtener_resumen_tablas_dw()
    total_hechos = int(df_tablas_dw.loc[df_tablas_dw["Tabla"] == "fact_permanencia", "Filas"].iloc[0])
    if total_hechos == 0:
        st.info(
            "El Data Warehouse aún no tiene datos. Carga las 5 fuentes y ejecuta el ETL en la pestaña 3."
        )
        return

    ia_estado = estado_proceso["ia"]
    if ia_estado["texto"] == "Pendiente":
        st.warning(
            "Los modelos de Inteligencia Artificial aún no se han ejecutado. Los indicadores de semáforo, "
            "proyección y presupuesto se mostrarán al ejecutar la pestaña 5."
        )
    elif ia_estado["texto"] == "Desactualizado":
        st.warning(
            "Las predicciones de IA están **desactualizadas** respecto a la última carga o cambio de parámetros. "
            "Reejecuta los modelos en la pestaña 5 para actualizar semáforo y proyecciones."
        )

    params_sem = obtener_mapa_parametros()
    meta_des = float(params_sem.get("promedio_nacional_pct", 2.08))
    meta_apc = float(params_sem.get("alumnos_por_pc_meta", 20.0))
    umbral_rojo = float(params_sem.get("semaforo_rojo", 0.60))
    umbral_amarillo = float(params_sem.get("semaforo_amarillo", 0.35))
    presupuesto_tope = float(params_sem.get("presupuesto_soles", 250000.0))

    opciones = obtener_opciones_filtros_bi()

    # ---- Filtros interactivos dinámicos (afectan a toda la pestaña) ----
    with st.container(border=True):
        c_f_head, c_f_clr = st.columns([4.0, 1.0])
        c_f_head.markdown("#### Filtros interactivos")
        c_f_clr.button("Limpiar filtros", use_container_width=True, key="btn_bi_limpiar", on_click=_limpiar_filtros_bi)

        r1_c1, r1_c2, r1_c3 = st.columns(3)
        anio_opts = ["Todos", *[str(a) for a in opciones["anios"]]]
        if st.session_state.get("bi_fil_anio") not in anio_opts:
            st.session_state["bi_fil_anio"] = "Todos"
        anio_txt = r1_c1.selectbox(
            "Año lectivo",
            options=anio_opts,
            key="bi_fil_anio",
        )
        anio_sel = int(anio_txt) if anio_txt != "Todos" else None

        dep_opts = ["Todos", *opciones["departamentos"]]
        if st.session_state.get("bi_fil_dep") not in dep_opts:
            st.session_state["bi_fil_dep"] = "Todos"
        dep_sel = r1_c2.selectbox(
            "Departamento",
            options=dep_opts,
            key="bi_fil_dep",
        )

        if dep_sel == "Todos":
            ugeles_disp = sorted({r["ugel"] for r in opciones["ugeles_por_dep"]})
        else:
            ugeles_disp = sorted(
                {r["ugel"] for r in opciones["ugeles_por_dep"] if r["departamento"] == dep_sel}
            )
        ugel_opts = ["Todas", *ugeles_disp]
        if st.session_state.get("bi_fil_ugel") not in ugel_opts:
            st.session_state["bi_fil_ugel"] = "Todas"
        ugel_sel = r1_c3.selectbox(
            "UGEL",
            options=ugel_opts,
            key="bi_fil_ugel",
        )

        r2_c1, r2_c2, r2_c3 = st.columns(3)
        niv_opts = ["Todos", *opciones["niveles"]]
        if st.session_state.get("bi_fil_nivel") not in niv_opts:
            st.session_state["bi_fil_nivel"] = "Todos"
        niv_sel = r2_c1.selectbox("Nivel educativo", options=niv_opts, key="bi_fil_nivel")

        rur_opts = ["Todas", *opciones["ruralidades"]]
        if st.session_state.get("bi_fil_rur") not in rur_opts:
            st.session_state["bi_fil_rur"] = "Todas"
        rur_sel = r2_c2.selectbox("Gradiente de ruralidad", options=rur_opts, key="bi_fil_rur")

        sem_opts = ["Todos", *opciones["semaforos"]]
        if st.session_state.get("bi_fil_sem") not in sem_opts:
            st.session_state["bi_fil_sem"] = "Todos"
        sem_sel = r2_c3.selectbox(
            "Semáforo de riesgo (IA)",
            options=sem_opts,
            format_func=lambda s: "Todos" if s == "Todos" else fmt_semaforo(s),
            key="bi_fil_sem",
        )

    # ---- Carga de datos agregados desde SQL (st.cache_data) ----
    kpis = obtener_kpis_ejecutivos_bi(
        anio=anio_sel,
        departamento=dep_sel,
        ugel=ugel_sel,
        nivel=niv_sel,
        ruralidad=rur_sel,
        semaforo=sem_sel,
        meta_alumnos_pc=meta_apc,
    )

    if int(kpis.get("total_ies") or 0) == 0:
        st.warning("No existen instituciones que coincidan con la combinación de filtros seleccionada.")
        return

    anio_ref_txt = str(kpis.get("anio_referencia") or "—")
    st.caption(
        f"Alcance filtrado: **{fmt_int(kpis.get('total_ies'))}** instituciones en "
        f"**{fmt_int(kpis.get('total_locales'))}** locales escolares · "
        f"Año de referencia para equipamiento: **{anio_ref_txt}**"
    )

    # ---- Tarjetas de los 9 KPIs frente a su meta ----
    st.markdown("#### Tarjetas de KPI ejecutivos vs. meta")
    tasa_des = kpis.get("tasa_desercion_pct")
    apc_val = kpis.get("alumnos_por_pc_operativa")
    eq_op = kpis.get("equipos_operativos_pct")
    disp_net = kpis.get("disponibilidad_internet_pct")
    cortes_h = kpis.get("horas_corte_energia_prom")
    ind_con = kpis.get("indice_conectividad_rural")

    k_r1 = st.columns(3)
    k_r1[0].metric(
        "Tasa de deserción (%)",
        f"{fmt_dec(tasa_des, 2)} %" if tasa_des is not None else "—",
        delta=(
            f"{float(tasa_des) - meta_des:+.2f} pp vs. meta (< {meta_des:.2f} %)"
            if tasa_des is not None
            else None
        ),
        delta_color="inverse",
        help=f"Desertores: {fmt_int(kpis.get('total_desertores'))} / Matrícula: {fmt_int(kpis.get('total_matricula'))}",
    )
    k_r1[1].metric(
        "Alumnos por PC operativa (nivel local)",
        fmt_dec(apc_val, 1) if apc_val is not None else "—",
        delta=(
            f"{float(apc_val) - meta_apc:+.1f} vs. meta (≤ {meta_apc:.0f})"
            if apc_val is not None
            else None
        ),
        delta_color="inverse",
        help=f"Calculado sobre {fmt_int(kpis.get('locales_con_censo'))} locales únicos (sin doble conteo de equipos).",
    )
    k_r1[2].metric(
        "Equipos operativos (%) (nivel local)",
        f"{fmt_dec(eq_op, 1)} %" if eq_op is not None else "—",
        delta=(
            f"{float(eq_op) - 80.9:+.1f} pp vs. meta (> 80.9 %)"
            if eq_op is not None
            else None
        ),
        delta_color="normal",
        help=f"{fmt_int(kpis.get('cpu_operativos'))} operativas de {fmt_int(kpis.get('cpu_total'))} totales (locales únicos).",
    )

    k_r2 = st.columns(3)
    k_r2[0].metric(
        "Disponibilidad de Internet (%)",
        f"{fmt_dec(disp_net, 1)} %" if disp_net is not None else "—",
        delta=(
            f"{float(disp_net) - 90.0:+.1f} pp vs. meta (≥ 90 %)"
            if disp_net is not None
            else "Sin lecturas IoT válidas en el filtro"
        ),
        delta_color="normal" if disp_net is not None else "off",
        help=f"Excluye {fmt_int(kpis.get('lecturas_sin_senal'))} lecturas 'Sin señal' de {fmt_int(kpis.get('lecturas_totales'))} totales.",
    )
    k_r2[1].metric(
        "Horas de corte de energía",
        f"{fmt_dec(cortes_h, 1)} h/mes" if cortes_h is not None else "—",
        delta=(
            f"{float(cortes_h) - 24.0:+.1f} h vs. meta (≤ 24 h/mes)"
            if cortes_h is not None
            else "Sin lecturas IoT en el filtro"
        ),
        delta_color="inverse" if cortes_h is not None else "off",
        help=f"Suma acumulada filtrada: {fmt_dec(kpis.get('horas_corte_energia_total'), 1)} h.",
    )
    k_r2[2].metric(
        "Índice de conectividad rural (0-100)",
        f"{fmt_dec(ind_con, 1)}" if ind_con is not None else "—",
        delta=(
            f"{float(ind_con) - 70.0:+.1f} pts vs. meta (≥ 70)"
            if ind_con is not None
            else None
        ),
        delta_color="normal",
        help=(
            f"Equip: {fmt_dec(kpis.get('comp_equipamiento_prom'), 1)} · "
            f"Internet: {fmt_dec(kpis.get('comp_internet_prom'), 1)} · "
            f"Energía: {fmt_dec(kpis.get('comp_energia_prom'), 1)}"
        ),
    )

    k_r3 = st.columns(3)
    prob_prom = kpis.get("prob_riesgo_prom")
    ies_eval_ia = int(kpis.get("ies_evaluadas_ia") or 0)
    ies_rojo = int(kpis.get("ies_rojo") or 0)
    k_r3[0].metric(
        "Prob. riesgo de deserción (IA)",
        fmt_dec(prob_prom, 3) if prob_prom is not None else "Pendiente",
        delta=(
            f"🔴 {fmt_int(ies_rojo)} en Rojo (umbral ≥ {umbral_rojo:.2f} · 🟡 ≥ {umbral_amarillo:.2f})"
            if ies_eval_ia > 0
            else "Ejecuta IA en pestaña 5"
        ),
        delta_color="off",
    )

    des_base_t2 = kpis.get("desertores_proy_t2_base")
    des_opt_t2 = kpis.get("desertores_proy_t2_optimista")
    anio_t2 = kpis.get("anio_proy_t2")
    k_r3[1].metric(
        f"Desertores proyectados ({anio_t2 or 'base + 2'})",
        fmt_int(des_opt_t2) if des_opt_t2 is not None else "Pendiente",
        delta=(
            f"{int(des_opt_t2) - int(des_base_t2):+,} vs. base ({fmt_int(des_base_t2)})"
            if des_opt_t2 is not None and des_base_t2 is not None
            else "Ejecuta IA en pestaña 5"
        ),
        delta_color="inverse" if des_opt_t2 is not None and des_base_t2 is not None else "off",
        help="Escenario optimista (con intervención) comparado frente al escenario base.",
    )

    inv_asig = kpis.get("inversion_asignada")
    locs_fin = int(kpis.get("locales_financiados") or 0)
    locs_def = int(kpis.get("locales_con_deficit") or 0)
    k_r3[2].metric(
        "Presupuesto intervención (IA)",
        fmt_soles(inv_asig) if locs_def > 0 else "Pendiente",
        delta=(
            f"{fmt_int(locs_fin)}/{fmt_int(locs_def)} locales dentro de {fmt_soles(presupuesto_tope)}"
            if locs_def > 0
            else "Ejecuta IA en pestaña 5"
        ),
        delta_color="off",
    )

    st.divider()

    # ---- Gráficos principales: Deserción por departamento y año + Reparto de semáforo ----
    col_g1, col_g2 = st.columns([1.3, 1.0])

    with col_g1:
        st.markdown("#### Tasa de deserción por departamento y año")
        df_des = obtener_desercion_departamento_anio(
            anio=anio_sel,
            departamento=dep_sel,
            ugel=ugel_sel,
            nivel=niv_sel,
            ruralidad=rur_sel,
            semaforo=sem_sel,
        )
        if df_des.empty:
            st.caption("Sin datos suficientes para graficar la tendencia de deserción.")
        else:
            fig_des = px.line(
                df_des,
                x="anio",
                y="tasa_desercion_pct",
                color="departamento",
                markers=True,
                hover_data={
                    "matricula_inicial": ":,",
                    "desertores": ":,",
                    "instituciones": ":,",
                    "tasa_desercion_pct": ":.2f",
                },
                labels={
                    "anio": "Año",
                    "tasa_desercion_pct": "Tasa de deserción (%)",
                    "departamento": "Departamento",
                },
            )
            fig_des.add_hline(
                y=meta_des,
                line_dash="dash",
                line_color="#dc2626",
                annotation_text=f"Meta (< {meta_des:.2f} %)",
                annotation_position="top left",
            )
            fig_des.update_layout(
                height=390,
                margin=dict(l=10, r=15, t=25, b=20),
                xaxis=dict(dtick=1),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_des, use_container_width=True)
            st.download_button(
                label="Exportar tasa por departamento y año (CSV)",
                data=df_des.to_csv(index=False).encode("utf-8-sig"),
                file_name="tasa_desercion_departamento_anio.csv",
                mime="text/csv",
                key="dl_bi_des_dep_anio",
            )

    with col_g2:
        st.markdown("#### Reparto del semáforo de riesgo (IA)")
        df_sem_glob, df_sem_dep = obtener_reparto_semaforo_bi(
            anio=anio_sel,
            departamento=dep_sel,
            ugel=ugel_sel,
            nivel=niv_sel,
            ruralidad=rur_sel,
            semaforo=sem_sel,
        )
        if df_sem_glob.empty:
            st.caption("Sin evaluaciones de semáforo disponibles.")
        else:
            mapa_etiq_sem = {
                "Rojo": "🔴 Rojo",
                "Amarillo": "🟡 Amarillo",
                "Verde": "🟢 Verde",
                "Sin evaluar": "⚪ Sin evaluar",
            }
            mapa_col_sem = {
                "🔴 Rojo": "#dc2626",
                "🟡 Amarillo": "#f59e0b",
                "🟢 Verde": "#16a34a",
                "⚪ Sin evaluar": "#94a3b8",
            }
            df_sem_glob["Etiqueta"] = df_sem_glob["semaforo"].map(lambda s: mapa_etiq_sem.get(s, s))
            fig_sem = px.pie(
                df_sem_glob,
                names="Etiqueta",
                values="instituciones",
                color="Etiqueta",
                color_discrete_map=mapa_col_sem,
                hole=0.45,
            )
            fig_sem.update_traces(textinfo="label+value+percent")
            fig_sem.update_layout(
                height=240,
                margin=dict(l=10, r=10, t=10, b=10),
                showlegend=True,
            )
            st.plotly_chart(fig_sem, use_container_width=True)

            df_sem_dep["Etiqueta"] = df_sem_dep["semaforo"].map(lambda s: mapa_etiq_sem.get(s, s))
            fig_sem_dep = px.bar(
                df_sem_dep,
                x="departamento",
                y="instituciones",
                color="Etiqueta",
                color_discrete_map=mapa_col_sem,
                barmode="stack",
                labels={"departamento": "Departamento", "instituciones": "Instituciones", "Etiqueta": "Semáforo"},
            )
            fig_sem_dep.update_layout(
                height=210,
                margin=dict(l=10, r=10, t=10, b=20),
                showlegend=False,
            )
            st.plotly_chart(fig_sem_dep, use_container_width=True)

    # ---- Índice de conectividad rural y sus 3 componentes ----
    df_ind = obtener_indice_conectividad_bi(
        anio=anio_sel,
        departamento=dep_sel,
        ugel=ugel_sel,
        nivel=niv_sel,
        ruralidad=rur_sel,
        semaforo=sem_sel,
        meta_alumnos_pc=meta_apc,
    )
    if not df_ind.empty:
        with st.expander("Componentes del Índice de conectividad rural por departamento y ruralidad", expanded=False):
            df_ind_dep = (
                df_ind.groupby("departamento", as_index=False)[
                    ["equipamiento", "internet", "energia", "indice_conectividad_rural"]
                ]
                .mean()
                .round(1)
            )
            df_melt = df_ind_dep.melt(
                id_vars="departamento",
                value_vars=["equipamiento", "internet", "energia", "indice_conectividad_rural"],
                var_name="Componente",
                value_name="Puntaje",
            )
            nombres_comp = {
                "equipamiento": "Equipamiento (40%)",
                "internet": "Internet (30%)",
                "energia": "Energía (30%)",
                "indice_conectividad_rural": "Índice total",
            }
            df_melt["Componente"] = df_melt["Componente"].map(nombres_comp)
            fig_ind = px.bar(
                df_melt,
                x="departamento",
                y="Puntaje",
                color="Componente",
                barmode="group",
                labels={"departamento": "Departamento", "Puntaje": "Puntaje (0-100)"},
            )
            fig_ind.add_hline(y=70, line_dash="dash", line_color="#0f766e", annotation_text="Meta (≥ 70)")
            fig_ind.update_layout(height=340, margin=dict(l=10, r=10, t=20, b=20))
            st.plotly_chart(fig_ind, use_container_width=True)
            st.dataframe(df_ind, use_container_width=True, hide_index=True)
            st.download_button(
                label="Exportar índice de conectividad (CSV)",
                data=df_ind.to_csv(index=False).encode("utf-8-sig"),
                file_name="indice_conectividad_rural_acr.csv",
                mime="text/csv",
                key="dl_bi_indice_con",
            )

    st.divider()

    # ---- Panel de alertas activas (§9) ----
    st.markdown("#### Panel de alertas activas por desvío de metas")
    resumen_al, df_alertas = obtener_alertas_activas_bi(
        anio=anio_sel,
        departamento=dep_sel,
        ugel=ugel_sel,
        nivel=niv_sel,
        ruralidad=rur_sel,
        semaforo=sem_sel,
        meta_desercion=meta_des,
        meta_alumnos_pc=meta_apc,
    )
    a1, a2, a3, a4, a5, a6 = st.columns(6)
    a1.metric("🔴 Semáforo Rojo", fmt_int(resumen_al["semaforo_rojo"]))
    a2.metric(f"Deserción > {meta_des:.2f} %", fmt_int(resumen_al["desercion_sobre_meta"]))
    a3.metric("Internet < 90 %", fmt_int(resumen_al["internet_bajo_90"]))
    a4.metric("Cortes > 24 h/mes", fmt_int(resumen_al["cortes_sobre_24"]))
    a5.metric(f"Alum/PC > {meta_apc:.0f}", fmt_int(resumen_al["alumnos_pc_sobre_meta"]))
    a6.metric("Anomalía IoT", fmt_int(resumen_al["anomalia_iot"]))

    if df_alertas.empty:
        st.success("🟢 Sin alertas activas bajo los filtros actuales.")
    else:
        regla_al_sel = st.selectbox(
            "Filtrar por tipo de alerta",
            options=[
                "Todas las alertas",
                "Semáforo Rojo",
                "Deserción sobre meta",
                "Internet < 90 %",
                "Cortes > 24 h/mes",
                "Alumnos por PC > meta",
                "Anomalía IoT",
            ],
            key="bi_alerta_filtro_regla",
        )
        mapa_col_al = {
            "Semáforo Rojo": "al_rojo",
            "Deserción sobre meta": "al_desercion",
            "Internet < 90 %": "al_internet",
            "Cortes > 24 h/mes": "al_energia",
            "Alumnos por PC > meta": "al_alumnos_pc",
            "Anomalía IoT": "al_iot",
        }
        df_al_fil = df_alertas.copy()
        if regla_al_sel != "Todas las alertas":
            df_al_fil = df_al_fil[df_al_fil[mapa_col_al[regla_al_sel]]]

        df_al_show = pd.DataFrame(
            {
                "Prioridad": df_al_fil["prioridad_alerta"],
                "Semáforo": df_al_fil["semaforo"].map(fmt_semaforo),
                "Cód. modular": df_al_fil["cod_modular"],
                "Institución": df_al_fil["nombre_ie"],
                "Departamento": df_al_fil["departamento"],
                "Distrito": df_al_fil["distrito"],
                "Alertas activas": df_al_fil["alertas_activas"],
                "Deserción (%)": df_al_fil["tasa_desercion_pct"].map(lambda v: fmt_dec(v, 2)),
                "Alum/PC local": df_al_fil["alumnos_por_pc_local"].map(lambda v: fmt_dec(v, 1)),
                "Disp. Internet (%)": df_al_fil["disp_prom"].map(lambda v: fmt_dec(v, 1)),
                "Cortes (h/mes)": df_al_fil["cortes_prom"].map(lambda v: fmt_dec(v, 1)),
            }
        )
        st.caption(f"Instituciones con al menos una alerta activa: **{fmt_int(len(df_al_show))}**.")
        st.dataframe(df_al_show, use_container_width=True, hide_index=True)
        st.download_button(
            label="Exportar alertas activas (CSV)",
            data=df_al_fil.to_csv(index=False).encode("utf-8-sig"),
            file_name="alertas_activas_acr.csv",
            mime="text/csv",
            key="dl_bi_alertas",
        )

    st.divider()

    # ---- Reporte completo de instituciones (usado en Mapa y en Reporte por institución) ----
    df_rep_ie = obtener_reporte_instituciones_bi(
        anio=anio_sel,
        departamento=dep_sel,
        ugel=ugel_sel,
        nivel=niv_sel,
        ruralidad=rur_sel,
        semaforo=sem_sel,
        meta_alumnos_pc=meta_apc,
    )

    # ---- Mapa geográfico de instituciones coloreadas por semáforo ----
    st.markdown("#### Mapa de instituciones educativas por semáforo de riesgo")
    df_mapa = df_rep_ie.dropna(subset=["latitud", "longitud"]).copy() if not df_rep_ie.empty else pd.DataFrame()
    if df_mapa.empty:
        st.caption("No hay coordenadas válidas (latitud/longitud) para mostrar el mapa.")
    else:
        df_mapa["semaforo_etiqueta"] = df_mapa["semaforo"].map(
            lambda s: fmt_semaforo(s) if s in ("Rojo", "Amarillo", "Verde") else "⚪ Sin evaluar"
        )
        df_mapa["tasa_txt"] = df_mapa["tasa_desercion_pct"].map(lambda v: fmt_dec(v, 2))
        df_mapa["apc_txt"] = df_mapa["alumnos_por_pc_local"].map(lambda v: fmt_dec(v, 1))
        df_mapa["ind_txt"] = df_mapa["indice_conectividad_rural"].map(lambda v: fmt_dec(v, 1))

        fig_mapa = px.scatter_map(
            df_mapa,
            lat="latitud",
            lon="longitud",
            color="semaforo_etiqueta",
            color_discrete_map={
                "🔴 Rojo": "#dc2626",
                "🟡 Amarillo": "#f59e0b",
                "🟢 Verde": "#16a34a",
                "⚪ Sin evaluar": "#64748b",
            },
            hover_name="nombre_ie",
            hover_data={
                "cod_modular": True,
                "cod_local": True,
                "departamento": True,
                "distrito": True,
                "nivel": True,
                "gradiente_ruralidad": True,
                "tasa_txt": True,
                "apc_txt": True,
                "ind_txt": True,
                "latitud": False,
                "longitud": False,
                "semaforo_etiqueta": True,
            },
            labels={
                "semaforo_etiqueta": "Semáforo",
                "cod_modular": "Cód. modular",
                "cod_local": "Cód. local",
                "departamento": "Departamento",
                "distrito": "Distrito",
                "nivel": "Nivel",
                "gradiente_ruralidad": "Ruralidad",
                "tasa_txt": "Deserción (%)",
                "apc_txt": "Alum/PC local",
                "ind_txt": "Índice conect.",
            },
            center={
                "lat": float(df_mapa["latitud"].mean()),
                "lon": float(df_mapa["longitud"].mean()),
            },
            zoom=5.2 if dep_sel == "Todos" else 7.2,
            map_style="open-street-map",
            height=480,
        )
        fig_mapa.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_mapa, use_container_width=True)

    st.divider()

    # ---- Drill-down: Departamento -> Distrito -> Institución con migas de pan en st.session_state ----
    st.markdown("#### Exploración jerárquica (Drill-down: Departamento → Distrito → Institución)")

    st.session_state.setdefault("bi_dd_departamento", None)
    st.session_state.setdefault("bi_dd_ubigeo", None)
    st.session_state.setdefault("bi_dd_distrito", None)
    st.session_state.setdefault("bi_dd_cod_modular", None)

    # Mantener coherencia cuando el filtro superior de departamento está fijado
    if dep_sel != "Todos" and st.session_state["bi_dd_departamento"] != dep_sel:
        st.session_state["bi_dd_departamento"] = dep_sel
        st.session_state["bi_dd_ubigeo"] = None
        st.session_state["bi_dd_distrito"] = None
        st.session_state["bi_dd_cod_modular"] = None

    dd_dep = st.session_state["bi_dd_departamento"]
    dd_ubigeo = st.session_state["bi_dd_ubigeo"]
    dd_dist = st.session_state["bi_dd_distrito"]
    dd_ie = st.session_state["bi_dd_cod_modular"]

    # Migas de pan interactivas guardadas en st.session_state
    b1, b2, b3, b4 = st.columns(4)
    if b1.button("🇵🇪 Todos los departamentos", use_container_width=True, key="dd_crumb_nac"):
        st.session_state["bi_dd_departamento"] = dep_sel if dep_sel != "Todos" else None
        st.session_state["bi_dd_ubigeo"] = None
        st.session_state["bi_dd_distrito"] = None
        st.session_state["bi_dd_cod_modular"] = None
        st.rerun()

    if dd_dep:
        if b2.button(f"📍 {dd_dep}", use_container_width=True, key="dd_crumb_dep"):
            st.session_state["bi_dd_ubigeo"] = None
            st.session_state["bi_dd_distrito"] = None
            st.session_state["bi_dd_cod_modular"] = None
            st.rerun()
    else:
        b2.button("📍 Departamento: —", disabled=True, use_container_width=True, key="dd_crumb_dep_off")

    if dd_ubigeo:
        if b3.button(f"🏘️ {dd_dist}", use_container_width=True, key="dd_crumb_dist"):
            st.session_state["bi_dd_cod_modular"] = None
            st.rerun()
    else:
        b3.button("🏘️ Distrito: —", disabled=True, use_container_width=True, key="dd_crumb_dist_off")

    if dd_ie:
        b4.button(f"🏫 IE {dd_ie}", disabled=True, use_container_width=True, key="dd_crumb_ie_on")
    else:
        b4.button("🏫 Institución: —", disabled=True, use_container_width=True, key="dd_crumb_ie_off")

    # Contenido según nivel actual del drill-down
    if dd_ie is not None:
        _renderizar_ficha_institucion_bi(dd_ie)
    elif dd_ubigeo is not None:
        df_dd_ie = obtener_reporte_instituciones_bi(
            anio=anio_sel,
            departamento=dd_dep or dep_sel,
            ugel=ugel_sel,
            nivel=niv_sel,
            ruralidad=rur_sel,
            semaforo=sem_sel,
            ubigeo_filtro=dd_ubigeo,
            meta_alumnos_pc=meta_apc,
        )
        if df_dd_ie.empty:
            st.info("No hay instituciones para este distrito con los filtros aplicados.")
        else:
            c_sel_ie, c_btn_ie = st.columns([3.0, 1.0])
            opts_ie = df_dd_ie["cod_modular"].tolist()
            ie_nombres = {
                r["cod_modular"]: f"{r['cod_modular']} — {r['nombre_ie']} ({fmt_semaforo(r['semaforo'])})"
                for _, r in df_dd_ie.iterrows()
            }
            ie_elegida = c_sel_ie.selectbox(
                f"Instituciones en {dd_dist} ({dd_ubigeo})",
                options=opts_ie,
                format_func=lambda c: ie_nombres.get(c, c),
                key="dd_sel_ie",
            )
            if c_btn_ie.button("Ver ficha de institución ➡", type="primary", use_container_width=True, key="dd_go_ie"):
                st.session_state["bi_dd_cod_modular"] = ie_elegida
                st.rerun()

            df_dd_ie_show = pd.DataFrame(
                {
                    "Semáforo": df_dd_ie["semaforo"].map(fmt_semaforo),
                    "Cód. modular": df_dd_ie["cod_modular"],
                    "Institución": df_dd_ie["nombre_ie"],
                    "Cód. local": df_dd_ie["cod_local"],
                    "Nivel": df_dd_ie["nivel"],
                    "Ruralidad": df_dd_ie["gradiente_ruralidad"],
                    "Matrícula": df_dd_ie["matricula_inicial"].map(fmt_int),
                    "Deserción (%)": df_dd_ie["tasa_desercion_pct"].map(lambda v: fmt_dec(v, 2)),
                    "Prob. riesgo": df_dd_ie["prob_riesgo"].map(lambda v: fmt_dec(v, 3)),
                    "Alum/PC local": df_dd_ie["alumnos_por_pc_local"].map(lambda v: fmt_dec(v, 1)),
                    "Índice conect.": df_dd_ie["indice_conectividad_rural"].map(lambda v: fmt_dec(v, 1)),
                }
            )
            st.dataframe(df_dd_ie_show, use_container_width=True, hide_index=True)
            st.download_button(
                label=f"Exportar instituciones de {dd_dist} (CSV)",
                data=df_dd_ie.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"drilldown_ie_{dd_ubigeo}.csv",
                mime="text/csv",
                key="dl_bi_dd_ie",
            )
    elif dd_dep is not None:
        df_dd_dist = obtener_reporte_distritos_bi(
            anio=anio_sel,
            departamento=dd_dep,
            ugel=ugel_sel,
            nivel=niv_sel,
            ruralidad=rur_sel,
            semaforo=sem_sel,
            meta_alumnos_pc=meta_apc,
        )
        if df_dd_dist.empty:
            st.info(f"No hay distritos disponibles en {dd_dep} bajo los filtros actuales.")
        else:
            c_sel_dist, c_btn_dist = st.columns([3.0, 1.0])
            opts_ub = df_dd_dist["ubigeo"].tolist()
            dist_etiquetas = {
                r["ubigeo"]: f"{r['distrito']} (ubigeo {r['ubigeo']} · {r['provincia']} · Deserción {fmt_dec(r['tasa_desercion_pct'], 2)} %)"
                for _, r in df_dd_dist.iterrows()
            }
            ub_elegido = c_sel_dist.selectbox(
                f"Distritos de {dd_dep}",
                options=opts_ub,
                format_func=lambda u: dist_etiquetas.get(u, u),
                key="dd_sel_dist",
            )
            if c_btn_dist.button("Explorar instituciones ➡", type="primary", use_container_width=True, key="dd_go_dist"):
                fila_d = df_dd_dist[df_dd_dist["ubigeo"] == ub_elegido].iloc[0]
                st.session_state["bi_dd_ubigeo"] = ub_elegido
                st.session_state["bi_dd_distrito"] = str(fila_d["distrito"])
                st.session_state["bi_dd_cod_modular"] = None
                st.rerun()

            fig_dd_dist = px.bar(
                df_dd_dist.head(20),
                x="distrito",
                y="tasa_desercion_pct",
                color="indice_conectividad_rural",
                color_continuous_scale="Tealgrn",
                labels={
                    "distrito": "Distrito",
                    "tasa_desercion_pct": "Tasa de deserción (%)",
                    "indice_conectividad_rural": "Índice conect.",
                },
                title=f"Distritos de {dd_dep} por tasa de deserción (%) e índice de conectividad",
            )
            fig_dd_dist.add_hline(y=meta_des, line_dash="dash", line_color="#dc2626")
            fig_dd_dist.update_layout(height=340, margin=dict(l=10, r=10, t=40, b=20))
            st.plotly_chart(fig_dd_dist, use_container_width=True)

            st.dataframe(df_dd_dist, use_container_width=True, hide_index=True)
            st.download_button(
                label=f"Exportar distritos de {dd_dep} (CSV)",
                data=df_dd_dist.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"drilldown_distritos_{dd_dep.lower()}.csv",
                mime="text/csv",
                key="dl_bi_dd_dist",
            )
    else:
        df_dd_dep = obtener_drilldown_departamentos_bi(
            anio=anio_sel,
            departamento=dep_sel,
            ugel=ugel_sel,
            nivel=niv_sel,
            ruralidad=rur_sel,
            semaforo=sem_sel,
            meta_alumnos_pc=meta_apc,
        )
        if df_dd_dep.empty:
            st.info("No hay departamentos disponibles con los filtros actuales.")
        else:
            c_sel_dep, c_btn_dep = st.columns([3.0, 1.0])
            dep_elegido = c_sel_dep.selectbox(
                "Seleccionar departamento para profundizar",
                options=df_dd_dep["departamento"].tolist(),
                key="dd_sel_dep",
            )
            if c_btn_dep.button("Explorar distritos ➡", type="primary", use_container_width=True, key="dd_go_dep"):
                st.session_state["bi_dd_departamento"] = dep_elegido
                st.session_state["bi_dd_ubigeo"] = None
                st.session_state["bi_dd_distrito"] = None
                st.session_state["bi_dd_cod_modular"] = None
                st.rerun()

            fig_dd_dep = px.bar(
                df_dd_dep,
                x="departamento",
                y="tasa_desercion_pct",
                color="indice_conectividad_rural",
                color_continuous_scale="Tealgrn",
                labels={
                    "departamento": "Departamento",
                    "tasa_desercion_pct": "Tasa de deserción (%)",
                    "indice_conectividad_rural": "Índice conect.",
                },
                title="Comparativo departamental: deserción (%) e índice de conectividad rural",
            )
            fig_dd_dep.add_hline(y=meta_des, line_dash="dash", line_color="#dc2626")
            fig_dd_dep.update_layout(height=340, margin=dict(l=10, r=10, t=40, b=20))
            st.plotly_chart(fig_dd_dep, use_container_width=True)

            st.dataframe(df_dd_dep, use_container_width=True, hide_index=True)
            st.download_button(
                label="Exportar resumen por departamento (CSV)",
                data=df_dd_dep.to_csv(index=False).encode("utf-8-sig"),
                file_name="drilldown_departamentos_acr.csv",
                mime="text/csv",
                key="dl_bi_dd_dep",
            )

    st.divider()

    # ---- Reportes analíticos por distrito e institución con exportación CSV ----
    st.markdown("#### Reportes analíticos y exportación CSV")
    sub_dist, sub_ie = st.tabs(["Reporte por distrito", "Reporte por institución"])

    with sub_dist:
        df_rep_dist = obtener_reporte_distritos_bi(
            anio=anio_sel,
            departamento=dep_sel,
            ugel=ugel_sel,
            nivel=niv_sel,
            ruralidad=rur_sel,
            semaforo=sem_sel,
            meta_alumnos_pc=meta_apc,
        )
        busq_dist = st.text_input(
            "Buscar distrito, provincia o ubigeo",
            key="bi_rep_dist_busq",
        ).strip().lower()
        df_rd_fil = df_rep_dist.copy()
        if busq_dist and not df_rd_fil.empty:
            mask = (
                df_rd_fil["distrito"].fillna("").str.lower().str.contains(busq_dist)
                | df_rd_fil["provincia"].fillna("").str.lower().str.contains(busq_dist)
                | df_rd_fil["ubigeo"].fillna("").str.lower().str.contains(busq_dist)
            )
            df_rd_fil = df_rd_fil[mask]

        st.caption(f"Mostrando **{fmt_int(len(df_rd_fil))}** distritos (equipamiento agregado a nivel local sin doble conteo).")
        st.dataframe(df_rd_fil, use_container_width=True, hide_index=True)
        st.download_button(
            label="Exportar reporte por distrito (CSV)",
            data=df_rd_fil.to_csv(index=False).encode("utf-8-sig"),
            file_name="reporte_distritos_acr.csv",
            mime="text/csv",
            key="dl_bi_rep_dist",
        )

    with sub_ie:
        busq_ie = st.text_input(
            "Buscar código modular, nombre de institución, local o distrito",
            key="bi_rep_ie_busq",
        ).strip().lower()
        df_ri_fil = df_rep_ie.copy()
        if busq_ie and not df_ri_fil.empty:
            mask = (
                df_ri_fil["cod_modular"].fillna("").str.lower().str.contains(busq_ie)
                | df_ri_fil["nombre_ie"].fillna("").str.lower().str.contains(busq_ie)
                | df_ri_fil["cod_local"].fillna("").str.lower().str.contains(busq_ie)
                | df_ri_fil["distrito"].fillna("").str.lower().str.contains(busq_ie)
            )
            df_ri_fil = df_ri_fil[mask]

        if not df_ri_fil.empty:
            df_ri_show = df_ri_fil.copy()
            df_ri_show["semaforo"] = df_ri_show["semaforo"].map(fmt_semaforo)
            st.caption(f"Mostrando **{fmt_int(len(df_ri_show))}** instituciones educativas.")
            st.dataframe(df_ri_show, use_container_width=True, hide_index=True)
        else:
            st.caption("No hay instituciones que coincidan con la búsqueda.")

        st.download_button(
            label="Exportar reporte por institución (CSV)",
            data=df_ri_fil.to_csv(index=False).encode("utf-8-sig"),
            file_name="reporte_instituciones_acr.csv",
            mime="text/csv",
            key="dl_bi_rep_ie",
        )


def renderizar_tab_datastudio() -> None:
    """Pestaña 8: enlace externo e incrustación centrada del dashboard de Data Studio."""
    st.subheader("8. Dashboard DataStudio")
    st.write("Dashboard de Data Studio conectado a la vista `vw_bi_dashboard` de la base de datos.")
    st.link_button("Abrir en Data Studio", url=DATASTUDIO_URL_ABRIR)

    iframe_html = (
        '<div style="display: flex; justify-content: center;">'
        f'<iframe width="1200" height="5500" src="{DATASTUDIO_URL_EMBED}" '
        'frameborder="0" style="border:0" allowfullscreen '
        'sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox">'
        "</iframe>"
        "</div>"
    )
    components.html(iframe_html, height=5520, scrolling=False)


def main() -> None:
    st.title("Aula Conectada Rural")
    st.caption(
        "Plataforma de Business Intelligence de 7 capas para el análisis de permanencia escolar "
        "en comunidades rurales del Perú."
    )

    try:
        inicializar_esquema()
        inicializar_capa_semantica()
        estado_fuentes = obtener_estado_fuentes()
        estado_proceso = obtener_estado_proceso()
    except Exception as exc:
        st.error(f"Error de conexión o inicialización en PostgreSQL (Neon): {exc}")
        st.info(
            "Verifica que `DATABASE_URL` esté configurada en `.streamlit/secrets.toml` o como "
            "variable de entorno."
        )
        st.divider()
        st.caption("Prototipo académico — datos simulados")
        return

    renderizar_indicador_proceso(estado_proceso)

    tabs = st.tabs(
        [
            "Fuentes de datos",
            "Staging Area",
            "Proceso ETL",
            "Data Warehouse",
            "Inteligencia artificial",
            "Capa semántica",
            "Visualización BI",
            "Dashboard DataStudio",
        ]
    )

    with tabs[0]:
        renderizar_tab_fuentes(estado_fuentes)

    with tabs[1]:
        renderizar_tab_staging(estado_fuentes, estado_proceso)

    with tabs[2]:
        renderizar_tab_etl(estado_fuentes, estado_proceso)

    with tabs[3]:
        renderizar_tab_dw()

    with tabs[4]:
        renderizar_tab_ia(estado_proceso)

    with tabs[5]:
        renderizar_tab_semantica(estado_proceso)

    with tabs[6]:
        renderizar_tab_bi(estado_proceso)

    with tabs[7]:
        renderizar_tab_datastudio()

    st.divider()
    st.markdown(
        "<div style='text-align:center; color:#64748b; font-size:0.85rem; padding:6px 0;'>"
        "Prototipo académico — datos simulados"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
