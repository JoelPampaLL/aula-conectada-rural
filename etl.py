"""Perfilado de staging, control de calidad genérico y pipeline ETL completo (Extract, Transform, Load)."""

from __future__ import annotations

import re
import time
from typing import Any, Callable

import numpy as np
import pandas as pd

from db import (
    FUENTES_CONFIG,
    cargar_dw_masivo,
    leer_tabla_staging,
    obtener_siguiente_lote_etl,
)
from modelo_aula_conectada import limpiar_tablas

MARCADORES_TEXTO_FALTANTE = {"no registra", "n/d", "sin dato", "n/a", "na", "-"}

ETIQUETAS_SEVERIDAD = {
    "Alta": "🔴 Alta",
    "Media": "🟡 Media",
    "Baja": "🟢 Baja",
}

NOMBRES_MESES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


def _es_vacio(serie: pd.Series) -> pd.Series:
    return serie.fillna("").astype(str).str.strip() == ""


def _agregar_hallazgo(
    hallazgos: list[dict[str, Any]],
    severidad: str,
    fuente: str,
    regla: str,
    columna_o_clave: str,
    cantidad: int,
    detalle: str,
) -> None:
    if cantidad <= 0:
        return
    hallazgos.append(
        {
            "severidad": severidad,
            "severidad_etiqueta": ETIQUETAS_SEVERIDAD[severidad],
            "fuente": fuente,
            "regla": regla,
            "columna_o_clave": columna_o_clave,
            "cantidad": int(cantidad),
            "detalle": detalle,
        }
    )


def _serie_numerica(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col].fillna("").astype(str).str.strip(), errors="coerce")


def perfilar_tabla_staging(fuente_id: str, df: pd.DataFrame) -> pd.DataFrame:
    """Genera perfilado inicial por columna sin transformar el DataFrame crudo."""
    cfg = FUENTES_CONFIG[fuente_id]
    columnas = cfg["columnas"]
    tipos = cfg["tipos"]
    cols_numericas = set(cfg["columnas_numericas"])
    total_filas = len(df)

    registros: list[dict[str, Any]] = []
    for col in columnas:
        serie = df[col].fillna("").astype(str) if col in df.columns else pd.Series([], dtype=str)
        vacio_mask = _es_vacio(serie)
        cant_vacios = int(vacio_mask.sum())
        pct_vacios = round((cant_vacios / total_filas * 100.0), 1) if total_filas > 0 else 0.0

        no_vacios = serie[~vacio_mask]
        cant_no_vacios = len(no_vacios)

        if col in cols_numericas:
            convertidos = pd.to_numeric(no_vacios, errors="coerce")
            invalidos_mask = convertidos.isna()
            cant_no_num = int(invalidos_mask.sum())
            pct_conv = (
                round(((cant_no_vacios - cant_no_num) / cant_no_vacios * 100.0), 1)
                if cant_no_vacios > 0
                else 100.0
            )
            ejemplos = list(dict.fromkeys(no_vacios[invalidos_mask].str.strip().tolist()))[:3]
            ejemplos_txt = ", ".join(ejemplos) if ejemplos else "—"
        else:
            cant_no_num = 0
            pct_conv = None
            ejemplos_txt = "—"

        registros.append(
            {
                "Columna": col,
                "Tipo esperado": tipos.get(col, "Texto"),
                "Filas": total_filas,
                "Vacíos": cant_vacios,
                "% Vacíos": pct_vacios,
                "% Convertible a número": pct_conv,
                "Textos en col. numérica": cant_no_num,
                "Ejemplos no numéricos": ejemplos_txt,
            }
        )

    return pd.DataFrame(registros)


def ejecutar_control_calidad_staging(
    tablas: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, list[str]]:
    """Ejecuta reglas genéricas de control de calidad sobre datos crudos en staging."""
    hallazgos: list[dict[str, Any]] = []
    advertencias: list[str] = []

    for fuente_id, cfg in FUENTES_CONFIG.items():
        df = tablas.get(fuente_id)
        if df is None or df.empty:
            continue

        # 1. Códigos con longitud incorrecta o caracteres no numéricos
        for col_cod, largo_esperado in cfg.get("longitudes_codigo", {}).items():
            serie = df[col_cod].fillna("").astype(str).str.strip()
            no_vacio = serie != ""
            invalido = no_vacio & (~serie.str.fullmatch(rf"\d{{{largo_esperado}}}"))
            cant = int(invalido.sum())
            if cant > 0:
                ejemplos = ", ".join(list(dict.fromkeys(serie[invalido].tolist()))[:3])
                _agregar_hallazgo(
                    hallazgos,
                    "Alta",
                    fuente_id,
                    f"Código con formato o longitud distinta de {largo_esperado} dígitos",
                    col_cod,
                    cant,
                    f"Ejemplos: {ejemplos}",
                )

        # 2. Duplicados por clave natural
        clave_nat = cfg["clave_natural"]
        subset_df = df[clave_nat].apply(lambda s: s.fillna("").astype(str).str.strip())
        dups = int(subset_df.duplicated(keep="first").sum())
        _agregar_hallazgo(
            hallazgos,
            "Alta",
            fuente_id,
            "Registros duplicados por clave natural",
            " + ".join(clave_nat),
            dups,
            "Registros repetidos más allá de la primera ocurrencia",
        )

        # 3. Textos en columnas numéricas
        for col_num in cfg["columnas_numericas"]:
            serie = df[col_num].fillna("").astype(str).str.strip()
            no_vacio = serie != ""
            conv = pd.to_numeric(serie[no_vacio], errors="coerce")
            inval = conv.isna()
            cant = int(inval.sum())
            if cant > 0:
                tokens = ", ".join(list(dict.fromkeys(serie[no_vacio][inval].tolist()))[:3])
                _agregar_hallazgo(
                    hallazgos,
                    "Media",
                    fuente_id,
                    "Texto no numérico en columna numérica",
                    col_num,
                    cant,
                    f"Valores detectados: {tokens}",
                )

        # 4. Marcadores de dato faltante en columnas de texto
        cols_texto = [c for c in cfg["columnas"] if c not in cfg["columnas_numericas"]]
        for col_txt in cols_texto:
            serie = df[col_txt].fillna("").astype(str).str.strip()
            marcador = serie.str.lower().isin(MARCADORES_TEXTO_FALTANTE)
            cant = int(marcador.sum())
            if cant > 0:
                tokens = ", ".join(list(dict.fromkeys(serie[marcador].tolist()))[:3])
                _agregar_hallazgo(
                    hallazgos,
                    "Media",
                    fuente_id,
                    "Valor marcador de dato faltante en columna de texto",
                    col_txt,
                    cant,
                    f"Valores detectados: {tokens}",
                )

        # 5. Vacíos por columna (distinguiendo vacíos estructurales esperados)
        for col in cfg["columnas"]:
            vacio_mask = _es_vacio(df[col])
            cant_vac = int(vacio_mask.sum())
            if cant_vac == 0:
                continue

            if fuente_id == "T3" and col == "carga_bateria_solar_pct":
                _agregar_hallazgo(
                    hallazgos,
                    "Baja",
                    fuente_id,
                    "Celda vacía esperada cuando el local no cuenta con panel solar",
                    col,
                    cant_vac,
                    "Vacío estructural permitido",
                )
            elif fuente_id == "T3" and col in {
                "ancho_banda_prom_mbps",
                "latencia_prom_ms",
                "disponibilidad_internet_pct",
            }:
                sin_senal = df["estado_sensor"].fillna("").astype(str).str.strip() == "Sin señal"
                vac_sin_senal = int((vacio_mask & sin_senal).sum())
                vac_otros = int((vacio_mask & (~sin_senal)).sum())
                _agregar_hallazgo(
                    hallazgos,
                    "Media",
                    fuente_id,
                    "Lectura sin métricas de red (sensor «Sin señal»)",
                    col,
                    vac_sin_senal,
                    "Debe conservarse como «Sin señal» y excluirse del promedio",
                )
                _agregar_hallazgo(
                    hallazgos,
                    "Alta",
                    fuente_id,
                    "Métrica de red vacía con sensor operativo o intermitente",
                    col,
                    vac_otros,
                    "Falta dato de telemetría con sensor no marcado como Sin señal",
                )
            elif fuente_id == "T4" and col in {
                "cpu_total",
                "cpu_operativos",
                "cpu_en_reparacion",
                "cpu_mas_3_anios",
                "laptops_xo",
                "tabletas",
                "n_equipos_red",
            }:
                no_declaro = df["declaro_computo"].fillna("").astype(str).str.strip().str.lower() == "no"
                vac_estructural = int((vacio_mask & no_declaro).sum())
                vac_inesperado = int((vacio_mask & (~no_declaro)).sum())
                _agregar_hallazgo(
                    hallazgos,
                    "Baja",
                    fuente_id,
                    "Celda vacía esperada (declaro_computo = No)",
                    col,
                    vac_estructural,
                    "Vacío estructural permitido según ficha censal",
                )
                _agregar_hallazgo(
                    hallazgos,
                    "Media",
                    fuente_id,
                    "Celda vacía en columna de equipamiento pese a declarar cómputo",
                    col,
                    vac_inesperado,
                    "Requiere revisión en ETL",
                )
            else:
                _agregar_hallazgo(
                    hallazgos,
                    "Media",
                    fuente_id,
                    "Valores vacíos / nulos",
                    col,
                    cant_vac,
                    "Celdas sin dato en staging",
                )

    # 6. Reglas de rango por fuente
    if "T1" in tablas and not tablas["T1"].empty:
        t1 = tablas["T1"]
        tasa = _serie_numerica(t1, "tasa_desercion_pct")
        asist = _serie_numerica(t1, "asistencia_prom_pct")
        mat_ini = _serie_numerica(t1, "matricula_inicial")
        des = _serie_numerica(t1, "desertores")
        ret = _serie_numerica(t1, "retirados")
        doc_tot = _serie_numerica(t1, "docentes_total")
        doc_tit = _serie_numerica(t1, "docentes_titulados")

        _agregar_hallazgo(
            hallazgos, "Alta", "T1", "Valor fuera de rango (< 0 o > 100)", "tasa_desercion_pct",
            int(((tasa < 0) | (tasa > 100)).sum()), "Porcentaje inválido"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T1", "Valor fuera de rango (< 0 o > 100)", "asistencia_prom_pct",
            int(((asist < 0) | (asist > 100)).sum()), "Porcentaje inválido"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T1", "Conteo negativo en variables de matrícula", "matricula_inicial / retirados / desertores",
            int(((mat_ini < 0) | (ret < 0) | (des < 0)).sum()), "Conteos negativos no permitidos"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T1", "Docentes titulados mayor al total de docentes", "docentes_titulados > docentes_total",
            int((doc_tit > doc_tot).sum()), "Inconsistencia aritmética"
        )

        anios_validos = sorted(
            {int(a) for a in _serie_numerica(t1, "anio").dropna().tolist()}
        )
        tiene_consecutivos = any(
            anios_validos[i + 1] - anios_validos[i] == 1
            for i in range(len(anios_validos) - 1)
        )
        if not tiene_consecutivos:
            msg = (
                "T1 no cuenta con al menos 2 años consecutivos "
                f"(años detectados: {anios_validos or 'ninguno'})."
            )
            advertencias.append(msg)
            _agregar_hallazgo(
                hallazgos, "Media", "T1", "Mínimo operativo: menos de 2 años consecutivos", "anio", 1, msg
            )

    if "T2" in tablas and not tablas["T2"].empty:
        t2 = tablas["T2"]
        lat = _serie_numerica(t2, "latitud")
        lon = _serie_numerica(t2, "longitud")
        alt = _serie_numerica(t2, "altitud_msnm")
        dist = _serie_numerica(t2, "distancia_capital_distrital_km")
        tiempo = _serie_numerica(t2, "tiempo_acceso_horas")

        _agregar_hallazgo(
            hallazgos, "Alta", "T2", "Latitud fuera de rango [-90, 90]", "latitud",
            int(((lat < -90) | (lat > 90)).sum()), "Coordenada inválida"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T2", "Longitud fuera de rango [-180, 180]", "longitud",
            int(((lon < -180) | (lon > 180)).sum()), "Coordenada inválida"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T2", "Valor negativo en variable geográfica", "altitud / distancia / tiempo_acceso",
            int(((alt < 0) | (dist < 0) | (tiempo < 0)).sum()), "No se admiten valores negativos"
        )

        ies_unicas = int(
            t2["cod_modular"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()
        )
        if ies_unicas < 20:
            msg = f"T2 tiene solo {ies_unicas} instituciones únicas (mínimo recomendado: 20)."
            advertencias.append(msg)
            _agregar_hallazgo(
                hallazgos, "Media", "T2", "Mínimo operativo: menos de 20 instituciones", "cod_modular", ies_unicas, msg
            )

    if "T3" in tablas and not tablas["T3"].empty:
        t3 = tablas["T3"]
        latencia = _serie_numerica(t3, "latencia_prom_ms")
        disp = _serie_numerica(t3, "disponibilidad_internet_pct")
        bw = _serie_numerica(t3, "ancho_banda_prom_mbps")
        cortes = _serie_numerica(t3, "horas_corte_energia")
        bat = _serie_numerica(t3, "carga_bateria_solar_pct")

        _agregar_hallazgo(
            hallazgos, "Alta", "T3", "Latencia negativa", "latencia_prom_ms",
            int((latencia < 0).sum()), "Lecturas con latencia_prom_ms < 0"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T3", "Disponibilidad de Internet > 100 % o < 0 %", "disponibilidad_internet_pct",
            int(((disp > 100) | (disp < 0)).sum()), "Porcentaje fuera de [0, 100]"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T3", "Valores negativos en ancho de banda o cortes", "ancho_banda_prom_mbps / horas_corte_energia",
            int(((bw < 0) | (cortes < 0)).sum()), "Magnitudes físicas negativas"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T3", "Carga solar fuera de [0, 100]", "carga_bateria_solar_pct",
            int(((bat < 0) | (bat > 100)).sum()), "Porcentaje fuera de [0, 100]"
        )

    if "T4" in tablas and not tablas["T4"].empty:
        t4 = tablas["T4"]
        cpu_tot = _serie_numerica(t4, "cpu_total")
        cpu_op = _serie_numerica(t4, "cpu_operativos")
        _agregar_hallazgo(
            hallazgos, "Alta", "T4", "Computadoras operativas mayor que el total declarado", "cpu_operativos > cpu_total",
            int((cpu_op > cpu_tot).sum()), "Debe limitarse a cpu_total en el ETL"
        )

        negativos_t4 = 0
        for col_eq in ["n_ie_en_local", "cpu_total", "cpu_operativos", "cpu_en_reparacion", "cpu_mas_3_anios", "laptops_xo", "tabletas", "n_equipos_red"]:
            negativos_t4 += int((_serie_numerica(t4, col_eq) < 0).sum())
        _agregar_hallazgo(
            hallazgos, "Alta", "T4", "Conteo negativo en equipamiento del local", "equipos",
            negativos_t4, "Conteos negativos no permitidos"
        )

    if "T5" in tablas and not tablas["T5"].empty:
        t5 = tablas["T5"]
        mes_n = _serie_numerica(t5, "mes")
        precip = _serie_numerica(t5, "precipitacion_mm")
        dias_sc = _serie_numerica(t5, "dias_sin_clases_clima")
        pob = _serie_numerica(t5, "pobreza_monetaria_pct")
        hog_int = _serie_numerica(t5, "hogares_con_internet_pct")

        _agregar_hallazgo(
            hallazgos, "Alta", "T5", "Mes fuera del rango 1 a 12", "mes",
            int(((mes_n < 1) | (mes_n > 12)).sum()), "Valor de mes inválido"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T5", "Precipitación negativa", "precipitacion_mm",
            int((precip < 0).sum()), "Precipitación < 0 mm"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T5", "Días sin clases fuera de rango [0, 31]", "dias_sin_clases_clima",
            int(((dias_sc < 0) | (dias_sc > 31)).sum()), "Fuera de rango mensual"
        )
        _agregar_hallazgo(
            hallazgos, "Alta", "T5", "Porcentaje distrital fuera de [0, 100]", "pobreza_monetaria_pct / hogares_con_internet_pct",
            int(((pob < 0) | (pob > 100) | (hog_int < 0) | (hog_int > 100)).sum()), "Porcentaje inválido"
        )

    # 7. Claves huérfanas entre fuentes presentes
    if "T2" in tablas and not tablas["T2"].empty:
        t2 = tablas["T2"]
        cod_mod_t2 = set(t2["cod_modular"].fillna("").astype(str).str.strip()) - {""}
        cod_loc_t2 = set(t2["cod_local"].fillna("").astype(str).str.strip()) - {""}
        ubigeo_t2 = set(t2["ubigeo"].fillna("").astype(str).str.strip()) - {""}

        chequeos_fk = [
            ("T1", "cod_modular", cod_mod_t2, "T1.cod_modular → T2.cod_modular"),
            ("T3", "cod_modular", cod_mod_t2, "T3.cod_modular → T2.cod_modular"),
            ("T4", "cod_local", cod_loc_t2, "T4.cod_local → T2.cod_local"),
            ("T5", "ubigeo", ubigeo_t2, "T5.ubigeo → T2.ubigeo"),
        ]
        for fid_src, col_fk, conjunto_t2, desc_rel in chequeos_fk:
            df_src = tablas.get(fid_src)
            if df_src is None or df_src.empty:
                continue
            vals = df_src[col_fk].fillna("").astype(str).str.strip()
            huerfanos = (vals != "") & (~vals.isin(conjunto_t2))
            cant = int(huerfanos.sum())
            if cant > 0:
                ejemplos = ", ".join(list(dict.fromkeys(vals[huerfanos].tolist()))[:3])
                _agregar_hallazgo(
                    hallazgos,
                    "Alta",
                    fid_src,
                    f"Clave huérfana ({desc_rel})",
                    col_fk,
                    cant,
                    f"Claves sin correspondencia en T2: {ejemplos}",
                )

    columnas_salida = [
        "severidad",
        "severidad_etiqueta",
        "fuente",
        "regla",
        "columna_o_clave",
        "cantidad",
        "detalle",
    ]
    if not hallazgos:
        return pd.DataFrame(columns=columnas_salida), advertencias

    orden_sev = {"Alta": 0, "Media": 1, "Baja": 2}
    df_hallazgos = pd.DataFrame(hallazgos)
    df_hallazgos["_ord"] = df_hallazgos["severidad"].map(orden_sev).fillna(9)
    df_hallazgos = (
        df_hallazgos.sort_values(["_ord", "fuente", "cantidad"], ascending=[True, True, False])
        .drop(columns=["_ord"])
        .reset_index(drop=True)
    )
    return df_hallazgos, advertencias


# --------------------------------------------------------------------------------------
# ETAPA 2: PIPELINE ETL (EXTRACT -> TRANSFORM -> LOAD AL DATA WAREHOUSE)
# --------------------------------------------------------------------------------------
def _to_int(valor: Any) -> int | None:
    if valor is None or pd.isna(valor):
        return None
    try:
        return int(round(float(valor)))
    except Exception:
        return None


def _to_float(valor: Any, decimales: int | None = None) -> float | None:
    if valor is None or pd.isna(valor):
        return None
    try:
        num = float(valor)
        if decimales is not None:
            return round(num, decimales)
        return num
    except Exception:
        return None


def _to_bool_si_no(valor: Any) -> bool | None:
    if valor is None or pd.isna(valor):
        return None
    txt = str(valor).strip().lower()
    if txt in {"sí", "si", "true", "1", "s"}:
        return True
    if txt in {"no", "false", "0", "n"}:
        return False
    return None


def _to_str(valor: Any) -> str | None:
    if valor is None or pd.isna(valor):
        return None
    txt = str(valor).strip()
    return txt if txt != "" else None


def _fmt_ejemplo(valor: Any) -> str:
    if valor is None or pd.isna(valor):
        return "NULL (nulo)"
    txt = str(valor).strip()
    return txt if txt != "" else "Vacío"


def _evaluar_reglas_antes_despues(
    raw1: pd.DataFrame,
    raw2: pd.DataFrame,
    raw3: pd.DataFrame,
    raw4: pd.DataFrame,
    raw5: pd.DataFrame,
    t1: pd.DataFrame,
    t2: pd.DataFrame,
    t3: pd.DataFrame,
    t4: pd.DataFrame,
    t5: pd.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Calcula el conteo antes/después y un registro ejemplo para cada regla de la sección 5."""
    reglas: list[dict[str, Any]] = []
    ejemplos: list[dict[str, str]] = []

    def add_regla(
        fuente: str,
        regla: str,
        accion: str,
        antes: int,
        despues: int,
        ejemplo: dict[str, str] | None = None,
    ) -> None:
        reglas.append(
            {
                "Fuente": fuente,
                "Regla (Sección 5)": regla,
                "Transformación aplicada": accion,
                "Antes (staging)": int(antes),
                "Después (limpio)": int(despues),
                "Estado": "🟢 Corregida / aplicada" if despues == 0 or fuente == "T3" and "Sin señal" in regla else "🟡 Revisar",
            }
        )
        if ejemplo is not None:
            ejemplos.append(ejemplo)

    # 1. T1 ubigeo != 6
    m1_ubi = raw1["ubigeo"].notna() & (raw1["ubigeo"].astype(str).str.strip().str.len() != 6)
    d1_ubi = t1["ubigeo"].notna() & (t1["ubigeo"].astype(str).str.strip().str.len() != 6)
    ej = None
    if m1_ubi.any():
        r_raw = raw1.loc[m1_ubi].iloc[0]
        m_clean = t1["id_registro"] == r_raw["id_registro"]
        val_desp = t1.loc[m_clean, "ubigeo"].iloc[0] if m_clean.any() else str(r_raw["ubigeo"]).zfill(6)
        ej = {
            "Fuente": "T1",
            "Regla": "ubigeo sin cero inicial (< 6 dígitos)",
            "Registro": str(r_raw["id_registro"]),
            "Columna": "ubigeo",
            "Antes (staging)": _fmt_ejemplo(r_raw["ubigeo"]),
            "Después (limpio)": _fmt_ejemplo(val_desp),
        }
    add_regla("T1", "ubigeo con longitud distinta de 6 dígitos", "Completar con ceros a la izquierda (zfill 6)", int(m1_ubi.sum()), int(d1_ubi.sum()), ej)

    # 2. T1 duplicados cod_modular + anio
    m1_dup = raw1.duplicated(["cod_modular", "anio"], keep="first")
    d1_dup = t1.duplicated(["cod_modular", "anio"], keep="first")
    ej = None
    if m1_dup.any():
        r_raw = raw1.loc[m1_dup].iloc[0]
        ej = {
            "Fuente": "T1",
            "Regla": "Duplicados por cod_modular + anio",
            "Registro": f"{r_raw['cod_modular']} ({r_raw['anio']})",
            "Columna": "id_registro / filas",
            "Antes (staging)": f"{int(m1_dup.sum()) + int((~m1_dup).sum())} filas (incl. duplicado {r_raw['id_registro']})",
            "Después (limpio)": f"{len(t1)} filas (conservado 1er registro)",
        }
    add_regla("T1", "Duplicados por cod_modular + anio", "Conservar el primer registro", int(m1_dup.sum()), int(d1_dup.sum()), ej)

    # 3. T1 asistencia_prom_pct texto no numérico
    m1_asist = raw1["asistencia_prom_pct"].notna() & pd.to_numeric(raw1["asistencia_prom_pct"], errors="coerce").isna()
    ej = None
    if m1_asist.any():
        r_raw = raw1.loc[m1_asist].iloc[0]
        m_clean = t1["id_registro"] == r_raw["id_registro"]
        val_desp = t1.loc[m_clean, "asistencia_prom_pct"].iloc[0] if m_clean.any() else np.nan
        ej = {
            "Fuente": "T1",
            "Regla": "asistencia_prom_pct con texto no numérico",
            "Registro": str(r_raw["id_registro"]),
            "Columna": "asistencia_prom_pct",
            "Antes (staging)": _fmt_ejemplo(r_raw["asistencia_prom_pct"]),
            "Después (limpio)": _fmt_ejemplo(val_desp),
        }
    add_regla("T1", "asistencia_prom_pct con texto («No registra»)", "Convertir a nulo (sin imputar)", int(m1_asist.sum()), 0, ej)

    # 4. T1 matricula_final vacía
    m1_mf = raw1["matricula_final"].isna()
    d1_mf = t1["matricula_final"].isna()
    ej = None
    if m1_mf.any():
        r_raw = raw1.loc[m1_mf].iloc[0]
        ej = {
            "Fuente": "T1",
            "Regla": "matricula_final vacía",
            "Registro": str(r_raw["id_registro"]),
            "Columna": "matricula_final",
            "Antes (staging)": _fmt_ejemplo(r_raw["matricula_final"]),
            "Después (limpio)": "NULL (nulo conservado)",
        }
    reglas.append(
        {
            "Fuente": "T1",
            "Regla (Sección 5)": "matricula_final vacía",
            "Transformación aplicada": "Conservar como nulo (sin imputación)",
            "Antes (staging)": int(m1_mf.sum()),
            "Después (limpio)": int(d1_mf.sum()),
            "Estado": "🟢 Nulo conservado",
        }
    )
    if ej is not None:
        ejemplos.append(ej)

    # 5. T2 lengua_predominante = No registra (y exclusión del DW)
    m2_len = raw2["lengua_predominante"].fillna("").astype(str).str.strip() == "No registra"
    d2_len = t2["lengua_predominante"].fillna("").astype(str).str.strip() == "No registra"
    ej = None
    if m2_len.any():
        r_raw = raw2.loc[m2_len].iloc[0]
        ej = {
            "Fuente": "T2",
            "Regla": "lengua_predominante = «No registra» (excluida del DW)",
            "Registro": str(r_raw["cod_modular"]),
            "Columna": "lengua_predominante",
            "Antes (staging)": _fmt_ejemplo(r_raw["lengua_predominante"]),
            "Después (limpio)": "NULL (y excluida del DW)",
        }
    add_regla("T2", "lengua_predominante = «No registra»", "Convertir a nulo y omitir en DW", int(m2_len.sum()), int(d2_len.sum()), ej)

    # 6. T2 tiempo_acceso_horas vacío
    m2_tiem = raw2["tiempo_acceso_horas"].isna()
    d2_tiem = t2["tiempo_acceso_horas"].isna()
    ej = None
    if m2_tiem.any():
        r_raw = raw2.loc[m2_tiem].iloc[0]
        m_clean = t2["cod_modular"] == r_raw["cod_modular"]
        val_desp = round(float(t2.loc[m_clean, "tiempo_acceso_horas"].iloc[0]), 2) if m_clean.any() else np.nan
        ej = {
            "Fuente": "T2",
            "Regla": "tiempo_acceso_horas vacío",
            "Registro": f"{r_raw['cod_modular']} ({r_raw['tipo_acceso']})",
            "Columna": "tiempo_acceso_horas",
            "Antes (staging)": _fmt_ejemplo(r_raw["tiempo_acceso_horas"]),
            "Después (limpio)": f"{val_desp} h (estimado)",
        }
    add_regla("T2", "tiempo_acceso_horas vacío", "Estimar: distancia ÷ velocidad + 0.25 h", int(m2_tiem.sum()), int(d2_tiem.sum()), ej)

    # 7. T2 altitud_msnm vacía
    m2_alt = raw2["altitud_msnm"].isna()
    d2_alt = t2["altitud_msnm"].isna()
    ej = None
    if m2_alt.any():
        r_raw = raw2.loc[m2_alt].iloc[0]
        m_clean = t2["cod_modular"] == r_raw["cod_modular"]
        val_desp = round(float(t2.loc[m_clean, "altitud_msnm"].iloc[0]), 1) if m_clean.any() else np.nan
        ej = {
            "Fuente": "T2",
            "Regla": "altitud_msnm vacía",
            "Registro": f"{r_raw['cod_modular']} ({r_raw['distrito']})",
            "Columna": "altitud_msnm",
            "Antes (staging)": _fmt_ejemplo(r_raw["altitud_msnm"]),
            "Después (limpio)": f"{val_desp} m s. n. m. (promedio distrital)",
        }
    add_regla("T2", "altitud_msnm vacía", "Imputar promedio del distrito", int(m2_alt.sum()), int(d2_alt.sum()), ej)

    # 8. T3 latencia_prom_ms negativa
    m3_lat = pd.to_numeric(raw3["latencia_prom_ms"], errors="coerce") < 0
    d3_lat = t3["latencia_prom_ms"] < 0
    ej = None
    if m3_lat.any():
        r_raw = raw3.loc[m3_lat].iloc[0]
        m_clean = t3["id_lectura"] == r_raw["id_lectura"]
        val_desp = t3.loc[m_clean, "latencia_prom_ms"].iloc[0] if m_clean.any() else np.nan
        ej = {
            "Fuente": "T3",
            "Regla": "latencia_prom_ms negativa",
            "Registro": str(r_raw["id_lectura"]),
            "Columna": "latencia_prom_ms",
            "Antes (staging)": _fmt_ejemplo(r_raw["latencia_prom_ms"]),
            "Después (limpio)": _fmt_ejemplo(val_desp),
        }
    add_regla("T3", "latencia_prom_ms negativa", "Marcar lectura inválida (nulo)", int(m3_lat.sum()), int(d3_lat.sum()), ej)

    # 9. T3 disponibilidad_internet_pct > 100
    m3_disp = pd.to_numeric(raw3["disponibilidad_internet_pct"], errors="coerce") > 100
    d3_disp = t3["disponibilidad_internet_pct"] > 100
    ej = None
    if m3_disp.any():
        r_raw = raw3.loc[m3_disp].iloc[0]
        m_clean = t3["id_lectura"] == r_raw["id_lectura"]
        val_desp = t3.loc[m_clean, "disponibilidad_internet_pct"].iloc[0] if m_clean.any() else 100.0
        ej = {
            "Fuente": "T3",
            "Regla": "disponibilidad_internet_pct > 100",
            "Registro": str(r_raw["id_lectura"]),
            "Columna": "disponibilidad_internet_pct",
            "Antes (staging)": _fmt_ejemplo(r_raw["disponibilidad_internet_pct"]),
            "Después (limpio)": _fmt_ejemplo(val_desp),
        }
    add_regla("T3", "disponibilidad_internet_pct > 100", "Truncar a 100 %", int(m3_disp.sum()), int(d3_disp.sum()), ej)

    # 10. T3 lecturas sin métricas de red (Sin señal)
    m3_ss = (raw3["estado_sensor"].fillna("").astype(str).str.strip() == "Sin señal") & raw3["disponibilidad_internet_pct"].isna()
    d3_ss = (t3["estado_sensor"].fillna("").astype(str).str.strip() == "Sin señal") & t3["disponibilidad_internet_pct"].isna()
    ej = None
    if m3_ss.any():
        r_raw = raw3.loc[m3_ss].iloc[0]
        ej = {
            "Fuente": "T3",
            "Regla": "Lecturas sin métricas de red (Sin señal)",
            "Registro": str(r_raw["id_lectura"]),
            "Columna": "estado_sensor / disponibilidad",
            "Antes (staging)": "Sin señal / Vacío",
            "Después (limpio)": "Sin señal / NULL (excluido de promedios)",
        }
    reglas.append(
        {
            "Fuente": "T3",
            "Regla (Sección 5)": "Lecturas sin métricas de red (sensor «Sin señal»)",
            "Transformación aplicada": "Conservar con estado «Sin señal» y excluir de promedios",
            "Antes (staging)": int(m3_ss.sum()),
            "Después (limpio)": int(d3_ss.sum()),
            "Estado": "🟢 Conservada (excluida del promedio)",
        }
    )
    if ej is not None:
        ejemplos.append(ej)

    # 11. T4 ubigeo != 6
    m4_ubi = raw4["ubigeo"].notna() & (raw4["ubigeo"].astype(str).str.strip().str.len() != 6)
    d4_ubi = t4["ubigeo"].notna() & (t4["ubigeo"].astype(str).str.strip().str.len() != 6)
    ej = None
    if m4_ubi.any():
        r_raw = raw4.loc[m4_ubi].iloc[0]
        m_clean = t4["id_censo"] == r_raw["id_censo"]
        val_desp = t4.loc[m_clean, "ubigeo"].iloc[0] if m_clean.any() else str(r_raw["ubigeo"]).zfill(6)
        ej = {
            "Fuente": "T4",
            "Regla": "ubigeo sin cero inicial (< 6 dígitos)",
            "Registro": str(r_raw["id_censo"]),
            "Columna": "ubigeo",
            "Antes (staging)": _fmt_ejemplo(r_raw["ubigeo"]),
            "Después (limpio)": _fmt_ejemplo(val_desp),
        }
    add_regla("T4", "ubigeo con longitud distinta de 6 dígitos", "Completar con ceros a la izquierda (zfill 6)", int(m4_ubi.sum()), int(d4_ubi.sum()), ej)

    # 12. T4 cpu_operativos > cpu_total
    m4_cpu = pd.to_numeric(raw4["cpu_operativos"], errors="coerce") > pd.to_numeric(raw4["cpu_total"], errors="coerce")
    d4_cpu = t4["cpu_operativos"] > t4["cpu_total"]
    ej = None
    if m4_cpu.any():
        r_raw = raw4.loc[m4_cpu].iloc[0]
        m_clean = t4["id_censo"] == r_raw["id_censo"]
        val_desp = _to_int(t4.loc[m_clean, "cpu_operativos"].iloc[0]) if m_clean.any() else _to_int(r_raw["cpu_total"])
        ej = {
            "Fuente": "T4",
            "Regla": "cpu_operativos > cpu_total",
            "Registro": f"{r_raw['id_censo']} (total={r_raw['cpu_total']})",
            "Columna": "cpu_operativos",
            "Antes (staging)": _fmt_ejemplo(r_raw["cpu_operativos"]),
            "Después (limpio)": _fmt_ejemplo(val_desp),
        }
    add_regla("T4", "cpu_operativos > cpu_total", "Limitar al valor de cpu_total", int(m4_cpu.sum()), int(d4_cpu.sum()), ej)

    # 13. T4 tipo_internet = No registra
    m4_int = raw4["tipo_internet"].fillna("").astype(str).str.strip() == "No registra"
    d4_int = t4["tipo_internet"].fillna("").astype(str).str.strip() == "No registra"
    ej = None
    if m4_int.any():
        r_raw = raw4.loc[m4_int].iloc[0]
        ej = {
            "Fuente": "T4",
            "Regla": "tipo_internet = «No registra»",
            "Registro": str(r_raw["id_censo"]),
            "Columna": "tipo_internet",
            "Antes (staging)": _fmt_ejemplo(r_raw["tipo_internet"]),
            "Después (limpio)": "NULL (nulo)",
        }
    add_regla("T4", "tipo_internet = «No registra»", "Convertir a nulo", int(m4_int.sum()), int(d4_int.sum()), ej)

    # 14. T5 precipitacion_mm vacía
    m5_pre = raw5["precipitacion_mm"].isna()
    d5_pre = t5["precipitacion_mm"].isna()
    ej = None
    if m5_pre.any():
        r_raw = raw5.loc[m5_pre].iloc[0]
        m_clean = t5["id_registro"] == r_raw["id_registro"]
        val_desp = round(float(t5.loc[m_clean, "precipitacion_mm"].iloc[0]), 1) if m_clean.any() else np.nan
        ej = {
            "Fuente": "T5",
            "Regla": "precipitacion_mm vacía",
            "Registro": f"{r_raw['id_registro']} (ubigeo {r_raw['ubigeo']})",
            "Columna": "precipitacion_mm",
            "Antes (staging)": _fmt_ejemplo(r_raw["precipitacion_mm"]),
            "Después (limpio)": f"{val_desp} mm (mediana del distrito)",
        }
    add_regla("T5", "precipitacion_mm vacía", "Imputar mediana del distrito", int(m5_pre.sum()), int(d5_pre.sum()), ej)

    # 15. T5 evento_climatico = N/D
    m5_ev = raw5["evento_climatico"].fillna("").astype(str).str.strip() == "N/D"
    d5_ev = t5["evento_climatico"].fillna("").astype(str).str.strip() == "N/D"
    ej = None
    if m5_ev.any():
        r_raw = raw5.loc[m5_ev].iloc[0]
        m_clean = t5["id_registro"] == r_raw["id_registro"]
        val_desp = t5.loc[m_clean, "evento_climatico"].iloc[0] if m_clean.any() else "Sin dato"
        ej = {
            "Fuente": "T5",
            "Regla": "evento_climatico = «N/D»",
            "Registro": str(r_raw["id_registro"]),
            "Columna": "evento_climatico",
            "Antes (staging)": _fmt_ejemplo(r_raw["evento_climatico"]),
            "Después (limpio)": _fmt_ejemplo(val_desp),
        }
    add_regla("T5", "evento_climatico = «N/D»", "Reemplazar por «Sin dato»", int(m5_ev.sum()), int(d5_ev.sum()), ej)

    return reglas, ejemplos


def ejecutar_etl_completo(
    callback_progreso: Callable[[float, str], None] | None = None,
) -> dict[str, Any]:
    """Ejecuta Extract (stg_*) -> Transform (limpiar_tablas + huérfanas) -> Load masivo e idempotente al DW."""
    inicio = time.perf_counter()
    progreso = callback_progreso or (lambda pct, msg: None)

    # 1. EXTRACT
    progreso(0.05, "Extract: leyendo las 5 tablas crudas desde staging (stg_*)...")
    raw_crudos: dict[str, pd.DataFrame] = {}
    for fid in ("T1", "T2", "T3", "T4", "T5"):
        df_stg = leer_tabla_staging(fid)
        if df_stg.empty:
            raise ValueError(f"La fuente {fid} no tiene datos en staging. Carga las 5 fuentes primero.")
        raw_crudos[fid] = df_stg.replace("", np.nan)

    raw1, raw2, raw3, raw4, raw5 = (
        raw_crudos["T1"],
        raw_crudos["T2"],
        raw_crudos["T3"],
        raw_crudos["T4"],
        raw_crudos["T5"],
    )

    # 2. TRANSFORM (import directo de modelo_aula_conectada.limpiar_tablas sin modificarlo)
    progreso(0.25, "Transform: aplicando limpiar_tablas() sobre los datos crudos...")
    t1, t2, t3, t4, t5, log_limpieza = limpiar_tablas(raw1, raw2, raw3, raw4, raw5)

    # Normalización de claves de texto para integridad referencial en DW
    t1["cod_modular"] = t1["cod_modular"].fillna("").astype(str).str.strip()
    t1["ubigeo"] = t1["ubigeo"].fillna("").astype(str).str.strip().str.zfill(6)

    t2["cod_modular"] = t2["cod_modular"].fillna("").astype(str).str.strip()
    t2["cod_local"] = t2["cod_local"].fillna("").astype(str).str.strip()
    t2["ubigeo"] = t2["ubigeo"].fillna("").astype(str).str.strip().str.zfill(6)

    t3["cod_modular"] = t3["cod_modular"].fillna("").astype(str).str.strip()
    t3["id_sensor"] = t3["id_sensor"].fillna("").astype(str).str.strip()
    t3["mes"] = t3["mes"].fillna("").astype(str).str.strip()

    t4["cod_local"] = t4["cod_local"].fillna("").astype(str).str.strip()
    t4["ubigeo"] = t4["ubigeo"].fillna("").astype(str).str.strip().str.zfill(6)

    t5["ubigeo"] = t5["ubigeo"].fillna("").astype(str).str.strip().str.zfill(6)

    # Evaluación Antes / Después de todas las reglas de la sección 5
    progreso(0.40, "Transform: evaluando métricas Antes / Después por cada regla...")
    reglas_comparativas, ejemplos_reglas = _evaluar_reglas_antes_despues(
        raw1, raw2, raw3, raw4, raw5, t1, t2, t3, t4, t5
    )

    # Exclusión y reporte de filas con claves huérfanas o sin clave (sin romper el proceso)
    huerfanas_reporte: list[dict[str, Any]] = []

    mask_t2_valid = (t2["cod_modular"] != "") & (t2["cod_local"] != "") & (t2["ubigeo"] != "")
    if (~mask_t2_valid).any():
        huerfanas_reporte.append(
            {
                "Fuente": "T2",
                "Relación": "Clave primaria incompleta (cod_modular/cod_local/ubigeo)",
                "Filas excluidas": int((~mask_t2_valid).sum()),
                "Ejemplos": ", ".join(t2.loc[~mask_t2_valid, "cod_modular"].head(5).tolist()),
            }
        )
        t2 = t2[mask_t2_valid].copy()
    t2 = t2.drop_duplicates("cod_modular", keep="first").copy()

    cod_mod_validos = set(t2["cod_modular"])
    cod_loc_validos = set(t2["cod_local"])
    ubigeo_validos = set(t2["ubigeo"])

    # Huérfanas T1 -> T2
    mask_t1_huerf = ~t1["cod_modular"].isin(cod_mod_validos) | t1["anio"].isna()
    if mask_t1_huerf.any():
        huerfanas_reporte.append(
            {
                "Fuente": "T1",
                "Relación": "T1.cod_modular → T2.cod_modular",
                "Filas excluidas": int(mask_t1_huerf.sum()),
                "Ejemplos": ", ".join(list(dict.fromkeys(t1.loc[mask_t1_huerf, "cod_modular"].tolist()))[:5]),
            }
        )
        t1 = t1[~mask_t1_huerf].copy()

    # Huérfanas T3 -> T2 y validación de formato mes AAAA-MM
    mask_t3_mes_valido = t3["mes"].str.fullmatch(r"\d{4}-(0[1-9]|1[0-2])")
    mask_t3_huerf = (~t3["cod_modular"].isin(cod_mod_validos)) | (~mask_t3_mes_valido)
    if mask_t3_huerf.any():
        huerfanas_reporte.append(
            {
                "Fuente": "T3",
                "Relación": "T3.cod_modular → T2.cod_modular (o mes inválido)",
                "Filas excluidas": int(mask_t3_huerf.sum()),
                "Ejemplos": ", ".join(list(dict.fromkeys(t3.loc[mask_t3_huerf, "cod_modular"].tolist()))[:5]),
            }
        )
        t3 = t3[~mask_t3_huerf].copy()
    t3 = t3.drop_duplicates(["id_sensor", "mes"], keep="first").copy()

    # Huérfanas T4 -> T2
    mask_t4_huerf = (~t4["cod_local"].isin(cod_loc_validos)) | t4["anio_censo"].isna()
    if mask_t4_huerf.any():
        huerfanas_reporte.append(
            {
                "Fuente": "T4",
                "Relación": "T4.cod_local → T2.cod_local",
                "Filas excluidas": int(mask_t4_huerf.sum()),
                "Ejemplos": ", ".join(list(dict.fromkeys(t4.loc[mask_t4_huerf, "cod_local"].tolist()))[:5]),
            }
        )
        t4 = t4[~mask_t4_huerf].copy()
    t4 = t4.drop_duplicates(["cod_local", "anio_censo"], keep="first").copy()

    # Huérfanas T5 -> T2
    mask_t5_huerf = (~t5["ubigeo"].isin(ubigeo_validos)) | t5["anio"].isna() | t5["mes"].isna()
    if mask_t5_huerf.any():
        huerfanas_reporte.append(
            {
                "Fuente": "T5",
                "Relación": "T5.ubigeo → T2.ubigeo",
                "Filas excluidas": int(mask_t5_huerf.sum()),
                "Ejemplos": ", ".join(list(dict.fromkeys(t5.loc[mask_t5_huerf, "ubigeo"].tolist()))[:5]),
            }
        )
        t5 = t5[~mask_t5_huerf].copy()
    t5 = t5.drop_duplicates(["ubigeo", "anio", "mes"], keep="first").copy()

    # 3. CONSTRUCCIÓN DEL ESQUEMA COPO DE NIEVE
    progreso(0.55, "Transform: normalizando dimensiones territoriales e institucionales...")

    # Dimensiones territoriales
    departamentos = sorted({_to_str(v) for v in t2["departamento"].dropna().unique() if _to_str(v)})
    dim_departamento = [(dep,) for dep in departamentos]

    prov_pares = sorted(
        {
            (_to_str(r["provincia"]), _to_str(r["departamento"]))
            for _, r in t2[["provincia", "departamento"]].drop_duplicates().iterrows()
            if _to_str(r["provincia"]) and _to_str(r["departamento"])
        }
    )

    dist_df = t2[["ubigeo", "distrito", "provincia", "departamento"]].drop_duplicates("ubigeo", keep="first")
    dim_distrito = [
        (r["ubigeo"], _to_str(r["distrito"]) or r["ubigeo"], _to_str(r["provincia"]), _to_str(r["departamento"]))
        for _, r in dist_df.iterrows()
    ]

    ugeles = sorted({_to_str(v) for v in t2["ugel"].dropna().unique() if _to_str(v)})
    dim_ugel = [(u,) for u in ugeles]

    # dim_ruralidad
    rur_map: dict[int, str] = {1: "Rural 1", 2: "Rural 2", 3: "Rural 3"}
    for val in t2["gradiente_ruralidad"].dropna().unique():
        m = re.search(r"(\d+)", str(val))
        if m:
            rur_map.setdefault(int(m.group(1)), str(val).strip())
    dim_ruralidad = sorted(rur_map.items())

    # dim_nivel
    nivel_a_id: dict[str, int] = {"Primaria": 1, "Secundaria": 2}
    niveles_datos = list(
        dict.fromkeys(
            [
                _to_str(v)
                for v in pd.concat([t2["nivel"], t1["nivel"]]).dropna().tolist()
                if _to_str(v)
            ]
        )
    )
    sig_niv_id = 3
    for niv in niveles_datos:
        if niv not in nivel_a_id:
            nivel_a_id[niv] = sig_niv_id
            sig_niv_id += 1
    dim_nivel = sorted((vid, vnom) for vnom, vid in nivel_a_id.items())

    # dim_tiempo (generada dinámicamente a partir de todos los años presentes en los datos)
    anios_set: set[int] = set()
    anios_set.update(int(a) for a in t1["anio"].dropna().unique())
    if not t3.empty:
        anios_set.update(int(m[:4]) for m in t3["mes"].dropna().unique())
    if not t4.empty:
        anios_set.update(int(a) for a in t4["anio_censo"].dropna().unique())
    if not t5.empty:
        anios_set.update(int(a) for a in t5["anio"].dropna().unique())

    dim_tiempo: list[tuple[int, int, int, int, str]] = []
    for anio in sorted(anios_set):
        dim_tiempo.append((anio * 100, anio, 0, 0, "Año completo"))
        for mes_num in range(1, 13):
            dim_tiempo.append(
                (
                    anio * 100 + mes_num,
                    anio,
                    mes_num,
                    (mes_num - 1) // 3 + 1,
                    NOMBRES_MESES[mes_num],
                )
            )

    # dim_local (una fila por cod_local; si un local aloja 2 IE conserva atributos físicos únicos)
    local_df = t2.drop_duplicates("cod_local", keep="first")
    dim_local = [
        (
            r["cod_local"],
            r["ubigeo"],
            _to_str(r["ugel"]),
            _to_float(r["latitud"], 5),
            _to_float(r["longitud"], 5),
            _to_int(r["altitud_msnm"]),
            _to_float(r["distancia_capital_distrital_km"], 1),
            _to_float(r["tiempo_acceso_horas"], 1),
            _to_str(r["tipo_acceso"]),
        )
        for _, r in local_df.iterrows()
    ]

    # dim_ie (sin lengua_predominante: queda solo en staging por ser atributo sensible)
    def _parse_id_rur(grad: Any) -> int | None:
        m = re.search(r"(\d+)", str(grad or ""))
        return int(m.group(1)) if m else None

    dim_ie = [
        (
            r["cod_modular"],
            _to_str(r["nombre_ie"]),
            nivel_a_id.get(_to_str(r["nivel"]) or ""),
            _parse_id_rur(r["gradiente_ruralidad"]),
            r["cod_local"],
            bool(_to_bool_si_no(r["kit_aula_conectada"])),
        )
        for _, r in t2.iterrows()
    ]

    # Agregación de T5 por distrito (ubigeo) y año según sección 5
    progreso(0.68, "Transform: agregando clima por distrito/año y enriqueciendo hechos...")
    t5["anio_int"] = t5["anio"].astype(int)
    t5["mes_int"] = t5["mes"].astype(int)
    t5_anual = (
        t5.sort_values(["ubigeo", "anio_int", "mes_int"])
        .groupby(["ubigeo", "anio_int"], as_index=False)
        .agg(
            precipitacion_anual_mm=("precipitacion_mm", lambda s: s.sum(min_count=1)),
            dias_sin_clases_clima=("dias_sin_clases_clima", lambda s: s.sum(min_count=1)),
            temp_min_anual_c=("temp_min_c", "min"),
            pobreza_pct=("pobreza_monetaria_pct", "first"),
            hogares_internet_pct=("hogares_con_internet_pct", "first"),
            cobertura_movil=("cobertura_movil", "first"),
        )
        .rename(columns={"anio_int": "anio"})
    )

    # Enriquecimiento de fact_permanencia (IE + año)
    ie_meta = t2[["cod_modular", "cod_local", "ubigeo"]].drop_duplicates("cod_modular")
    t1_enr = t1.copy()
    t1_enr["anio_int"] = t1_enr["anio"].astype(int)
    t1_enr = t1_enr.drop(columns=["ubigeo"], errors="ignore").merge(ie_meta, on="cod_modular", how="left")

    t4_prep = t4.copy()
    t4_prep["anio_int"] = t4_prep["anio_censo"].astype(int)
    cols_t4_join = [
        "cod_local",
        "anio_int",
        "declaro_computo",
        "cpu_total",
        "cpu_operativos",
        "cpu_en_reparacion",
        "cpu_mas_3_anios",
        "tiene_internet",
        "tipo_internet",
        "fuente_energia",
        "tiene_ups_estabilizador",
        "tiene_pozo_tierra",
        "n_equipos_red",
    ]
    t1_enr = t1_enr.merge(t4_prep[cols_t4_join], on=["cod_local", "anio_int"], how="left")
    t1_enr = t1_enr.merge(t5_anual, left_on=["ubigeo", "anio_int"], right_on=["ubigeo", "anio"], how="left")

    fact_permanencia = [
        (
            r["cod_modular"],
            int(r["anio_int"]) * 100,
            _to_int(r["matricula_inicial"]),
            _to_int(r["matricula_final"]),
            _to_int(r["retirados"]),
            _to_int(r["desertores"]),
            _to_float(r["tasa"], 2),
            _to_float(r["asistencia_prom_pct"], 1),
            _to_int(r["docentes_total"]),
            _to_int(r["docentes_titulados"]),
            _to_bool_si_no(r["declaro_computo"]),
            _to_int(r["cpu_total"]),
            _to_int(r["cpu_operativos"]),
            _to_int(r["cpu_en_reparacion"]),
            _to_int(r["cpu_mas_3_anios"]),
            _to_bool_si_no(r["tiene_internet"]),
            _to_str(r["tipo_internet"]),
            _to_str(r["fuente_energia"]),
            _to_bool_si_no(r["tiene_ups_estabilizador"]),
            _to_bool_si_no(r["tiene_pozo_tierra"]),
            _to_int(r["n_equipos_red"]),
            _to_float(r["precipitacion_anual_mm"], 1),
            _to_int(r["dias_sin_clases_clima"]),
            _to_float(r["temp_min_anual_c"], 1),
            _to_float(r["pobreza_pct"], 1),
            _to_float(r["hogares_internet_pct"], 1),
            _to_str(r["cobertura_movil"]),
            _to_str(r["estado_registro"]),
        )
        for _, r in t1_enr.iterrows()
    ]

    # Enriquecimiento de fact_conectividad (sensor + mes)
    t3_enr = t3.copy()
    t3_enr["anio_int"] = t3_enr["mes"].str[:4].astype(int)
    t3_enr["mes_int"] = t3_enr["mes"].str[5:7].astype(int)
    t3_enr = t3_enr.merge(ie_meta[["cod_modular", "ubigeo"]], on="cod_modular", how="left")
    t5_mes = t5[["ubigeo", "anio_int", "mes_int", "precipitacion_mm", "evento_climatico"]].drop_duplicates(
        ["ubigeo", "anio_int", "mes_int"]
    )
    t3_enr = t3_enr.merge(t5_mes, on=["ubigeo", "anio_int", "mes_int"], how="left")

    fact_conectividad = [
        (
            _to_str(r["id_lectura"]),
            r["cod_modular"],
            int(r["anio_int"]) * 100 + int(r["mes_int"]),
            r["id_sensor"],
            _to_str(r["tipo_enlace"]),
            _to_float(r["ancho_banda_prom_mbps"], 1),
            _to_float(r["latencia_prom_ms"], 0),
            _to_float(r["disponibilidad_internet_pct"], 1),
            _to_float(r["horas_corte_energia"], 1),
            _to_float(r["voltaje_prom_v"], 1),
            _to_float(r["carga_bateria_solar_pct"], 1),
            _to_float(r["temperatura_aula_c"], 1),
            _to_int(r["dispositivos_conectados_prom"]),
            _to_str(r["estado_sensor"]),
            _to_float(r["precipitacion_mm"], 1),
            _to_str(r["evento_climatico"]),
        )
        for _, r in t3_enr.iterrows()
    ]

    datos_dw = {
        "dim_departamento": dim_departamento,
        "dim_provincia": prov_pares,
        "dim_distrito": dim_distrito,
        "dim_ugel": dim_ugel,
        "dim_ruralidad": dim_ruralidad,
        "dim_nivel": dim_nivel,
        "dim_tiempo": dim_tiempo,
        "dim_local": dim_local,
        "dim_ie": dim_ie,
        "fact_permanencia": fact_permanencia,
        "fact_conectividad": fact_conectividad,
    }

    # Preparar registros para log_etl
    lote = obtener_siguiente_lote_etl()
    logs_etl: list[dict[str, Any]] = [
        {
            "regla": f"{r['Regla (Sección 5)']} ({r['Transformación aplicada']})",
            "fuente": r["Fuente"],
            "filas_afectadas": r["Antes (staging)"],
            "filas_cargadas": r["Después (limpio)"],
            "estado": "ok",
        }
        for r in reglas_comparativas
    ]
    for h in huerfanas_reporte:
        logs_etl.append(
            {
                "regla": f"Exclusión de clave huérfana ({h['Relación']})",
                "fuente": h["Fuente"],
                "filas_afectadas": h["Filas excluidas"],
                "filas_cargadas": 0,
                "estado": "ok",
            }
        )
    logs_etl.append(
        {
            "regla": "Carga idempotente completa al Data Warehouse",
            "fuente": "DW",
            "filas_afectadas": len(fact_permanencia) + len(fact_conectividad),
            "filas_cargadas": len(fact_permanencia) + len(fact_conectividad),
            "estado": "ok",
        }
    )

    duracion_ms = int((time.perf_counter() - inicio) * 1000)
    reporte_etl = {
        "lote": lote,
        "extract": {
            "T1": int(len(raw1)),
            "T2": int(len(raw2)),
            "T3": int(len(raw3)),
            "T4": int(len(raw4)),
            "T5": int(len(raw5)),
        },
        "log_limpiar_tablas": {k: int(v) for k, v in log_limpieza.items()},
        "reglas_comparativas": reglas_comparativas,
        "ejemplos_reglas": ejemplos_reglas,
        "huerfanas": huerfanas_reporte,
        "anios_dw": sorted(anios_set),
    }

    # 4. LOAD MASIVO AL DW
    progreso(0.80, "Load: cargando dimensiones y hechos al Data Warehouse en Neon...")
    conteos_dw = cargar_dw_masivo(
        datos_dw=datos_dw,
        logs_etl=logs_etl,
        reporte_etl=reporte_etl,
        lote=lote,
        duracion_ms=duracion_ms,
    )
    reporte_etl["conteos_dw"] = conteos_dw
    duracion_total_ms = int((time.perf_counter() - inicio) * 1000)
    progreso(1.0, f"ETL completado (lote {lote}) en {duracion_total_ms:,} ms.")

    return {
        "lote": lote,
        "duracion_ms": duracion_total_ms,
        "conteos_dw": conteos_dw,
        "reporte": reporte_etl,
    }
