#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modelo_aula_conectada.py  -  Capa de Inteligencia Artificial de Aula Conectada Rural
====================================================================================

Lee las 5 fuentes simuladas (ACR_T1 ... ACR_T5), aplica el ETL mínimo del Anexo 3 y
genera las salidas de los 4 módulos de IA descritos en el INF:

  Módulo 1  Riesgo de deserción por institución (clasificación) y proyección de
            desertores de los dos años siguientes por escenarios (base, optimista, pesimista).
  Módulo 2  Proyección de inversión (S/) en equipos y conectividad por local escolar.
  Módulo 3  Priorización de recursos (ranking costo-efectividad con tope presupuestal).
  Módulo 4  Detección de anomalías en la telemetría IoT y en los tiempos de acceso.

Diseño (coherente con el INF):
  * Variables del año t  ->  riesgo del año t+1. NO se usa la tasa del mismo año que
    se predice (evita fuga de información); sí se usa la tasa histórica hasta t.
  * Se entrena con agregados por institución. No usa datos personales ni atributos
    protegidos (por ejemplo, la lengua predominante queda fuera del modelo).
  * Es apoyo a la decisión: ordena prioridades, no decide por la UGEL.

GENERALIDAD: el script NO tiene años ni conteos fijos. Funciona con cualquier juego de 5 CSV que
conserve los mismos nombres de columna (por ejemplo, los datos que entregue el docente): el año base
es el último año de T1, se entrena con todos los pares de años consecutivos y se predice el año base + 1.
Las columnas de salida usan nombres genéricos (matricula_base, tasa_esperada_siguiente, *_t1, *_t2)
para que las tablas ia_* no cambien de esquema cuando cambian los años.

AVISO: las fuentes son SIMULADAS. Las métricas que imprime el script sirven para
comprobar que la tubería funciona; no son resultados de investigación ni deben
citarse como desempeño real del modelo.

Uso 1: línea de comandos
------------------------
    pip install pandas numpy scikit-learn
    python modelo_aula_conectada.py --datos ./datos_simulados --salida ./salidas_ia

Uso 2: como librería dentro de la aplicación web (Streamlit u otra app Python)
-----------------------------------------------------------------------------
    from modelo_aula_conectada import limpiar_tablas, ejecutar_ia, ErrorIA

    t1, t2, t3, t4, t5, log = limpiar_tablas(raw1, raw2, raw3, raw4, raw5)   # ETL "Transform"
    resultado = ejecutar_ia(t1, t2, t3, t4, t5,
                            config={"presupuesto_soles": 300000},          # opcional: sobrescribe supuestos
                            progreso=lambda pct, msg: barra.progress(pct, msg))
    resultado["pred_riesgo_ie"]        # DataFrame -> tabla ia_pred_riesgo_ie
    resultado["metricas"]              # dict con AUC, MAE, año base, etc.

`ejecutar_ia` NO modifica estado global (es seguro con varias sesiones simultáneas) y lanza `ErrorIA`
con un mensaje claro si los datos no alcanzan (por ejemplo, menos de 2 años consecutivos en T1).

Parámetros opcionales: --umbral 3.12  --presupuesto 250000  --semilla 42

Sobre el umbral de "riesgo alto": con el promedio nacional (2.08 %) el 61 % de las instituciones
de esta muestra ya queda en riesgo alto (son regiones de alta deserción) y el semáforo deja de
discriminar. Por eso se define como 1.5 veces el promedio nacional (3.12 %). Con datos reales
puede bajarse a 2.08 con --umbral 2.08.
Los supuestos (umbrales, costos, efectos) están en CONFIG y son editables.
"""
import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest, RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import (accuracy_score, f1_score, mean_absolute_error, precision_score,
                             r2_score, recall_score, roc_auc_score)
from sklearn.model_selection import GroupKFold


class ErrorIA(ValueError):
    """Error controlado: los datos no permiten ejecutar los modelos (mensaje apto para mostrar al usuario)."""

# --------------------------------------------------------------------------------------
# SUPUESTOS EDITABLES (los mismos que se declaran como "supuestos del equipo" en el INF)
# --------------------------------------------------------------------------------------
CONFIG = {
    "promedio_nacional_pct": 2.08,    # tasa de deserción nacional 2023-2024 (MINEDU)
    "multiplo_riesgo_alto": 1.5,      # riesgo alto = tasa >= 1.5 x promedio nacional (3.12 %)
    "semaforo_rojo": 0.60,            # probabilidad >= 0.60  -> Rojo
    "semaforo_amarillo": 0.35,        # 0.35 a 0.59           -> Amarillo; < 0.35 -> Verde
    "alumnos_por_pc_meta": 20,        # meta del KPI "alumnos por PC operativa"
    "costos_soles": {                 # costos unitarios REFERENCIALES (ajustar)
        "reparar": 250,
        "reponer": 2200,
        "dotar": 2500,
        "kit_conectividad": 6000,
    },
    "presupuesto_soles": 250000,      # tope de inversión para el ranking y el escenario optimista
    "baja_anual_base": 0.05,          # equipos que dejan de ser operativos por año (base)
    "baja_anual_extra_obsolescencia": 0.15,  # se suma proporcional al % de equipos con más de 3 años
    "efecto_intervencion": 0.20,      # reducción de la tasa en instituciones intervenidas (optimista)
    "shock_pesimista": 0.10,          # aumento de la tasa si empeoran conectividad/clima
    "contaminacion_anomalias": 0.03,  # proporción esperada de lecturas IoT anómalas
}

VELOCIDAD_KMH = {"Carretera asfaltada": 55, "Carretera afirmada": 32, "Trocha carrozable": 18,
                 "Fluvial": 13, "Camino de herradura": 3.5, "Lacustre": 14}

NOMBRES_FACTORES = {
    "tasa_t": "deserción del año base alta", "tasa_hist_prom": "historial de deserción alto",
    "matricula_inicial": "matrícula pequeña/variable", "var_matricula": "caída de matrícula",
    "asistencia_prom_pct": "asistencia baja", "pct_docentes_titulados": "pocos docentes titulados",
    "gradiente_num": "mayor aislamiento (Rural 3)", "distancia_km": "lejanía de la capital distrital",
    "tiempo_acceso_h": "largo tiempo de acceso", "altitud_msnm": "altitud",
    "alumnos_por_pc_op": "muchos alumnos por PC operativa", "pct_operativos": "pocos equipos operativos",
    "pct_mas_3_anios": "equipos obsoletos", "cpu_por_100_alumnos": "pocos equipos por alumno",
    "tiene_internet": "local sin Internet", "n_equipos_red": "pocos equipos de red",
    "tiene_ups": "sin UPS/estabilizador", "tiene_pozo": "sin pozo a tierra",
    "energia_red": "sin red eléctrica pública", "energia_sin": "sin energía eléctrica",
    "uptime_prom": "baja disponibilidad de Internet", "uptime_min": "caídas fuertes de Internet",
    "cortes_prom": "muchas horas de corte de energía", "latencia_prom": "alta latencia",
    "ancho_banda_prom": "poco ancho de banda", "meses_sin_senal": "meses sin señal",
    "precip_anual": "lluvias intensas", "dias_sin_clases": "días sin clases por clima",
    "temp_min_anual": "temperaturas mínimas extremas", "pobreza_pct": "pobreza distrital alta",
    "hogares_internet_pct": "pocos hogares con Internet", "cobertura_num": "baja cobertura móvil",
    "nivel_sec": "nivel secundaria",
}


# --------------------------------------------------------------------------------------
# 1. CARGA Y ETL MÍNIMO (replica las reglas del Anexo 3 del INF)
# --------------------------------------------------------------------------------------
def num(s):
    return pd.to_numeric(s, errors="coerce")


def leer_csv(carpeta: Path):
    """Lee los 5 CSV como texto (así se conservan los ceros iniciales)."""
    return (pd.read_csv(carpeta / "ACR_T1_siagie_matricula_desercion.csv", dtype=str),
            pd.read_csv(carpeta / "ACR_T2_geo_padron_ie_rural.csv", dtype=str),
            pd.read_csv(carpeta / "ACR_T3_iot_conectividad_aula.csv", dtype=str),
            pd.read_csv(carpeta / "ACR_T4_censo_equipamiento_local.csv", dtype=str),
            pd.read_csv(carpeta / "ACR_T5_api_clima_socioeconomico.csv", dtype=str))


def cargar_y_limpiar(carpeta: Path):
    return limpiar_tablas(*leer_csv(carpeta))


def limpiar_tablas(t1, t2, t3, t4, t5):
    """ETL 'Transform': recibe las 5 fuentes crudas (todo texto) y devuelve las 5 tablas limpias + un log de reglas.
    No modifica los DataFrames recibidos."""
    t1, t2, t3, t4, t5 = (t.copy() for t in (t1, t2, t3, t4, t5))
    log = {}

    # ---- T1: ubigeo a 6 dígitos, deduplicar, textos de relleno a nulo, recalcular tasa
    t1["ubigeo"] = t1["ubigeo"].str.zfill(6)
    n0 = len(t1)
    t1 = t1.drop_duplicates(["cod_modular", "anio"], keep="first").copy()
    log["T1_duplicados_eliminados"] = n0 - len(t1)
    for c in ["anio", "matricula_inicial", "matricula_final", "retirados", "desertores",
              "asistencia_prom_pct", "docentes_total", "docentes_titulados"]:
        t1[c] = num(t1[c])
    t1["tasa"] = t1["desertores"] / t1["matricula_inicial"] * 100
    t1["pct_docentes_titulados"] = t1["docentes_titulados"] / t1["docentes_total"].replace(0, np.nan)

    # ---- T2: numéricos, "No registra" a nulo, completar tiempo de acceso y altitud
    for c in ["latitud", "longitud", "altitud_msnm", "distancia_capital_distrital_km", "tiempo_acceso_horas"]:
        t2[c] = num(t2[c])
    t2.loc[t2["lengua_predominante"] == "No registra", "lengua_predominante"] = np.nan
    vel = t2["tipo_acceso"].map(VELOCIDAD_KMH)
    est = t2["distancia_capital_distrital_km"] / vel + 0.25
    log["T2_tiempo_acceso_estimado"] = int(t2["tiempo_acceso_horas"].isna().sum())
    t2["tiempo_acceso_horas"] = t2["tiempo_acceso_horas"].fillna(est)
    log["T2_altitud_completada"] = int(t2["altitud_msnm"].isna().sum())
    t2["altitud_msnm"] = t2["altitud_msnm"].fillna(t2.groupby("distrito")["altitud_msnm"].transform("mean"))
    t2["gradiente_num"] = t2["gradiente_ruralidad"].str.extract(r"(\d)").astype(float)

    # ---- T3: lecturas inválidas a nulo, disponibilidad truncada a 100
    for c in ["ancho_banda_prom_mbps", "latencia_prom_ms", "disponibilidad_internet_pct", "horas_corte_energia",
              "voltaje_prom_v", "carga_bateria_solar_pct", "temperatura_aula_c", "dispositivos_conectados_prom"]:
        t3[c] = num(t3[c])
    log["T3_latencia_invalida"] = int((t3["latencia_prom_ms"] < 0).sum())
    t3.loc[t3["latencia_prom_ms"] < 0, "latencia_prom_ms"] = np.nan
    log["T3_disponibilidad_truncada"] = int((t3["disponibilidad_internet_pct"] > 100).sum())
    t3["disponibilidad_internet_pct"] = t3["disponibilidad_internet_pct"].clip(upper=100)

    # ---- T4: ubigeo, operativos <= total, "No registra" a nulo
    t4["ubigeo"] = t4["ubigeo"].str.zfill(6)
    for c in ["anio_censo", "n_ie_en_local", "cpu_total", "cpu_operativos", "cpu_en_reparacion", "cpu_mas_3_anios",
              "laptops_xo", "tabletas", "n_equipos_red"]:
        t4[c] = num(t4[c])
    log["T4_operativos_mayor_total"] = int((t4["cpu_operativos"] > t4["cpu_total"]).sum())
    t4["cpu_operativos"] = np.minimum(t4["cpu_operativos"], t4["cpu_total"])
    t4.loc[t4["tipo_internet"] == "No registra", "tipo_internet"] = np.nan

    # ---- T5: numéricos, "N/D" -> "Sin dato", precipitación vacía = mediana del distrito
    for c in ["anio", "mes", "precipitacion_mm", "temp_media_c", "temp_min_c", "dias_sin_clases_clima",
              "pobreza_monetaria_pct", "hogares_con_internet_pct"]:
        t5[c] = num(t5[c])
    t5.loc[t5["evento_climatico"] == "N/D", "evento_climatico"] = "Sin dato"
    log["T5_precipitacion_imputada"] = int(t5["precipitacion_mm"].isna().sum())
    t5["precipitacion_mm"] = t5["precipitacion_mm"].fillna(t5.groupby("ubigeo")["precipitacion_mm"].transform("median"))
    return t1, t2, t3, t4, t5, log


# --------------------------------------------------------------------------------------
# 2. CONSTRUCCIÓN DE VARIABLES (una fila por institución y año base t)
# --------------------------------------------------------------------------------------
def agregados_estructurales(t2, t3, t5):
    """Variables que solo existen para el último año (IoT, clima): se asumen estables (limitación del prototipo)."""
    t3 = t3[t3["mes"].str[:4] == t3["mes"].str[:4].max()]   # último año de telemetría
    g = t3.groupby("cod_modular")
    iot = pd.DataFrame({
        "uptime_prom": g["disponibilidad_internet_pct"].mean(),
        "uptime_min": g["disponibilidad_internet_pct"].min(),
        "cortes_prom": g["horas_corte_energia"].mean(),
        "latencia_prom": g["latencia_prom_ms"].mean(),
        "ancho_banda_prom": g["ancho_banda_prom_mbps"].mean(),
        "meses_sin_senal": g["estado_sensor"].apply(lambda s: (s == "Sin señal").sum()),
    }).reset_index()
    iot["tiene_sensor"] = 1

    cob = {"Sin cobertura": 0, "2G/3G": 1, "4G": 2}
    if "precipitacion_mm" in t5.columns:          # T5 mensual (formato del CSV)
        t5 = t5[t5["anio"] == t5["anio"].max()]    # último año de clima
        g5 = t5.groupby("ubigeo")
        clima = pd.DataFrame({
            "precip_anual": g5["precipitacion_mm"].sum(),
            "dias_sin_clases": g5["dias_sin_clases_clima"].sum(),
            "temp_min_anual": g5["temp_min_c"].min(),
            "pobreza_pct": g5["pobreza_monetaria_pct"].first(),
            "hogares_internet_pct": g5["hogares_con_internet_pct"].first(),
            "cobertura_num": g5["cobertura_movil"].first().map(cob),
        }).reset_index()
    else:                                          # T5 ya agregado por distrito (p. ej. desde el DW): 1 fila por ubigeo
        clima = t5[["ubigeo", "precip_anual", "dias_sin_clases", "temp_min_anual", "pobreza_pct",
                    "hogares_internet_pct", "cobertura_movil"]].copy()
        clima["cobertura_num"] = clima["cobertura_movil"].map(cob)
        clima = clima.drop(columns="cobertura_movil")

    est = t2[["cod_modular", "cod_local", "ubigeo", "departamento", "distrito", "ugel", "nivel", "gradiente_num",
              "distancia_capital_distrital_km", "tiempo_acceso_horas", "altitud_msnm", "tipo_acceso"]].copy()
    est = est.rename(columns={"distancia_capital_distrital_km": "distancia_km", "tiempo_acceso_horas": "tiempo_acceso_h"})
    est["nivel_sec"] = (est["nivel"] == "Secundaria").astype(int)
    est = est.merge(iot, on="cod_modular", how="left").merge(clima, on="ubigeo", how="left")
    est["tiene_sensor"] = est["tiene_sensor"].fillna(0)
    return est


def features_anio(t, t1, t4, est):
    """Variables del año base t. Usa solo información disponible hasta t."""
    a = t1[t1["anio"] == t][["cod_modular", "matricula_inicial", "tasa", "asistencia_prom_pct",
                             "pct_docentes_titulados", "desertores"]].rename(columns={"tasa": "tasa_t"})
    hist = t1[t1["anio"] <= t].groupby("cod_modular")["tasa"].mean().rename("tasa_hist_prom")
    prev = t1[t1["anio"] == t - 1][["cod_modular", "matricula_inicial"]].rename(columns={"matricula_inicial": "mat_prev"})
    df = a.merge(hist, on="cod_modular", how="left").merge(prev, on="cod_modular", how="left")
    df["var_matricula"] = (df["matricula_inicial"] / df["mat_prev"] - 1).fillna(0)
    df = df.merge(est, on="cod_modular", how="left")

    # equipamiento del local (censo más cercano disponible: 2024 o 2025)
    cands = sorted(t4["anio_censo"].dropna().unique())
    previos = [y for y in cands if y <= t]
    anio_censo = previos[-1] if previos else cands[0]   # censo más reciente hasta t (o el primero disponible)
    loc = t4[t4["anio_censo"] == anio_censo].copy()
    mat_local = t1[t1["anio"] == t].merge(est[["cod_modular", "cod_local"]], on="cod_modular") \
        .groupby("cod_local")["matricula_inicial"].sum().rename("matricula_local")
    loc = loc.merge(mat_local, on="cod_local", how="left")
    ok = loc["declaro_computo"] == "Sí"
    loc["alumnos_por_pc_op"] = np.where(ok, loc["matricula_local"] / loc["cpu_operativos"].replace(0, np.nan), np.nan)
    loc["alumnos_por_pc_op"] = loc["alumnos_por_pc_op"].clip(upper=100)
    loc.loc[ok & (loc["cpu_operativos"] == 0), "alumnos_por_pc_op"] = 100
    loc["pct_operativos"] = np.where(ok, loc["cpu_operativos"] / loc["cpu_total"].replace(0, np.nan), np.nan)
    loc["pct_mas_3_anios"] = np.where(ok, loc["cpu_mas_3_anios"] / loc["cpu_total"].replace(0, np.nan), np.nan)
    loc["cpu_por_100_alumnos"] = np.where(ok, loc["cpu_total"] / loc["matricula_local"] * 100, np.nan)
    loc["tiene_internet"] = (loc["tiene_internet"] == "Sí").astype(int)
    loc["tiene_ups"] = (loc["tiene_ups_estabilizador"] == "Sí").astype(int)
    loc["tiene_pozo"] = (loc["tiene_pozo_tierra"] == "Sí").astype(int)
    loc["energia_red"] = (loc["fuente_energia"] == "Red pública").astype(int)
    loc["energia_sin"] = (loc["fuente_energia"] == "Sin energía").astype(int)
    cols = ["cod_local", "alumnos_por_pc_op", "pct_operativos", "pct_mas_3_anios", "cpu_por_100_alumnos",
            "tiene_internet", "n_equipos_red", "tiene_ups", "tiene_pozo", "energia_red", "energia_sin"]
    df = df.merge(loc[cols], on="cod_local", how="left")
    df["anio_base"] = t
    return df


FEATURES = ["tasa_t", "tasa_hist_prom", "matricula_inicial", "var_matricula", "asistencia_prom_pct",
            "pct_docentes_titulados", "gradiente_num", "distancia_km", "tiempo_acceso_h", "altitud_msnm", "nivel_sec",
            "alumnos_por_pc_op", "pct_operativos", "pct_mas_3_anios", "cpu_por_100_alumnos", "tiene_internet",
            "n_equipos_red", "tiene_ups", "tiene_pozo", "energia_red", "energia_sin",
            "uptime_prom", "uptime_min", "cortes_prom", "latencia_prom", "ancho_banda_prom", "meses_sin_senal",
            "tiene_sensor", "precip_anual", "dias_sin_clases", "temp_min_anual", "pobreza_pct",
            "hogares_internet_pct", "cobertura_num"]


# --------------------------------------------------------------------------------------
# 3. MÓDULO 1: riesgo de deserción (clasificación) + tasa esperada (regresión)
# --------------------------------------------------------------------------------------
def semaforo(p, cfg):
    return np.where(p >= cfg["semaforo_rojo"], "Rojo", np.where(p >= cfg["semaforo_amarillo"], "Amarillo", "Verde"))


def modulo1_riesgo(t1, t4, est, semilla, umbral, cfg):
    anios = sorted(int(a) for a in t1["anio"].dropna().unique())
    pares = [a for a in anios[:-1] if a + 1 in anios]       # años t con t+1 disponible
    if not pares:
        raise ErrorIA("T1 debe tener al menos dos años consecutivos para entrenar el modelo.")
    anio_base = anios[-1]
    tr = []
    for t in pares:  # variables de t -> resultado de t+1
        f = features_anio(t, t1, t4, est)
        y = t1[t1["anio"] == t + 1][["cod_modular", "tasa"]].rename(columns={"tasa": "tasa_siguiente"})
        tr.append(f.merge(y, on="cod_modular", how="inner"))
    train = pd.concat(tr, ignore_index=True)
    train["y"] = (train["tasa_siguiente"] >= umbral).astype(int)
    medianas = train[FEATURES].median().fillna(0)
    X = train[FEATURES].fillna(medianas)
    w = np.sqrt(train["matricula_inicial"].clip(lower=1))
    grupos = train["cod_modular"]

    if train["y"].nunique() < 2:
        raise ErrorIA("Con este umbral todas las instituciones quedan en la misma clase; ajusta --umbral.")
    oof_p = np.zeros(len(train)); oof_r = np.zeros(len(train))
    n_splits = max(2, min(5, grupos.nunique()))
    for tr_i, te_i in GroupKFold(n_splits=n_splits).split(X, train["y"], grupos):
        clf = RandomForestClassifier(n_estimators=300, min_samples_leaf=5, random_state=semilla, n_jobs=-1)
        clf.fit(X.iloc[tr_i], train["y"].iloc[tr_i], sample_weight=w.iloc[tr_i])
        oof_p[te_i] = clf.predict_proba(X.iloc[te_i])[:, 1]
        reg = RandomForestRegressor(n_estimators=300, min_samples_leaf=5, random_state=semilla, n_jobs=-1)
        reg.fit(X.iloc[tr_i], train["tasa_siguiente"].iloc[tr_i], sample_weight=w.iloc[tr_i])
        oof_r[te_i] = reg.predict(X.iloc[te_i])

    pred_clase = (oof_p >= 0.5).astype(int)
    metricas = {
        "aviso": "Datos simulados: las métricas solo verifican la tubería, no son desempeño real.",
        "filas_entrenamiento": int(len(train)), "umbral_riesgo_pct": umbral,
        "prevalencia_riesgo_alto": round(float(train["y"].mean()), 3),
        "validacion": f"GroupKFold({n_splits}) por institución (una institución nunca está en entrenamiento y prueba a la vez)",
        "anios_entrenamiento": pares, "anio_base": anio_base,
        "clasificacion": {
            "auc_modelo": round(float(roc_auc_score(train["y"], oof_p)), 3),
            "auc_linea_base_persistencia": round(float(roc_auc_score(train["y"], train["tasa_t"].fillna(0))), 3),
            "exactitud": round(float(accuracy_score(train["y"], pred_clase)), 3),
            "precision": round(float(precision_score(train["y"], pred_clase)), 3),
            "sensibilidad": round(float(recall_score(train["y"], pred_clase)), 3),
            "f1": round(float(f1_score(train["y"], pred_clase)), 3)},
        "regresion_tasa": {
            "mae_modelo_pp": round(float(mean_absolute_error(train["tasa_siguiente"], oof_r)), 3),
            "mae_linea_base_persistencia_pp": round(float(mean_absolute_error(train["tasa_siguiente"], train["tasa_t"].fillna(0))), 3),
            "r2_modelo": round(float(r2_score(train["tasa_siguiente"], oof_r)), 3)},
    }

    # ajuste final con todo el entrenamiento
    clf = RandomForestClassifier(n_estimators=500, min_samples_leaf=5, random_state=semilla, n_jobs=-1)
    clf.fit(X, train["y"], sample_weight=w)
    reg = RandomForestRegressor(n_estimators=500, min_samples_leaf=5, random_state=semilla, n_jobs=-1)
    reg.fit(X, train["tasa_siguiente"], sample_weight=w)
    imp = pd.DataFrame({"variable": FEATURES, "importancia": clf.feature_importances_}) \
        .sort_values("importancia", ascending=False).reset_index(drop=True)
    imp["descripcion"] = imp["variable"].map(NOMBRES_FACTORES)

    # predicción para el año base -> riesgo del año siguiente
    base = features_anio(anio_base, t1, t4, est)
    Xb = base[FEATURES].fillna(medianas)
    base["prob_riesgo"] = clf.predict_proba(Xb)[:, 1]
    base["tasa_esperada_siguiente"] = np.clip(reg.predict(Xb), 0, 15)
    base["semaforo"] = semaforo(base["prob_riesgo"], cfg)

    # factores principales: variables importantes en las que la institución está en el peor cuartil
    top = imp.head(12)["variable"].tolist()
    signo = {}
    for v in top:
        c = np.corrcoef(Xb[v], base["prob_riesgo"])[0, 1]
        signo[v] = 1 if (np.isnan(c) or c >= 0) else -1
    pesos = imp.set_index("variable")["importancia"]
    textos = []
    for i in range(len(base)):
        cand = []
        for v in top:
            pct = (Xb[v] <= Xb[v].iloc[i]).mean() * 100
            peor = pct if signo[v] == 1 else 100 - pct
            if peor >= 75 and v != "tiene_sensor":
                cand.append((pesos[v] * (peor - 50), NOMBRES_FACTORES.get(v, v)))
        cand.sort(reverse=True)
        textos.append("; ".join(t for _, t in cand[:3]) if cand else "Sin factor destacado")
    base["factores_principales"] = textos
    return base, imp, metricas


# --------------------------------------------------------------------------------------
# 4. MÓDULO 2: inversión por local (equipos y conectividad) y equipos operativos proyectados
# --------------------------------------------------------------------------------------
def proyectar_matricula(t1, anio_base):
    filas = []
    for cod, g in t1.groupby("cod_modular"):
        g = g.sort_values("anio")
        gb = g[g["anio"] == anio_base]
        if gb.empty:
            continue
        if len(g) >= 2:
            pend = np.polyfit(g["anio"], np.log(g["matricula_inicial"].clip(lower=1)), 1)[0]
            tasa = float(np.clip(math.exp(pend) - 1, -0.10, 0.05))
        else:
            tasa = 0.0
        nb = float(gb["matricula_inicial"].iloc[0])
        filas.append({"cod_modular": cod, "mat_base": nb, "mat_t1": nb * (1 + tasa), "mat_t2": nb * (1 + tasa) ** 2})
    return pd.DataFrame(filas)


def modulo2_inversion(t4, est, matp, anio_base, cfg):
    c = cfg["costos_soles"]; meta = cfg["alumnos_por_pc_meta"]
    mloc = matp.merge(est[["cod_modular", "cod_local"]], on="cod_modular").groupby("cod_local")[["mat_base", "mat_t1", "mat_t2"]].sum()
    cands = sorted(t4["anio_censo"].dropna().unique())
    previos = [y for y in cands if y <= anio_base]
    anio_censo = previos[-1] if previos else cands[-1]   # censo más reciente hasta el año base
    loc = t4[t4["anio_censo"] == anio_censo].merge(mloc, on="cod_local", how="left").dropna(subset=["mat_base"])
    ubi = est.drop_duplicates("cod_local")[["cod_local", "departamento", "distrito", "ugel"]]
    loc = loc.merge(ubi, on="cod_local", how="left")
    filas = []
    for _, r in loc.iterrows():
        declara = r["declaro_computo"] == "Sí"
        total = 0 if not declara or pd.isna(r["cpu_total"]) else int(r["cpu_total"])
        oper = 0 if not declara or pd.isna(r["cpu_operativos"]) else int(r["cpu_operativos"])
        rep_disp = 0 if not declara or pd.isna(r["cpu_en_reparacion"]) else int(r["cpu_en_reparacion"])
        old = 0 if not declara or pd.isna(r["cpu_mas_3_anios"]) else int(r["cpu_mas_3_anios"])
        necesarios = math.ceil(r["mat_base"] / meta)
        deficit = max(0, necesarios - oper)
        reparar = min(rep_disp, deficit)
        faltan = deficit - reparar
        reponer = min(faltan, max(0, total - oper - reparar))
        dotar = faltan - reponer
        kit = 1 if r["tiene_internet"] == "No" else 0
        costo = reparar * c["reparar"] + reponer * c["reponer"] + dotar * c["dotar"] + kit * c["kit_conectividad"]
        acciones = [n for n, v in (("Reparar", reparar), ("Reponer", reponer), ("Dotar", dotar), ("Kit de conectividad", kit)) if v > 0]
        pct_old = (old / total) if total > 0 else 0
        baja = min(0.30, cfg["baja_anual_base"] + cfg["baja_anual_extra_obsolescencia"] * pct_old)
        op26 = oper * (1 - baja); op27 = op26 * (1 - baja)
        filas.append({
            "cod_local": r["cod_local"], "ubigeo": r["ubigeo"], "departamento": r["departamento"], "distrito": r["distrito"],
            "ugel": r["ugel"], "anio_base": anio_base, "anio_censo": int(anio_censo),
            "matricula_local_base": int(r["mat_base"]), "cpu_total": total, "cpu_operativos": oper,
            "alumnos_por_pc_operativa": round(r["mat_base"] / oper, 1) if oper > 0 else np.nan,
            "tiene_internet": r["tiene_internet"], "fuente_energia": r["fuente_energia"],
            "equipos_a_reparar": reparar, "equipos_a_reponer": reponer, "equipos_a_dotar": dotar,
            "kit_conectividad": kit, "accion_sugerida": " + ".join(acciones) if acciones else "Sin acción de equipamiento",
            "costo_estimado_soles": int(costo),
            "operativos_proy_t1": round(op26, 1), "operativos_proy_t2": round(op27, 1),
            "alumnos_por_pc_proy_t2": round(r["mat_t2"] / op27, 1) if op27 > 0 else np.nan})
    return pd.DataFrame(filas)


# --------------------------------------------------------------------------------------
# 5. MÓDULO 3: priorización costo-efectividad con tope presupuestal
# --------------------------------------------------------------------------------------
def modulo3_priorizacion(riesgo, inversion, presupuesto):
    r = riesgo.copy()
    r["est_riesgo"] = r["prob_riesgo"] * r["matricula_inicial"]
    orden = {"Verde": 0, "Amarillo": 1, "Rojo": 2}
    peor = r.groupby("cod_local")["semaforo"].apply(lambda s: max(s, key=lambda x: orden[x]))
    loc = r.groupby("cod_local").agg(n_ie=("cod_modular", "count"), estudiantes_en_riesgo=("est_riesgo", "sum")).join(peor.rename("semaforo_max"))
    p = inversion.merge(loc, left_on="cod_local", right_index=True, how="left")
    p = p[p["costo_estimado_soles"] > 0].copy()
    p["estudiantes_en_riesgo"] = p["estudiantes_en_riesgo"].round(1)
    p["riesgo_por_mil_soles"] = (p["estudiantes_en_riesgo"] / p["costo_estimado_soles"] * 1000).round(2)
    p = p.sort_values("riesgo_por_mil_soles", ascending=False).reset_index(drop=True)
    p["ranking"] = p.index + 1
    p["costo_acumulado_soles"] = p["costo_estimado_soles"].cumsum()
    p["dentro_presupuesto"] = p["costo_acumulado_soles"] <= presupuesto
    cols = ["ranking", "cod_local", "ubigeo", "departamento", "distrito", "ugel", "n_ie", "semaforo_max",
            "estudiantes_en_riesgo", "accion_sugerida", "costo_estimado_soles", "riesgo_por_mil_soles",
            "costo_acumulado_soles", "dentro_presupuesto"]
    return p[cols]


# --------------------------------------------------------------------------------------
# 6. PROYECCIÓN DE DESERTORES 2026-2027 (escenarios) por departamento
# --------------------------------------------------------------------------------------
def modulo1_proyeccion(t1, riesgo, matp, priorizacion, anio_base, cfg):
    r = riesgo[["cod_modular", "cod_local", "departamento", "tasa_esperada_siguiente"]].merge(matp, on="cod_modular")
    # tendencia histórica departamental 2023->2025 (variación anual media de la tasa, acotada)
    h = t1.merge(riesgo[["cod_modular", "departamento"]], on="cod_modular")
    sm = h.groupby(["departamento", "anio"])[["desertores", "matricula_inicial"]].sum()
    dep = (sm["desertores"] / sm["matricula_inicial"] * 100).unstack()
    a0, a1 = dep.columns.min(), dep.columns.max()
    if a1 > a0:
        tend = ((dep[a1] / dep[a0].replace(0, np.nan)) ** (1 / (a1 - a0)) - 1).clip(-0.10, 0.10).fillna(0)
    else:
        tend = dep[a1] * 0
    r["tend"] = r["departamento"].map(tend)
    financiados = set(priorizacion.loc[priorizacion["dentro_presupuesto"], "cod_local"])
    r["intervenida"] = r["cod_local"].isin(financiados)
    ef, sh = cfg["efecto_intervencion"], cfg["shock_pesimista"]
    esc = {
        "base": (r["tasa_esperada_siguiente"], r["tasa_esperada_siguiente"] * (1 + r["tend"])),
        "pesimista": (r["tasa_esperada_siguiente"] * (1 + sh), r["tasa_esperada_siguiente"] * (1 + r["tend"]) * (1 + sh)),
        "optimista": (r["tasa_esperada_siguiente"], r["tasa_esperada_siguiente"] * (1 + r["tend"]) * np.where(r["intervenida"], 1 - ef, 1)),
    }
    filas = []
    for (d, a), g in h.groupby(["departamento", "anio"]):
        filas.append({"departamento": d, "anio": int(a), "escenario": "real", "tipo": "historico",
                      "matricula": int(g["matricula_inicial"].sum()), "desertores": int(g["desertores"].sum()),
                      "tasa_pct": round(g["desertores"].sum() / g["matricula_inicial"].sum() * 100, 2)})
    for nombre, (t26, t27) in esc.items():
        for anio, tasa, mat in ((anio_base + 1, t26, r["mat_t1"]), (anio_base + 2, t27, r["mat_t2"])):
            tmp = pd.DataFrame({"departamento": r["departamento"], "mat": mat, "des": mat * tasa / 100})
            for d, g in tmp.groupby("departamento"):
                filas.append({"departamento": d, "anio": anio, "escenario": nombre, "tipo": "proyeccion",
                              "matricula": int(round(g["mat"].sum())), "desertores": int(round(g["des"].sum())),
                              "tasa_pct": round(g["des"].sum() / g["mat"].sum() * 100, 2)})
    out = pd.DataFrame(filas).sort_values(["departamento", "escenario", "anio"]).reset_index(drop=True)
    return out, int(r["intervenida"].sum())


# --------------------------------------------------------------------------------------
# 7. MÓDULO 4: anomalías en telemetría IoT y en tiempos de acceso
# --------------------------------------------------------------------------------------
def _z_robusto(s):
    med = s.median()
    mad = (s - med).abs().median() * 1.4826
    if not mad or np.isnan(mad):
        mad = s.std() if s.std() and not np.isnan(s.std()) else 1.0
    return ((s - med) / mad).clip(-10, 10)


def modulo4_anomalias(t3, t2, semilla, cfg):
    """Anomalía = desviación respecto de la línea base del propio sensor (no un mal servicio crónico)."""
    d = t3.sort_values(["id_sensor", "mes"]).reset_index(drop=True).copy()
    cols = ["ancho_banda_prom_mbps", "latencia_prom_ms", "disponibilidad_internet_pct", "horas_corte_energia",
            "voltaje_prom_v", "dispositivos_conectados_prom"]
    z = d.groupby("id_sensor")[cols].transform(_z_robusto).fillna(0)
    iso = IsolationForest(n_estimators=300, contamination=cfg["contaminacion_anomalias"], random_state=semilla)
    iso.fit(z)
    d["score_anomalia"] = (-iso.score_samples(z)).round(4)
    d["anomalia_modelo"] = iso.predict(z) == -1

    med_disp = d.groupby("id_sensor")["disponibilidad_internet_pct"].transform("median")
    med_corte = d.groupby("id_sensor")["horas_corte_energia"].transform("median")
    motivo = pd.Series([""] * len(d), index=d.index)

    def marca(mask, texto):
        nonlocal motivo
        motivo = np.where(mask, np.where(motivo == "", texto, motivo + "; " + texto), motivo)
        motivo = pd.Series(motivo, index=d.index)

    marca(d["disponibilidad_internet_pct"].isna(), "Sensor sin reporte de red")
    marca(d["disponibilidad_internet_pct"] < med_disp - 15, "Caída de disponibilidad (>15 puntos bajo su línea base)")
    marca(d["horas_corte_energia"] > 2 * med_corte + 20, "Corte de energía atípico para el sensor")
    marca((d["voltaje_prom_v"] < 190) | (d["voltaje_prom_v"] > 240), "Voltaje fuera de rango")
    bajo = (d["disponibilidad_internet_pct"] < med_disp - 10).astype(int)
    sost = bajo.groupby(d["id_sensor"]).transform(lambda x: x.rolling(3).sum() == 3).fillna(False).astype(bool)
    marca(sost, "Caída sostenida (3 meses seguidos bajo su línea base)")
    d["motivo"] = motivo
    d["es_anomalia"] = d["anomalia_modelo"] | (d["motivo"] != "")
    d.loc[d["anomalia_modelo"] & (d["motivo"] == ""), "motivo"] = "Patrón atípico (modelo)"
    iot = d[d["es_anomalia"]][["id_lectura", "id_sensor", "cod_modular", "mes", "tipo_enlace", "disponibilidad_internet_pct",
                               "horas_corte_energia", "voltaje_prom_v", "score_anomalia", "motivo"]] \
        .sort_values("score_anomalia", ascending=False)

    # alerta crónica (no es anomalía): sensores con servicio deficiente de forma persistente
    g = d.groupby(["id_sensor", "cod_modular", "tipo_enlace"]).agg(
        disponibilidad_mediana_pct=("disponibilidad_internet_pct", "median"),
        horas_corte_mediana=("horas_corte_energia", "median")).reset_index()
    cron = g[(g["disponibilidad_mediana_pct"] < 60) | (g["horas_corte_mediana"] > 100)].copy()
    cron["alerta"] = np.where(cron["disponibilidad_mediana_pct"] < 60, "Disponibilidad mediana < 60 %", "") \
        + np.where((cron["disponibilidad_mediana_pct"] < 60) & (cron["horas_corte_mediana"] > 100), "; ", "") \
        + np.where(cron["horas_corte_mediana"] > 100, "Cortes de energía > 100 h/mes", "")
    cron = cron.sort_values("disponibilidad_mediana_pct")

    a = t2[["cod_modular", "distrito", "tipo_acceso", "distancia_capital_distrital_km", "tiempo_acceso_horas"]].copy()
    a["horas_por_km"] = a["tiempo_acceso_horas"] / a["distancia_capital_distrital_km"].clip(lower=0.5)
    lg = np.log(a["horas_por_km"].clip(lower=1e-3))
    a["z_tiempo"] = ((lg - lg.groupby(a["tipo_acceso"]).transform("mean")) / lg.groupby(a["tipo_acceso"]).transform("std")).round(2)
    acc = a[a["z_tiempo"].abs() > 2.5].sort_values("z_tiempo", key=abs, ascending=False)
    acc = acc.assign(motivo=np.where(acc["z_tiempo"] > 0, "Tiempo de acceso mayor al esperado para su distancia y tipo de acceso",
                                      "Tiempo de acceso menor al esperado para su distancia y tipo de acceso"))
    return iot, cron, acc[["cod_modular", "distrito", "tipo_acceso", "distancia_capital_distrital_km", "tiempo_acceso_horas", "z_tiempo", "motivo"]]


# --------------------------------------------------------------------------------------
# API PARA LA APLICACIÓN: ejecutar_ia
# --------------------------------------------------------------------------------------
def _asegurar_tipos(t1, t2, t3, t4, t5):
    """Convierte a número y deriva columnas calculadas. Es idempotente: sirve igual si las tablas vienen de
    limpiar_tablas() o de vistas del Data Warehouse con los mismos nombres de columna."""
    t1, t2, t3, t4, t5 = (t.copy() for t in (t1, t2, t3, t4, t5))
    for c in ["anio", "matricula_inicial", "matricula_final", "retirados", "desertores", "asistencia_prom_pct",
              "docentes_total", "docentes_titulados"]:
        t1[c] = num(t1[c])
    t1["tasa"] = t1["desertores"] / t1["matricula_inicial"] * 100
    t1["pct_docentes_titulados"] = t1["docentes_titulados"] / t1["docentes_total"].replace(0, np.nan)
    for c in ["latitud", "longitud", "altitud_msnm", "distancia_capital_distrital_km", "tiempo_acceso_horas"]:
        if c in t2.columns:
            t2[c] = num(t2[c])
    if "gradiente_num" not in t2.columns:
        t2["gradiente_num"] = t2["gradiente_ruralidad"].astype(str).str.extract(r"(\d)")[0].astype(float)
    for c in ["ancho_banda_prom_mbps", "latencia_prom_ms", "disponibilidad_internet_pct", "horas_corte_energia",
              "voltaje_prom_v", "carga_bateria_solar_pct", "temperatura_aula_c", "dispositivos_conectados_prom"]:
        t3[c] = num(t3[c])
    for c in ["anio_censo", "cpu_total", "cpu_operativos", "cpu_en_reparacion", "cpu_mas_3_anios", "n_equipos_red"]:
        t4[c] = num(t4[c])
    for c in t5.columns:
        if c not in ("ubigeo", "evento_climatico", "cobertura_movil"):
            t5[c] = num(t5[c])
    return t1, t2, t3, t4, t5


def ejecutar_ia(t1, t2, t3, t4, t5, config=None, umbral=None, presupuesto=None, semilla=42, progreso=None):
    """Ejecuta los 4 módulos de IA con las 5 tablas LIMPIAS y devuelve un diccionario de DataFrames.

    config      dict opcional que sobrescribe supuestos de CONFIG (costos, semáforo, efectos, presupuesto...).
    umbral      tasa (%) que define riesgo alto; por defecto promedio_nacional_pct x multiplo_riesgo_alto.
    presupuesto tope de inversión en soles (atajo de config["presupuesto_soles"]).
    progreso    función opcional progreso(pct: int, mensaje: str) para barras de progreso.
    Lanza ErrorIA si los datos no permiten calcular.
    """
    cfg = {**CONFIG, **(config or {})}
    cfg["costos_soles"] = {**CONFIG["costos_soles"], **(cfg.get("costos_soles") or {})}
    if presupuesto is not None:
        cfg["presupuesto_soles"] = presupuesto
    if umbral is None:
        umbral = cfg["promedio_nacional_pct"] * cfg["multiplo_riesgo_alto"]
    p = progreso or (lambda pct, msg: None)

    p(5, "Preparando datos...")
    t1, t2, t3, t4, t5 = _asegurar_tipos(t1, t2, t3, t4, t5)
    est = agregados_estructurales(t2, t3, t5)

    p(15, "Módulo 1: entrenando el modelo de riesgo de deserción...")
    riesgo, imp, metricas = modulo1_riesgo(t1, t4, est, semilla, umbral, cfg)
    anio_base = int(metricas["anio_base"])
    matp = proyectar_matricula(t1, anio_base)

    p(60, "Módulo 2: proyectando la inversión por local...")
    inv = modulo2_inversion(t4, est, matp, anio_base, cfg)

    p(70, "Módulo 3: priorizando con el tope presupuestal...")
    prio = modulo3_priorizacion(riesgo, inv, cfg["presupuesto_soles"])

    p(80, "Proyectando desertores por escenarios...")
    proy, n_interv = modulo1_proyeccion(t1, riesgo, matp, prio, anio_base, cfg)

    p(90, "Módulo 4: detectando anomalías...")
    an_iot, an_cron, an_acc = modulo4_anomalias(t3, t2, semilla, cfg)

    sal = riesgo[["cod_modular", "cod_local", "ubigeo", "departamento", "distrito", "ugel", "nivel", "anio_base",
                  "matricula_inicial", "tasa_t", "prob_riesgo", "semaforo", "tasa_esperada_siguiente", "factores_principales"]].copy()
    sal = sal.rename(columns={"matricula_inicial": "matricula_base", "tasa_t": "tasa_desercion_base_pct"})
    sal["anio_prediccion"] = anio_base + 1
    sal["prob_riesgo"] = sal["prob_riesgo"].round(3)
    sal["tasa_desercion_base_pct"] = sal["tasa_desercion_base_pct"].round(2)
    sal["tasa_esperada_siguiente"] = sal["tasa_esperada_siguiente"].round(2)
    sal = sal.sort_values("prob_riesgo", ascending=False).reset_index(drop=True)

    resumen = {"metricas": metricas, "supuestos": cfg,
               "semaforo_instituciones": sal["semaforo"].value_counts().to_dict(),
               "locales_con_accion": int((inv["costo_estimado_soles"] > 0).sum()),
               "costo_total_estimado_soles": int(inv["costo_estimado_soles"].sum()),
               "locales_financiados_en_presupuesto": int(prio["dentro_presupuesto"].sum()),
               "instituciones_en_locales_financiados": int(n_interv),
               "lecturas_iot_anomalas": int(len(an_iot)), "sensores_con_alerta_cronica": int(len(an_cron)),
               "anomalias_acceso": int(len(an_acc))}
    p(100, "Listo")
    return {"pred_riesgo_ie": sal, "proyeccion_desertores": proy, "inversion_local": inv, "priorizacion": prio,
            "anomalias_iot": an_iot, "alertas_cronicas_iot": an_cron, "anomalias_acceso": an_acc,
            "importancia_variables": imp.round(4), "metricas": resumen}


# --------------------------------------------------------------------------------------
# PRINCIPAL (línea de comandos)
# --------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="IA de Aula Conectada Rural")
    ap.add_argument("--datos", default="./datos_simulados", help="carpeta con los 5 CSV")
    ap.add_argument("--salida", default="./salidas_ia", help="carpeta de resultados")
    ap.add_argument("--umbral", type=float, default=CONFIG["promedio_nacional_pct"] * CONFIG["multiplo_riesgo_alto"],
                    help="tasa (%%) que define riesgo alto (por defecto 1.5 x promedio nacional = 3.12)")
    ap.add_argument("--presupuesto", type=float, default=CONFIG["presupuesto_soles"], help="tope de inversión (S/)")
    ap.add_argument("--semilla", type=int, default=42)
    a = ap.parse_args()
    out = Path(a.salida); out.mkdir(parents=True, exist_ok=True)

    print("1) Cargando y limpiando fuentes (ETL mínimo)...")
    t1, t2, t3, t4, t5, log = cargar_y_limpiar(Path(a.datos))
    try:
        r = ejecutar_ia(t1, t2, t3, t4, t5, umbral=a.umbral, presupuesto=a.presupuesto, semilla=a.semilla,
                        progreso=lambda pct, msg: print(f"[{pct:3d} %] {msg}"))
    except ErrorIA as e:
        raise SystemExit(f"No se pudo ejecutar: {e}")

    nombres = {"pred_riesgo_ie": "pred_riesgo_ie.csv", "importancia_variables": "importancia_variables.csv",
               "proyeccion_desertores": "proyeccion_desertores.csv", "inversion_local": "inversion_local.csv",
               "priorizacion": "priorizacion_intervencion.csv", "anomalias_iot": "anomalias_iot.csv",
               "alertas_cronicas_iot": "alertas_cronicas_iot.csv", "anomalias_acceso": "anomalias_acceso_ie.csv"}
    for clave, archivo in nombres.items():
        r[clave].to_csv(out / archivo, index=False, encoding="utf-8")
    resumen = {"limpieza_aplicada": log, **r["metricas"]}
    (out / "metricas_modelo.json").write_text(json.dumps(resumen, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    m = r["metricas"]["metricas"]
    print("\n=== RESUMEN ===")
    print("Semáforo (instituciones):", resumen["semaforo_instituciones"])
    print("AUC modelo vs. línea base:", m["clasificacion"]["auc_modelo"], "vs", m["clasificacion"]["auc_linea_base_persistencia"])
    print("MAE de la tasa (pp): modelo", m["regresion_tasa"]["mae_modelo_pp"], "vs persistencia",
          m["regresion_tasa"]["mae_linea_base_persistencia_pp"], "(si el modelo no supera la línea base, úsese con cautela)")
    print("Top 8 variables:", ", ".join(r["importancia_variables"].head(8)["variable"]))
    print(f"Inversión total estimada: S/ {resumen['costo_total_estimado_soles']:,} en {resumen['locales_con_accion']} locales; "
          f"{resumen['locales_financiados_en_presupuesto']} locales ({resumen['instituciones_en_locales_financiados']} instituciones) "
          f"entran en el presupuesto de S/ {a.presupuesto:,.0f}")
    print(f"Anomalías IoT: {resumen['lecturas_iot_anomalas']} lecturas | Sensores con alerta crónica: "
          f"{resumen['sensores_con_alerta_cronica']} | Anomalías de acceso: {resumen['anomalias_acceso']} instituciones")
    print(f"Archivos escritos en: {out.resolve()}")


if __name__ == "__main__":
    main()
