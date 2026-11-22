"""Capa de Inteligencia Artificial en vivo: lectura de vistas DW, ejecución de modelos y priorización."""

from __future__ import annotations

import time
from typing import Any, Callable

import pandas as pd

from db import (
    construir_config_ia,
    finalizar_ejecucion_ia_error,
    guardar_resultados_ia_masivo,
    iniciar_ejecucion_ia,
    leer_vistas_ml_dw,
)
from modelo_aula_conectada import ErrorIA, ejecutar_ia


def ejecutar_modelos_ia_en_vivo(
    callback_progreso: Callable[[float, str], None] | None = None,
) -> dict[str, Any]:
    """Entrena y predice en vivo leyendo vw_ml_t1..t5 desde Neon y guarda resultados en ia_*."""
    inicio = time.perf_counter()
    progreso = callback_progreso or (lambda pct, msg: None)

    progreso(0.02, "Leyendo parámetros desde sem_parametro...")
    config_ia = construir_config_ia()

    id_ejecucion = iniciar_ejecucion_ia(config_ia)
    try:
        progreso(0.04, "Leyendo vistas vw_ml_t1 a vw_ml_t5 desde el Data Warehouse...")
        v1, v2, v3, v4, v5 = leer_vistas_ml_dw()

        faltantes = [
            nombre
            for nombre, df in (
                ("vw_ml_t1", v1),
                ("vw_ml_t2", v2),
                ("vw_ml_t3", v3),
                ("vw_ml_t4", v4),
                ("vw_ml_t5", v5),
            )
            if df.empty
        ]
        if faltantes:
            raise ErrorIA(
                "No hay datos suficientes en el Data Warehouse para ejecutar los modelos "
                f"(vistas vacías: {', '.join(faltantes)}). Ejecuta primero el proceso ETL."
            )

        resultado = ejecutar_ia(
            v1,
            v2,
            v3,
            v4,
            v5,
            config=config_ia,
            progreso=lambda pct, msg: progreso(
                min(max(float(pct) / 100.0, 0.0), 0.95),
                str(msg),
            ),
        )

        progreso(0.97, "Guardando resultados de IA mediante carga masiva en PostgreSQL...")
        duracion_ms = int((time.perf_counter() - inicio) * 1000)
        guardar_resultados_ia_masivo(
            id_ejecucion=id_ejecucion,
            resultado=resultado,
            config_ia=config_ia,
            duracion_ms=duracion_ms,
        )
        progreso(1.0, f"Modelos ejecutados y guardados en {duracion_ms:,} ms.")

        return {
            "id_ejecucion": id_ejecucion,
            "duracion_ms": duracion_ms,
            "metricas": resultado["metricas"],
        }
    except ErrorIA as exc:
        duracion_ms = int((time.perf_counter() - inicio) * 1000)
        try:
            finalizar_ejecucion_ia_error(id_ejecucion, str(exc), duracion_ms)
        except Exception:
            pass
        raise
    except Exception as exc:
        duracion_ms = int((time.perf_counter() - inicio) * 1000)
        try:
            finalizar_ejecucion_ia_error(id_ejecucion, str(exc), duracion_ms)
        except Exception:
            pass
        raise


def recalcular_priorizacion_presupuesto(
    df_prio: pd.DataFrame,
    presupuesto_soles: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Recalcula costo acumulado y selección dentro del presupuesto sin volver a entrenar."""
    if df_prio.empty:
        return df_prio.copy(), {
            "locales_totales": 0,
            "locales_financiados": 0,
            "ie_cubiertas": 0,
            "inversion_asignada": 0,
            "saldo_disponible": int(presupuesto_soles),
            "estudiantes_riesgo_cubiertos": 0.0,
        }

    df = df_prio.sort_values("ranking", ascending=True).reset_index(drop=True).copy()
    df["costo_estimado_soles"] = pd.to_numeric(df["costo_estimado_soles"], errors="coerce").fillna(0).astype(int)
    df["costo_acumulado_soles"] = df["costo_estimado_soles"].cumsum().astype(int)
    df["dentro_presupuesto"] = df["costo_acumulado_soles"] <= float(presupuesto_soles)

    financiados = df[df["dentro_presupuesto"]]
    inv_asignada = int(financiados["costo_estimado_soles"].sum()) if not financiados.empty else 0
    resumen = {
        "locales_totales": int(len(df)),
        "locales_financiados": int(len(financiados)),
        "ie_cubiertas": int(pd.to_numeric(financiados["n_ie"], errors="coerce").fillna(0).sum())
        if not financiados.empty
        else 0,
        "inversion_asignada": inv_asignada,
        "saldo_disponible": int(presupuesto_soles) - inv_asignada,
        "estudiantes_riesgo_cubiertos": round(
            float(
                pd.to_numeric(financiados["estudiantes_en_riesgo"], errors="coerce")
                .fillna(0)
                .sum()
            ),
            1,
        )
        if not financiados.empty
        else 0.0,
    }
    return df, resumen
