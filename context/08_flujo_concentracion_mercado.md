# 08_flujo_concentracion_mercado

## Propósito
Este documento describe el flujo lógico y el pseudocódigo base de la herramienta analítica orientada a identificar escenarios de concentración de mercado y patrones de recurrencia en los pagos realizados desde Cuentas Maestras hacia beneficiarios.

El enfoque combina dos componentes:

1. análisis de concentración por beneficiario,
2. agrupamiento o clusterización de beneficiarios según su perfil de recepción.

La herramienta se diseña como apoyo para priorizar revisiones técnicas y auditorías. Sus resultados no constituyen por sí mismos una conclusión de irregularidad.

## Alcance actual
La versión inicial de esta herramienta se plantea sobre:

- `FACT_MOVIMIENTOS_CM`

y se apoya en:

- `DIM_CUENTAS_CM`
- `DIM_BENEFICIARIOS`, cuando aplique

En una evolución posterior, el análisis podrá ampliarse a `FACT_MOVIMIENTOS_CMP`.

## Objetivo analítico
Identificar beneficiarios que reciben:

- montos significativamente altos,
- pagos desde múltiples cuentas origen,
- pagos desde múltiples titulares,
- pagos desde múltiples territorios,
- recurrencia sostenida en el tiempo,

de tal forma que puedan priorizarse casos de concentración relevante para análisis posterior.

## Variables base del análisis

### Variables por movimiento
- `FECHA_MOVIMIENTO`
- `NUMERO_CM` como cuenta origen
- `NIT_TITULAR` obtenido mediante cruce con `DIM_CUENTAS_CM`
- `DIVIPOLA` obtenido mediante cruce con `DIM_CUENTAS_CM`
- `TIPO_ID_BENEFICIARIO`
- `ID_BENEFICIARIO`
- `CUENTA_BENEFICIARIO`
- `VALOR`
- `TIPO_MOVIMIENTO` o categoría derivada

### Unidad de análisis
Se define:

- `BENEFICIARIO_CLAVE = TIPO_ID_BENEFICIARIO + ID_BENEFICIARIO + CUENTA_BENEFICIARIO`

Esta unidad permite identificar mejor al receptor cuando existe reutilización de identificadores o múltiples cuentas asociadas.

## Variables agregadas por beneficiario y periodo
- `TOTAL_RECIBIDO`: suma de `VALOR`
- `N_CUENTAS_ORIGEN`: número de cuentas origen distintas
- `N_TITULARES_ORIGEN`: número de titulares distintos
- `N_TERRITORIOS_ORIGEN`: número de territorios distintos
- `N_TRANSACCIONES`: cantidad de movimientos
- `PROMEDIO_TRANSACCION`: promedio de `VALOR`
- `MAX_TRANSACCION`: valor máximo observado
- `RECURRENCIA`: número de meses en los que el beneficiario aparece recibiendo recursos, cuando se use ventana multimensual

## Indicadores derivados
- `PARTICIPACION = TOTAL_RECIBIDO / TOTAL_GENERAL`
- `RANKING_RECIBIDO`
- `TOP_N_ACUMULADO`
- indicadores de concentración agregada, cuando aplique

## Flujo general
1. Cargar movimientos del periodo desde `FACT_MOVIMIENTOS_CM`.
2. Filtrar movimientos relevantes de egreso según el criterio definido.
3. Enriquecer con información de cuenta, titular y territorio mediante `DIM_CUENTAS_CM`.
4. Construir el perfil agregado de cada beneficiario.
5. Calcular ranking y participación sobre el total del periodo.
6. Identificar casos de concentración relevante mediante umbrales.
7. Aplicar clusterización, si el tamaño y la calidad de los datos lo permiten.
8. Generar salidas explicables y exportables.

## Criterios de priorización
Un beneficiario puede marcarse como caso prioritario cuando se observe una combinación relevante de factores como:

- alto `TOTAL_RECIBIDO`,
- alta `PARTICIPACION`,
- alto `N_TITULARES_ORIGEN`,
- alto `N_TERRITORIOS_ORIGEN`,
- alta `RECURRENCIA`,
- pertenencia a un clúster de alta concentración.

## Pseudocódigo base

```python
def ejecutar_concentracion_beneficiarios(df_movimientos, df_cuentas, parametros):
    df = enriquecer_con_cuentas(df_movimientos, df_cuentas)
    df = filtrar_egresos(df)

    df["PERIODO_MES"] = df["FECHA_MOVIMIENTO"].dt.to_period("M")
    df["BENEFICIARIO_CLAVE"] = construir_beneficiario_clave(df)

    perfil = (
        df.groupby("BENEFICIARIO_CLAVE")
          .agg(
              TOTAL_RECIBIDO=("VALOR", "sum"),
              N_CUENTAS_ORIGEN=("NUMERO_CM", "nunique"),
              N_TITULARES_ORIGEN=("NIT_TITULAR", "nunique"),
              N_TERRITORIOS_ORIGEN=("DIVIPOLA", "nunique"),
              N_TRANSACCIONES=("VALOR", "count"),
              PROMEDIO_TRANSACCION=("VALOR", "mean"),
              MAX_TRANSACCION=("VALOR", "max")
          )
          .reset_index()
    )

    recurrencia = (
        df.groupby("BENEFICIARIO_CLAVE")["PERIODO_MES"]
          .nunique()
          .reset_index(name="RECURRENCIA")
    )

    perfil = perfil.merge(recurrencia, on="BENEFICIARIO_CLAVE", how="left")

    total_general = perfil["TOTAL_RECIBIDO"].sum()
    perfil["PARTICIPACION"] = perfil["TOTAL_RECIBIDO"] / total_general
    perfil = perfil.sort_values("TOTAL_RECIBIDO", ascending=False)
    perfil["RANKING_RECIBIDO"] = range(1, len(perfil) + 1)

    perfil["CASO_PRIORITARIO"] = marcar_casos_prioritarios(perfil, parametros)

    if len(perfil) >= parametros["MIN_BENEFICIARIOS_CLUSTER"]:
        perfil = aplicar_clusterizacion(perfil, parametros)

    exportar_resultados(perfil)
    return perfil