# 07_flujo_alertas_riesgo

## Propósito
Este documento describe el flujo lógico y el pseudocódigo base de la herramienta analítica orientada a identificar alertas de riesgo por traslado de recursos desde Cuentas Maestras, con base en patrones transaccionales que justifican revisión posterior.

La herramienta se diseña bajo un enfoque explicable y auditable. Por lo tanto, cada alerta debe estar acompañada por:
- la regla aplicada,
- las métricas calculadas,
- los movimientos soporte,
- y la evidencia mínima necesaria para revisión técnica.

Los resultados no constituyen conclusiones definitivas ni determinan por sí mismos una irregularidad. Su función es priorizar casos para auditoría o análisis posterior.

## Alcance actual
La versión inicial de esta herramienta se encuentra orientada al análisis de movimientos cargados en:

- `FACT_MOVIMIENTOS_CM`

Como apoyo al enriquecimiento del análisis, se utilizan también:

- `DIM_CUENTAS_CM`
- `DIM_TIPO_MOVIMIENTO`
- `DIM_BENEFICIARIOS` cuando aplique

En una evolución posterior, la lógica podrá extenderse a `FACT_MOVIMIENTOS_CMP`.

## Objetivo analítico
Identificar situaciones en las que los recursos podrían estar siendo trasladados con patrones que justifican revisión posterior, especialmente cuando se observan:

- egresos casi totales frente al ingreso mensual,
- transferencias significativas hacia un mismo beneficiario,
- transferencias hacia cuentas asociadas al mismo titular,
- fraccionamiento de pagos,
- salidas rápidas posteriores al ingreso.

## Variables base del análisis

### Variables por movimiento
- `FECHA_MOVIMIENTO`
- `NUMERO_CM` como cuenta origen
- `NIT_TITULAR` obtenido mediante cruce con `DIM_CUENTAS_CM`
- `TIPO_ID_BENEFICIARIO`
- `ID_BENEFICIARIO`
- `CUENTA_BENEFICIARIO`
- `VALOR`
- `TIPO_MOVIMIENTO` o la categoría derivada del movimiento (`INGRESO` / `EGRESO`)

### Variables derivadas por cuenta-mes
- `INGRESO_MES`: suma de `VALOR` para movimientos clasificados como ingreso en el mes
- `EGRESO_MES`: suma de `VALOR` para movimientos clasificados como egreso en el mes
- `RATIO_EGRESO_INGRESO`: `EGRESO_MES / INGRESO_MES`, cuando `INGRESO_MES > 0`
- `FECHA_PRIMER_INGRESO`: fecha mínima de ingreso en el mes, si existe
- `EGRESOS_EN_VENTANA`: suma de egresos dentro de la ventana definida a partir de `FECHA_PRIMER_INGRESO`
- `RATIO_SALIDA_RAPIDA`: `EGRESOS_EN_VENTANA / INGRESO_MES`, cuando `INGRESO_MES > 0`
- `EGRESO_MISMO_TITULAR`: suma de egresos donde el identificador del beneficiario coincide con el titular, según la regla de homologación aplicada
- `RATIO_MISMO_TITULAR`: `EGRESO_MISMO_TITULAR / INGRESO_MES`, cuando `INGRESO_MES > 0`

### Variables derivadas por cuenta-mes-beneficiario
Se define el beneficiario de análisis como:

- `BENEFICIARIO_CLAVE = TIPO_ID_BENEFICIARIO + ID_BENEFICIARIO + CUENTA_BENEFICIARIO`

Variables:
- `EGRESO_BENEF`: suma de egresos hacia un beneficiario específico en el mes
- `N_TX_BENEF`: número de transferencias hacia ese beneficiario en el mes
- `RATIO_BENEF`: `EGRESO_BENEF / INGRESO_MES`, cuando `INGRESO_MES > 0`
- `TOP_BENEF_RATIO`: mayor valor de `RATIO_BENEF` dentro de la cuenta y el mes

## Parámetros ajustables
- `UMBRAL_TRASLADO_TOTAL`
- `UMBRAL_SIGNIFICATIVO`
- `UMBRAL_SIGNIFICATIVO_MISMO_TITULAR`
- `N_MIN_TRANSACCIONES`
- `VENTANA_DIAS_RAPIDA`

Estos parámetros deben mantenerse externos o parametrizables para permitir ajustes por contexto, sector o recomendaciones metodológicas.

## Flujo general
1. Cargar movimientos del periodo desde `FACT_MOVIMIENTOS_CM`.
2. Enriquecer cada movimiento con información de la cuenta origen y del titular mediante `DIM_CUENTAS_CM`.
3. Clasificar cada movimiento como ingreso o egreso según `TIPO_MOVIMIENTO` o una categoría derivada.
4. Construir agregados por `cuenta-mes`.
5. Construir agregados por `cuenta-mes-beneficiario`.
6. Evaluar las reglas de alerta A1–A6.
7. Registrar alertas con métricas y evidencia soporte.
8. Generar reporte auditable y resumen por tipo de alerta y periodo.

## Reglas de alerta propuestas

### A1. Mismo titular, traslado casi total
Si:

`EGRESO_MISMO_TITULAR / INGRESO_MES >= UMBRAL_TRASLADO_TOTAL`

entonces generar alerta.

### A2. Traslado casi total de recursos en el mes
Si:

`EGRESO_MES / INGRESO_MES >= UMBRAL_TRASLADO_TOTAL`

entonces generar alerta.

### A3. Concentración en un único destino
Si el mayor valor de:

`EGRESO_BENEF / INGRESO_MES >= UMBRAL_SIGNIFICATIVO`

entonces generar alerta.

### A4. Mismo titular, traslado proporcional significativo
Si:

`EGRESO_MISMO_TITULAR / INGRESO_MES >= UMBRAL_SIGNIFICATIVO_MISMO_TITULAR`

entonces generar alerta.

### A5. Fraccionamiento al mismo beneficiario
Si:

- `N_TX_BENEF >= N_MIN_TRANSACCIONES`
- y `EGRESO_BENEF / INGRESO_MES >= UMBRAL_SIGNIFICATIVO`

entonces generar alerta.

### A6. Salida rápida tras el ingreso
Si:

`EGRESOS_EN_VENTANA / INGRESO_MES >= UMBRAL_SIGNIFICATIVO`

dentro de la ventana definida por `FECHA_PRIMER_INGRESO + VENTANA_DIAS_RAPIDA`, entonces generar alerta.

## Pseudocódigo base

```python
def ejecutar_alertas_riesgo(df_movimientos, df_cuentas, parametros):
    df = enriquecer_con_cuentas(df_movimientos, df_cuentas)
    df = clasificar_ingreso_egreso(df)

    df["PERIODO_MES"] = df["FECHA_MOVIMIENTO"].dt.to_period("M")

    resumen_cuenta_mes = calcular_agregados_cuenta_mes(df)
    resumen_beneficiario = calcular_agregados_beneficiario(df)

    alertas = []

    for cuenta_mes in resumen_cuenta_mes:
        ingreso_mes = cuenta_mes["INGRESO_MES"]

        if ingreso_mes is None or ingreso_mes <= 0:
            continue

        if cuenta_mes["RATIO_MISMO_TITULAR"] >= parametros["UMBRAL_TRASLADO_TOTAL"]:
            alertas.append(construir_alerta("A1", cuenta_mes))

        if cuenta_mes["RATIO_EGRESO_INGRESO"] >= parametros["UMBRAL_TRASLADO_TOTAL"]:
            alertas.append(construir_alerta("A2", cuenta_mes))

        if cuenta_mes["RATIO_MISMO_TITULAR"] >= parametros["UMBRAL_SIGNIFICATIVO_MISMO_TITULAR"]:
            alertas.append(construir_alerta("A4", cuenta_mes))

        if cuenta_mes["RATIO_SALIDA_RAPIDA"] >= parametros["UMBRAL_SIGNIFICATIVO"]:
            alertas.append(construir_alerta("A6", cuenta_mes))

    for cuenta_mes_benef in resumen_beneficiario:
        if cuenta_mes_benef["RATIO_BENEF"] >= parametros["UMBRAL_SIGNIFICATIVO"]:
            alertas.append(construir_alerta("A3", cuenta_mes_benef))

        if (
            cuenta_mes_benef["N_TX_BENEF"] >= parametros["N_MIN_TRANSACCIONES"]
            and cuenta_mes_benef["RATIO_BENEF"] >= parametros["UMBRAL_SIGNIFICATIVO"]
        ):
            alertas.append(construir_alerta("A5", cuenta_mes_benef))

    df_alertas = consolidar_alertas(alertas)
    exportar_alertas(df_alertas)
    return df_alertas
