"""Colnames for dataframes in the project"""
import enum

class DfVentasNames(enum.Enum):
    """Column names for the ventas dataframe"""
    SK_DIA = 'sk_dia'
    SK_MES = 'sk_mes'
    SK_MATERIAL = 'sk_material'
    SK_TIENDA = 'sk_tienda'
    DESC_MATERIAL = 'desc_material'
    DESC_GRUPO_ARTICULO = 'desc_grupo_articulo'
    DESC_CATEGORIA = 'desc_categoria'
    SK_CATEGORIA = 'sk_categoria'
    MTO_VENTA_NETA = 'mto_venta_neta'
    MTO_VENTA_NETA_PROMOCION = 'mto_venta_neta_promocion'
    CNT_VENTA = 'cnt_venta'
    CNT_VENTA_PROMOCION = 'cnt_venta_promocion'

class DfVentasPreprocessedNames(enum.Enum):
    """Column names for the dates dataframe"""
    SK_MATERIAL = 'sk_material'
    SK_TIENDA = 'sk_tienda'
    FECHA = 'fecha'
    CNT_VENTA = 'cnt_venta'
    PRECIO_RATIO = 'precio_ratio'

    AÑO_NUEVO = 'año_nuevo'
    JUEVES_SANTO = 'jueves_santo'
    VIERNES_SANTO = 'viernes_santo'
    DOMINGO_DE_RESURRECCION = 'domingo_de_resurrección'
    DIA_DEL_TRABAJO = 'día_del_trabajo'
    SAN_PEDRO_Y_SAN_PABLO = 'san_pedro_y_san_pablo'
    DIA_DE_LA_INDEPENDENCIA = 'día_de_la_independencia'
    DIA_DE_LA_GRAN_PARADA_MILITAR = 'día_de_la_gran_parada_militar'
    BATALLA_DE_JUNIN = 'batalla_de_junín'
    SANTA_ROSA_DE_LIMA = 'santa_rosa_de_lima'
    COMBATE_DE_ANGAMOS = 'combate_de_angamos'
    TODOS_LOS_SANTOS = 'todos_los_santos'
    INMACULADA_CONCEPCION = 'inmaculada_concepción'
    BATALLA_DE_AYACUCHO = 'batalla_de_ayacucho'
    NAVIDAD_DEL_SEÑOR = 'navidad_del_señor'

    WEEKEND_TRUE = 'weekend_True'

    DAY_OF_WEEK_2 = 'day_of_week_2'
    DAY_OF_WEEK_3 = 'day_of_week_3'
    DAY_OF_WEEK_4 = 'day_of_week_4'
    DAY_OF_WEEK_5 = 'day_of_week_5'
    DAY_OF_WEEK_6 = 'day_of_week_6'
    DAY_OF_WEEK_7 = 'day_of_week_7'

    WEEK_OF_MONTH_2 = 'week_of_month_2'
    WEEK_OF_MONTH_3 = 'week_of_month_3'
    WEEK_OF_MONTH_4 = 'week_of_month_4'
    WEEK_OF_MONTH_5 = 'week_of_month_5'
    WEEK_OF_MONTH_6 = 'week_of_month_6'

    MONTH_2 = 'month_2'
    MONTH_3 = 'month_3'
    MONTH_4 = 'month_4'
    MONTH_5 = 'month_5'
    MONTH_6 = 'month_6'
    MONTH_7 = 'month_7'
    MONTH_8 = 'month_8'
    MONTH_9 = 'month_9'
    MONTH_10 = 'month_10'
    MONTH_11 = 'month_11'
    MONTH_12 = 'month_12'
