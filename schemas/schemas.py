from pyspark.sql.types import *

schema_df_ventas = StructType([
    StructField("sk_dia", LongType(), False),
    StructField("sk_mes", LongType(), False),
    StructField("sk_material", LongType(), False),
    StructField("sk_tienda", LongType(), False),
    StructField("desc_material", StringType(), False),
    StructField("desc_grupo_articulo", StringType(), False),
    StructField("desc_categoria", StringType(), False),
    StructField("sk_categoria", LongType(), False),
    StructField("mto_venta_neta", FloatType(), False),
    StructField("mto_venta_neta_promocion", FloatType(), False),
    StructField("cnt_venta", FloatType(), False),
    StructField("cnt_venta_promocion", FloatType(), False),
])

schema_df_filtered = StructType([
    StructField('fecha', DateType(), True),
    StructField("mes", StringType(), True),
    StructField('sk_dia', LongType (), True), 
    StructField('sk_mes', LongType(), True), 
    StructField('sk_material', LongType(), True), 
    StructField('sk_tienda', LongType(), True), 
    StructField('desc_material', StringType(), True), 
    StructField('desc_grupo_articulo', StringType(), True), 
    StructField('desc_categoria', StringType(), True), 
    StructField('sk_categoria', LongType(), True), 
    StructField('mto_venta_neta', DoubleType(), True), 
    StructField('mto_venta_neta_promocion', DoubleType(), True), 
    StructField('cnt_venta', DoubleType(), True), 
    StructField('cnt_venta_promocion', DoubleType(), True),
            ])


schema_df_price = StructType([
    StructField('fecha', DateType(), True),
    StructField("mes", StringType(), True),
    StructField('sk_dia', LongType(), True), 
    StructField('sk_mes', LongType(), True), 
    StructField('sk_material', LongType(), True), 
    StructField('sk_tienda', LongType(), True), 
    StructField('desc_material', StringType(), True), 
    StructField('desc_grupo_articulo', StringType(), True), 
    StructField('desc_categoria', StringType(), True), 
    StructField('sk_categoria', LongType(), True), 
    StructField('mto_venta_neta', DoubleType(), True), 
    StructField('mto_venta_neta_promocion', DoubleType(), True), 
    StructField('cnt_venta', DoubleType(), True), 
    StructField('cnt_venta_promocion', DoubleType(), True),
    StructField('mto_venta_neta_regular', DoubleType(), True),
    StructField('cnt_venta_regular', DoubleType(), True),
    StructField('precio_final', DoubleType(), True),
    StructField('precio_regular', DoubleType(), True),
                            ])


schema_df_precio_ratio = StructType([
    StructField('fecha', DateType(), True),
    StructField("mes", StringType(), True),
    StructField('sk_dia', LongType(), True), 
    StructField('sk_mes', LongType(), True), 
    StructField('sk_material', LongType(), True), 
    StructField('sk_tienda', LongType(), True), 
    StructField('desc_material', StringType(), True), 
    StructField('desc_grupo_articulo', StringType(), True), 
    StructField('desc_categoria', StringType(), True), 
    StructField('sk_categoria', LongType(), True), 
    StructField('mto_venta_neta', DoubleType(), True), 
    StructField('mto_venta_neta_promocion', DoubleType(), True), 
    StructField('cnt_venta', DoubleType(), True), 
    StructField('cnt_venta_promocion', DoubleType(), True),
    StructField('mto_venta_neta_regular', DoubleType(), True),
    StructField('cnt_venta_regular', DoubleType(), True),
    StructField('precio_final', DoubleType(), True),
    StructField('precio_regular', DoubleType(), True),
    StructField('precio_ratio', DoubleType(), True),
                            ])


schema_df_selected = StructType([
    StructField('sk_material', LongType(), True),
    StructField('sk_tienda', LongType(), True),
    StructField('fecha', DateType(), True),
    StructField('mes', StringType(), True),
    StructField('cnt_venta', DoubleType(), True),
    StructField('precio_ratio', DoubleType(), True),
                            ])

schema_df_holiday = StructType([
    StructField('sk_material', LongType(), True), 
    StructField('sk_tienda', LongType(), True), 
    StructField('fecha', TimestampType(), True), 
    StructField('mes', StringType(), True), 
    StructField('cnt_venta', DoubleType(), True), 
    StructField('precio_ratio', DoubleType(), True), 
    StructField('año_nuevo', BooleanType(), True), 
    StructField('jueves_santo', BooleanType(), True), 
    StructField('viernes_santo', BooleanType(), True), 
    StructField('domingo_de_resurrección', BooleanType(), True), 
    StructField('día_del_trabajo', BooleanType(), True), 
    StructField('san_pedro_y_san_pablo', BooleanType(), True), 
    StructField('día_de_la_independencia', BooleanType(), True), 
    StructField('día_de_la_gran_parada_militar', BooleanType(), True), 
    StructField('batalla_de_junín', BooleanType(), True), 
    StructField('santa_rosa_de_lima', BooleanType(), True), 
    StructField('combate_de_angamos', BooleanType(), True), 
    StructField('todos_los_santos', BooleanType(), True), 
    StructField('inmaculada_concepción', BooleanType(), True), 
    StructField('batalla_de_ayacucho', BooleanType(), True), 
    StructField('navidad_del_señor', BooleanType(), True)
    ])


schema_df_dates = StructType([
    StructField('sk_material', LongType(), True),
    StructField('sk_tienda', LongType(), True), 
    StructField('fecha', TimestampType(), True), 
    StructField('cnt_venta', DoubleType(), True), 
    StructField('precio_ratio', DoubleType(), True), 
    
    StructField('año_nuevo', BooleanType(), True), 
    StructField('jueves_santo', BooleanType(), True), 
    StructField('viernes_santo', BooleanType(), True), 
    StructField('domingo_de_resurrección', BooleanType(), True), 
    StructField('día_del_trabajo', BooleanType(), True), 
    StructField('san_pedro_y_san_pablo', BooleanType(), True), 
    StructField('día_de_la_independencia', BooleanType(), True), 
    StructField('día_de_la_gran_parada_militar', BooleanType(), True), 
    StructField('batalla_de_junín', BooleanType(), True), 
    StructField('santa_rosa_de_lima', BooleanType(), True), 
    StructField('combate_de_angamos', BooleanType(), True), 
    StructField('todos_los_santos', BooleanType(), True), 
    StructField('inmaculada_concepción', BooleanType(), True), 
    StructField('batalla_de_ayacucho', BooleanType(), True), 
    StructField('navidad_del_señor', BooleanType(), True),
      

    StructField('weekend_True', BooleanType(), True), 

    StructField('day_of_week_2', BooleanType(), True), 
    StructField('day_of_week_3', BooleanType(), True), 
    StructField('day_of_week_4', BooleanType(), True), 
    StructField('day_of_week_5', BooleanType(), True), 
    StructField('day_of_week_6', BooleanType(), True), 
    StructField('day_of_week_7', BooleanType(), True), 

    StructField('week_of_month_2', BooleanType(), True), 
    StructField('week_of_month_3', BooleanType(), True), 
    StructField('week_of_month_4', BooleanType(), True), 
    StructField('week_of_month_5', BooleanType(), True), 
    StructField('week_of_month_6', BooleanType(), True), 

    StructField('month_2', BooleanType(), True), 
    StructField('month_3', BooleanType(), True), 
    StructField('month_4', BooleanType(), True),
    StructField('month_5', BooleanType(), True), 
    StructField('month_6', BooleanType(), True), 
    StructField('month_7', BooleanType(), True), 
    StructField('month_8', BooleanType(), True), 
    StructField('month_9', BooleanType(), True), 
    StructField('month_10', BooleanType(), True), 
    StructField('month_11', BooleanType(), True), 
    StructField('month_12', BooleanType(), True)
      ])

schema_df_model = StructType([
    StructField('sk_material', LongType(), True),
    StructField('sk_tienda', LongType(), True), 
    StructField('fecha', TimestampType(), True), 
    StructField('cnt_venta', DoubleType(), True), 
    StructField('precio_ratio', DoubleType(), True), 
    
    StructField('año_nuevo', BooleanType(), True), 
    StructField('jueves_santo', BooleanType(), True), 
    StructField('viernes_santo', BooleanType(), True), 
    StructField('domingo_de_resurrección', BooleanType(), True), 
    StructField('día_del_trabajo', BooleanType(), True), 
    StructField('san_pedro_y_san_pablo', BooleanType(), True), 
    StructField('día_de_la_independencia', BooleanType(), True), 
    StructField('día_de_la_gran_parada_militar', BooleanType(), True), 
    StructField('batalla_de_junín', BooleanType(), True), 
    StructField('santa_rosa_de_lima', BooleanType(), True), 
    StructField('combate_de_angamos', BooleanType(), True), 
    StructField('todos_los_santos', BooleanType(), True), 
    StructField('inmaculada_concepción', BooleanType(), True), 
    StructField('batalla_de_ayacucho', BooleanType(), True), 
    StructField('navidad_del_señor', BooleanType(), True),
      
    StructField('weekend_True', BooleanType(), True), 

    StructField('day_of_week_2', BooleanType(), True), 
    StructField('day_of_week_3', BooleanType(), True), 
    StructField('day_of_week_4', BooleanType(), True), 
    StructField('day_of_week_5', BooleanType(), True), 
    StructField('day_of_week_6', BooleanType(), True), 
    StructField('day_of_week_7', BooleanType(), True), 

    StructField('week_of_month_2', BooleanType(), True), 
    StructField('week_of_month_3', BooleanType(), True), 
    StructField('week_of_month_4', BooleanType(), True), 
    StructField('week_of_month_5', BooleanType(), True), 
    StructField('week_of_month_6', BooleanType(), True), 

    StructField('month_2', BooleanType(), True), 
    StructField('month_3', BooleanType(), True), 
    StructField('month_4', BooleanType(), True),
    StructField('month_5', BooleanType(), True), 
    StructField('month_6', BooleanType(), True), 
    StructField('month_7', BooleanType(), True), 
    StructField('month_8', BooleanType(), True), 
    StructField('month_9', BooleanType(), True), 
    StructField('month_10', BooleanType(), True), 
    StructField('month_11', BooleanType(), True), 
    StructField('month_12', BooleanType(), True),
    
    StructField('precio_ratio_lag1', DoubleType(), True),
    StructField('precio_ratio_lag1_log', DoubleType(), True),
    StructField('precio_ratio_lag2', DoubleType(), True),
    StructField('precio_ratio_lag2_log', DoubleType(), True),
    StructField('precio_ratio_log', DoubleType(), True),
    StructField('cnt_venta_log', DoubleType(), True),
    ])


schema_df_model_train = StructType([

    StructField('sk_material', LongType(), True),
    StructField('sk_tienda', LongType(), True), 

    StructField('cnt_venta_log', DoubleType(), True),
    StructField('precio_ratio_log', DoubleType(), True),
    StructField('precio_ratio_lag1_log', DoubleType(), True),
    StructField('precio_ratio_lag2_log', DoubleType(), True),

    StructField('año_nuevo', BooleanType(), True), 
    StructField('jueves_santo', BooleanType(), True), 
    StructField('viernes_santo', BooleanType(), True), 
    StructField('domingo_de_resurrección', BooleanType(), True), 
    StructField('día_del_trabajo', BooleanType(), True), 
    StructField('san_pedro_y_san_pablo', BooleanType(), True), 
    StructField('día_de_la_independencia', BooleanType(), True), 
    StructField('día_de_la_gran_parada_militar', BooleanType(), True), 
    StructField('batalla_de_junín', BooleanType(), True), 
    StructField('santa_rosa_de_lima', BooleanType(), True), 
    StructField('combate_de_angamos', BooleanType(), True), 
    StructField('todos_los_santos', BooleanType(), True), 
    StructField('inmaculada_concepción', BooleanType(), True), 
    StructField('batalla_de_ayacucho', BooleanType(), True), 
    StructField('navidad_del_señor', BooleanType(), True),
      
    StructField('weekend_True', BooleanType(), True), 

    StructField('day_of_week_2', BooleanType(), True), 
    StructField('day_of_week_3', BooleanType(), True), 
    StructField('day_of_week_4', BooleanType(), True), 
    StructField('day_of_week_5', BooleanType(), True), 
    StructField('day_of_week_6', BooleanType(), True), 
    StructField('day_of_week_7', BooleanType(), True), 

    StructField('week_of_month_2', BooleanType(), True), 
    StructField('week_of_month_3', BooleanType(), True), 
    StructField('week_of_month_4', BooleanType(), True), 
    StructField('week_of_month_5', BooleanType(), True), 
    StructField('week_of_month_6', BooleanType(), True), 

    StructField('month_2', BooleanType(), True), 
    StructField('month_3', BooleanType(), True), 
    StructField('month_4', BooleanType(), True),
    StructField('month_5', BooleanType(), True), 
    StructField('month_6', BooleanType(), True), 
    StructField('month_7', BooleanType(), True), 
    StructField('month_8', BooleanType(), True), 
    StructField('month_9', BooleanType(), True), 
    StructField('month_10', BooleanType(), True), 
    StructField('month_11', BooleanType(), True), 
    StructField('month_12', BooleanType(), True),

    StructField('test', IntegerType(), True),
    
    ])


schema_model_results = StructType([
    StructField("model_id", StringType(), True),
    StructField("sk_material", StringType(), True),
    StructField("sk_tienda", StringType(), True),
    StructField("alpha", FloatType(), True),
    StructField("r2", FloatType(), True),
    StructField("betas", StringType(), True),
    StructField("beta_elasticity", FloatType(), True),
    StructField("fecha_ejecucion", TimestampType(), True)
])
