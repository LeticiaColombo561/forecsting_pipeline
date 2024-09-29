
def format_categorias(categorias):
    """
    Convert the list of categories to a comma-separated string
    formatted for SQL query.
    """
    return ", ".join(f"'{cat}'" for cat in categorias)


def query_ventas_marcas_propias(categorias, tiendas):
      """
      Build and execute the SQL query for ventas marcas propias.
      """
      categorias_str = format_categorias(categorias)
      query = f"""
      select 
            cast(v.sk_dia as integer) as sk_dia,
            cast(v.sk_mes as integer) as sk_mes,
            cast(v.sk_material as integer) as sk_material,
            cast(v.sk_tienda as integer) as sk_tienda,
            h.desc_material,
            h.desc_grupo_articulo,
            h.desc_categoria,
            cast(h.sk_categoria as integer) as sk_categoria,
            sum(v.mto_venta_neta) as mto_venta_neta,
            sum(v.mto_venta_neta_promocion) as mto_venta_neta_promocion,
            sum(v.cnt_venta) cnt_venta,
            sum(v.cnt_venta_promocion) as cnt_venta_promocion
      from prod.fact_venta_mat v
      inner join (
            select distinct sk_material, desc_material, 
                              sk_grupo_articulo, desc_grupo_articulo, 
                              sk_categoria, desc_categoria
            from dev.hierarchy_material_category h
      ) h on v.sk_material = h.sk_material
      inner join (
            select distinct id_es_marca_propia, sk_material, sk_marca 
            from prod.dim_material
            where id_es_marca_propia=1
            and year=extract(year from (DATE_SUB(CURRENT_DATE, 1))) 
            and month=extract(month from (DATE_SUB(CURRENT_DATE, 1))) 
            and day=extract(day from (DATE_SUB(CURRENT_DATE, 5))) 
      ) m on v.sk_material = m.sk_material
      where v.sk_dia between 20220101 and 
            DATE_FORMAT(TO_DATE(DATE_SUB(CURRENT_DATE, 1), 'yyyyMMdd'),'yyyyMMdd')
      and desc_categoria in ({categorias_str})
      and sk_tienda in ({tiendas})
      group by 1,2,3,4,5,6,7,8
      """
      return query
