from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import col

conf = SparkConf().setAppName("Clientes_Pedidos")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


entrada = 'file:///home/luciana/repo/desafio_agregacao/entradas/clientes_pedidos.csv'
saida = 'file:///home/luciana/repo/desafio_agregacao/saidas/clientes_pedidos'

df_clientes_pedidos = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").load(entrada)


df_clientes_pedidos.createOrReplaceTempView('tmp_cli_pedido')


query_filter_idade_bf = """
select 
    codigo_pedido as codigo_pedido,
    codigo_cliente as codigo_cliente,
    from_unixtime(data_pedido,'yyyy-MM-dd') as data_pedido,
    floor(datediff(current_date(), to_date(data_nascimento_cliente)) / 365.25) as idade
from tmp_cli_pedido
where floor(datediff(current_date(), to_date(data_nascimento_cliente)) / 365.25) < 30
and from_unixtime(data_pedido,'yyyy-MM-dd') in ('2016-11-25','2017-11-24','2018-11-23')
"""
df_filter_idade_bf = sqlContext.sql(query_filter_idade_bf)
df_filter_idade_bf.createOrReplaceTempView('tmp_cli_idade')


query_num_pedido = """
select
    codigo_cliente as codigo_cliente,
    sum(qtd_compra) as numero_pedidos
from (
    select 
        codigo_cliente,
        data_pedido,
        count(codigo_pedido) as qtd_compra
    from tmp_cli_idade 
    group by codigo_cliente, data_pedido
    having count(codigo_pedido) > 1
) qtd_data
group by codigo_cliente
"""
df_num_pedido = sqlContext.sql(query_num_pedido)
df_num_pedido.persist()


query_lista_pedido ="""
select
    codigo_cliente as codigo_cliente,
    concat('[',concat_ws(',',collect_list(pedido)),']') as lista_pedidos
from (
    select 
        codigo_cliente,
        concat('[',codigo_pedido,',',data_pedido,']') as pedido
    from tmp_cli_idade 
) pedidos
group by codigo_cliente
"""
df_lista_pedido = sqlContext.sql(query_lista_pedido)
df_lista_pedido.persist()


query_idade = """
select 
    codigo_cliente as codigo_cliente,
    idade as idade
from tmp_cli_idade
group by codigo_cliente, idade
"""
df_idade = sqlContext.sql(query_idade)
df_idade.persist()


df_final = df_num_pedido.alias('a').join(df_lista_pedido.alias('b'),col('b.codigo_cliente') == col('a.codigo_cliente')).join(df_idade.alias('c'),col('c.codigo_cliente') == col('a.codigo_cliente')).select(col('a.codigo_cliente'),col('a.numero_pedidos'),col('b.lista_pedidos'),col('c.idade'))


df_final.repartition(1).write.format("com.databricks.spark.csv").option("header", "false").mode("overwrite").save(saida)



