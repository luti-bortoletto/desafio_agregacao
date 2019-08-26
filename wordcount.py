from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
import os

conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

entrada = 'file://' + os.path.dirname(os.path.realpath(__file__)) + '/entradas/wordcount.txt'
saida = 'file://' + os.path.dirname(os.path.realpath(__file__)) + '/saidas/wordcount'


#Cria o RDD com o conteúdo do wordcount.txt
contentRDD = sc.textFile(entrada)

#Elimina as linha em branco
filter_empty_lines = contentRDD.filter(lambda x: len(x) > 0)

#Splita as palavras pelo espaço em branco entre elas
words = filter_empty_lines.flatMap(lambda x: x.split(' '))

#Map-Reduce da contagem das palavras
wordcount = words.map(lambda x:(x,1)) .reduceByKey(lambda x, y: x + y) .map(lambda x: (x[1], x[0])).sortByKey(False)

#transforma o rdd em dataframe 
df_wordcount = sqlContext.createDataFrame(wordcount).toDF("quantidade", "palavra")

#criacao de tabela temporaria
df_wordcount.createOrReplaceTempView('tmp_wordcount')

#
query_final = sqlContext.sql("SELECT palavra,quantidade FROM tmp_wordcount where length(palavra) < 11 union all SELECT 'MAIORES QUE 10' as palavra,Sum(quantidade) as quantidade FROM tmp_wordcount where length(palavra) > 10")


#salvando em arquivo o resultado final
query_final.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").mode("overwrite").save(saida)





