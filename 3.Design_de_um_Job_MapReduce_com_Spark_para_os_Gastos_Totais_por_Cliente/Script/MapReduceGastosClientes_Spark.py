from pyspark import SparkConf, SparkContext

# Configuração do Spark Context, pois o job será executado via linha de comando com o spark-submit
conf = SparkConf().setMaster("local").setAppName("GastosPorCliente")
sc = SparkContext(conf=conf)

# Função de mapeamento que separa cada um dos campos no dataset
def MapCliente(line):
    campos = line.split(',')
    return (int(campos[1]), float(campos[2]))  # Retorna uma tupla (ID_Cliente, Gasto_Total)

# Leitura do dataset a partir do HDFS
input = sc.textFile("hdfs://localhost:9000/user/projetos/design_mapreduce_gastos_totais/datasets/gastos-cliente.csv")

mappedInput = input.map(MapCliente)

# Operação de redução por chave para calcular o total gasto por cliente
totalPorCliente = mappedInput.reduceByKey(lambda x, y: x + y)

# Imprime o resultado
resultados = totalPorCliente.collect()
for resultado in resultados:
    print(resultado)

# Salvar os resultados completos no HDFS
totalPorCliente.saveAsTextFile("hdfs://localhost:9000/user/projetos/design_mapreduce_gastos_totais/saida_resultados_completos")


