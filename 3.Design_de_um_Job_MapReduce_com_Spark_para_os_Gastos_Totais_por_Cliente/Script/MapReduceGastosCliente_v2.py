# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRGastos_Cliente(MRJob):
    
    def mapper(self, _, line):
        values = line.split(',')

        # Sabendo que o dataset contém 3 colunas sem cabeçalho representado por ID, ID_Cliente e Gasto_Total.
        # Vamos mapear apenas ID_Cliente e Gasto_Total.

        id_cliente = values[1]
        gasto_total = float(values[2])  # Convertendo para somar no reducer

        # Emitir o par chave-valor
        yield id_cliente, gasto_total

    def reducer(self, id_cliente, gastos):
        # Somando todas as ocorrências por cliente
        total_gasto = sum(gastos)
        # Emitir o resultado (todos os clientes e seus gastos totais)
        yield None, (total_gasto, id_cliente)

    def reducer_final(self, _, id_cliente_gasto):
        # Ordenando os resultados por gasto e pegando os 15 maiores
        top_15 = sorted(id_cliente_gasto, reverse=True, key=lambda x: x[0])[:15]

        # Exibir no terminal somente os 15 maiores
        for gasto, id_cliente in top_15:
            print(f"Cliente {id_cliente} gastou {gasto}")

        # Salvar todos os resultados no HDFS
        with open('/mnt/data/resultados_completos.txt', 'w') as f:
            for gasto, id_cliente in id_cliente_gasto:
                f.write(f"Cliente {id_cliente} gastou {gasto}\n")

    def steps(self):
        # Definir os passos para o job
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_final)
        ]

if __name__ == '__main__':
    MRGastos_Cliente.run()
