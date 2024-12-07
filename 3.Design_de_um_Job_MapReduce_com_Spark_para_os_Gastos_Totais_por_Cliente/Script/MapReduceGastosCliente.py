# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class MRGastos_Cliente(MRJob):
	def mapper(self, key, line):
		values = line.split(',')

		# Sabendo que o dataset contém 3 colunas sem cabeçalho representado por ID, ID_Cliente e Gasto_Total.
		# Vamos mapeas apenas ID_Cliente e Gasto_Total.

		id_cliente = values[1]
		gasto_total = float(values[2])  # Convertendo para somar no reducer

		# Emitir o par chave-valor
		yield id_cliente, gasto_total


	def reducer(self, id_cliente, gastos):

		# Somando todas as ocorrências por cliente
		yield id_cliente, sum(gastos)

if __name__ == '__main__':
	MRGastos_Cliente.run()