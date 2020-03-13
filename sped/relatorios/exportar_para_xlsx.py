# -*- coding: utf-8 -*-

Autor = 'Claudio Fernandes de Souza Rodrigues (claudiofsr@yahoo.com)'
Data  = '12 de Março de 2020 (início: 10 de Janeiro de 2020)'

import sys, itertools, re
import xlsxwriter # pip install xlsxwriter
from sped.relatorios.switcher import My_Switch
from sped.relatorios.get_sped_info import SPED_EFD_Info

# Versão mínima exigida: python 3.6.0
python_version = sys.version_info
if python_version < (3,6,0):
	print('versão mínima exigida do python é 3.6.0')
	print('versão atual', "%s.%s.%s" % (python_version[0],python_version[1],python_version[2]))
	exit()

# Python OOP: Atributos e Métodos (def, funções)
class Exportar_Excel:
	"""
	Converter arquivo de formato CSV para XLSX do Excel
	"""

	# initialize the attributes of the class
	
	def __init__(self, efd_dict, arquivo_excel, verbose=False):
		self.efd_info_total = efd_dict
		self.output_excel = arquivo_excel
		self.verbose = verbose

	@property
	def salvar_arquivo_no_hd(self):

		# Create an new Excel file.
		workbook = xlsxwriter.Workbook(self.output_excel)

		workbook.set_properties({
			'title':    str(self.output_excel)[:-5],
			'subject':  'Informações obtidas de arquivos SPED (http://sped.rfb.gov.br)',
			'author':   '',
			'manager':  '',
			'company':  '',
			'category': '',
			'keywords': 'SPED (Sistema Público de Escrituração Digital), EFD Contribuições, EFD ICMS_IPI',
			'comments': 'Created with XlsxWriter and Python Sped',
		})

		# Set up some formatting
		header_format = workbook.add_format({
						'align':'center', 'valign':'vcenter', 
						'bg_color':'#C5D9F1', 'text_wrap':True, 
						'font_size':10})
		
		select_value = My_Switch(SPED_EFD_Info.registros_totais,verbose=self.verbose)
		select_value.formatar_valores_das_colunas()
		myValue = select_value.dicionario

		select_format = My_Switch(SPED_EFD_Info.registros_totais,verbose=self.verbose)
		select_format.formatar_colunas_do_arquivo_excel(workbook)
		myFormat = select_format.dicionario

		# Para cada efd_tipo, obter a largura máxima de cada coluna
		largura_max = {}

		split_number = 500_000 # limitar o número de linhas em cada aba (worksheet)

		# efd_info_total['EFD Contribuições'] = [{'coluna01':valor01,'coluna02':valor02}, {}, ...]
		# Cada efd_tipo possui uma lista com informações em dicionários
		# Cada dicionário de dicionários é uma linha com informações de SPED EFD

		for efd_tipo in self.efd_info_total:

			lista = self.efd_info_total[efd_tipo]
			colunas_nomes = list(lista[0].keys())
			#print(f"\n{efd_tipo = } ; {colunas_nomes = }\n")

			for row_index, my_dict in enumerate(lista, 0):

				# Após concatenar EFDs de meses distintos, refazer a contagem do número de linhas
				if 'Linhas' in my_dict:
					my_dict['Linhas'] = row_index + 2

				colunas_valores = list(my_dict.values())

				num_aba   = row_index // split_number + 1 # parte inteira da divisão
				num_linha = row_index  % split_number + 1 # módulo da divisão ou resto

				if num_linha == 1:

					num = f' {num_aba:02d}' if num_aba > 1 else ''
					
					worksheet = workbook.add_worksheet(efd_tipo + str(num))

					# https://xlsxwriter.readthedocs.io/worksheet.html
					worksheet_name = worksheet.get_name()

					# First, we find the length of the name of each column
					largura_max[worksheet_name] = [len(c) for c in colunas_nomes]

					# imprimir os nomes das colunas em (0,0)
					worksheet.write_row(0, 0, colunas_nomes, header_format)
				
				for column_index, cell in enumerate(colunas_valores, 0):

					column_name  = colunas_nomes[column_index]
					column_value = str(cell)

					# reter largura máxima
					if len(column_value) > largura_max[worksheet_name][column_index]:
						largura_max[worksheet_name][column_index] = len(column_value)

					if len(column_value) > 0:
						worksheet.write(num_linha, column_index, myValue[column_name](cell), myFormat[column_name])
					else:
						# Write cell with row/column notation.
						worksheet.write(num_linha, column_index, cell)
		
		# configurações finais de cada aba
		for worksheet in workbook.worksheets():

			# https://xlsxwriter.readthedocs.io/worksheet.html
			worksheet_name = worksheet.get_name()

			# definindo a altura da primeira linha, row_index == 0
			worksheet.set_row(0, 30)

			# Freeze pane on the top row.
			worksheet.freeze_panes(1, 0)

			# Ajustar largura das colunas com os valores máximos
			largura_min = 4
			for i, width in enumerate(largura_max[worksheet_name],0):
				if width > 120: # largura máxima
					width = 120
				worksheet.set_column(i, i, width + largura_min)
			
			# Set the autofilter( $first_row, $first_col, $last_row, $last_col )
			worksheet.autofilter(0, 0, 0, len(largura_max[worksheet_name]) - 1)

		workbook.close()
