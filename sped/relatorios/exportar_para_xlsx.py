# -*- coding: utf-8 -*-

python_sped_relatorios_author='Claudio Fernandes de Souza Rodrigues (claudiofsr@yahoo.com)'
python_sped_author='Sergio Garcia (sergio@ginx.com.br)'
date='28 de Março de 2020 (início: 10 de Janeiro de 2020)'
download_url='https://github.com/claudiofsr/python-sped'
license='MIT'

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
			'subject':  '',
			'author':   '',
			'manager':  '',
			'company':  '',
			'category': 'Arquivos SPED EFD (http://sped.rfb.gov.br)',
			'keywords': 'SPED (Sistema Público de Escrituração Digital), EFD Contribuições, EFD ICMS_IPI',
			'comments': 'Created with XlsxWriter and Python Sped (relatorios) ' + \
						download_url + ' ('+ python_sped_relatorios_author + ')'
		})

		# Set up some formatting
		header_format = workbook.add_format({
			'align':'center', 'valign':'vcenter', 
			'bg_color':'#C5D9F1', 'text_wrap':True,
			'font':'Calibri', 'font_size':9
		})
		
		select_value = My_Switch(SPED_EFD_Info.registros_totais,verbose=self.verbose)
		select_value.formatar_valores_das_colunas()
		myValue = select_value.dicionario

		select_format = My_Switch(SPED_EFD_Info.registros_totais,verbose=self.verbose)
		select_format.formatar_colunas_do_arquivo_excel(workbook)
		myColumn = select_format.dicionario

		# Para cada efd_tipo, obter a largura máxima de cada coluna
		largura_max = {}

		split_number = 500_000 # limitar o número de linhas em cada aba (worksheet)

		# Dado efd_info_total[efd_tipo], tal que efd_tipo = 'EFD Contribuições', 'EFD ICMS_IPI', ... 
		# Por exemplo: efd_info_total['EFD Contribuições'] = lista
		# Cada efd_info_total[efd_tipo] guarda uma lista tal que cada item é um dicionário
		# Cada dicionário possui informações de uma linha de SPED EFD ou informações Consolidadas
		# lista = [
		# 	{'coluna01':linha001_valor01,'coluna02':linha001_valor02, ..., 'coluna05':linha001_valor05}, 
		#	{'coluna01':linha002_valor01,'coluna02':linha002_valor02, ..., 'coluna05':linha002_valor05},
		#   ...
		#	{'coluna01':linha100_valor01,'coluna02':linha100_valor02, ..., 'coluna05':linha100_valor05},
		# ]

		for efd_tipo in self.efd_info_total:

			lista = self.efd_info_total[efd_tipo]
			
			for row_index, dicionario in enumerate(lista, 0):

				# Após concatenar EFDs de meses distintos, refazer a contagem do número de linhas
				if 'Linhas' in dicionario:
					dicionario['Linhas'] = row_index + 2

				num_aba   = row_index // split_number + 1 # parte inteira da divisão
				num_linha = row_index  % split_number + 1 # módulo da divisão ou resto

				if num_linha == 1:

					num = f' {num_aba:02d}' if num_aba > 1 else ''
					
					worksheet = workbook.add_worksheet(efd_tipo + str(num))

					# https://xlsxwriter.readthedocs.io/worksheet.html
					worksheet_name = worksheet.get_name()

					# imprimir os nomes das colunas em (0,0)
					worksheet.write_row(0, 0, dicionario.keys(), header_format)

					# First, we find the length of the name of each column
					largura_max[worksheet_name] = {}
					for column_name in dicionario.keys():
						largura_max[worksheet_name][column_name] = len(column_name)
				
				for column_index, (column_name, value) in enumerate(dicionario.items(), 0):

					valor_formatado = value

					if len( str(value) ) > 0:
						valor_formatado  = myValue[column_name](value)
						coluna_formatada = myColumn[column_name]
						worksheet.write(num_linha, column_index, valor_formatado, coluna_formatada)
					else:
						# Write cell with row/column notation.
						worksheet.write(num_linha, column_index, value)
					
					# reter largura máxima
					if len( str(valor_formatado) ) > largura_max[worksheet_name][column_name]:
						largura_max[worksheet_name][column_name] = len( str(valor_formatado) )
		
		# configurações finais de cada aba
		for worksheet in workbook.worksheets():

			# https://xlsxwriter.readthedocs.io/worksheet.html
			worksheet_name = worksheet.get_name()

			# definindo a altura da primeira linha, row_index == 0
			worksheet.set_row(0, 42)

			# Freeze pane on the top row.
			worksheet.freeze_panes(1, 0)

			# Ajustar largura das colunas com os valores máximos
			largura_min = 4
			for index, (column_name, width) in enumerate(largura_max[worksheet_name].items(),0):
				match_periodo   = re.search(r'Período de Apuração', column_name, flags=re.IGNORECASE)
				match_valor     = re.search(r'Valor|VL_|Percentual', column_name, flags=re.IGNORECASE)
				match_vinculado = re.search(r'vinculad(a|o) à Receita', column_name, flags=re.IGNORECASE)
				if match_periodo and width > 14:
					largura_min = 0
					width = 14
				if match_valor and width > 16:
					largura_min = 0
					width = 16
				if match_vinculado and width > 20:
					largura_min = 0
					width = 20
				if width > 120: # largura máxima
					largura_min = 0
					width = 120
				worksheet.set_column(index, index, width + largura_min)
			
			# Set the autofilter( $first_row, $first_col, $last_row, $last_col )
			worksheet.autofilter(0, 0, 0, len(largura_max[worksheet_name]) - 1)

		workbook.close()
