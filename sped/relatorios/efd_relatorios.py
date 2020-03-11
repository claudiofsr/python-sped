#!/usr/bin/env python3
# -*- coding: utf-8 -*-

Autor = 'Claudio Fernandes de Souza Rodrigues (claudiofsr@yahoo.com)'
Data  = '11 de Março de 2020 (início: 29 de Janeiro de 2020)'
Home  = 'https://github.com/claudiofsr/python-sped'

# Instruções (no Linux):
# Para obter uma cópia, na linha de comando, execute:
# > git clone git@github.com:claudiofsr/python-sped.git

# Em seguida, vá ao diretório python-sped:
# > cd python-sped

# Para instalar o módulo do SPED em seu sistema execute, como superusuário:
# > python setup.py install

# Em um diretório que contenha arquivos de SPED EFD, 
# execute no terminal o camando:
# > efd_relatorios

import sys, os, re
from time import time, sleep
from sped import __version__
from sped.relatorios.find_efd_files import ReadFiles, Total_Execution_Time
from sped.relatorios.get_sped_info import SPED_EFD_Info
from sped.relatorios.exportar_para_xlsx import Exportar_Excel

import locale
locale.setlocale(locale.LC_NUMERIC, 'pt_BR.utf8') # 'pt_BR.utf8', 'pt_BR.UTF-8'

import numpy as np
import pandas as pd

import psutil
from time import time, sleep
from multiprocessing import Pool # take advantage of multiple cores

num_cpus = psutil.cpu_count(logical=True)

# Versão mínima exigida: python 3.6.0
python_version = sys.version_info
if python_version < (3,6,0):
	print('versão mínima exigida do python é 3.6.0')
	print('versão atual', "%s.%s.%s" % (python_version[0],python_version[1],python_version[2]))
	exit()

def get_sped_info(numero_do_arquivo, sped_file_path, lista_de_arquivos):

	tipo_da_efd = lista_de_arquivos.informations[sped_file_path]['tipo']
	codificacao = lista_de_arquivos.informations[sped_file_path]['codificação']
	
	# Instantiate an object of type SPED_EFD_Info
	csv_file = SPED_EFD_Info(sped_file_path, numero_do_arquivo, encoding=codificacao, efd_tipo=tipo_da_efd, verbose=False)
	csv_file.imprimir_arquivo_csv()

	return csv_file.efd_info_mensal # lista de dicionários

def make_target_name(arquivos_escolhidos):
	data_ini = {}
	data_fim = {}

	for file_path in arquivos_escolhidos.values():
		# PISCOFINS_20150701_20150731_12345678912345_... .txt
		# 12345678912345-123456789123-20170101-20170131-1-...-SPED-EFD.txt
		data01 = re.search(r'PISCOFINS_(\d{8})_(\d{8})', file_path, flags=re.IGNORECASE)
		data02 = re.search(r'\d{14}.\d+.(\d{8}).(\d{8}).*SPED-EFD', file_path, flags=re.IGNORECASE)

		if data01:
			data_ini[ data01.group(1) ] = 1
			data_fim[ data01.group(2) ] = 1
		if data02:
			data_ini[ data02.group(1) ] = 1
			data_fim[ data02.group(2) ] = 1
	
	#print(f'{data_ini = } ; {data_fim = }')
	ini = list(sorted(data_ini.keys()))[0]
	fim = list(sorted(data_fim.keys()))[-1]

	target_name = f'Info do Contribuinte - SPED EFD - {ini} a {fim}'

	return target_name

def consolidacao_das_operacoes_por_cst(efd_info_mensal, efd_info_total):

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.set_option.html
	# pd.options.display.precision = 2
	pd.options.display.float_format = '{:14.2f}'.format
	pd.options.display.max_rows = 100
	pd.options.display.max_colwidth = 100
	
	colunas_selecionadas = [ 
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração',
		'CST_PIS_COFINS', 
		'Valor do Item', 'VL_BC_PIS','VL_BC_COFINS',
		'VL_PIS', 'VL_COFINS', 'VL_ISS', 'VL_BC_ICMS', 'VL_ICMS',
	]

	info = [{key: my_dict[key] for key in my_dict if key in colunas_selecionadas} for my_dict in efd_info_mensal]

	#for d in info[0:4]:
	#	print(f'{d = }')

	df = pd.DataFrame(info)

	# if you want to operate on multiple columns, put them in a list like so:
	cols = [
		'Valor do Item', 'VL_BC_PIS','VL_BC_COFINS', 'VL_PIS', 'VL_COFINS', 
		'VL_ISS', 'VL_BC_ICMS', 'VL_ICMS',
	]

	# pass them to df.replace(), specifying each char and it's replacement:
	df[cols] = df[cols].replace({'[$%]': '', ',': '.','^$': 0}, regex=True)
	df[cols] = df[cols].astype(float)

	# reter/extrair os dois primeiros dígitos
	df['CST_PIS_COFINS']=df['CST_PIS_COFINS'].str.extract(r'(^\d{2})')

	# CST de entradas e saídas
	grupo_entra = df[ df['CST_PIS_COFINS'].astype(int, errors='ignore') >= 50 ].groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'CST_PIS_COFINS'], as_index=False).sum()
	grupo_saida = df[ df['CST_PIS_COFINS'].astype(int, errors='ignore') <= 49 ].groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'CST_PIS_COFINS'], as_index=False).sum()

	grupo_total_entra = grupo_entra.groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'],as_index=False).sum()
	grupo_total_saida = grupo_saida.groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'],as_index=False).sum()
	
	grupo_total_entra['CST_PIS_COFINS'] = 'Total das Entradas'
	grupo_total_saida['CST_PIS_COFINS'] = 'Total das Saídas'

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html
	concatenar = [grupo_saida, grupo_total_saida, grupo_entra, grupo_total_entra]
	resultado = pd.concat(concatenar, axis=0, sort=False, ignore_index=True)

	# Pandas Replace NaN with blank/empty string
	resultado.replace(np.nan, '', regex=True, inplace=True)
	#resultado.reset_index(drop=True, inplace=True)

	# Inicialmente os dígitos foram uteis para ordenação dos meses. Agora não mais!
	# Ao imprimir, reter apenas os nomes dos meses: '01 Janeiro' --> 'Janeiro'.
	resultado['Mês do Período de Apuração']=resultado['Mês do Período de Apuração'].str.extract(r'^\d+\s*(.*)\s*$')

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.formats.style.Styler.to_excel.html
	# resultado.style.to_excel('consolidacao_das_operacoes_por_cst.xlsx', engine='xlsxwriter', sheet_name='EFD Contribuições', index=False)

	# https://stackoverflow.com/questions/26716616/convert-a-pandas-dataframe-to-a-dictionary
	# records - each row becomes a dictionary where key is column name and value is the data in the cell
	efd_info_total['Consolidacao EFD Contrib'] = resultado.to_dict('records')

	# How to print one pandas column without index?
	resultado = resultado.to_string(index=False)

	print(f'{resultado}\n')

def consolidacao_das_operacoes_por_cfop(efd_info_mensal, efd_info_total):

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.set_option.html
	# pd.options.display.precision = 2
	pd.options.display.float_format = '{:14.2f}'.format
	pd.options.display.max_rows = 100
	pd.options.display.max_colwidth = 100

	colunas_selecionadas = [ 
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração',
		'CST_ICMS', 'CFOP', 'ALIQ_ICMS',
		'Valor do Item', 'VL_BC_PIS','VL_BC_COFINS',
		'VL_PIS', 'VL_COFINS', 'VL_ISS', 'VL_BC_ICMS', 'VL_ICMS',
	]

	info = [{key: my_dict[key] for key in my_dict if key in colunas_selecionadas} for my_dict in efd_info_mensal]

	#for d in info[0:4]:
	#	print(f'{d = }')

	df = pd.DataFrame(info)

	# if you want to operate on multiple columns, put them in a list like so:
	cols = ['Valor do Item', 'VL_BC_PIS','VL_BC_COFINS', 'VL_PIS', 'VL_COFINS', 
		'VL_ISS', 'VL_BC_ICMS', 'VL_ICMS',
	]

	# pass them to df.replace(), specifying each char and it's replacement:
	df[cols] = df[cols].replace({'[$%]': '', ',': '.','^$': 0}, regex=True)
	df[cols] = df[cols].astype(float)

	# reter/extrair os três primeiros dígitos
	df['CST_ICMS']=df['CST_ICMS'].str.extract(r'(^\d{3})')

	# reter/extrair os quatro primeiros dígitos
	df['CFOP']=df['CFOP'].str.extract(r'(^\d{4})')

	# CFOP de entradas e saídas
	grupo_entra = df[ df['CFOP'].astype(int, errors='ignore') <  4000 ].groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'CST_ICMS', 'CFOP', 'ALIQ_ICMS'], as_index=False).sum()
	grupo_saida = df[ df['CFOP'].astype(int, errors='ignore') >= 4000 ].groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'CST_ICMS', 'CFOP', 'ALIQ_ICMS'], as_index=False).sum()

	grupo_total_entra = grupo_entra.groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'],as_index=False).sum()
	grupo_total_saida = grupo_saida.groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'],as_index=False).sum()

	grupo_total_entra['CFOP'] = 'Total das Entradas'
	grupo_total_saida['CFOP'] = 'Total das Saídas'

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html
	concatenar = [grupo_saida, grupo_total_saida, grupo_entra, grupo_total_entra]
	resultado = pd.concat(concatenar, axis=0, sort=False, ignore_index=True)
	
	# Pandas Replace NaN with blank/empty string
	resultado.replace(np.nan, '', regex=True, inplace=True)
	#resultado.reset_index(drop=True, inplace=True)

	# Inicialmente os dígitos foram uteis para ordenação dos meses. Agora não mais!
	# Ao imprimir, reter apenas os nomes dos meses: '01 Janeiro' --> 'Janeiro'.
	resultado['Mês do Período de Apuração']=resultado['Mês do Período de Apuração'].str.extract(r'^\d+\s*(.*)\s*$')

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.formats.style.Styler.to_excel.html
	# resultado.style.to_excel('consolidacao_das_operacoes_por_cfop.xlsx', engine='xlsxwriter', sheet_name='EFD ICMS_IPI', index=False)

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html
	# records - each row becomes a dictionary where key is column name and value is the data in the cell
	efd_info_total['Consolidacao EFD ICMS_IPI'] = resultado.to_dict('records')

	# How to print one pandas column without index?
	resultado = resultado.to_string(index=False)

	print(f'{resultado}\n')

def main():

	print(f'\n Python Sped - versão: {__version__}\n')
	
	dir_path = os.getcwd() # CurrentDirectory
	extensao = 'txt'
	
	lista_de_arquivos = ReadFiles(root_path = dir_path, extension = extensao)
	
	arquivos_efd_contrib  = list(lista_de_arquivos.find_all_efd_contrib) # SPED EFD Contrib
	arquivos_efd_icms_ipi = list(lista_de_arquivos.find_all_efd_icmsipi) # SPED EFD ICMS_IPI
	
	arquivos_sped_efd = arquivos_efd_contrib + arquivos_efd_icms_ipi

	if len(arquivos_sped_efd) == 0:
		print(f"\tDiretório atual: '{dir_path}'.")
		print(f"\tNenhum arquivo SPED EFD foi encontrado neste diretório.\n")
		exit()
	
	print(" Arquivo(s) de SPED EFD encontrado(s) neste diretório:\n")
	
	for index,file_path in enumerate(arquivos_sped_efd,1):
		print( f"{index:>6}: {file_path}")
		for attribute, value in lista_de_arquivos.get_file_info(file_path).items():
			print(f'{attribute:>25}: {value}')

	# argumentos: sys.argv: argv[0], argv[1], argv[2], ...
	command_line = sys.argv[1:]
	
	if len(command_line) == 0:
		print("\n Selecione os arquivos pelos números correspondentes.")
		print(" Use dois pontos .. para indicar intervalo.")
		print(" Modos de uso:\n")
		print("\tExemplo A (selecionar apenas o arquivo 4): \n\tefd_relatorios 4 \n")
		print("\tExemplo B (selecionar os arquivos de 1 a 6): \n\tefd_relatorios 1 2 3 4 5 6 \n")
		print("\tExemplo C (selecionar os arquivos de 1 a 6): \n\tefd_relatorios 1..6 \n")
		print("\tExemplo D (selecionar os arquivos 2, 4 e 8): \n\tefd_relatorios 2 4 8 \n")
		print("\tExemplo E (selecionar os arquivos de 1 a 5, 7, 9, 12 a 15 e 18): \n\tefd_relatorios 1..5 7 9 12..15 18 \n")
		exit()
	else:
		# concatenate item in list to strings
		opcoes = ' '.join(command_line)
		comando_inicial = opcoes
		# remover todos os caracteres, exceto dígitos, pontos e espaços em branco
		opcoes = re.sub(r'[^\d\.\s]', '', opcoes)
		# substituir dois ou mais espaços em branco por apenas um.
		opcoes = re.sub(r'\s{2,}', ' ', opcoes)
		# remover os possíveis espaços em branco do início e do final da variável
		opcoes = opcoes.strip()
		# remover possíveis espaços: '32 ..  41' --> '32..41' ou também '32... ..  41' --> '32..41'
		opcoes = re.sub(r'(?<=\d)[\.\s]*\.[\.\s]*(?=\d)', '..', opcoes)
		# string.split(separator, maxsplit): maxsplit -1 split "all occurrences"
		# command_line = opcoes.split(r' ', -1)
		# split string based on regex
		command_line = re.split(r'\s+', opcoes)
	
	arquivos_escolhidos = {} # usar dicionário para evitar repetição

	for indice in command_line: # exemplo: ('5', '17', '32..41')

		apenas_um_digito = re.search(r'^(\d+)$', indice)
		intervalo_digito = re.search(r'^(\d+)\.{2}(\d+)$', indice)

		if apenas_um_digito: # exemplo: '17'
			value_1 = int(apenas_um_digito.group(1)) # group(1) will return the 1st capture.
			if value_1 > len(arquivos_sped_efd) or value_1 <= 0:
				print(f"\nArquivo número {value_1} não encontrado!\n")
				exit()
			sped_file = arquivos_sped_efd[value_1 - 1]
			arquivos_escolhidos[value_1] = sped_file

		elif intervalo_digito: # exemplo: '32..41'
			value_1 = int(intervalo_digito.group(1)) # 32
			value_2 = int(intervalo_digito.group(2)) # 41
			if value_1 > len(arquivos_sped_efd) or value_1 <= 0:
				print(f"\nArquivo número {value_1} não encontrado!\n")
				exit()
			if value_2 > len(arquivos_sped_efd) or value_2 <= 0:
				print(f"\nArquivo número {value_2} não encontrado!\n")
				exit()
			
			if value_2 >= value_1: # ordem crescente
				intervalo = range(value_1, value_2 + 1)
			else:                  # ordem decrescente
				intervalo = reversed(range(value_2, value_1 + 1))

			for value in list(intervalo):
				sped_file = arquivos_sped_efd[value - 1]
				arquivos_escolhidos[value] = sped_file
		else:
			print(f"\nOpção {indice} inválida!\n")
			exit()
	
	print(f"\nArquivo(s) SPED EFD selecionado(s): '{comando_inicial}' -> {list(arquivos_escolhidos.keys())}\n")
	for sped_file in arquivos_escolhidos.values():
		print(f'\t{sped_file}')
	print()

	print(f"Analisar informações do(s) arquivo(s) SPED EFD:\n")

	start = time()

	target_name = make_target_name(arquivos_escolhidos)
	final_file_excel = target_name + ".xlsx"

	# https://sebastianraschka.com/Articles/2014_multiprocessing.html
	# https://stackoverflow.com/questions/26068819/how-to-kill-all-pool-workers-in-multiprocess
	# https://www.programcreek.com/python/index/175/multiprocessing
	pool    = Pool( processes = int(max(1, num_cpus - 2)) )
	results = [ pool.apply_async(get_sped_info, args=(k,v,lista_de_arquivos)) for (k,v) in arquivos_escolhidos.items() ]
	output  = [ p.get() for p in results ]
	pool.close()

	# output  = [ [{}, {}, ...], [{}, {}, ...], ... ] --> [{},{},{},...]
	# converter 'lista de lista de dicionario' em 'lista de dicionário'
	efd_info_mensal_efd_contrib = [my_dict for lista in output for my_dict in lista if my_dict['EFD Tipo'] == 'EFD Contribuições']
	efd_info_mensal_efd_icmsipi = [my_dict for lista in output for my_dict in lista if my_dict['EFD Tipo'] == 'EFD ICMS_IPI']

	# dicionario com informações do SPED EFD que será convertido em .xlsx
	efd_info_total = {}

	print(f"\nSalvar informações no formato XLSX do Excel:\n\n\t'{final_file_excel}'")

	unificar_efds = True

	if unificar_efds:
		# Unificar em 'Itens de Docs Fiscais' as informações de ['EFD Contribuições', 'EFD ICMS_IPI']
		efd_info_total['Itens de Docs Fiscais'] = efd_info_mensal_efd_contrib + efd_info_mensal_efd_icmsipi

	if len(efd_info_mensal_efd_contrib) > 0:
		#efd_info_total['EFD Contribuições'] = efd_info_mensal_efd_contrib
		print('\nConsolidação das Operações Segregadas por CST (EFD Contribuições):')
		consolidacao_das_operacoes_por_cst(efd_info_mensal_efd_contrib, efd_info_total)
	
	if len(efd_info_mensal_efd_icmsipi) > 0:
		#efd_info_total['EFD ICMS_IPI'] = efd_info_mensal_efd_icmsipi
		print('\nConsolidação das Operações Segregadas por CFOP (EFD ICMS_IPI):')
		consolidacao_das_operacoes_por_cfop(efd_info_mensal_efd_icmsipi, efd_info_total)
	
	excel_file = Exportar_Excel(efd_info_total, final_file_excel, verbose=False)
	excel_file.salvar_info

	end = time()

	print(f'Total Execution Time: {Total_Execution_Time(start,end)}\n')

if __name__ == '__main__':
	main()
