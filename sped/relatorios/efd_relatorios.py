#!/usr/bin/env python3
# -*- coding: utf-8 -*-

python_sped_relatorios_author='Claudio Fernandes de Souza Rodrigues (claudiofsr@yahoo.com)'
python_sped_author='Sergio Garcia (sergio@ginx.com.br)'
date='22 de Março de 2020 (início: 10 de Janeiro de 2020)'
download_url='https://github.com/claudiofsr/python-sped'
license='MIT'

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
	# pd.describe_option() # offline documentation
	pd.options.display.float_format = '{: .2f}'.format
	pd.options.display.max_rows = 100
	pd.options.display.max_colwidth = 100
	
	colunas_selecionadas = [ 
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração',
		'CST_PIS_COFINS', 'Valor do Item', 'VL_BC_PIS','VL_BC_COFINS',
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

	for col in cols:
		# pass them to df.replace(), specifying each char and it's replacement:
		df[col] = df[col].replace({r'[R$%]':'', r'^\s*$': 0, ',':'.'}, regex=True)
		df[col] = df[col].astype(float)
	
	df['CST_PIS_COFINS'] = df['CST_PIS_COFINS'].astype(int, errors='ignore')

	# CST de entradas e saídas
	grupo_entra = df[ df['CST_PIS_COFINS'] >= 50 ].groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'CST_PIS_COFINS'
	]).sum().reset_index()

	grupo_saida = df[ df['CST_PIS_COFINS'] <= 49 ].groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'CST_PIS_COFINS'
	]).sum().reset_index()

	grupo_mensal_entra = grupo_entra.groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'],as_index=False).sum()
	grupo_mensal_saida = grupo_saida.groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'],as_index=False).sum()
	
	grupo_mensal_entra['CST_PIS_COFINS'] = 'Total das Entradas'
	grupo_mensal_saida['CST_PIS_COFINS'] = 'Total das Saídas'

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html
	concatenar = [grupo_saida, grupo_mensal_saida, grupo_entra, grupo_mensal_entra]
	resultado = pd.concat(concatenar, axis=0, sort=False, ignore_index=True)

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_values.html
	resultado.sort_values(by=[
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'
	], ascending=[True,True,True], inplace=True,)

	# Pandas Replace NaN with blank/empty string
	resultado.replace(np.nan, '', regex=True, inplace=True)
	#resultado.reset_index(drop=True, inplace=True)

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.formats.style.Styler.to_excel.html
	# resultado.style.to_excel('consolidacao_das_operacoes_por_cst.xlsx', engine='xlsxwriter', sheet_name='EFD Contribuições', index=False)

	# https://stackoverflow.com/questions/26716616/convert-a-pandas-dataframe-to-a-dictionary
	# records - each row becomes a dictionary where key is column name and value is the data in the cell
	efd_info_total['Consolidação EFD Contrib'] = resultado.to_dict('records')

	# How to print one pandas column without index?
	resultado = resultado.to_string(index=False)

	print(f'{resultado}\n')

def consolidacao_das_operacoes_por_cfop(efd_info_mensal, efd_info_total):

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.set_option.html
	# pd.describe_option() # offline documentation
	pd.options.display.float_format = '{: .2f}'.format
	pd.options.display.max_rows = 100
	pd.options.display.max_colwidth = 100

	colunas_selecionadas = [ 
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração',
		'CST_ICMS', 'CFOP', 'ALIQ_ICMS', 'Valor do Item', 
		'VL_BC_PIS','VL_BC_COFINS', 'VL_PIS', 'VL_COFINS', 'VL_ISS', 
		'VL_BC_ICMS', 'VL_ICMS', 
		# 'VL_ICMS_RECOLHER', 'VL_ICMS_RECOLHER_OA'
	]

	info = [{key: my_dict[key] for key in my_dict if key in colunas_selecionadas} for my_dict in efd_info_mensal]

	df = pd.DataFrame(info)

	# if you want to operate on multiple columns, put them in a list like so:
	cols = ['Valor do Item', 'VL_BC_PIS','VL_BC_COFINS', 'VL_PIS', 'VL_COFINS', 
		'VL_ISS', 'VL_BC_ICMS', 'VL_ICMS', 
		# 'VL_ICMS_RECOLHER', 'VL_ICMS_RECOLHER_OA'
	]

	for col in cols:
		# pass them to df.replace(), specifying each char and it's replacement:
		df[col] = df[col].replace({r'[R$%]':'', r'^\s*$': 0, ',':'.'}, regex=True)
		df[col] = df[col].astype(float)

	# reter/extrair os três primeiros dígitos
	df['CST_ICMS']=df['CST_ICMS'].str.extract(r'^(\d{3})')

	# reter/extrair os quatro primeiros dígitos
	df['CFOP']=df['CFOP'].str.extract(r'^(\d{4})')

	# CFOP de entradas e saídas
	grupo_entra = df[ df['CFOP'].astype(int, errors='ignore') <  4000 ].groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'CST_ICMS', 'CFOP', 'ALIQ_ICMS'
	]).sum().reset_index()

	grupo_saida = df[ df['CFOP'].astype(int, errors='ignore') >= 4000 ].groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'CST_ICMS', 'CFOP', 'ALIQ_ICMS'
	]).sum().reset_index()

	grupo_mensal_entra = grupo_entra.groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'],as_index=False).sum()
	grupo_mensal_saida = grupo_saida.groupby(['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'],as_index=False).sum()

	grupo_mensal_entra['CFOP'] = 'Total das Entradas'
	grupo_mensal_saida['CFOP'] = 'Total das Saídas'

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html
	concatenar = [grupo_saida, grupo_mensal_saida, grupo_entra, grupo_mensal_entra]
	resultado = pd.concat(concatenar, axis=0, sort=False, ignore_index=True)

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_values.html
	resultado.sort_values(by=[
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'
	], ascending=[True,True,True], inplace=True,)
	
	# Pandas Replace NaN with blank/empty string
	resultado.replace(np.nan, '', regex=True, inplace=True)
	#resultado.reset_index(drop=True, inplace=True)

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.formats.style.Styler.to_excel.html
	# resultado.style.to_excel('consolidacao_das_operacoes_por_cfop.xlsx', engine='xlsxwriter', sheet_name='EFD ICMS_IPI', index=False)

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html
	# records - each row becomes a dictionary where key is column name and value is the data in the cell
	efd_info_total['Consolidação EFD ICMS_IPI'] = resultado.to_dict('records')

	# How to print one pandas column without index?
	resultado = resultado.to_string(index=False)

	print(f'{resultado}\n')

def classificacao_da_receita_bruta(efd_info_mensal, efd_info_total):

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.set_option.html
	# pd.describe_option() # offline documentation
	pd.options.display.float_format = '{: .2f}'.format
	pd.options.display.max_rows = 100
	pd.options.display.max_colwidth = 100
	
	colunas_selecionadas = [
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração',
		'CST_PIS_COFINS', 'Valor do Item'
	]

	info = [{key: my_dict[key] for key in my_dict if key in colunas_selecionadas} for my_dict in efd_info_mensal]

	df = pd.DataFrame(info)

	df['Valor do Item'] = df['Valor do Item'].replace({r'[R$%]':'', r'^\s*$': 0, ',':'.'}, regex=True)
	df['Valor do Item'] = df['Valor do Item'].astype(float, errors='ignore')

	df['CST_PIS_COFINS'] = df['CST_PIS_COFINS'].astype(str, errors='ignore')

	### --- Receita Bruta para fins de Rateio --- ###
	# -------------------- start ------------------ #

	condition0 = df['CST_PIS_COFINS'].str.contains(r'^0[1-9]') # CST = 01 a 09

	grupo_saida = df[condition0].groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'CST_PIS_COFINS'
	])['Valor do Item'].sum().reset_index()

	# how do I insert a column at a specific column index in pandas?
	grupo_saida.insert(loc=3, column='Classificação da Receita Bruta', value=0)

	# https://kite.com/python/answers/how-to-change-values-in-a-pandas-dataframe-column-based-on-a-condition-in-python
	condition1 = grupo_saida['CST_PIS_COFINS'].str.contains(r'^0[1235]') # CST = 01, 02, 03, 05
	condition2 = grupo_saida['CST_PIS_COFINS'].str.contains(r'^0[4679]') # CST = 04, 06, 07, 09
	condition3 = grupo_saida['CST_PIS_COFINS'].str.contains(r'^08')      # CST = 08

	grupo_saida.loc[ condition1, 'Classificação da Receita Bruta'] = '1 Receita Bruta Não Cumulativa - Tributada no Mercado Interno (soma CST 01, 02, 03 e 05)'
	grupo_saida.loc[ condition2, 'Classificação da Receita Bruta'] = '2 Receita Bruta Não Cumulativa - Não Tributada no Mercado Interno (soma CST 04, 06, 07 e 09)'
	grupo_saida.loc[ condition3, 'Classificação da Receita Bruta'] = '3 Receita Bruta Não Cumulativa de Exportação (CST 08)'

	grupo_receita = grupo_saida.groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'Classificação da Receita Bruta'
	]).sum().reset_index() # .agg({'Valor do Item': 'sum'})
	
	# https://stackoverflow.com/questions/23377108/pandas-percentage-of-total-with-groupby
	# Pandas percentage of total with groupby
	grupo_receita['Percentual de Rateio'] = grupo_receita.groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração'
	])['Valor do Item'].apply(lambda x: 100 * x / float(x.sum()))

	grupo_receita_soma = grupo_saida.groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração',
	])['Valor do Item'].sum().reset_index()

	grupo_receita_soma['Classificação da Receita Bruta'] = '5 Receita Bruta Total (soma CST 01 a 09)'
	grupo_receita_soma['Percentual de Rateio'] = 100

	concatenar = [grupo_receita, grupo_receita_soma]
	receita_bruta = pd.concat(concatenar, axis=0, sort=False, ignore_index=True)
	receita_bruta.sort_values(by=[
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração', 'Classificação da Receita Bruta'
	], ascending=True, inplace=True,)

	# pass them to df.replace(), specifying each char and it's replacement:
	receita_bruta['Classificação da Receita Bruta'] = receita_bruta['Classificação da Receita Bruta'].replace({r'^\d+\s*': ''}, regex=True)

	efd_info_total['Receita Bruta'] = receita_bruta.to_dict('records')

	# How to print one pandas column without index?
	receita_bruta = receita_bruta.to_string(index=False)

	print(f'{receita_bruta}\n')

	# -------------------- final ------------------ #
	### --- Receita Bruta para fins de Rateio --- ###

def consolidacao_das_operacoes_por_natureza(efd_info_mensal, efd_info_total):

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.set_option.html
	# pd.describe_option() # offline documentation
	pd.options.display.float_format = '{: .2f}'.format
	pd.options.display.max_rows = 100
	pd.options.display.max_colwidth = 100
	verbose = False
	
	colunas_selecionadas = [
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração',
		'IND_ORIG_CRED', 'CST_PIS_COFINS', 'ALIQ_PIS', 'ALIQ_COFINS', 
		'NAT_BC_CRED', 'Valor do Item', 'VL_BC_PIS','VL_BC_COFINS', 
		'VL_PIS', 'VL_COFINS',
	]

	info = [{key: my_dict[key] for key in my_dict if key in colunas_selecionadas} for my_dict in efd_info_mensal]

	df = pd.DataFrame(info)

	cols = [
		'ALIQ_PIS', 'ALIQ_COFINS', 'Valor do Item', 'VL_BC_PIS','VL_BC_COFINS', 
		'VL_PIS', 'VL_COFINS'
	]

	for col in cols:
		# pass them to df.replace(), specifying each char and it's replacement:
		df[col] = df[col].replace({r'[R$%]':'', r'^\s*$': 0, ',':'.'}, regex=True)
		df[col] = df[col].astype(float, errors='ignore')

	### --- Apresentação dos tipos de Receita Bruta em colunas distintas --- ###
	# -------------------------------- start --------------------------------- #

	colunas_info = ['CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração']
	
	# df['CST_PIS_COFINS'] e df['NAT_BC_CRED'] são strings de dois dígitos: '\d{2}'

	# RBNC Trib MI, CST = 01, 02, 03, 05
	condicao01 = df['CST_PIS_COFINS'].str.contains(r'^0[1235]')

	# RBNC Não Trib MI, CST = 04, 06, 07, 09
	condicao02 = df['CST_PIS_COFINS'].str.contains(r'^0[4679]')

	 # RBNC de Exportação, CST = 08
	condicao03 = df['CST_PIS_COFINS'].str.contains(r'^08')

	# Receita Bruta Total, CST = 01 a 09	
	condicao05 = df['CST_PIS_COFINS'].str.contains(r'^0[1-9]')

	# RBNC Trib MI, CST = 01, 02, 03, 05
	# Atribuir o valor de Zero para 'Filtro01' caso a condição01 não seja satisfeita
	# DataFrame.where(self, cond, other=nan, inplace=False, ...)[source]
	df['Filtro01'] = df['Valor do Item'].copy() # copiar coluna 'Valor do Item'
	df['Filtro01'].where(condicao01, 0, inplace=True)
	grupo01 = df.groupby(colunas_info)['Filtro01'].sum().reset_index()
	del df['Filtro01']

	if verbose:
		print(f'\ngrupo01 (RBNC Trib MI, CST = 01, 02, 03, 05):')
		print(f'{grupo01}\n')

	# RBNC Não Trib MI, CST = 04, 06, 07, 09
	# Atribuir o valor de Zero para 'Filtro02' caso a condição02 não seja satisfeita
	df['Filtro02'] = df['Valor do Item'].copy() # copiar coluna 'Valor do Item'
	df['Filtro02'].where(condicao02, 0, inplace=True)
	grupo02 = df.groupby(colunas_info)['Filtro02'].sum().reset_index()
	del df['Filtro02']

	if verbose:
		print(f'\ngrupo02 (RBNC Não Trib MI, CST = 04, 06, 07, 09):')
		print(f'{grupo02}\n')

	# RBNC de Exportação, CST = 08
	# Atribuir o valor de Zero para 'Filtro03' caso a condição03 não seja satisfeita
	df['Filtro03'] = df['Valor do Item'].copy() # copiar coluna 'Valor do Item'
	df['Filtro03'].where(condicao03, 0, inplace=True)
	grupo03 = df.groupby(colunas_info)['Filtro03'].sum().reset_index()
	del df['Filtro03']

	if verbose:
		print(f'\ngrupo03 (RBNC de Exportação, CST = 08):')
		print(f'{grupo03}\n')

	# Receita Bruta Total, CST = 01 a 09
	# Atribuir o valor de Zero para 'Filtro05' caso a condição05 não seja satisfeita
	df['Filtro05'] = df['Valor do Item'].copy() # copiar coluna 'Valor do Item'
	df['Filtro05'].where(condicao05, 0, inplace=True)
	grupo05 = df.groupby(colunas_info)['Filtro05'].sum().reset_index()
	del df['Filtro05']

	# https://stackoverflow.com/questions/11346283/renaming-columns-in-pandas
	# grupo05.rename({'Filtro05': 'Receita Bruta Total'}, axis=1, inplace=True)

	if verbose:
		print(f'\ngrupo05 (Receita Bruta Total, CST = 01 a 09):')
		print(f'{grupo05}\n')

	grupo05['RBNC Trib MI'       ] = grupo01['Filtro01']
	grupo05['RBNC Não Trib MI'   ] = grupo02['Filtro02']
	grupo05['RBNC de Exportação' ] = grupo03['Filtro03'] 
	grupo05['Receita Bruta Total'] = grupo05['Filtro05']
	del grupo05['Filtro05']
	
	if verbose:
		print(f'\ngrupo05 (informações reunidas):')
		print(f'{grupo05}\n')

	# -------------------------------- final --------------------------------- #
	### --- Apresentação dos tipos de Receita Bruta em colunas distintas --- ###

	# Créditos de PIS/COFINS: (50 <= CST <= 66) & (1 <= NAT_BC_CRED <= 18)
	# CST_PIS_COFINS, Intervalo dos Créditos: (50 <= CST <= 56) & (60 <= CST <= 66)
	# NAT_BC_CRED,    Intervalo da Base de Cálculo: 01 <= NAT_BC_CRED <= 18

	condition_CST = df['CST_PIS_COFINS'].str.contains(r'^[56][0-6]')
	condition_NAT = df['NAT_BC_CRED'   ].str.contains(r'^0[1-9]|1[0-8]')

	# Créditos de PIS/COFINS: (50 <= CST <= 66) & (1 <= NAT_BC_CRED <= 18)
	grupo = df[condition_CST & condition_NAT].groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Mês do Período de Apuração',
		'IND_ORIG_CRED', 'CST_PIS_COFINS', 'ALIQ_PIS', 'ALIQ_COFINS', 'NAT_BC_CRED',
	]).sum().reset_index()

	if verbose:
		print(f'\ngrupo [(50 <= CST <= 66) & (1 <= NAT_BC_CRED <= 18)]:')
		print(f'{grupo}\n')

	# corrigindo, calculando valores
	grupo['VL_PIS'   ] = grupo['VL_BC_PIS'   ] * grupo['ALIQ_PIS'   ] / 100
	grupo['VL_COFINS'] = grupo['VL_BC_COFINS'] * grupo['ALIQ_COFINS'] / 100

	# https://thispointer.com/pandas-merge-dataframes-on-specific-columns-or-on-index-in-python-part-2/
	grupo = grupo.merge(grupo05, on=colunas_info)

	# Trimestres do Ano
	# how do I insert a column at a specific column index in pandas?
	grupo.insert(loc=2, column='Trimestre do Período de Apuração', value='00')

	# https://kite.com/python/answers/how-to-change-values-in-a-pandas-dataframe-column-based-on-a-condition-in-python
	condition1 = grupo['Mês do Período de Apuração'].str.contains(r'^0[1-3]') # 01 <= Mês <= 03
	condition2 = grupo['Mês do Período de Apuração'].str.contains(r'^0[4-6]') # 04 <= Mês <= 06
	condition3 = grupo['Mês do Período de Apuração'].str.contains(r'^0[7-9]') # 07 <= Mês <= 09
	condition4 = grupo['Mês do Período de Apuração'].str.contains(r'^1[0-2]') # 10 <= Mês <= 12

	grupo.loc[ condition1, 'Trimestre do Período de Apuração'] = '01'
	grupo.loc[ condition2, 'Trimestre do Período de Apuração'] = '02'
	grupo.loc[ condition3, 'Trimestre do Período de Apuração'] = '03'
	grupo.loc[ condition4, 'Trimestre do Período de Apuração'] = '04'

	# Tipos de Créditos
	# how do I insert a column at a specific column index in pandas?
	grupo.insert(loc=4, column='Tipo de Crédito', value='01: Alíquota Básica')

	# https://kite.com/python/answers/how-to-change-values-in-a-pandas-dataframe-column-based-on-a-condition-in-python
	condition2 = (grupo['ALIQ_PIS'] != 1.65) | (grupo['ALIQ_COFINS'] != 7.60)    # | or condition
	condition6 = grupo['CST_PIS_COFINS'].str.contains(r'^6[0-6]')                # 60 <= CST <= 66
	condition8 = grupo['IND_ORIG_CRED'].str.contains(r'Importação')
	condition9 = grupo['NAT_BC_CRED'].str.contains(r'^18')                       # NAT_BC_CRED = 18

	grupo.loc[ condition2, 'Tipo de Crédito'] = '02: Alíquotas Diferenciadas'
	grupo.loc[ condition6, 'Tipo de Crédito'] = '06: Presumido da Agroindústria'
	grupo.loc[ condition8, 'Tipo de Crédito'] = '08: Importação'
	grupo.loc[ condition9, 'Tipo de Crédito'] = '09: Atividade Imobiliária'

	rateio_teste_inicial = True

	# CST 50 e 60: '50 - Operação com Direito a Crédito - Vinculada Exclusivamente a Receita Tributada no Mercado Interno'
	# CST 51 e 61: '51 - Operação com Direito a Crédito - Vinculada Exclusivamente a Receita Não Tributada no Mercado Interno'
	# CST 52 e 62: '52 - Operação com Direito a Crédito - Vinculada Exclusivamente a Receita de Exportação'
	# CST 53 e 63: Operação com Direito a Crédito - Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno.
	# CST 54 e 64: Operação com Direito a Crédito - Vinculada a Receitas Tributadas no Mercado Interno e de Exportação
	# CST 55 e 65: Operação com Direito a Crédito - Vinculada a Receitas Não-Tributadas no Mercado Interno e de Exportação
	# CST 56 e 66: Operação com Direito a Crédito - Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno, e de Exportação

	if rateio_teste_inicial:

		condition50 = grupo['CST_PIS_COFINS'].str.contains(r'^[56]0') # CST = 50 ou 60
		condition51 = grupo['CST_PIS_COFINS'].str.contains(r'^[56]1') # CST = 51 ou 61
		condition52 = grupo['CST_PIS_COFINS'].str.contains(r'^[56]2') # CST = 52 ou 62
		condition53 = grupo['CST_PIS_COFINS'].str.contains(r'^[56]3') # CST = 53 ou 63
		condition54 = grupo['CST_PIS_COFINS'].str.contains(r'^[56]4') # CST = 54 ou 64
		condition55 = grupo['CST_PIS_COFINS'].str.contains(r'^[56]5') # CST = 55 ou 65
		condition56 = grupo['CST_PIS_COFINS'].str.contains(r'^[56]6') # CST = 56 ou 66

		grupo.loc[ condition50, 'Crédito vinculado à Receita Tributada no MI'    ] = grupo['VL_BC_COFINS']
		grupo.loc[ condition50, 'Crédito vinculado à Receita Não Tributada no MI'] = 0
		grupo.loc[ condition50, 'Crédito vinculado à Receita de Exportação'      ] = 0
		#grupo.loc[ condition50, 'Crédito vinculado à Receita Bruta Cumulativa'  ] = 0

		grupo.loc[ condition51, 'Crédito vinculado à Receita Tributada no MI'    ] = 0
		grupo.loc[ condition51, 'Crédito vinculado à Receita Não Tributada no MI'] = grupo['VL_BC_COFINS']
		grupo.loc[ condition51, 'Crédito vinculado à Receita de Exportação'      ] = 0
		#grupo.loc[ condition51, 'Crédito vinculado à Receita Bruta Cumulativa'  ] = 0

		grupo.loc[ condition52, 'Crédito vinculado à Receita Tributada no MI'    ] = 0
		grupo.loc[ condition52, 'Crédito vinculado à Receita Não Tributada no MI'] = 0
		grupo.loc[ condition52, 'Crédito vinculado à Receita de Exportação'      ] = grupo['VL_BC_COFINS']
		#grupo.loc[ condition52, 'Crédito vinculado à Receita Bruta Cumulativa'  ] = 0

		grupo.loc[ condition53, 'Crédito vinculado à Receita Tributada no MI'    ] = grupo['VL_BC_COFINS'] * grupo['RBNC Trib MI'      ] / (grupo['RBNC Trib MI'    ] + grupo['RBNC Não Trib MI'  ])
		grupo.loc[ condition53, 'Crédito vinculado à Receita Não Tributada no MI'] = grupo['VL_BC_COFINS'] * grupo['RBNC Não Trib MI'  ] / (grupo['RBNC Trib MI'    ] + grupo['RBNC Não Trib MI'  ])
		grupo.loc[ condition53, 'Crédito vinculado à Receita de Exportação'      ] = 0
		#grupo.loc[ condition53, 'Crédito vinculado à Receita Bruta Cumulativa'  ] = 0

		grupo.loc[ condition54, 'Crédito vinculado à Receita Tributada no MI'    ] = grupo['VL_BC_COFINS'] * grupo['RBNC Trib MI'      ] / (grupo['RBNC Trib MI'    ] + grupo['RBNC de Exportação'])
		grupo.loc[ condition54, 'Crédito vinculado à Receita Não Tributada no MI'] = 0
		grupo.loc[ condition54, 'Crédito vinculado à Receita de Exportação'      ] = grupo['VL_BC_COFINS'] * grupo['RBNC de Exportação'] / (grupo['RBNC Trib MI'    ] + grupo['RBNC de Exportação'])
		#grupo.loc[ condition54, 'Crédito vinculado à Receita Bruta Cumulativa'  ] = 0

		grupo.loc[ condition55, 'Crédito vinculado à Receita Tributada no MI'    ] = 0
		grupo.loc[ condition55, 'Crédito vinculado à Receita Não Tributada no MI'] = grupo['VL_BC_COFINS'] * grupo['RBNC Não Trib MI'  ] / (grupo['RBNC Não Trib MI'] + grupo['RBNC de Exportação'])
		grupo.loc[ condition55, 'Crédito vinculado à Receita de Exportação'      ] = grupo['VL_BC_COFINS'] * grupo['RBNC de Exportação'] / (grupo['RBNC Não Trib MI'] + grupo['RBNC de Exportação'])
		#grupo.loc[ condition55, 'Crédito vinculado à Receita Bruta Cumulativa'  ] = 0

		grupo.loc[ condition56, 'Crédito vinculado à Receita Tributada no MI'    ] = grupo['VL_BC_COFINS'] * grupo['RBNC Trib MI'      ] / grupo['Receita Bruta Total']
		grupo.loc[ condition56, 'Crédito vinculado à Receita Não Tributada no MI'] = grupo['VL_BC_COFINS'] * grupo['RBNC Não Trib MI'  ] / grupo['Receita Bruta Total']
		grupo.loc[ condition56, 'Crédito vinculado à Receita de Exportação'      ] = grupo['VL_BC_COFINS'] * grupo['RBNC de Exportação'] / grupo['Receita Bruta Total']
		#grupo.loc[ condition56, 'Crédito vinculado à Receita Bruta Cumulativa'  ] = 0
	
	# Formatting numeric columns with a specified number of decimal digits
	grupo['ALIQ_PIS'   ]=grupo['ALIQ_PIS'   ].map('{: .4f}'.format, na_action='ignore')
	grupo['ALIQ_COFINS']=grupo['ALIQ_COFINS'].map('{: .4f}'.format, na_action='ignore')

	grupo_tipo_de_credito = grupo.groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Trimestre do Período de Apuração', 'Mês do Período de Apuração', 'Tipo de Crédito'
	]).sum().reset_index()

	grupo_tipo_de_credito['NAT_BC_CRED'] = 'Créditos - ' + grupo_tipo_de_credito['Tipo de Crédito'].str.extract(r'^\d{2}:\s*(.*)$') + ' (Soma Parcial)'

	grupo_mensal = grupo.groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Trimestre do Período de Apuração', 'Mês do Período de Apuração'
	]).sum().reset_index()

	grupo_mensal['NAT_BC_CRED'] = 'Créditos (Soma Mensal)'

	grupo_trimestral = grupo_mensal.groupby([
		'CNPJ Base', 'Ano do Período de Apuração', 'Trimestre do Período de Apuração'
	]).sum().reset_index()

	grupo_trimestral['NAT_BC_CRED'] = 'Créditos (Soma Trimestral)'

	grupo_total = grupo_trimestral.groupby([
		'CNPJ Base'
	]).sum().reset_index()

	grupo_total['NAT_BC_CRED'] = 'Créditos (Soma Total)'

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html
	concatenar = [grupo, grupo_tipo_de_credito, grupo_mensal, grupo_trimestral, grupo_total]
	resultado = pd.concat(concatenar, axis=0, sort=False, ignore_index=True)

	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_values.html
	resultado.sort_values(by=[
		'CNPJ Base', 'Ano do Período de Apuração', 'Trimestre do Período de Apuração', 
		'Mês do Período de Apuração', 'Tipo de Crédito'
	], ascending=True, inplace=True,)

	# Delete column from pandas DataFrame
	# Colunas temporárias necessárias para cálculos
	for coluna in [
		'Valor do Item', 'RBNC Trib MI', 'RBNC Não Trib MI', 
		'RBNC de Exportação', 'Receita Bruta Total'
	]:
		del resultado[coluna]

	# Pandas Replace NaN with blank/empty string
	resultado.replace(np.nan, '', regex=True, inplace=True)

	# https://stackoverflow.com/questions/26716616/convert-a-pandas-dataframe-to-a-dictionary
	# records - each row becomes a dictionary where key is column name and value is the data in the cell
	efd_info_total['BC dos Créditos'] = resultado.to_dict('records')

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
	else:
		# As informações da EFD serão colocadas em abas distintas a depender de 'EFD Tipo'
		if len(efd_info_mensal_efd_contrib) > 0:
			efd_info_total['EFD Contribuições'] = efd_info_mensal_efd_contrib
		if len(efd_info_mensal_efd_icmsipi) > 0:
			efd_info_total['EFD ICMS_IPI'] = efd_info_mensal_efd_icmsipi

	if len(efd_info_mensal_efd_contrib) > 0:
		print('\nConsolidação das Operações Segregadas por CST (EFD Contribuições):')
		consolidacao_das_operacoes_por_cst(efd_info_mensal_efd_contrib, efd_info_total)
		print(f'\nClassificação da Receita Bruta para fins de Rateio de Créditos:')
		classificacao_da_receita_bruta(efd_info_mensal_efd_contrib, efd_info_total)
		print('\nBase de Cálculos dos Créditos (EFD Contribuições):')
		consolidacao_das_operacoes_por_natureza(efd_info_mensal_efd_contrib, efd_info_total)
	
	if len(efd_info_mensal_efd_icmsipi) > 0:
		print('\nConsolidação das Operações Segregadas por CFOP (EFD ICMS_IPI):')
		consolidacao_das_operacoes_por_cfop(efd_info_mensal_efd_icmsipi, efd_info_total)
	
	excel_file = Exportar_Excel(efd_info_total, final_file_excel, verbose=False)
	excel_file.salvar_arquivo_no_hd

	end = time()

	# https://www.geeksforgeeks.org/python-program-to-print-emojis
	print(f'Total Execution Time: {Total_Execution_Time(start,end)}\t\N{grinning face}\n')

if __name__ == '__main__':
	main()
