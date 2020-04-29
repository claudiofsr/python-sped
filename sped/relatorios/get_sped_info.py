# -*- coding: utf-8 -*-

python_sped_relatorios_author='Claudio Fernandes de Souza Rodrigues (claudiofsr@yahoo.com)'
python_sped_author='Sergio Garcia (sergio@ginx.com.br)'
date='29 de Abril de 2020 (início: 10 de Janeiro de 2020)'
download_url='https://github.com/claudiofsr/python-sped'
license='MIT'

import os, re, sys, itertools, csv
from time import time, sleep
from sped.efd.pis_cofins.arquivos import ArquivoDigital as ArquivoDigital_PIS_COFINS
from sped.efd.icms_ipi.arquivos   import ArquivoDigital as ArquivoDigital_ICMS_IPI
from sped.relatorios.efd_tabelas  import EFD_Tabelas
from sped.relatorios.switcher import My_Switch

# Versão mínima exigida: python 3.6.0
python_version = sys.version_info
if python_version < (3,6,0):
	print('versão mínima exigida do python é 3.6.0')
	print('versão atual', "%s.%s.%s" % (python_version[0],python_version[1],python_version[2]))
	exit()

# Python OOP: Atributos e Métodos (def, funções)
class SPED_EFD_Info:
	"""
	Imprimir SPED EFD Contribuições ou ICMS_IPI no formato .csv tal que contenha 
	todas as informações suficientes para verificar a correção dos lançamentos ou 
	apuração das contribuições de PIS/COFINS ou do ICMS segundo a legislação vigente.
	"""
	
	# class or static variable
	
	# Python 3 Deep Dive (Part 4 - OOP)/03. Project 1/03. Project Solution - Transaction Numbers
	contador_de_linhas = itertools.count(1) # 1 é o valor inicial do contador
	
	### --- registros e colunas --- ###
	
	 # 'Data da Emissão do Documento Fiscal'
	registros_de_data_emissao  = ['DT_DOC', 'DT_DOC_INI', 'DT_REF_INI', 'DT_OPER']

	# 'Data da Entrada/Aquisição/Execução ou da Saída/Prestação/Conclusão'
	registros_de_data_execucao = ['DT_EXE_SERV', 'DT_E_S', 'DT_ENT', 'DT_A_P', 'DT_DOC_FIN', 'DT_REF_FIN']

	# merge/concatenating two lists in Python
	registros_de_data = ['DT_INI', 'DT_FIN'] + registros_de_data_emissao + registros_de_data_execucao 

	registros_de_identificacao_do_item = ['DESCR_ITEM', 'TIPO_ITEM', 'COD_NCM']

	registros_de_cadastro_do_participante = ['NOME_participante', 'CNPJ_participante', 'CPF_participante']

	registros_de_plano_de_contas = ['COD_NAT_CC', 'NOME_CTA']

	registros_de_codigo_cst = ['CST_PIS', 'CST_COFINS']

	registros_de_chave_eletronica = ['CHV_NFE', 'CHV_CTE', 'CHV_NFSE', 'CHV_DOCe', 'CHV_CFE', 'CHV_NFE_CTE']

	# adicionado 'VL_OPR' para EFD ICMS_IPI
	registros_de_valor_do_item = ['VL_DOC', 'VL_BRT', 'VL_OPER', 'VL_OPR', 'VL_OPER_DEP', 'VL_BC_CRED', 
		'VL_BC_EST', 'VL_TOT_REC', 'VL_REC_CAIXA', 'VL_REC_COMP', 'VL_REC', 'VL_ITEM']
	
	colunas_de_rateio = [
		'Classificação da Receita Bruta', 'Percentual de Rateio dos Créditos',
		'RBNC Trib MI', 'RBNC Não Trib MI', 'RBNC de Exportação',
		'Receita Bruta Total',
		'Base de Cálculo dos Créditos vinculada à Receita Tributada no MI', 
		'Base de Cálculo dos Créditos vinculada à Receita Não Tributada no MI',
		'Base de Cálculo dos Créditos vinculada à Receita de Exportação', 
		'Base de Cálculo dos Créditos vinculada à Receita Bruta Cumulativa',
		'Crédito de PIS/PASEP vinculado à Receita Tributada no MI', 
		'Crédito de PIS/PASEP vinculado à Receita Não Tributada no MI',
		'Crédito de PIS/PASEP vinculado à Receita de Exportação', 
		'Crédito de PIS/PASEP vinculado à Receita Bruta Cumulativa',
		'Crédito de COFINS vinculado à Receita Tributada no MI',
		'Crédito de COFINS vinculado à Receita Não Tributada no MI',
		'Crédito de COFINS vinculado à Receita de Exportação', 
		'Crédito de COFINS vinculado à Receita Bruta Cumulativa',
	]

	colunas_adicionais = ['Trimestre do Período de Apuração','IND_ORIG_CRED']

	# Imprimir as informações desta coluna, nesta ordem
	colunas_selecionadas = [
		'Linhas', 'EFD Tipo', 'Arquivo da SPED EFD', 'Nº da Linha da EFD', 'CNPJ Base', 'CNPJ', 
		'NOME', 'Mês do Período de Apuração', 'Ano do Período de Apuração', 'Tipo de Operação',
		'Tipo de Crédito', 'REG', 'CST_PIS_COFINS', 'NAT_BC_CRED', 'CFOP', 'COD_PART', 
		*registros_de_cadastro_do_participante, 'CNPJ_CPF_PART', 'Data de Emissão', 'Data de Execução', 
		'COD_ITEM', *registros_de_identificacao_do_item, 'Chave Eletrônica', 'COD_MOD', 'NUM_DOC', 
		'NUM_ITEM', 'COD_CTA', *registros_de_plano_de_contas, 'Valor do Item', 
		'VL_BC_PIS', 'VL_BC_COFINS', 'ALIQ_PIS', 'ALIQ_COFINS', 'VL_PIS', 'VL_COFINS', 
		'VL_ISS', 'CST_ICMS', 'VL_BC_ICMS', 'ALIQ_ICMS', 'VL_ICMS', 
		# 'VL_ICMS_RECOLHER', 'VL_ICMS_RECOLHER_OA'
	]
	
	# evitar duplicidade: Is there a more Pythonic way to prevent adding a duplicate to a list?
	registros_totais = set(
		registros_de_data + registros_de_identificacao_do_item + registros_de_plano_de_contas + 
		registros_de_codigo_cst + registros_de_chave_eletronica + registros_de_valor_do_item + 
		colunas_selecionadas + colunas_de_rateio + colunas_adicionais)

	# https://www.geeksforgeeks.org/classmethod-in-python/
	# https://realpython.com/instance-class-and-static-methods-demystified/
	@staticmethod
	def natureza_da_bc_dos_creditos():
		"""
		Tabela CFOP - Operações Geradoras de Créditos - Versão 1.0.0
		Atualizada em 13.03.2020
		http://sped.rfb.gov.br/arquivo/show/1681
		"""
		info = {} # dicionário[cfop] = código da Natuzera da BC dos Cŕeditos

		for cfop in [
				1102,1113,1117,1118,1121,1159,1251,1403,
				1652,2102,2113,2117,2118,2121,2159,2251,
				2403,2652,3102,3251,3652,
			]:
			# Código 01 - CFOP de 'Aquisição de Bens para Revenda'
			info[cfop] = '01' # padronizar: string de dois dígitos
		for cfop in [
				1101,1111,1116,1120,1122,1126,1128,1132,
				1135,1401,1407,1456,1556,1651,1653,2101,
				2111,2116,2120,2122,2126,2128,2132,2135,
				2401,2407,2456,2556,2651,2653,3101,3126,
				3128,3556,3651,3653,
			]:
			# Código 02 - CFOP de 'Aquisição de Bens Utilizados como Insumo'
			info[cfop] = '02'
		for cfop in [1124,1125,1933,2124,2125,2933]:
			# Código 03 - CFOP de 'Aquisição de Serviços Utilizados como Insumos'
			info[cfop] = '03'
		for cfop in [
				1201,1202,1203,1204,1206,1207,1215,1216,
				1410,1411,1660,1661,1662,2201,2202,2206,
				2207,2215,2216,2410,2411,2660,2661,2662,
			]:
			# Código 12 - CFOP de 'Devolução de Vendas Sujeitas à Incidência Não-Cumulativa'
			info[cfop] = '12'
		for cfop in [1922,2922]:
			# Código 13 - CFOP de 'Outras Operações com Direito a Crédito'
			info[cfop] = '13'
		
		return info
	
	# initialize the attributes of the class
	
	def __init__(self, file_path, numero_do_arquivo, encoding=None, efd_tipo=None, verbose=False):

		self.file_path = file_path
		self.numero_do_arquivo = numero_do_arquivo
				
		if encoding is None:
			self.encoding = 'UTF-8'
		else:
			self.encoding = encoding

		if efd_tipo is None or re.search(r'PIS|COFINS|Contrib', efd_tipo, flags=re.IGNORECASE):
			self.objeto_sped = ArquivoDigital_PIS_COFINS() # instanciar objeto sped_efd
			self.efd_tipo = 'EFD Contribuições'
		elif re.search(r'ICMS|IPI', efd_tipo, flags=re.IGNORECASE):
			self.objeto_sped = ArquivoDigital_ICMS_IPI()   # instanciar objeto sped_efd
			self.efd_tipo = 'EFD ICMS_IPI'
		else:
			raise ValueError(f'efd_tipo = {efd_tipo} inválido!')
		
		if not isinstance(verbose, bool):
			raise ValueError(f'verbose deve ser uma variável boolean (True or False). verbose = {verbose} é inválido!')
		else:
			self.verbose = verbose
		
		self.basename = os.path.basename(self.file_path)

		self.myDict = {}

		self.efd_info_mensal = []
	
	def obter_info_dos_itens(self):

		select_object = My_Switch(type(self).registros_totais,verbose=self.verbose)
		select_object.formatar_valores_entrada()
		self.myDict = select_object.dicionario
						
		self.objeto_sped.readfile(self.file_path, codificacao=self.encoding, verbose=self.verbose)

		self.codigo_da_natureza = type(self).natureza_da_bc_dos_creditos()

		if self.verbose:
			print(f'self.codigo_da_natureza = {self.codigo_da_natureza} ; len(self.codigo_da_natureza) = {len(self.codigo_da_natureza)}\n')
		
		self.info_dos_estabelecimentos = self.cadastro_dos_estabelecimentos(self.objeto_sped)

		if self.verbose:
			print(f'self.info_dos_estabelecimentos = {self.info_dos_estabelecimentos} ; len(self.info_dos_estabelecimentos) = {len(self.info_dos_estabelecimentos)}\n')

		self.info_do_participante = self.cadastro_do_participante(self.objeto_sped)
		
		if self.verbose:
			print(f'self.info_do_participante = {self.info_do_participante} ; len(self.info_do_participante) = {len(self.info_do_participante)}\n')
		
		self.info_do_item = self.identificacao_do_item(self.objeto_sped)

		if self.verbose:
			print(f'self.info_do_item = {self.info_do_item} ; len(self.info_do_item) = {len(self.info_do_item)}\n')
		
		self.info_da_conta = self.plano_de_contas_contabeis(self.objeto_sped)

		if self.verbose:
			print(f'self.info_da_conta = {self.info_da_conta} ; len(self.info_da_conta) = {len(self.info_da_conta)}\n')
		
		self.info_de_abertura = self.obter_info_de_abertura(self.objeto_sped)
		
		filename = os.path.splitext(self.file_path)[0] # ('./efd_info', '.py')
		arquivo_csv   = filename + '.csv'
		
		self.info_dos_blocos(self.objeto_sped, output_filename=arquivo_csv)
	
	def __repr__(self):
		# https://stackoverflow.com/questions/25577578/access-class-variable-from-instance
    	# Devo substituir 'self.__class__.static_var' por 'type(self).static_var' ?
		return f'{type(self).__name__}(file_path={self.file_path!r}, encoding={self.encoding!r}, efd_tipo={self.efd_tipo!r}, verbose={self.verbose!r})'

	# https://stackoverflow.com/questions/9573244/how-to-check-if-the-string-is-empty
	def isBlank(self,myString):
		'''
		How to check if the string is empty?
		use the fact that empty sequences are false
		'''
		if myString and myString.strip():
			# myString is not None AND myString is not empty or blank
			return False
		# myString is None OR myString is empty or blank
		return True
	
	def isNotBlank(self,myString):
		return bool(myString and myString.strip())

	def formatar_valor(self,nome,val):
		"""
		Evitar n repetições de 'if condicao_j then A_j else B_j' tal que 1 <= j <= n, 
		usar dicionário: myDict[key] = funtion_key(value)
		Better optimization technique using if/else or dictionary
		A series of if/else statement which receives the 'string' returns the appropriate function for it.
		A dictionary maintaining the key-value pair. key as strings, and values as the function objects, 
		and one main function to search and return the function object.
		"""
		
		#if val is None or self.isBlank(str(val)):
		#	return ''

		# https://stackoverflow.com/questions/11445226/better-optimization-technique-using-if-else-or-dictionary
		# https://softwareengineering.stackexchange.com/questions/182093/why-store-a-function-inside-a-python-dictionary/182095
		# https://stackoverflow.com/questions/9168340/using-a-dictionary-to-select-function-to-execute
		try:
			# https://stackoverflow.com/questions/25577578/access-class-variable-from-instance
			# val_formated = self.__class__.myDict[nome](val)
			val_formated = self.myDict[nome](val)
		except:
			val_formated = val
		#print(f'nome = {nome} ; val = {val} ; val_formated = {val_formated}')
		return val_formated

	def cadastro_dos_estabelecimentos(self,sped_efd):
		"""
		Registro 0140: Tabela de Cadastro de Estabelecimentos
		O Registro 0140 tem por objetivo relacionar e informar os estabelecimentos da pessoa jurídica.
		"""
		blocoZero = sped_efd._blocos['0'] # Ler apenas o bloco 0.
		info = {}
		for registro in blocoZero.registros:
			REG = registro.valores[1]
			if REG != '0140':
				continue
			codigo_cnpj = None
			for campo in registro.campos:
				valor = registro.valores[campo.indice]
				if campo.nome == 'CNPJ' and re.search(r'^\d{14}$', str(valor)):
					codigo_cnpj = valor
					break
			for campo in registro.campos:
				valor = registro.valores[campo.indice]
				if campo.nome == 'NOME' and codigo_cnpj is not None:
					info[codigo_cnpj] = valor
					break
		return info

	# https://stackoverflow.com/questions/25577578/access-class-variable-from-instance
	def cadastro_do_participante(self,sped_efd):
		"""
		Registro 0150: Tabela de Cadastro do Participante
		Retorno desta função:
		info_do_participante[codigo_do_participante][campo] = descricao
		"""
		blocoZero = sped_efd._blocos['0'] # Ler apenas o bloco 0.
		info = {}
		for registro in blocoZero.registros:
			REG = registro.valores[1]
			if REG != '0150':
				continue
			codigo_do_participante = None
			for campo in registro.campos:
				valor = registro.valores[campo.indice]
				# Fazer distinção entre 'NOME' do Registro0000 e 'NOME' do Registro0150
				nome  = campo.nome + '_participante'
				if campo.nome == 'COD_PART':
					codigo_do_participante = valor
					info[codigo_do_participante] = {}
				if nome in type(self).registros_de_cadastro_do_participante and codigo_do_participante is not None:
					info[codigo_do_participante][nome] = valor
		return info

	def identificacao_do_item(self,sped_efd):
		"""
		Registro 0200: Tabela de Identificação do Item (Produtos e Serviços)
		Retorno desta função:
		info_do_item[codigo_do_item][campo] = descricao
		"""
		blocoZero = sped_efd._blocos['0'] # Ler apenas o bloco 0.
		info = {}
		for registro in blocoZero.registros:
			REG = registro.valores[1]
			if REG != '0200':
				continue
			codigo_do_item = None
			for campo in registro.campos:
				valor = registro.valores[campo.indice]
				if campo.nome == 'COD_ITEM':
					codigo_do_item = valor
					info[codigo_do_item] = {}
				if campo.nome in type(self).registros_de_identificacao_do_item and codigo_do_item is not None:
					info[codigo_do_item][campo.nome] = valor
		return info

	def plano_de_contas_contabeis(self,sped_efd):
		"""
		Registro 0500: Plano de Contas Contábeis
		Retorno desta função:
		info_do_item[codigo_do_item][campo] = descricao
		"""
		blocoZero = sped_efd._blocos['0'] # Ler apenas o bloco 0.
		info = {}
		for registro in blocoZero.registros:
			REG = registro.valores[1]
			if REG != '0500':
				continue
			codigo_do_item = None
			for campo in registro.campos:
				valor = registro.valores[campo.indice]
				if campo.nome == 'COD_CTA':
					codigo_do_item = valor
					info[codigo_do_item] = {}
					break
			for campo in registro.campos:
				valor = registro.valores[campo.indice]
				if campo.nome in type(self).registros_de_plano_de_contas and codigo_do_item is not None:
					info[codigo_do_item][campo.nome] = valor
		return info

	def obter_info_de_abertura(self,sped_efd):
		registro = sped_efd._registro_abertura
		REG = registro.valores[1]
		nivel = registro.nivel

		# Utilizar uma combinação de valores para identificar univocamente um item.
		combinacao = 'registro de abertura'
		
		info_de_abertura = {}
		
		# https://www.geeksforgeeks.org/python-creating-multidimensional-dictionary/
		info_de_abertura.setdefault(nivel, {}).setdefault(combinacao, {})['Nível Hierárquico'] = nivel
		
		if self.verbose:
			print(f'registro.as_line() = {registro.as_line()} ; REG = {REG} ; nivel = {nivel}')
			print(f'info_de_abertura = {info_de_abertura}\n')
		
		for campo in registro.campos:
			
			valor = registro.valores[campo.indice]
			
			if campo.nome in type(self).colunas_selecionadas:
				info_de_abertura[nivel][combinacao][campo.nome] = valor	
			if campo.nome == 'DT_INI':
					ddmmaaaa = valor
					info_de_abertura[nivel][combinacao]['Data de Emissão'] = ddmmaaaa
					info_de_abertura[nivel][combinacao]['Mês do Período de Apuração'] = ddmmaaaa[2:4]
					info_de_abertura[nivel][combinacao]['Ano do Período de Apuração'] = ddmmaaaa[4:8]
			if campo.nome == 'DT_FIN':
					info_de_abertura[nivel][combinacao]['Data de Execução'] = valor
			if self.verbose:
				valor_formatado = self.formatar_valor(nome=campo.nome, val=valor)
				print(f'campo.indice = {campo.indice:>2} ; campo.nome = {campo.nome:>22} ; registro.valores[{campo.indice:>2}] = {valor:<50} ; valor_formatado = {valor_formatado}')		

		if self.verbose:
			print(f'\ninfo_de_abertura = {info_de_abertura}\n')
		
		return info_de_abertura
	
	def obter_tipo_de_credito(self,dict_info):
		''' 
		Veja Tabela "4.3.6 - Tabela Código de Tipo de Crédito" e comentários do Campo 02 do Registro M100 do Guia PRÁTICO.
		Os códigos dos tipos de créditos são definidos a partir das informações de CST e Alíquota constantes nos documentos e operações registrados nos blocos A, C, D e F.
		Dentro dos grupos, a alíquota informada determina se o código será o 101 (alíquotas básicas), 102 (alíquotas diferenciadas), 103 (alíquotas em reais) ou 105 (embalagens para revenda).
		Os códigos vinculados à importação (108, 208 e 308) são obtidos através da informação de CFOP iniciado em 3 (quando existente) ou pelo campo IND_ORIG_CRED nos demais casos.
		O código 109 (atividade imobiliária) é obtido diretamente dos registros F205 e F210, bem como os códigos relativos ao estoque de abertura (104, 204 e 304), 
		os quais são obtidos diretamente do registro F150 (NAT_BC_CRED = 18).
		'''
		tipo_de_credito = ''
		aliq_basica_pis    = 1.6500 # 4 casas decimais
		aliq_basica_cofins = 7.6000 # 4 casas decimais

		percentual = {}
		aliquotas_de_cred_presumido = {}

		# A alíquota do crédito presumido é uma fração percentual da alíquota básica.
		percentual[1] = 0.20 # Lei 10.925, Art. 8o, § 3o, inciso V.    # pis = 0.3300 ; confins = 1.5200
		percentual[2] = 0.35 # Lei 10.925, Art. 8o, § 3o, inciso III.  # pis = 0.5775 ; confins = 2.6600
		percentual[3] = 0.50 # Lei 10.925, Art. 8o, § 3o, inciso IV.   # pis = 0.8250 ; confins = 3.8000
		percentual[4] = 0.60 # Lei 10.925, Art. 8o, § 3o, inciso I.    # pis = 0.9900 ; confins = 4.5600
		percentual[5] = 0.10 # Lei 12.599, Art. 5o, § 1o  # pis = 0.1650 ; confins = 0.7600 --> crédito presumido - exportação de café, produtos com ncm 0901.1
		percentual[6] = 0.80 # Lei 12.599, Art. 6o, § 2o  # pis = 1.3200 ; confins = 6.0800 --> crédito presumido - industrialização do café, aquisição dos produtos com ncm 0901.1 utilizados na elaboração dos produtos com 0901.2 e 2101.1

		for key in percentual.keys():
			alpis = f'{aliq_basica_pis    * percentual[key]:.4f}' # 4 casas decimais
			alcof = f'{aliq_basica_cofins * percentual[key]:.4f}' # 4 casas decimais
			chave = alpis + alcof  # exemplo de chave = '0.99004.5600'
			aliquotas_de_cred_presumido[chave] = 1

		if (set(['ALIQ_PIS', 'ALIQ_COFINS','CST_PIS_COFINS','IND_ORIG_CRED']).issubset(dict_info) and
			re.search(r'\d', dict_info['ALIQ_PIS']) and 
			re.search(r'\d', dict_info['ALIQ_COFINS']) and
			re.search(r'\d', dict_info['CST_PIS_COFINS'])
		):
			cst = int(dict_info['CST_PIS_COFINS'])
			origem = int(dict_info['IND_ORIG_CRED'])

			aliq_pis = My_Switch.formatar_valores_reais(dict_info['ALIQ_PIS'])
			aliq_cof = My_Switch.formatar_valores_reais(dict_info['ALIQ_COFINS'])

			if   origem == 0 and 50 <= cst <= 56:
				tipo_de_credito = '01 - ' + EFD_Tabelas.tabela_tipo_de_credito['01']     # 'Alíquota Básica'
				if aliq_pis != aliq_basica_pis or aliq_cof != aliq_basica_cofins:
					tipo_de_credito = '02 - ' + EFD_Tabelas.tabela_tipo_de_credito['02'] # 'Alíquotas Diferenciadas'	
				#print(f'{aliq_pis = } ; {aliq_basica_pis = } ; {aliq_cof = } ; {aliq_basica_cofins = } ; {tipo_de_credito = }\n')
			elif origem == 0 and 60 <= cst <= 66:
				aliq_pis = f'{aliq_pis:.4f}' # 4 casas decimais
				aliq_cof = f'{aliq_cof:.4f}' # 4 casas decimais
				chave = aliq_pis + aliq_cof
				tipo_de_credito = '07 - ' + EFD_Tabelas.tabela_tipo_de_credito['07']     # 'Outros Créditos Presumidos'
				if chave in aliquotas_de_cred_presumido:
					tipo_de_credito = '06 - ' + EFD_Tabelas.tabela_tipo_de_credito['06'] # 'Presumido da Agroindústria'
			elif origem == 1 and 50 <= cst <= 66:
				tipo_de_credito = '08 - ' + EFD_Tabelas.tabela_tipo_de_credito['08']     # 'Importação'
		
		if 'NAT_BC_CRED' in dict_info and re.search(r'^\d+$', dict_info['NAT_BC_CRED']):
			natureza = int(dict_info['NAT_BC_CRED'])
			if natureza == 18:
				tipo_de_credito = '04 - ' + EFD_Tabelas.tabela_tipo_de_credito['09']     # 'Estoque de Abertura'

		return tipo_de_credito
	
	def adicionar_informacoes(self,dict_info):
		"""
		Adicionar informações em dict_info
		Formatar alguns de seus campos com o uso de tabelas ou funções
		"""

		dict_info['Arquivo da SPED EFD'] = self.basename
		dict_info['Linhas'] = next(type(self).contador_de_linhas)
		dict_info['EFD Tipo'] = self.efd_tipo # 'EFD Contribuições' ou 'EFD ICMS_IPI'

		# adicionar informação de 'Tipo de Operação'
		if self.efd_tipo == 'EFD Contribuições':
			if 'CST_PIS_COFINS' in dict_info and re.search(r'\d{1,2}', dict_info['CST_PIS_COFINS']):
				cst = int(dict_info['CST_PIS_COFINS'])
				if 1 <= cst <= 49:
					dict_info['Tipo de Operação'] = 'Saída'
				elif 50 <= cst <= 99:
					dict_info['Tipo de Operação'] = 'Entrada'
		elif self.efd_tipo == 'EFD ICMS_IPI':
			if 'CFOP' in dict_info and re.search(r'\d{4}', dict_info['CFOP']):
				cfop = int(dict_info['CFOP'])
				if cfop >= 4000:
					dict_info['Tipo de Operação'] = 'Saída'
				else:
					dict_info['Tipo de Operação'] = 'Entrada'
		
		# Adicionar informação de NAT_BC_CRED para os créditos (50 <= cst <= 66) 
		# quando houver informação do CFOP e NAT_BC_CRED estiver vazio.
		if (# 'CFOP' in dict_info and 'NAT_BC_CRED' in dict_info and 'CST_PIS_COFINS' in dict_info
			set(['CFOP','NAT_BC_CRED','CST_PIS_COFINS']).issubset(dict_info)
			and re.search(r'\d{4}', dict_info['CFOP']) and self.isBlank(dict_info['NAT_BC_CRED'])
			and re.search(r'\d{1,2}', dict_info['CST_PIS_COFINS'])):

			cfop = int(dict_info['CFOP'])
			cst  = int(dict_info['CST_PIS_COFINS'])
			msg_padrao  = f'CFOP {cfop} não define a NAT_BC_CRED de acordo com a '
			msg_padrao += f'<Tabela CFOP - Operações Geradoras de Créditos> atualizada em 13.03.2020'
			if 50 <= cst <= 66:
				dict_info['NAT_BC_CRED'] = self.codigo_da_natureza.get(cfop, msg_padrao)
		
		# Índice de Origem do Crédito: Leia os comentários do 'Registro M100: Crédito de PIS/Pasep Relativo ao Período'.
		# Os códigos vinculados à importação (108, 208 e 308) são obtidos através da informação de CFOP 
		# iniciado em 3 (quando existente) ou pelo campo IND_ORIG_CRED nos demais casos.
		indicador_de_origem = 0 # Default Value: 0 - Mercado Interno ; 1 - Mercado Externo (Importação).
		if (('CFOP' in dict_info and re.search(r'^3\d{3}', dict_info['CFOP'])) or
			('IND_ORIG_CRED' in dict_info and dict_info['IND_ORIG_CRED'] == '1')):
			indicador_de_origem = 1
		dict_info['IND_ORIG_CRED'] = indicador_de_origem

		dict_info['Tipo de Crédito'] = self.obter_tipo_de_credito(dict_info)
		del dict_info['IND_ORIG_CRED']

		# Adicionar informação de cadastro do participante obtido do Registro 0150
		# info_do_participante[codigo_do_participante][campo] = descricao
		if 'COD_PART' in dict_info and dict_info['COD_PART'] in self.info_do_participante:
			codigo_do_participante = dict_info['COD_PART']
			for campo in self.info_do_participante[codigo_do_participante]:
				dict_info[campo] = self.info_do_participante[codigo_do_participante][campo]

		# Adicionar informação de identificação do item obtido do Registro 0200
		# info_do_item[codigo_do_item][campo] = descricao
		if 'COD_ITEM' in dict_info and dict_info['COD_ITEM'] in self.info_do_item:
			codigo_do_item = dict_info['COD_ITEM']
			for campo in self.info_do_item[codigo_do_item]:
				dict_info[campo] = self.info_do_item[codigo_do_item][campo]
		
		# Adicionar informação do plano de contas obtido do Registro 0500
		# info_da_conta[codigo_da_conta][campo] = descricao
		if 'COD_CTA' in dict_info and dict_info['COD_CTA'] in self.info_da_conta:
			codigo_da_conta = dict_info['COD_CTA']
			for campo in self.info_da_conta[codigo_da_conta]:
				val = str(self.info_da_conta[codigo_da_conta][campo])
				if campo == 'COD_NAT_CC' and re.search(r'\d{1,2}', val):
					val = val.zfill(2) # val = f'{int(val):02d}'
					val = val + ' - ' + EFD_Tabelas.tabela_natureza_da_conta[val]
				dict_info[campo] = val
		
		# Ao final, formatar alguns valores dos campos
		for campo in dict_info.copy():
			valor_formatado  = self.formatar_valor(nome=campo, val=dict_info[campo])
			dict_info[campo] = valor_formatado
		
		if 'CNPJ' in dict_info and len(dict_info['CNPJ']) == 18:
			dict_info['CNPJ Base'] = dict_info['CNPJ'][:10]
		
		return dict_info
	
	def gerar_dict_de_combinacao(self, registro):

		"""
		obter informações para definição da chave de combinação
		"""

		registros_de_combinacao = [
			'CST_PIS', 'CST_COFINS', 'CST_ICMS', 'CFOP',
			'VL_BC_PIS', 'VL_BC_COFINS', 'VL_BC_ICMS'
		]

		# definir valores iniciais: comb[campo] = ''
		comb = {campo: '' for campo in registros_de_combinacao}

		for campo in registro.campos:
			if campo.nome in registros_de_combinacao:
				comb[campo.nome] = registro.valores[campo.indice]
		
		# Fazer pareamento de registros com informações dependentes.
		# No registro C191/D101/... há informações de PIS/PASEP
		# No registro C195/D105/... há informações de COFINS
		# Fazer pareamento dos registros C191 com C195, D101 com D105, ...
		# Adicionar ao dict comb dois campos: 'cst_contrib' e 'bc_contrib'.
		comb['cst_contrib'] = max(comb['CST_PIS'], comb['CST_COFINS'])
		comb['bc_contrib']  = max(comb['VL_BC_PIS'], comb['VL_BC_COFINS'])

		return comb
	
	def info_dos_blocos(self,sped_efd,output_filename):
		
		#my_regex = r'^[1A-K]' # Ler os blocos 1 e A a K.
		my_regex = r'^[A-K]' # Ler os blocos A a K.
		
		if self.efd_tipo == 'EFD Contribuições':
			campos_necessarios = ['CST_PIS', 'CST_COFINS', 'VL_BC_PIS', 'VL_BC_COFINS']
			# Bastariam os seguintes campos, desde que os registros de PIS/PASEP ocorressem sempre anteriores 
			# aos registros de COFINS: campos_necessarios = ['CST_COFINS', 'VL_BC_COFINS']
		elif self.efd_tipo == 'EFD ICMS_IPI':
			campos_necessarios = ['CST_ICMS', 'VL_BC_ICMS']
			
		for key in sped_efd._blocos.keys():

			if not re.search(my_regex, key, flags=re.IGNORECASE):
				continue
			
			bloco = sped_efd._blocos[key]
			count = 1
			
			info = self.info_de_abertura
			
			for registro in bloco.registros:
				
				REG = registro.valores[1]

				if self.efd_tipo == 'EFD ICMS_IPI' and REG == 'C170':
					continue
				
				try:
					nivel_anterior = nivel
					num_de_campos_anterior = num_de_campos
				except:
					nivel_anterior = registro.nivel + 1
					num_de_campos_anterior = len(registro.campos) + 1
				
				nivel = registro.nivel # nível atual
				num_de_campos = len(registro.campos)

				# gerar dicionário cujas chaves geram uma combinação 
				comb = self.gerar_dict_de_combinacao(registro)

				# Utilizar uma combinação de valores para identificar univocamente um item.
				# O pareamento entre itens de PIS e COFINS ocorre dinamicamente, linha a linha.
				combinacao = f"{comb['cst_contrib']}_{comb['CST_ICMS']}_{comb['CFOP']}_{comb['bc_contrib']}_{comb['VL_BC_ICMS']}"
				
				if self.verbose:
					print(f'\ncount = {count:>2} ; key = {key} ; REG = {REG} ; nivel_anterior = {nivel_anterior} ; nivel = {nivel} ; ', end='')
					print(f'num_de_campos_anterior = {num_de_campos_anterior} ; num_de_campos = {num_de_campos} ')
					print(f"CST_PIS = {comb['CST_PIS']} ; CST_COFINS = {comb['CST_COFINS']} ; cst_contrib = {comb['cst_contrib']} ; ", end='')
					print(f"VL_BC_PIS = {comb['VL_BC_PIS']} ; VL_BC_COFINS = {comb['VL_BC_COFINS']} ; bc_contrib = {comb['bc_contrib']}")
					print(f'registro.as_line() = {registro.as_line()}')
				
				# As informações do pai e respectivos filhos devem ser apagadas quando 
				# o nivel hierárquico regride dos filhos para pais diferentes.
				if nivel < nivel_anterior or (nivel == nivel_anterior and num_de_campos < num_de_campos_anterior):
					if self.verbose:
						if nivel < nivel_anterior:
							print(f'\n nivel atual: nivel = {nivel} < nivel_anterior = {nivel_anterior} ; ', end='')
						if nivel == nivel_anterior and num_de_campos < num_de_campos_anterior:
							print(f'\n numero de campos atual: num_de_campos = {num_de_campos} < num_de_campos_anterior = {num_de_campos_anterior} ; ', end='')
						print(f'deletar informações em info a partir do nível {nivel} em diante:')
					
					# Delete items from dictionary while iterating: 
					# https://www.geeksforgeeks.org/python-delete-items-from-dictionary-while-iterating/
					for nv in list(info):
						if nv >= nivel:
							del info[nv]
							if self.verbose:
								print(f'\t *** deletar informações do nível {nv}: del info[{nv}] ***')
					print() if self.verbose else 0
				
				# https://www.geeksforgeeks.org/python-creating-multidimensional-dictionary/
				info.setdefault(nivel, {}).setdefault(combinacao, {})['Nível Hierárquico'] = nivel
				info[nivel][combinacao]['CST_PIS_COFINS'] = comb['cst_contrib']
				
				for campo in registro.campos:
					try:
						valor = registro.valores[campo.indice]
					except:
						valor = f'{REG}[{campo.indice}:{campo.nome}] sem valor definido'
					
					#if self.verbose or REG == 'E110':
					if self.verbose:
						valor_formatado = self.formatar_valor(nome=campo.nome, val=valor)
						print(f'campo.indice = {campo.indice:>2} ; campo.nome = {campo.nome:>22} ; registro.valores[{campo.indice:>2}] = {valor:<50} ; valor_formatado = {valor_formatado}')
					
					if campo.nome not in type(self).registros_totais: # filtrar registros_totais
						continue
					
					# reter em info{} as informações dos registros contidos em registros_totais
					info[nivel][combinacao][campo.nome] = valor
					
					if campo.nome in type(self).registros_de_valor_do_item:
						info[nivel][combinacao]['Valor do Item'] = valor
					if campo.nome in type(self).registros_de_data_emissao  and len(valor) == 8:
						info[nivel][combinacao]['Data de Emissão'] = valor
					if campo.nome in type(self).registros_de_data_execucao and len(valor) == 8:
						info[nivel][combinacao]['Data de Execução'] = valor
					if campo.nome in type(self).registros_de_chave_eletronica:
						info[nivel][combinacao]['Chave Eletrônica'] = valor
					# Informar nomes dos estabelecimentos de cada CNPJ
					if campo.nome == 'CNPJ' and valor in self.info_dos_estabelecimentos:
						info[nivel][combinacao]['NOME'] = self.info_dos_estabelecimentos[valor]
				
				if self.verbose:
					print(f'\n-->info[nivel][combinacao] = info[{nivel}][{combinacao}] = {info[nivel][combinacao]}\n')
				
				#https://stackoverflow.com/questions/3931541/how-to-check-if-all-of-the-following-items-are-in-a-list
				# set(['a', 'c']).issubset(['a', 'b', 'c', 'd']) or set(lista1).issubset(lista2)

				if set(campos_necessarios).issubset( info[nivel][combinacao] ):
					# import this: Zen of Python: Flat is better than nested.
					flattened_info = {} # eliminar os dois niveis [nivel][combinacao] e trazer todas as informações para apenas uma dimensão.
					seen_column = set() # evitar duplicidade: Is there a more Pythonic way to prevent adding a duplicate to a list?

					# em info{} há os registros_totais, em flattened_info{} apenas as colunas para impressao
					for coluna in type(self).colunas_selecionadas:
						flattened_info[coluna] = '' # atribuir valor inicial para todas as colunas
						
						if coluna in info[nivel][combinacao]:
							flattened_info[coluna] = info[nivel][combinacao][coluna] # eliminar os dois niveis [nivel][combinacao]
							seen_column.add(coluna)
							if self.verbose:
								print(f'nivel = {nivel:<10} ; combinacao = {combinacao:35} ; coluna = {coluna:>35} = {info[nivel][combinacao][coluna]:<35} ; info[nivel][combinacao] = {info[nivel][combinacao]}')
							continue

						for nv in sorted(info,reverse=True): # nível em ordem decrescente
							if coluna in seen_column:        # informações já obtidas
								break                        # as informações obtidas do nível mais alto prevalecerá
							for comb in info[nv]:
								if coluna in info[nv][comb]:
									flattened_info[coluna] = info[nv][comb][coluna] # eliminar os dois niveis [nivel][combinacao]
									seen_column.add(coluna)
									if self.verbose:
										print(f'nivel = {nivel} ; nv = {nv} ; combinacao = {comb:35} ; coluna = {coluna:>35} = {info[nv][comb][coluna]:<35} ; info[nivel][combinacao] = {info[nv][comb]}')
					
					print() if self.verbose else 0
					
					flattened_info['Nº da Linha da EFD'] = registro.numero_da_linha
					
					# Adicionar informações em flattened_info ou formatar alguns de seus campos com o uso de tabelas ou funções
					flattened_info = self.adicionar_informacoes(flattened_info)
					
					self.efd_info_mensal.append(flattened_info)
				
				# Se verbose == True, limitar tamanho do arquivo impresso
				# Imprimir apenas os 20 primeiros registros de cada Bloco
				count += 1
				if self.verbose and count > 20:
					break

		print(f"arquivo[{self.numero_do_arquivo:2d}]: '{self.file_path}'.")
