##################################################################
# objetivo: limpar e ajustar os arquivos necessários para análise
##################################################################

import os
import pandas as pd

def files_cleaning(path_source, path_destination):
    """
    FUNCTION TO CLEAN THE FILES DATA, PERFORMING:
        - TARIFA AS FLOAT AND WITH . AS DECIMAL SEPARATOR;
        - YEAR AND MONTH IN ONE SINGLE COLUMN ('PERIODO');
        - UNGROUPING DATAFRAME FOR THE QUANTITY OF SEATS SOLD;
        - DEFLATING PRICES WITH CONSUMER INDEX (IPCA series) TO 12/1993=100.
    """
    print('###########################################################################')
    print('INICIANDO O PROCESSO DE LIMPEZA DOS DADOS')

    # listando os arquivos dentro do path_source
    print('procurando os arquivos no diretório')
    files = os.listdir(path_source)
    # listando os arquivos com o nome correto dentro da lista de arquivos
    files_list = []
    for file in files:
        if len(file) == 10:
            files_list.append(file)
        else:
            pass

    # listando os periodos dos arquivos da anac:
    file_names_list = []
    for file in files_list:
        file_names_list.append(file.strip('.CSV'))

    # importando a série histórica do IPCA:
    print('importando arquivo do IPCA')
    ipca = pd.read_csv('ipca_historico.csv',';')
    ipca.columns = ['periodo','ano','mes','indicador','var_mes','var_3_meses','var_6_meses','var_ano','var_12_meses']
    ipca['periodo'] = ipca.periodo.astype(str)
    ipca = ipca[ipca.periodo.isin(file_names_list)]
    ipca_index = ipca.indicador.values
    print('série do IPCA importada com sucesso')

    # lendo, corrigindo os dados e salvando em outro arquivo
    print('#########################')
    print('iniciando a limpeza dos arquivos')
    colunas = ['ANO','MES','EMPRESA','ORIGEM','DESTINO','TARIFA','ASSENTOS']
    import_data = []
    for file in files_list:
        print('arquivo: '+path_source+'/'+file)
        file_df = pd.read_csv(path_source+'/'+file, sep=';',header=1,names=colunas)
        # corrigindo a tarifa para float:
        file_df['TARIFA'] = file_df.TARIFA.str.replace(',','.').astype(float)
        print('tarifa corrigida para float')
        # adicionando coluna com o período (ano + mes)
        file_df['PERIODO'] = file_df.ANO.astype(str)+file_df.MES.astype(str).str.zfill(2)
        print('coluna PERIODO adicionada com sucesso')
        # trazendo o indicador de inflacao para o período
        ipca_index = ipca[ipca.periodo == file.strip('.CSV')]
        ipca_index = ipca_index.indicador.values
        print('ipca de ' + file.strip('.CSV'), ipca_index)
        # deflacionando a série, fórmula: tarifa/índice*100
        # Número Índice (Dez/93 = 100)
        tarifa_deflacionada = (file_df.TARIFA * 100) / ipca_index
        file_df['TARIFA DEFLACIONADA'] = tarifa_deflacionada
        # desagrupando os valores por assento:
        file_df = file_df.loc[file_df.index.repeat(file_df.ASSENTOS)]
        # excluindo coluna ASSENTOS:
        file_df = file_df[['PERIODO','ANO','MES','EMPRESA','ORIGEM','DESTINO','TARIFA','TARIFA DEFLACIONADA']]
        print('ajustes finalizados com sucesso')
        # gravando arquivo ajustado:
        file_df.to_csv(path_destination+'/tarifas_df_' + file.strip('.CSV') + '.csv')
        file_df = None
        print('arquivo salvo com sucesso na pasta ' + path_destination)

    print('FIM DO PROCESSO')
    print('###########################################################################')