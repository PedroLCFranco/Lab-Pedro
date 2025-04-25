import numpy as np
from datetime import  datetime, timedelta, date, timezone
import shutil
import pandas as pd
import inspect
import os
import requests
import re
import json
import time
import logging
import inspect

from requests.auth import HTTPBasicAuth

from pandas.core.frame import DataFrame


from azure.storage.blob import BlobClient, BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.blob import DelimitedTextDialect, DelimitedJsonDialect


import azure.identity
import pyarrow.fs
import pyarrowfs_adlgen2

import io

class Util():

    @property
    def datalakes(self):
        return [
            {
                "DEV": "datalakedev",
            }
        ]
    
    @property
    def containers(self):
        return [
            {
                "DC_DEV": "dadopedrodev"
            }
        ]

    def get_full_path(self, path:str, file:str) -> str:
        '''
        Concatena o nome do arquivo + o caminho
        '''
        try:
            print(path + '- CAMINHO DO ARQUIVO')
            print(file + '- ARQUIVO')
            full_path = None
            full_path = os.path.join(path, file)

            return full_path
        except Exception as e:
            raise Exception("Error. " + self.get_atual_function_name(False) , e)
        
    def get_data_file(self, file:str) -> datetime:
        '''
        Captura hora do arquivo na pasta no formato, ano, mes, dia, hora e min.
        '''
        try:
            dt_modificacao_timestamp = os.path.getmtime(file)
            dt_modificacao = datetime.fromtimestamp(dt_modificacao_timestamp)
            formato_data = "%Y-%m-%d %H:%M"
            dt_modificacao_formatada = dt_modificacao.strftime(formato_data)

            return dt_modificacao_formatada
        except Exception as e:
            raise Exception("Error. " + self.get_atual_function_name(False) , e)    
    
    def get_df_from_excel_csv(self, caminho_completo:str, sheet_name:str=None, skiprows:int=None, colunas=None):
        
        try:
            self.gera_log(nm_funcao = self.get_atual_function_name(True))

            if '.XLSX' or 'XLSM' in caminho_completo.upper():
                if sheet_name == None:
                    df = pd.read_excel(caminho_completo, engine='openpyxl', skiprows=skiprows)
                else:
                    df = pd.read_excel(caminho_completo, engine='openpyxl', sheet_name=sheet_name, skiprows=skiprows)

            elif '.CSV' in caminho_completo.upper():
                df = pd.read_csv(caminho_completo, skiprows=skiprows)

            else:
                raise Exception("Impossível ler arquivo: " + caminho_completo)
        
            if colunas is not None:
                df = df.rename(columns=colunas)
                df.columns = df.columns.str.strip()
            return df
        except Exception as e:
            raise Exception("Error. " + self.get_atual_function_name(False), e)
        
        
    def get_default_credential(self) -> DefaultAzureCredential:
        try:  
        
            return DefaultAzureCredential()
        
        except Exception as e:
            raise Exception("Error. " + self.__UTIL__.get_atual_function_name(False), e)
        
    def initialize_storage_account_ad_dfs(self, str_fs_name:str) -> DataLakeServiceClient:
        '''datalakecoolinovacaodev'''
        try:  
            default_credential = self.get_default_credential()

            if self.__service_client__ == None:
                self.__service_client__ = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                    "https", str_fs_name), credential=default_credential)
                
            return self.__service_client__
        
        except Exception as e:
            raise Exception("Error. " + self.__UTIL__.get_atual_function_name(False), e)
    
    def initialize_storage_account_ad_blob(self, str_fs_name:str, str_container_name:str, str_file_name:str) -> BlobClient:
        '''dadocrucooldevinovacao
        Cnaes.zip'''
        try:  
            
            storage_url = 'https://{}.blob.core.windows.net'.format(str_fs_name)
            
            default_credential = self.get_default_credential()

            # if self.__blob_client__ == None:
            self.__blob_client__ = BlobClient(storage_url, container_name=str_container_name, blob_name=str_file_name, credential=default_credential)
                
            return self.__blob_client__
                
        except Exception as e:
            raise Exception("Error. " + self.__UTIL__.get_atual_function_name(False), e)
        
    def initialize_storage_account_ad_blob_service(self, str_fs_name:str, str_container_name:str, str_file_name:str) -> BlobServiceClient:
        '''dadocrucooldevinovacao
        Cnaes.zip'''
        try:  
            
            storage_url = 'https://{}.blob.core.windows.net'.format(str_fs_name)
            
            default_credential = self.get_default_credential()

            self.__blob_service_client__ = BlobServiceClient(storage_url, container_name=str_container_name, blob_name=str_file_name, credential=default_credential)
                
            return self.__blob_service_client__
                
        except Exception as e:
            raise Exception("Error. " + self.__UTIL__.get_atual_function_name(False), e)
    
    def download_to_data_lake(self, str_fs_name:str, str_container:str, str_file:str, str_url_file_download: str):
        '''datalakecoolinovacaodev'
          'dadocrucooldevinovacao'
          'cad_fi.csv'
          'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv'''
        try:
            blob_client = self.initialize_storage_account_ad_blob(str_fs_name, str_container, str_file)
            if not blob_client.exists():
                blob_client.upload_blob(str_file)
                blob_client.start_copy_from_url(str_url_file_download)
                # blob_client.get_blob_properties().copy.status

        except Exception as e:
            raise Exception("Error. " + self.__UTIL__.get_atual_function_name(False), e)
        
        
    def gera_caminho_pasta_cloud(self, str_path:str):
        '''cnae/tb_cnae/pt=2013'''
        try:
            str_retorno = ''

            str_retorno = str_path + '/pt_dt_carga=' +  str(datetime.now().strftime('%Y-%m-%d'))

            return str_retorno
        
        except Exception as e:
            raise Exception("Error. " + self.__UTIL__.get_atual_function_name(False), e)
        

    def gera_arquivo_controle(self, dt_mod, caminhos_cloud):
        try:
            dt_now = datetime.now()

            self.__UTIL__.gera_log(nm_funcao = self.__UTIL__.get_atual_function_name(False), texto_log = 'Inicio gera arquivo controle')
            
            str_file_finaxis = caminhos_cloud[0]["str_file"]
            # Criando o Dataframe
            dados = f"{str_file_finaxis} | {dt_mod.strftime('%Y-%m-%d %H:%M')} | {dt_now.strftime('%Y-%m-%d %H:%M')}"

            # Conversão de string em uma lista de listas
            data_list = [row.split(" | ") for row in dados.split("\n")]

            # Criação do dataframe e salvar em CSV
            df = pd.DataFrame(data_list, columns=["nm_arquivo", "dt_modificacao", "dt_carga"])
            
            str_path_upload:str = self.gera_caminho_pasta_cloud(caminhos_cloud[0]["str_path_ctr"])

            if str_path_upload != '':
                self.upload_to_data_lake(caminhos_cloud[0]["str_fs_name"], caminhos_cloud[0]["str_container"], str_path_upload, caminhos_cloud[0]["str_file"], df, b_csv_parquet=False)
            else:
                self.__UTIL__.gera_log(nm_funcao = self.__UTIL__.get_atual_function_name(False), texto_log = 'Caminho cloud não encontrado')

            self.__UTIL__.gera_log(nm_funcao = self.__UTIL__.get_atual_function_name(False), texto_log = 'Fim gera gera arquivo controle')

        except Exception as e:
            raise Exception("Error. " + self.__UTIL__.get_atual_function_name(False) , e)
        
    def last_parquet_on_path(self, str_ds_name:str, str_container:str, str_path:str, file_type:str = '.parquet', method:int = 0, qtde_dias_anteriores_checagem:int = 360, qtde_dias_posteriores_checagem:int = 1, str_api:str = None):
        try:
            '''
            datalakecoolinovacaodev
            dadocrucooldevinovacao
            dossie/dossie/pdf/pt_dt_carga=2023-09-28

            method = 0 >> procurar usando BlobServiceClient
            method = 1 >> procurar usando BlobClient
            '''
            logging.info('Iniciou pegar ultimo parquet')

            logging.info('Desliga log Azure')
            
            if method == 0:
                logging.info('Instancia BlobServiceClient')
                blob_service_client:BlobServiceClient = self.initialize_storage_account_ad_blob_service(str_fs_name=str_ds_name, str_container_name = str_container, str_file_name=str_path)

                logging.info('Lê container')
                container_client = blob_service_client.get_container_client(str_container)

                logging('Lista blobs')
                blob_list = container_client.list_blobs(name_starts_with=str_path)

                last_file = None
                ultima_data = None
                
                logging.info('Percorre blobs para identificar data da última pasta')
                for blob in blob_list:
                    logging.info('Blob Atual >> ' + blob.name)
                    if blob.size > 0 and blob.name.endswith(file_type):
                        data_str = blob.name.split('pt_dt_carga=')[1].split('/')[0].replace('_', '-')
                        data_arquivo = pd.to_datetime(data_str, format='%Y-%m-%d', errors='coerce')
                        if data_arquivo is not None:
                            if ultima_data is None or data_arquivo > ultima_data:
                                ultima_data = data_arquivo
                                last_file = os.path.dirname(blob.name)
                    else:
                        if blob.name.endswith(file_type):
                            self.initialize_storage_account_ad_dfs(str_ds_name)

                            directory_client = self.get_directory_data_lake(str_container, self.tabelas[0]["AR_CORR"] + '/' + str_api)
                            new_sub_path = 'pt_dt_carga=' + str(datetime.now().strftime('%Y-%m-%d'))
                            directory_client.create_sub_directory(new_sub_path)

                            directory_client = self.get_directory_data_lake(str_container, blob.name)                           
                            new_path = self.gera_caminho_pasta_cloud(self.tabelas[0]["AR_CORR"] + '/' + str_api)
                            new_dir_name = new_path + '/' + blob.name.split("/")[len(blob.name.split("/"))-1]
                            directory_client.rename_directory(new_name=f"{directory_client.file_system_name}/{new_dir_name}")
            else: #method == 1
                data_atual = datetime.now()
                data_atual = data_atual.replace(hour=0, minute=0, second=0, microsecond=0)
                data_atual = data_atual + timedelta(days=qtde_dias_posteriores_checagem) # por precaução checa um dia a frente ou mais, dependendo do parâmetro

                data_checagem = data_atual

                last_file = None

                logging.info('Checa os blobs existem de ' + data_atual.__format__('%Y-%m-%d') + ' até ' + (data_atual - timedelta(days=qtde_dias_anteriores_checagem)).__format__('%Y-%m-%d'))
                while last_file == None:
                    str_path_checagem = (str_path if str_path[-1:] == '/' else str_path + '/')
                    str_path_checagem = str_path_checagem + 'pt_dt_carga=' + data_checagem.__format__('%Y-%m-%d')
                    
                    logging.info('Checa blob ' + str_path_checagem)

                    blob_client:BlobClient = self.initialize_storage_account_ad_blob(str_fs_name=str_ds_name, str_container_name = str_container, str_file_name=str_path_checagem)
                    
                    if blob_client.exists():
                        last_file = str_path_checagem

                        logging.info('Blob localizado')

                    data_checagem = data_checagem - timedelta(days=1)

                    delta:timedelta = data_atual - data_checagem

                    if delta.days >= qtde_dias_anteriores_checagem:
                        logging.info('Nenhum blob foi localizado no range de datas')
                        break
            
            logging.info(f'Ultima pasta - {str(last_file)}')                
            
            if last_file is not None:
                logging.info('Lê arquivos da última pasta')

                if file_type == '.csv':
                    df = self.read_csv_from_data_lake(str_ds_name, str_container, last_file, str_file=None)
                else:
                    df = self.read_parquet_from_data_lake(str_ds_name, str_container, last_file, str_file=None)
                
                logging.info(f'Ultima pasta - {last_file}')

                return df
            else:                
                raise Exception("Nenhum arquivo " + file_type + " encontrado na pasta com o padrão especificado.")

        except Exception as e:
            raise Exception("Error. " + self.__UTIL__.get_atual_function_name(False), e)