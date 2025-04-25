import os
import pandas as pd
import re
from bs4 import BeautifulSoup
from requests import get
from datetime import datetime

import logging
from utils import utils

class ReceitaDadosPublicos():
    
    def __init__(self, debug:bool = False):
        r""" Contrutor da classe
        """
        self.utils = utils()
        try:
            self.app = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
        except:
            self.app = __name__
            
        self.postgres_hook=     None
        self.postgres_cursor=   None
        self.postgres_session=  None
        self.postgres_psycopg=  None 
        self.postgres_session=  None 

        self.__DT_EXECUCAO__ = None
        self.__DEBUG__ = debug    
        self.__log_ativo__ = True

    @property
    def caminhos_cloud(self):
        return [
            {
                "str_fs_name": "datalakedev",
                "str_container": "dadopedrodev",
                "str_path": "receita_dados_publicos",
                "str_path_ctr": "controle/tb_controle_receita_dados_publicos",
                "str_file": "receita_dados_publicos.csv",
            }
        ]

    @property
    def caminhos_site(self):
        return [
            {
                "str_download_file": "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/",
                "str_download_file_rt": "https://arquivos.receitafederal.gov.br/dados/cnpj/regime_tributario/",
            }
        ]
    
    @property
    def estabelecimento(self):
        return 'Estabelecimentos{0}.zip'
    
    @property
    def empresa(self):
        return 'Empresas{0}.zip'
    
    @property
    def socios(self):
        return 'Socios{0}.zip'
    
    @property
    def regime_tributario(self):
        return '{0}.zip'
    
    @property
    def dados(self):
        return '{0}.zip'
    
    @property
    def periodos_n(self):
        return ['0','1','2','3','4','5','6','7','8','9']
    
    @property
    def periodos_dados(self):
        return ['Cnaes','Naturezas','Paises','Motivos','Municipios','Simples','Qualificacoes']
    
    @property
    def periodos_rt(self):
        return ['Imunes e isentas','Lucro Arbitrado','Lucro Presumido','Lucro Real']

    def confere_data_execucao(self) -> bool:

        try:
            logging.info('Inicio check_dia_execucao')

            b_retorno:bool = False
            dt_mod_site = self.captura_data_site()

            logging.info('Inicio get data cloud')

            dt_upd_cloud: datetime = None

            try:
                df_ctr = self.__UTIL_AZ__.last_parquet_on_path(self.caminhos_cloud[0]["str_fs_name"], self.caminhos_cloud[0]["str_container"], self.caminhos_cloud[0]["str_path_ctr"])
                df_ctr = df_ctr.sort_values(by=['dt_modificacao'], ascending=False)

                dt_upd_cloud = datetime.strptime(str(df_ctr['dt_modificacao'][0]), '%Y-%m-%d %H:%M')
            
            except:
                logging.info('Arquivo ou pasta não econtrado Cloud')
            
            if dt_upd_cloud != None and (dt_mod_site > dt_upd_cloud):
                b_retorno = True 
            else:
                b_retorno = False

            return b_retorno
        
        except Exception as e:
            raise Exception("Error. " + self.utils.get_atual_function_name(False) , e)
        
    def captura_data_site(self) -> datetime:

        try:
            logging.info('Inicio get data site')

            links:dict = {'DA': 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/', 'RT': 'https://arquivos.receitafederal.gov.br/dados/cnpj/regime_tributario/'}

            vet_arquivos_datas = []
            logging.info('Recolhendo informações do site')
            for var_links in links:
                link = get(links[var_links])
                link.encoding ='utf-8'

                soup = BeautifulSoup(link.text, 'html.parser')
                posts = soup.find_all('tr')
                logging.info('Formatando data')
                for tr in posts:
                    if 'td' in str(tr):
                        if re.search(r'(\d+-\d+-\d+)',str(tr)):
                            vet_arquivos_datas.append([str(tr('td')[1]),str(tr('td')[2])])
            logging.info('Inicio get data')
            vet_arquivos_datas = list(map(lambda row: [str(row[0]).replace('<td>', '').replace('</td>', ''), str(row[1]).replace(' ', '').replace('<td>', '').replace('</td>', '')], vet_arquivos_datas))
            vet_arquivos_datas = list(map(lambda row:[row[0][row[0].index('>')+1:].replace('</a> ','').replace('</a>',''), row[1][row[1].index('>')+1:]], vet_arquivos_datas))
            vet_arquivos_datas = list(map(lambda row:[row[0],row[1][:10] +' '+ row[1][-5:]], vet_arquivos_datas))  
            
            logging.info('Separando data de um arquivo')
            dt_arquivo = list(filter(lambda row: row[0] == 'Empresas0.zip', vet_arquivos_datas))[0][1]
            dt_arquivo_modificado = datetime.strptime(dt_arquivo, "%Y-%m-%d %H:%M")

            return dt_arquivo_modificado
        
        except Exception as e:
            raise Exception("Error. " + self.utils.get_atual_function_name(False), e)
        
    def baixar_receita_dados_publicos(self):

        try:
            nm_funcao = self.utils.get_atual_function_name(fl_retorna_param = False)

            logging.info("Iniciando Processamento Download receita dados publicos")

            logging.info("Formatando os nomes dos arquivos")
            # Formatando nomes
            for period in self.periodos_n + self.periodos_dados + self.periodos_rt :
                logging.info("Baixando os arquivos de acordo com os nomes corretos")
                file_name_1 = self.empresa.format(period)
                file_name_2 = self.socios.format(period)
                file_name_3 = self.estabelecimento.format(period)
                file_name_4 = self.dados.format(period) if period in self.periodos_dados else ''
                file_name_5 = self.regime_tributario.format(period) if period in self.periodos_rt else ''
                # Pastas onde os arquivos se encontram na CLOUD
                pasta_arquivos = {
                    'Empresas0.zip': 'tb_empresa', 'Empresas1.zip': 'tb_empresa','Empresas2.zip': 'tb_empresa', 'Empresas3.zip': 'tb_empresa','Empresas4.zip': 'tb_empresa','Empresas5.zip': 'tb_empresa',  'Empresas6.zip': 'tb_empresa', 'Empresas7.zip': 'tb_empresa','Empresas8.zip': 'tb_empresa','Empresas9.zip': 'tb_empresa'
                    ,'Socios0.zip': 'tb_socios', 'Socios1.zip': 'tb_socios', 'Socios2.zip': 'tb_socios', 'Socios3.zip': 'tb_socios', 'Socios4.zip': 'tb_socios', 'Socios5.zip': 'tb_socios', 'Socios6.zip': 'tb_socios', 'Socios7.zip': 'tb_socios', 'Socios8.zip': 'tb_socios', 'Socios9.zip': 'tb_socios'
                    ,'Estabelecimentos0.zip': 'tb_estabelecimento','Estabelecimentos1.zip': 'tb_estabelecimento','Estabelecimentos2.zip': 'tb_estabelecimento','Estabelecimentos3.zip': 'tb_estabelecimento','Estabelecimentos4.zip': 'tb_estabelecimento','Estabelecimentos5.zip': 'tb_estabelecimento','Estabelecimentos6.zip': 'tb_estabelecimento','Estabelecimentos7.zip': 'tb_estabelecimento','Estabelecimentos8.zip': 'tb_estabelecimento','Estabelecimentos9.zip': 'tb_estabelecimento'
                    ,'Motivos.zip':'tb_motivo'
                    ,'Municipios.zip':'tb_municipio'
                    ,'Naturezas.zip':'tb_natureza'
                    ,'Paises.zip':'tb_pais'
                    ,'Qualificacoes.zip': 'tb_qualificacao'
                    ,'Simples.zip': 'tb_simples'
                    ,'Imunes e isentas.zip': 'tb_imunes_isentas'
                    ,'Lucro Arbitrado.zip': 'tb_lucro_arbitrado'
                    ,'Lucro Presumido 1.zip': 'tb_lucro_presumido'
                    ,'Lucro Real.zip': 'tb_lucro_real'
                    ,'Cnaes.zip': 'tb_cnae'
                }

                # Criando um loop para gerar ir de um por um nos arquivos e fazer o download até o ultimo da lista
                for file_name in [file_name_1, file_name_2, file_name_3, file_name_4, file_name_5]:
                    logging.info("Comparando data do arquivo o data teste")

                    if file_name is None:
                        continue
                    # Separação dos links do regime tributario para os demais arquivos
                    if file_name in file_name_5:
                        LINK = self.caminhos_site[0]["str_download_file_rt"]
                    else:
                        LINK = self.caminhos_site[0]["str_download_file"]
                    logging.info("Fazendo download do arquivo")                    

                    # Determinar a pasta de destino com base no nome do arquivo
                    # Caso exista o nome do arquivo em pasta_arquivos segue o código se não existir o file_name em pasta_arquivos retornara None e voltará para o inicio do código
                    pasta_destino = pasta_arquivos.get(file_name)
                    if pasta_destino is None:
                        logging.info(texto_log=f"Arquivo não encontrado para o período {period}")
                        continue  # Volta para o início do loop se o nome do arquivo não estiver na pasta_arquivos
                    str_path_upload:str = self.utils.gera_caminho_pasta_cloud(pasta_destino)

                    # Download sendo efetuado diretamente na CLOUD
                    if str_path_upload != '':
                        self.utils.download_to_data_lake(self.caminhos_cloud[0]["str_fs_name"], self.caminhos_cloud[0]["str_container"] +'/'+ self.caminhos_cloud[0]["str_path"] +'/'+ str_path_upload, file_name, f'{LINK}{file_name}')
                    else:
                        logging.info('Caminho cloud não encontrado')
                    
                logging.info("Fim do processo de download")
        
        except Exception as e:
            raise Exception("Error. " + self.utils.get_atual_function_name(False), e)

    def cria_arquivo_controle(self):
        try:
            logging.info('Inicio gera ardquivo controle')

            dt_mod_site = self.captura_data_site()
            caminhos_cloud = self.caminhos_cloud

            self.__UTIL_AZ__.gera_arquivo_controle(dt_mod_site, caminhos_cloud)

        except Exception as e:
            raise Exception("Error. " + self.utils.get_atual_function_name(False) , e)
    
if __name__ == '__main__':
    service=ReceitaDadosPublicos(True)
    service.confere_data_execucao()
    service.captura_data_site()
    service.baixar_receita_dados_publicos()
    service.cria_arquivo_controle()