import threading
from concurrent.futures import ThreadPoolExecutor, wait
from pyspark.sql.functions import *
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
import jaydebeapi
import os
import sys
import pathlib
from time import sleep

if getattr(sys, 'frozen', False):
    APP_HOME = os.path.dirname(sys.executable)
else:
    APP_HOME = os.path.dirname(os.path.abspath(__file__))
    APP_HOME = pathlib.Path(APP_HOME).parent

def threaded(func):
    def wrapper(*args, **kwargs):
        return threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True).start()
    return wrapper

def make_txt_file(name, body):
    FILE_PATH = os.path.join(APP_HOME, f'manual_migrations/{name}.txt')
    with open(FILE_PATH, "w") as file:
        file.write(body)
        file.close()

class ETL_session(tk.Toplevel):

    # Dicionário para organizar migração paralela
    dependency_futures = {}
    delayed_tables = []
    delayed_sources = []
    S = threading.Semaphore()
    _state = 'idle'

    def __init__(self, master, pg_conf, ora_conf, user, tables, sources, etl, pg_jar, ora_jar, schema = None):
        try:
            super().__init__(master)
            self.protocol('WM_DELETE_WINDOW', self.stop_etl)
            # Strings de conexão de acordo com o padrão jdbc
            # pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}?user={pg_user}&password={pg_password}"
            self.pg_url = f"jdbc:postgresql://{pg_conf['host']}:{pg_conf['port']}/{pg_conf['database']}"
            # ora_url = f"jdbc:oracle:thin:{ora_user}/{ora_password}@//{ora_host}:{ora_port}/{ora_database}"
            self.ora_url = f"jdbc:oracle:thin:@//{ora_conf['host']}:{ora_conf['port']}/{ora_conf['service']}"
            self.pg_driver = "org.postgresql.Driver"
            self.ora_driver = "oracle.jdbc.driver.OracleDriver"
            self.ora_user = ora_conf['user']
            self.ora_password = ora_conf['password']
            self.pg_user = pg_conf['user']
            self.pg_password = pg_conf['password']
            self.user = user
            self.tables = tables
            self.sources = sources
            self.etl = etl
            self.schema = schema
            self.master = master
            # Conexões genéricas para execução de DML/DDL (PySpark apenas executa queries para coleta de dados)
            # (auto-commit ON, usar a função commit() ou fetch() em operações DML/DDL causará erro,
            #  operações DML/DDL executadas seram aplicadas na base de dados automaticamente!)
            self.pg_conn = jaydebeapi.connect(self.pg_driver, self.pg_url, [pg_conf['user'], pg_conf['password']], pg_jar)
            self.oracle_conn = jaydebeapi.connect(self.ora_driver, self.ora_url, [ora_conf['user'], ora_conf['password']], ora_jar)
        except Exception as e:
            print("Erro de conexão: " + str(e))

    def draw_app_window(self):
        self.geometry('500x600')
        self.information_label = ttk.Label(self, text='Migração em progresso...')
        self.information_label.pack(pady=10)

        self.information_display = ScrolledText(self, wrap=tk.WORD, state='disabled')
        self.information_display.config(height=20, width=50)
        self.information_display.pack(padx=10, pady=10)

        self.button = ttk.Button(self, text='Cancelar', command=self.stop_etl)
        self.button.pack(pady=10, side='bottom')

        self.done_drawing = True

    @threaded
    def write2display(self, msg):
        self.S.acquire()
        if msg and self.winfo_exists():
            self.information_display.config(state='normal')
            self.information_display.insert(tk.END, msg + '\n')
            self.information_display.see('end')
            self.information_display.config(state='disabled')
        self.S.release()

    def stop_etl(self):
        for process in self.dependency_futures.values():
            if not process.done():
                self._state = 'failed'
            process.cancel()
            del process
        self.S.acquire()
        self.pg_conn.close()
        self.oracle_conn.close()
        self.destroy()
        self.S.release()

    # Começa a extrair os dados das tabelas do cluster cujo foi estabelecida a conexão
    @threaded
    def start_etl(self):
        try:
            self._state = 'executing'
            self.done_drawing = False
            self.draw_app_window()
            while not self.done_drawing:
                pass
            query = "SELECT nspname FROM pg_catalog.pg_namespace"
            pg_schemas = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
            pg_schemas = [pg_schemas[i]['nspname'] for i in range(len(pg_schemas))]
            if self.schema not in pg_schemas:
                cur = self.pg_conn.cursor()
                cur.execute(f"CREATE SCHEMA {self.schema}")
                cur.close()
                self.write2display(f'Schema {self.schema} criado no Postgres!')
            query = f'''SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}' '''
            self.pg_tables = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
            query = f'''SELECT proname FROM pg_proc p join pg_namespace n on n.oid = p.pronamespace where nspname = '{self.schema}' '''
            self.pg_source = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
            # Executa extração de cada tabela em threads assincronas
            # O número no método define quantas tabelas serão extraídas simultaneamente
            # Aumentar este valor fará o processo mais rápido mas poderá causar instabilidades
            with ThreadPoolExecutor(5) as executor:
                for table in self.tables:
                    self.dependency_futures[table] = executor.submit(self.extract_table, table)
                wait(list(self.dependency_futures.values()))
                while len(self.delayed_tables) > 0:
                    for table in self.delayed_tables:
                        self.dependency_futures[table] = executor.submit(self.extract_table, table)
                        self.delayed_tables.remove(table)
                    wait(list(self.dependency_futures.values()))
                for source in self.sources:
                    self.dependency_futures[source[0]] = executor.submit(self.extract_source, source)
                wait(list(self.dependency_futures.values()))
                while len(self.delayed_sources) > 0:
                    for source in self.delayed_sources:
                        self.dependency_futures[source[0]] = executor.submit(self.extract_source, source)
                        self.delayed_sources.remove(source)
                    wait(list(self.dependency_futures.values()))
            sleep(1)
            self.write2display('Concluído!')
            self.information_label.config(text='Migração concluída!')
            self.button.config(text='Ok')
            self._state = 'success'
        except Exception as e:
            self.write2display('Migração falhou!')
            self._state = 'failed'
            self.error_message = e

    # Coleta informações sobre uma tabela do cluster
    def extract_table(self, table_name):
        self.write2display(f'Coletando {table_name}...')
        table_data = {}
        # Pega informações sobre as colunas da tabela
        data = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, dbtable=f'{self.user}.{table_name}').load()
        # Muda os nomes das tabelas para letras minusculas
        data = data.select([col(x).alias(x.lower()) for x in data.columns])
        table_data['data'] = data
        # Encontra as chaves primarias
        pk_query = f"SELECT cols.table_name, cons.constraint_name, cols.column_name \
                    FROM all_cons_columns cols, all_constraints cons \
                    WHERE cols.owner = '{self.user}' AND cons.owner = '{self.user}' AND cols.table_name = '{table_name}' AND cons.table_name = '{table_name}' \
                    AND cons.constraint_type = 'P' \
                    AND cons.constraint_name = cols.constraint_name \
                    AND cols.owner = cons.owner"
        pk = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=pk_query).load().collect()
        if pk:
            pk = [pk[i]['COLUMN_NAME'] for i in range(len(pk))]
            table_data['pk'] = pk
        # Encontra as chaves estrangeiras
        fk_query = f"SELECT cols.table_name, cons.constraint_name, cols.column_name \
                    FROM all_constraints cons, all_cons_columns cols \
                    WHERE cols.owner = '{self.user}' AND cons.owner = '{self.user}' \
                    AND cols.table_name = '{table_name}' AND cons.table_name = '{table_name}' \
                    AND cons.constraint_type = 'R' \
                    AND cons.constraint_name = cols.constraint_name \
                    AND cols.owner = cons.owner \
                    ORDER BY cols.table_name"
        fk = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=fk_query).load().collect()
        fk_dict = {}
        # Organiza as chaves estrangeiras em relação aos nomes de seus constraints
        for key in fk:
            if key['CONSTRAINT_NAME'] in fk_dict.keys():
                fk_dict[key['CONSTRAINT_NAME']].append(key['COLUMN_NAME'])
            else:
                fk_dict[key['CONSTRAINT_NAME']] = [key['COLUMN_NAME']]
        if fk:
            # Encontra as referencias das chaves estrangeiras
            table_data['fk'] = {}
            for constraint, columns in fk_dict.items():
                ref_query = f"SELECT cols.table_name, cols.column_name, cons.delete_rule, cols.owner \
                            FROM all_cons_columns cols, all_constraints cons \
                            WHERE cols.constraint_name in \
                            (SELECT r_constraint_name FROM all_constraints \
                            WHERE owner = '{self.user}' AND constraint_name = '{constraint}') \
                            AND cols.constraint_name = cons.r_constraint_name \
                            AND cons.owner = '{self.user}' AND cons.table_name = '{table_name}'"
                ref = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=ref_query).load().collect()
                ref_columns = [ref[i]['COLUMN_NAME'] for i in range(len(ref))]
                table_data['fk'][constraint] = {'src_table': table_name,'src_column': columns,'ref_table': ref[0]['TABLE_NAME'], 'ref_column': ref_columns, 'on_delete': ref[0]['DELETE_RULE']}
        # Coleta as dependencias da tabela
        dp_query = f"select name, type, referenced_name, referenced_type from all_dependencies where name in (select name from all_dependencies where referenced_name = '{table_name}')"
        dependencies = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=dp_query).load().collect()
        # Coleta as informações especificas às colunas
        tab_query = f"select column_name, data_type, data_default from all_tab_columns where table_name = '{table_name}'"
        cdata = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=tab_query).load().collect()
        # Identifica e classifica as dependencias
        for dep in dependencies:
            # Chaves primarias com auto incremento definida com trigger
            if dep['TYPE'] == 'TRIGGER' and dep['REFERENCED_TYPE'] == 'SEQUENCE':
                if len(pk) == 1:
                    table_data['auto'] = pk[0]
                else:
                    query = f"select column_name from all_triggers where trigger_name = '{dep['NAME']}'"
                    column = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
                    if column[0]:
                        table_data['auto'] = column[0]
                table_data['seq'] = dep['REFERENCED_NAME']
        for column in cdata:
            # Chave primaria com auto incremento definida com IDENTITY
            if column['DATA_DEFAULT']:
                table_data['auto'] = pk[pk.index(column['COLUMN_NAME'])]
                table_data['seq'] = column['DATA_DEFAULT'].split('.')[1]
        if table_name in self.pg_tables:
            self.write2display(f'{table_name} já existe no postgres, substituindo...')
        if fk:
            for value in table_data['fk'].values():
                # Força a extração de uma tabela dependente caso não já tenha iniciado
                if self.dependency_futures[value['ref_table']]._state == 'PENDING' and value['ref_table'] != table_name:
                    self.write2display(f"Tabela {table_name} depende da tabela {value['ref_table']}, adiando extração...")
                    self.delayed_tables.append(table_name)
                    return
                # Espera a tabela a qual depende ser carregada
                if not self.dependency_futures[value['ref_table']].done() and value['ref_table'] != table_name:
                    wait([self.dependency_futures[value['ref_table']]])
        self.write2display(f"Extração concluída na tabela {table_name}")
        self.load_table(table_data, table_name.lower())
        return

    # Carrega dados de uma tabela para a base de dados alvo
    def load_table(self, df, tbl):
        # Estabelece conexão genérica
        cur = self.pg_conn.cursor()
        if tbl in [self.pg_tables[i]['table_name'] for i in range(len(self.pg_tables))]:
            self.write2display(f"Tabela {tbl} já existe no schema {self.schema}, substituindo...")
            cur.execute(f"DROP TABLE {self.schema}.{tbl} CASCADE")
        self.write2display(f"Carregando {df['data'].count()} colunas da tabela {self.schema}.{tbl}...")
        # Carrega a informação extraida sem dependencias ou constraints
        df['data'].repartition(5).write.mode('overwrite').format('jdbc').options(url=self.pg_url, user=self.pg_user, password=self.pg_password, driver=self.pg_driver, dbtable=f'{self.schema}.{tbl}', batch=1000000).save()
        self.write2display(f'Carregando dependencias da tabela {tbl}...')
        # Adiciona dependencia de chave primária
        pk_columns = self.list2str(df['pk'])
        cur.execute(f"ALTER TABLE {self.schema}.{tbl} ADD PRIMARY KEY {pk_columns}")
        if df['auto']:
            last_val = df['data'].agg({df['auto']: 'max'}).collect()[0]
            cur.execute(f'''CREATE SEQUENCE IF NOT EXISTS {tbl}_{df['auto'].lower()}_seq START WITH {int(last_val[f"max({df['auto']})"])}''')
            cur.execute(f'''ALTER TABLE {self.schema}.{tbl} ALTER COLUMN "{df['auto'].lower()}" SET DEFAULT nextval('{tbl}_{df['auto'].lower()}_seq')''')
            cur.execute(f"ALTER SEQUENCE {tbl}_{df['auto'].lower()}_seq OWNER TO postgres")
        # Adiciona dependencias de chaves estrangeiras
        try:
            for key, value in df['fk'].items():
                cur.execute(f"ALTER TABLE {self.schema}.{value['src_table'].lower()} ADD CONSTRAINT {key} FOREIGN KEY {self.list2str(value['src_column'])} REFERENCES {value['ref_table'].lower()} {self.list2str(value['ref_column'])} ON DELETE {value['on_delete']}")
        except:
            pass
        self.write2display(f"{df['data'].count()} colunas importadas da tabela {tbl} para postgres!")
        cur.close()

    def list2str(self, lista):
        res = '('
        for value in lista:
            res += str(value.lower())
            if value != lista[-1]:
                res += ','
        res += ')'
        return res
    
    # Organizador para extração paralela de sources
    def extract_source(self, data):
        name = data[0]
        type = data[1]
        self.write2display(f'Extraindo {type} {name}')
        if type == 'VIEW':
            query = f"SELECT text FROM all_views WHERE owner = '{self.user}' AND view_name = '{name}'"
        else:
            query = f"SELECT text, line FROM all_source WHERE owner = '{self.user}' AND name = '{name}' ORDER BY line"
        source = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        query = f"SELECT referenced_name, referenced_type FROM all_dependencies WHERE owner = '{self.user}' AND name = '{name}' AND (referenced_type = 'PROCEDURE' OR referenced_type = 'FUNCTION' OR referenced_type = 'VIEW')"
        dependencies = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        if type == 'VIEW':
            source_body = [f"{self.normalize_name(name)} AS {source[0]['TEXT']}"]
        else:
            source_body = [source[i]['TEXT'] for i in range(len(source))]
        dependency = [[dependencies[i]['REFERENCED_NAME'], dependencies[i]['REFERENCED_TYPE']] for i in range(len(dependencies))]
        for dep in dependency:
            if self.dependency_futures[dep[0]]._state == 'PENDING' and dep[0] != name:
                self.write2display(f'Source {name} depende do source {dep[0]}, adiando extração...')
                self.delayed_sources.append(data)
                return
            if not self.dependency_futures[dep[0]].done() and dep[0] != name:
                wait([self.dependency_futures[dep[0]]])
        self.transform_source(source_body, type)
        return

    # Remove schema e aspas do nome de um objeto
    def normalize_name(self, name, schema = None):
        if '.' in name:
            name = name.split('.')
        if type(name) == list:
            for i in range(len(name)):
                if '"' in name[i]:
                    name[i] = name[i].removeprefix('"')
                    name[i] = name[i].removesuffix('"')
            if schema:
                return f"{schema}.{name[1]}"
            return name[1]
        elif '"' in name:
            name = name.removeprefix('"')
            name = name.removesuffix('"')
        if schema:
            return f"{schema}.{name}"
        return name

    # Traduz elementos oracle genêricos em elementos postgresql
    def transform_attributes(self, tokens):
        began = False
        for i in range(len(tokens)):
            if tokens[i].lower() == 'begin':
                began = True
                continue
            if tokens[i].lower() in ['in', 'out', 'in out'] and not began:
                aux = tokens[i]
                tokens.remove(tokens[i])
                if '(' in tokens[i-1]:
                    tokens[i-1] = tokens[i-1][1:]
                    aux = '(' + str(aux)
                else:
                    aux = ',' + str(aux)
                tokens.insert(i-1, aux)
            if tokens[i].lower() == 'cursor':
                tokens[i], tokens[i+1] = tokens[i+1], tokens[i]
                if tokens[i+2].lower() == 'is':
                    tokens[i+2] = 'FOR'
            if '.' in tokens[i]:
                tokens[i] = tokens[i].replace(':new', 'NEW')
                tokens[i] = tokens[i].replace(':old', 'OLD')
            if 'number' in tokens[i].lower():
                tokens[i] = tokens[i].lower().replace('number', 'numeric')
            if 'varchar2' in tokens[i].lower():
                tokens[i] = tokens[i].lower().replace('varchar2', 'varchar')
            if tokens[i].lower() != 'exception':
                # Remove variáveis de exceção
                if 'exception' in tokens[i].lower():
                    tokens.remove(tokens[i])
                    tokens.remove(tokens[i-1])
                # Postgresql não possui pragma
                if tokens[i].lower() == 'pragma':
                    tokens.remove(tokens[i+1])
                    tokens.remove(tokens[i])
                # Modifica funções de raise
                if tokens[i].lower() == 'raise':
                    exc_name = tokens[i+1]
                    tokens.replace(tokens[i+1], 'EXCEPTION')
                    tokens.insert(i+2, f"'EXCEPTION_{exc_name}'")
                if 'raise' in tokens[i].lower() and '(' in tokens[i]:
                    exc_aux = ''
                    while ')' not in tokens[i]:
                        exc_aux += tokens[i] + ' '
                        tokens.remove(tokens[i])
                    exc_aux += tokens[i]
                    tokens.remove(tokens[i])
                    aux_line = f"RAISE EXCEPTION '{exc_aux}'".split()
                    aux_line.reverse()
                    for token in aux_line:
                        tokens.insert(i, token)

    def transform_system_function(self, tokens):
        # Muda o formato de algumas system calls do Oracle para Postgresql
        for i in range(len(tokens)):
            # Muda dbms_output para raise notice
            if 'dbms_output' in tokens[i].lower():
                end = start = i
                while end < len(tokens):
                    if '(' in tokens[end]:
                        if tokens[end].count(')') > 1:          
                            break
                    elif ')' in tokens[end] or ';' in tokens[end]:
                        break
                    end += 1
                # Delimita o escopo da função e a separa do corpo
                data = tokens[start:end+1]
                data = ' '.join(data)
                text_data = data.split('\'')
                attribute_data = data.split('||')
                for text in text_data:
                    if 'dbms_output' in text or '||' in text:
                        text_data.remove(text)
                for attribute in attribute_data:
                    if '\'' in attribute:
                        attribute_data.remove(attribute)
                new_line = "RAISE NOTICE "
                new_line += '\''
                for text in text_data:
                    new_line += text + ' %'
                new_line += '\''
                for attribute in attribute_data:
                    if ')' in attribute:
                        attribute = attribute[:attribute.index(')')] + ';'
                    new_line += ',' + attribute
                # Remove os tokens modificados e insere os novos
                new_line = new_line.split()
                new_line.reverse()
                for j in range(start, end+1):
                    tokens.pop(start)
                for token in new_line:
                    tokens.insert(start, token)
                self.transform_system_function(tokens)
                break
            # Muda dbms_lock para pg_sleep
            if 'dbms_lock' in tokens[i].lower():
                start = tokens[i].index('(')
                end = tokens[i].index(')')
                delay = tokens[i][start+1:end]
                tokens.remove(tokens[i])
                tokens.insert(i, f"pg_sleep({delay});")
                tokens.insert(i, 'PERFORM')
                self.transform_system_function(tokens)
                break

    # Postgresql só executa trigger com chamada de função
    def create_trigger_function(self, data, name, attributes = None):
        fn_name = f"fn_{name}"
        func_body = f"CREATE OR REPLACE FUNCTION {fn_name}() RETURNS TRIGGER LANGUAGE PLPGSQL AS \n$$\n"
        for i, token in enumerate(data):
            if i == 0:
                # Funções de trigger no postgres não podem receber parâmetros diretamente
                # Força inicialização de parâmetros na área de declarações
                if 'declare' in token.lower():
                    func_body += 'DECLARE\n'
                    if attributes:
                        k = 0
                        for name, type in attributes.items():
                            func_body += f"{name} {type} := TG_ARGV[{k}];\n"
                            k += 1
                    continue
            if token.lower() == 'from':
                if data[i+1].lower() == 'dual;':
                    func_body += ';'
                    continue
            if token.lower() == 'dual;':
                continue
            if token.lower() in ['exception', 'end', 'end;']:
                func_body += 'RETURN NEW;\n'
                break
            func_body += token + ' '
            if token[-1] == ';' or token.lower() == 'begin':
                func_body += '\n'
        func_body += '\nEND;\n$$;'
        cur = self.pg_conn.cursor()
        cur.execute(func_body)
        cur.close()
        self.write2display(f"Bloco plsql encapsulado na função {fn_name} e carregado no Postgresql!")
        return fn_name

    # Cria uma função de trigger baseada em uma função/procedure genérica
    def adapt2trig(self, src_name):
        query = f"SELECT text FROM all_source WHERE owner = '{self.user}' AND name = '{src_name.upper()}' ORDER BY line"
        data = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        data_body = [data[i]['TEXT'] for i in range(len(data))]
        tokens = []
        # Quebra o corpo dos dados em tokens para processamento
        for i, line in enumerate(data_body):
            line = line.split()
            # Checa de o nome está concatenado com os atributos
            if i == 0:
                if '(' in line[1]:
                    name = line[1][:line[1].index('(')]
                    source_name = self.normalize_name(name)
                    line[1] = line[1][line[1].index('(')-1:]
                    line.insert(1, source_name)
                else:
                    source_name = self.normalize_name(line[1])
                    line[1] = source_name
            for token in line:
                tokens.append(token)
        self.transform_attributes(tokens)
        self.transform_system_function(tokens)
        attributes = {}
        i = 2
        while ')' not in tokens[i-1]:
            if '(' in tokens[i]:
                tokens[i] = tokens[i][1:]
            if tokens[i].lower() in ['in', 'out', 'in out']:
                if ')' in tokens[i+2]:
                    attributes[tokens[i+1]] = tokens[i+2][:-1]
                else:
                    attributes[tokens[i+1]] = tokens[i+2]
                i += 3
            else:
                if ')' in tokens[i+1]:
                    attributes[tokens[i]] = tokens[i+1][:-1]
                else:
                    attributes[tokens[i]] = tokens[i+1]
                i += 2
        while tokens[i].lower() not in ['as', 'is']:
            i += 1
        if tokens[i+1].lower() != 'begin':
            tokens.insert(i+1, 'DECLARE\n')
        return self.create_trigger_function(tokens[i+1:], source_name, attributes)

    def search4loop(self, tokens):
        began = False
        looper = None
        for i in range(len(tokens)):
            if tokens[i].lower() == 'begin':
                began = True
            if not began:
                continue
            else:
                if tokens[i].lower() == 'for':
                    looper = tokens[i+1]
                    break
        return looper

    # TO DO: Detectar funções internas e trata-las de alguma forma
    def transform_source(self, data, type):
        source_body = f"CREATE OR REPLACE {type} "
        tokens = []
        # Quebra o corpo dos dados em tokens para processamento
        for i, line in enumerate(data):
            line = line.split()
            # Checa de o nome está concatenado com os atributos
            if i == 0:
                if type != 'VIEW':
                    if '(' in line[1]:
                        name = line[1][:line[1].index('(')]
                        norm_name = self.normalize_name(name)
                        line[1] = line[1][line[1].index('('):]
                        line.insert(1, norm_name)
                    else:
                        norm_name = self.normalize_name(line[1])
                        line[1] = norm_name
                else:
                    norm_name = self.normalize_name(line[0])
            for token in line:
                tokens.append(token)
        self.transform_attributes(tokens)
        self.transform_system_function(tokens)
        looper = self.search4loop(tokens)
        source_body += norm_name + ' '
        began = False
        factory_func = False
        cursor = None
        unsupported = None
        if type == 'TRIGGER':
            for i in range(2, len(tokens)):
                if 'DBMS_' in tokens[i]:
                    unsupported = tokens[i]
                if tokens[i].lower() == 'on':
                    self.normalize_name(tokens[i+1])
                    source_body += '\n'
                if tokens[i].lower() == 'declare' or tokens[i].lower() == 'begin':
                    # Pl/sql block trigger
                    block_data = tokens[i:]
                    func = self.create_trigger_function(block_data, norm_name)
                    source_body += f"EXECUTE FUNCTION {func}();"
                    break
                # Function call trigger
                if tokens[i].lower() == 'call':
                    # Separa nome e atributos
                    if '(' in tokens[i+1]:
                        name = tokens[i+1][:tokens[i+1].index('(')]
                        tokens[i+1] = tokens[i+1][tokens[i+1].index('('):]
                    else:
                        name = tokens[i+1]
                        tokens.remove(tokens[i+1])
                    func = self.adapt2trig(name)
                    source_body += f"EXECUTE FUNCTION {func}"
                    continue
                if i == len(tokens) - 1:
                    source_body += tokens[i] + ';'
                else:
                    source_body += tokens[i] + ' '
        elif type in ['FUNCTION', 'PROCEDURE']:
            i = 2
            while i < len(tokens):
                if 'DBMS_' in tokens[i]:
                    unsupported = tokens[i]
                if tokens[i].lower() == 'return':
                    tokens[i] = 'RETURNS'
                if tokens[i].lower() == 'begin':
                    if began:
                        # Lascou
                        factory_func = True
                    else:
                        began = True
                        source_body += 'BEGIN\n'
                        i += 1
                        continue
                if tokens[i].lower() == 'cursor':
                    cursor = tokens[i-1]
                if tokens[i].lower() in ['is', 'as']:
                    source_body += '\nLANGUAGE PLPGSQL AS\n$$\nDECLARE\n'
                    if looper and looper != cursor:
                        source_body += f'{looper} record;\n'
                    i += 1
                    continue
                if tokens[i].lower() == 'for' and began:
                    if '(' in tokens[i+3]:
                        tokens[i+3] = tokens[i+3][1:]
                        j = i+3
                        while ')' not in tokens[j]:
                            j += 1
                        tokens[j] = tokens[j][:-1]
                if tokens[i].lower() == 'execute':
                    if tokens[i+1].lower() == 'immediate':
                        source_body += 'EXECUTE'
                        i += 2
                        continue
                if tokens[i].lower() in ['exception', 'end', 'end;']:
                    if tokens[i].lower() == 'end':
                        if tokens[i+1].lower() == 'loop;':
                            source_body += 'end loop;'
                            i += 2
                            continue
                    source_body += '\n'
                    break
                source_body += tokens[i]
                if tokens[i][-1] == ';' or tokens[i].lower() == 'loop':
                    source_body += '\n'
                else:
                    source_body += ' '
                i += 1
            source_body += "EXCEPTION WHEN OTHERS THEN\nraise notice 'Transaction has failed and rolledback!';\nraise notice '% %', SQLERRM, SQLSTATE;"
            source_body += "\nEND;\n$$;"
        elif type == 'VIEW':
            for token in tokens[1:]:
                if '"' in token:
                    if ',' in token:
                        if token[-1] == ',':
                            token = token[:-1]
                        else:
                            aux_token = token.split(',')
                            for i, sub_token in enumerate(aux_token):
                                if '"' in sub_token:
                                    sub_token = sub_token.removeprefix('"')
                                    sub_token = sub_token.removesuffix('"')
                                if i < len(aux_token) - 1:
                                    sub_token += ','
                                source_body += sub_token + ' '
                            continue
                    token = token.removeprefix('"')
                    token = token.removesuffix('"')
                source_body += token + ' '
        cur = self.pg_conn.cursor()
        if factory_func:
            self.write2display(f"Função {norm_name} detectada como factory, não é possível garantir o funcionamento da migração\nFunção {norm_name} sera adicionada na pasta manual_migrations para migração manual!")
            make_txt_file(norm_name, source_body)
            return
        if unsupported:
            self.write2display(f"System call não suportada detectada em {norm_name}\n{norm_name} será adicionado em manual_migrations para migração manual!")
            make_txt_file(norm_name, source_body)
            return
        if norm_name in [self.pg_source[i]['proname'] for i in range(len(self.pg_source))]:
            self.write2display(f'Substituindo {type} {norm_name} no Postgresql...')
            cur.execute(f"DROP {type} {norm_name} CASCADE;")
        else:
            self.write2display(f'Carregando {type} {norm_name} no Postgresql...')
        try:
            print(source_body)
            cur.execute(source_body)
            cur.close()
            self.write2display(f"{type} {norm_name} carregado no Postgresql!")
        except Exception as e:
            print(f'Error: ' + str(e))