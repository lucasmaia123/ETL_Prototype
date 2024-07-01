import threading
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from copy import deepcopy
import jaydebeapi
from code.etl import ETL_session, tk, ttk
from subprocess import PIPE, Popen
import os
import sys
import pathlib
import datetime
import shlex

if getattr(sys, 'frozen', False):
    APP_HOME = os.path.dirname(sys.executable)
else:
    APP_HOME = os.path.dirname(os.path.abspath(__file__))
    APP_HOME = pathlib.Path(APP_HOME).parent
LOG_FILE_PATH = os.path.join(APP_HOME, 'logs.txt')

# Decorator para invocar função como thread
def threaded(func):
    def wrapper(*args, **kwargs):
        return threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True).start()
    return wrapper

class etl_UI(tk.Frame):

    user = None
    etl = None
    no_backup = False
    active_sessions = []

    def __init__(self, master, conf, pg_jar, ora_jar):
        self.master = master
        self.master.title('ETL APP PROTOTYPE')
        super().__init__(master)
        self.pack()
        self.conf = conf
        self.pg_jar = pg_jar
        self.ora_jar = ora_jar
        self.pg_conn = None
        self.ora_conn = None
        self.master.protocol('WM_DELETE_WINDOW', self.end_process)
        self.draw_start_menu()

    # Limpa processos da memória e fecha o app
    def end_process(self):
        if self.pg_conn:
            self.pg_conn.close()
        if self.ora_conn:
            self.ora_conn.close()
        if self.etl:
            self.etl.stop()
        self.master.quit()
        self.master.destroy()

    # Desenha menu de login
    def draw_start_menu(self):

        # limpa a janela
        for frame in self.winfo_children():
            frame.destroy()
        self.master.geometry('800x600')

        upper_frame = ttk.Frame(self)
        upper_frame.pack(side='top', expand=True, fill='x')
        middle_frame = ttk.Frame(self)
        middle_frame.pack(fill='x')
        lower_frame = ttk.Frame(self)
        lower_frame.pack(fill='x', pady=30)
        label_frame = ttk.Frame(self)
        label_frame.pack(side='bottom', fill='x')

        self.conn_mode = tk.StringVar()

        # Desenha os gadgets do app
        ttk.Label(upper_frame, text='Especifique as configurações para conexão nas bases de dados').pack(pady=20)
        ttk.Label(middle_frame, text='Oracle host').grid(row=0, column=1, padx=5, pady=10)
        self.ora_host_entry = ttk.Entry(middle_frame, width=30)
        self.ora_host_entry.grid(row=1, column=1)
        ttk.Label(middle_frame, text='Oracle port').grid(row=2, column=1, padx=5, pady=10)
        self.ora_port_entry = ttk.Entry(middle_frame, width=30)
        self.ora_port_entry.grid(row=3, column=1)
        ttk.Label(middle_frame, text='Oracle username').grid(row=4, column=1, padx=5, pady=10)
        self.ora_user_entry = ttk.Entry(middle_frame, width=30)
        self.ora_user_entry.grid(row=5, column=1)
        ttk.Label(middle_frame, text='Oracle password').grid(row=6, column=1, padx=5, pady=10)
        self.ora_password_entry = ttk.Entry(middle_frame, width=30, show='*')
        self.ora_password_entry.grid(row=7, column=1)
        ttk.Label(middle_frame, text='Oracle service Name').grid(row=8, column=1, padx=5, pady=10)
        self.ora_service_mode = ttk.Radiobutton(middle_frame, variable=self.conn_mode, value='name')
        self.ora_service_mode.grid(row=9, column=0)        
        self.ora_service_entry = ttk.Entry(middle_frame, width=30)
        self.ora_service_entry.grid(row=9, column=1)
        ttk.Label(middle_frame, text='Oracle SID').grid(row=10, column=1, padx=5, pady=10)
        self.ora_SID_mode = ttk.Radiobutton(middle_frame, variable=self.conn_mode, value='SID')
        self.ora_SID_mode.grid(row=11, column=0, padx=10)
        self.ora_SID_entry = ttk.Entry(middle_frame, width=30)
        self.ora_SID_entry.grid(row=11, column=1, padx=10)

        self.ora_service_mode.config(command=lambda:[self.ora_service_entry.config(state='normal'), self.ora_SID_entry.config(state='disabled')])
        self.ora_SID_mode.config(command=lambda:[self.ora_service_entry.config(state='disabled'), self.ora_SID_entry.config(state='normal')])

        self.ora_service_mode.invoke()

        ttk.Label(middle_frame, text='Postgresql host').grid(row=0, column=2, padx=20, pady=10)
        self.pg_host_entry = ttk.Entry(middle_frame, width=30)
        self.pg_host_entry.grid(row=1, column=2, padx=20)
        ttk.Label(middle_frame, text='Postgresql port').grid(row=2, column=2, padx=20, pady=10)
        self.pg_port_entry = ttk.Entry(middle_frame, width=30)
        self.pg_port_entry.grid(row=3, column=2, padx=20)
        ttk.Label(middle_frame, text='Postgresql username').grid(row=4, column=2, padx=20, pady=10)
        self.pg_user_entry = ttk.Entry(middle_frame, width=30)
        self.pg_user_entry.grid(row=5, column=2, padx=20)
        ttk.Label(middle_frame, text='Postgresql password').grid(row=6, column=2, padx=20, pady=10)
        self.pg_password_entry = ttk.Entry(middle_frame, width=30, show='*')
        self.pg_password_entry.grid(row=7, column=2, padx=20)
        ttk.Label(middle_frame, text='Postgresql database').grid(row=8, column=2, padx=20, pady=10)
        self.pg_database_entry = ttk.Entry(middle_frame, width=30)
        self.pg_database_entry.grid(row=9, column=2, padx=20)

        test_button = ttk.Button(lower_frame, text='Testar conexão')
        test_button.pack(side='left')
        next_button = ttk.Button(lower_frame, text='Prosseguir')
        next_button.pack(side='right')

        response_label = ttk.Label(label_frame, text='')
        response_label.grid(row=6, column=0)

        test_button.config(command=lambda: self.initialize_conn(response_label, True))
        next_button.config(command=lambda: self.initialize_conn(response_label))

    # Coleta os atributos que o usuário forneceu e cria variáveis de conexão
    def initialize_conn(self, response, test = False):
        
        self.ora_driver = "oracle.jdbc.driver.OracleDriver"
        self.pg_driver = "org.postgresql.Driver"

        self.ora_host = self.ora_host_entry.get()
        self.ora_port = self.ora_port_entry.get()
        self.ora_user = self.ora_user_entry.get()
        self.ora_password = self.ora_password_entry.get()
        if self.conn_mode.get() == 'name':
            self.ora_service = self.ora_service_entry.get()
        if self.conn_mode.get() == 'SID':
            self.ora_service = self.ora_SID_entry.get()

        self.pg_host = self.pg_host_entry.get()
        self.pg_port = self.pg_port_entry.get()
        self.pg_user = self.pg_user_entry.get()
        self.pg_password = self.pg_password_entry.get()
        self.pg_database = self.pg_database_entry.get()

        if '' in [self.ora_host, self.ora_port, self.ora_user, self.ora_password, self.pg_host, self.pg_port, self.pg_user, self.pg_password, self.pg_database]:
            response.config(text='Preencha todos os parâmetros de conexão!')
            return

        # Estabelece conexão ou apenas testa 
        if self.conn_test(response) and not test:
            response.config(text='Conectando...')
            self.update()
            # Inicializa uma sessão do pyspark para começar a migração
            if self.etl == None:
                self.etl = SparkSession(SparkContext(conf=self.conf))
            self.write2log(f'iniciando sessão de ETL de Oracle para Postgresql!')
            self.ora_dba = False
            if not self.ora_system_user:
                query = f"SELECT granted_role FROM dba_role_privs WHERE grantee = '{self.ora_user.upper()}'"
                priv = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
                priv = [priv[i]['GRANTED_ROLE'] for i in range(len(priv))]
                if 'DBA' in priv:
                    self.ora_dba = True
                    self.ora_system_user = True
            # Checa se é superuser no Postgresql
            query = f"SELECT usesuper FROM pg_user WHERE usename = '{self.pg_user}'"
            superuser = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
            superuser = superuser[0]['usesuper']
            if superuser:
                self.pg_superuser = True
            else:
                self.pg_superuser = False
            if self.ora_system_user:
                self.draw_schema_selection_window()
            else:
                # Caso usuário seja genérico, o schema usado tem o nome do usuário
                self.draw_table_selection_window(self.ora_user.upper())

    # Testa as credenciais de conexão fornecidas
    def conn_test(self, output):
        try:
            # Checa se é usuário admin no Oracle
            if self.ora_user.lower() in ['system', 'sys as sysdba']:
                self.ora_system_user = True
            elif self.ora_user.lower() == 'sys':
                self.ora_system_user = True
                self.ora_user = f'{self.ora_user} as sysdba'
            else:
                self.ora_system_user = False
            # Estabelece conexão genêrica
            self.pg_url = f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"
            if self.conn_mode.get() == 'name':
                self.ora_url = f"jdbc:oracle:thin:@//{self.ora_host}:{self.ora_port}/{self.ora_service}"
            if self.conn_mode.get() == 'SID':
                self.ora_url = f"jdbc:oracle:thin:@{self.ora_host}:{self.ora_port}:{self.ora_service}"
            if self.pg_conn != None:
                self.pg_conn.close()
            self.pg_conn = jaydebeapi.connect(self.pg_driver, self.pg_url, [self.pg_user, self.pg_password], self.pg_jar)
            if self.ora_conn != None:
                self.ora_conn.close()
            self.ora_conn = jaydebeapi.connect(self.ora_driver, self.ora_url, [self.ora_user, self.ora_password], self.ora_jar)
            output.config(text='Conexão estabelecida com sucesso!')
            return True
        except Exception as e:
            output.config(text='Conexão falhou!\nVerifique se as suas credenciais estão corretas!')
            print('Error: ' + str(e))
            return False

    # Cria um backup de um schema antes de realizar o ETL.
    # Considere modificar essa função para fazer backup apenas das tabelas migradas
    # caso esteja demorando muito para criar o backup de todo o schema
    def create_backup(self, schema):
        BACKUP_FILE = os.path.join(APP_HOME, f'backups/{schema}_backup.dmp')
        command = f"pg_dump --no-password -h {self.pg_host} -p {self.pg_port} -d {self.pg_database} -U {self.pg_user} -n {schema} -Fc -f {BACKUP_FILE}"
        p = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, env={'PGPASSWORD': self.pg_password})
        p.wait()
    
    # Restora ao estado antes do ETL
    def restore_from_backup(self, schema):
        BACKUP_FILE = os.path.join(APP_HOME, f'backups/{schema}_backup.dmp')
        command = f"pg_restore --no-password -h {self.pg_host} -p {self.pg_port} -d {self.pg_database} -U {self.pg_user} {BACKUP_FILE}"
        command = shlex.split(command)
        p = Popen(command, shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE, env={'PG_PASSWORD': self.pg_password})
        p.wait()

    # Escreve log no logs.txt
    def write2log(self, data):
        with open(LOG_FILE_PATH, "a") as log_file:
            data = datetime.datetime.strftime(datetime.datetime.now(), "%d/%m/%Y %H:%M:%S") + ': ' + data + '\n'
            log_file.write(data)
            log_file.close()

    # Ferramenta para mensagem de erros
    def message_window(self, message):
        window = tk.Toplevel(self.master)
        window.title('Error message')
        ttk.Label(window, text=message).pack(padx=10, pady=10)
        ttk.Button(window, text='Ok', command=lambda: window.destroy()).pack(side='bottom', pady=10)

    # Caso usuário de sistema oracle, lista os schemas da base de dados
    def draw_schema_selection_window(self):
        for frame in self.winfo_children():
           frame.destroy()
        self.master.geometry('800x600')

        # Coleta usuários (schemas em Oracle) que não são padrões do Oracle
        query = "SELECT username FROM all_users WHERE oracle_maintained = 'N'"
        users = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        users = [users[i]['USERNAME'] for i in range(len(users))]

        ttk.Label(self, text='Conectado como usuário do sistema\nSelecione o usuário/schema que deseja migrar').pack(padx=20, pady=10)
        user_frame = ttk.Frame(self, height=300)
        user_frame.pack(fill='x', expand=True)

        scrollBar = tk.Scrollbar(user_frame, orient='vertical')
        scrollBar.pack(side='right', fill='y')

        listbox = tk.Listbox(user_frame, selectmode='single', exportselection=False, yscrollcommand=scrollBar.set)
        listbox.pack(padx=10, pady=10, expand=True, fill='both')

        for user in users:
            listbox.insert(tk.END, user)

        scrollBar.config(command=listbox.yview)

        self.user_migration = tk.BooleanVar()

        lower_frame = ttk.Frame(self)
        lower_frame.pack()
        ttk.Checkbutton(lower_frame, text='Migrar usuário (Cria novo usuário e migra todos os seus objetos)', variable=self.user_migration, onvalue=True, offvalue=False).pack(side='top', pady=10)
        ttk.Button(lower_frame, text='Voltar', command=lambda:self.draw_start_menu()).pack(side='left', padx=20, pady=10)
        ttk.Button(lower_frame, text='Prosseguir', command=lambda:self.pass_schema_selection(listbox)).pack(side='right', padx=20, pady=10)

    # Função auxiliar para a seleção de schema
    def pass_schema_selection(self, listbox):
        try:
            schema = listbox.get(listbox.curselection()[0])
            self.pg_users = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query='SELECT usename FROM pg_user').load().collect()
            self.pg_users = [self.pg_users[i]['usename'] for i in range(len(self.pg_users))]
            if self.user_migration.get() and schema:
                if not self.pg_superuser:
                    self.message_window('Para migrar um usuário, por favor logue como um superuser postgres!')
                    return
                else:
                    window = tk.Toplevel(self)
                    ttk.Label(window, text=f'A seguinte ação irá criar um novo usuário {schema} e migrar todos os objetos!').pack(padx=10, pady=10)
                    if schema.lower() in self.pg_users:
                        ttk.Button(window, text='Aceitar', command=lambda:[self.user_exists(schema.lower()), window.destroy()]).pack(side='right', padx=20, pady=10)
                    else:
                        ttk.Button(window, text='Aceitar', command=lambda:[self.migrate_user_window(schema.lower()), window.destroy()]).pack(side='right', padx=20, pady=10)
                    ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)
            elif schema:
                self.draw_table_selection_window(schema)
        except Exception as e:
            print('Error: ' + str(e))

    # UI para pegar configurações do novo usuário
    def migrate_user_window(self, user, delete=False):
        window = tk.Toplevel(self)
        ttk.Label(window, text=f'Digite a senha para o novo usuário {user}:').pack(padx=10, pady=10)
        password_entry = ttk.Entry(window, width=30)
        password_entry.pack(padx=10, pady=10)
        create_db = tk.BooleanVar()
        c1 = ttk.Checkbutton(window, text='Migrar usuário para a base de dados conectada', variable=create_db, onvalue=False)
        c1.pack()
        c2 = ttk.Checkbutton(window, text='Criar nova base de dados', variable=create_db, onvalue=True)
        c2.pack()
        c1.invoke()
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)
        ttk.Button(window, text='Prosseguir', command=lambda:self.migrate_user(user, password_entry.get(), create_db.get(), delete, window)).pack(side='right', padx=20, pady=10)

    # Pergunta o que fazer quando usuário já existe
    def user_exists(self, user):
        window = tk.Toplevel(self)
        ttk.Label(window, text=f'Usuário {user} já existe na base de dados!\nQual ação deseja tomar?').pack(padx=10, pady=10)
        ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)
        ttk.Button(window, text='Deletar e prosseguir', command=lambda:[self.delete_user(user), window.destroy()]).pack(side='left', padx=20, pady=10)
        ttk.Button(window, text='Manter e prosseguir', command=lambda:[self.migrate_user_window(user), window.destroy()]).pack(side='left', padx=20, pady=10)

    def delete_user(self, user):
        if user == 'postgres':
            self.message_window('Você não pode deletar o usuário postgres!')
            return
        query = f"select nsp.nspname as SchemaName, cls.relname as ObjectName, rol.rolname as ObjectOwner, case cls.relkind \
                    when 'r' then 'TABLE' \
                    when 'm' then 'MATERIALIZED_VIEW' \
                    when 'i' then 'INDEX' \
                    when 'S' then 'SEQUENCE' \
                    when 'v' then 'VIEW' \
                    when 'c' then 'TYPE' \
                    else cls.relkind::text \
                end as ObjectType \
            from pg_class cls \
            join pg_roles rol \
                on rol.oid = cls.relowner \
            join pg_namespace nsp \
                on nsp.oid = cls.relnamespace \
            where nsp.nspname not in ('information_schema', 'pg_catalog') \
                and nsp.nspname not like 'pg_toast%' \
                and rol.rolname = '{user}' \
            order by nsp.nspname, cls.relname"
        objects = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
        if len(objects) == 0:
            self.migrate_user_window(user, delete=True)
        else:
            window = tk.Toplevel(self)
            ttk.Label(window, text='Prosseguir com esta ação irá deletar os seguintes objetos:').pack(padx=10, pady=10)
            box_frame = ttk.Frame(window)
            box_frame.pack()
            scrollBar = tk.Scrollbar(box_frame, orient='vertical')
            scrollBar.pack(side='right', fill='y')
            textBox = tk.Text(box_frame, yscrollcommand=scrollBar.set, state='normal', height=10)
            textBox.pack(pady=10)
            textBox.insert(tk.END, 'Schema    Nome    Tipo')
            textBox.insert(tk.END, '-' * 30)
            for object in objects:
                textBox.insert(tk.END, f"{object['schemaname']}    {object['objectname']}   {object['objecttype']}\n")
            textBox.config(state='disabled')
            ttk.Label(window, text='Você tem certeza que deseja prosseguir?').pack(padx=10, pady=10)
            ttk.Button(window, text='Cancelar', command=window.destroy).pack(side='left', padx=20, pady=10)
            ttk.Button(window, text='Sim', command=lambda:[self.migrate_user_window(user, delete=True), window.destroy()]).pack(side='right', padx=20, pady=10)

    # Executa a migração de usuário
    def migrate_user(self, user, password, create_db, delete_user, window):
        if not user or not password:
            return
        else:
            window.destroy()

        cur = self.pg_conn.cursor()

        if delete_user:
            cur.execute(f'DROP OWNED BY {user} CASCADE')
            cur.execute(f'DROP USER IF EXISTS {user}')
        elif user in self.pg_users:
                # Mantem usuário, apenas altera a senha
                cur.execute(f"ALTER USER {user} WITH PASSWORD '{password}'")
        elif user == self.ora_user and self.ora_dba:
            # Caso usuário oracle seja dba, cria um superuser em postgres
            cur.execute(f"CREATE USER {user} WITH SUPERUSER PASSWORD '{password}'")
        else:
            cur.execute(f"CREATE USER {user} WITH PASSWORD '{password}'")
        
        # Migra todas as tabelas e procedures relacionadas ao usuário
        query = f"SELECT table_name FROM all_tables WHERE owner = '{user}'"
        tables = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        tables = [tables[i]['TABLE_NAME'] for i in range(len(tables))]
        query = f"SELECT name, type FROM all_source where owner = '{self.user}' AND line = 1 UNION SELECT view_name, 'VIEW' FROM all_views where owner = '{self.user}'"
        sources = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        sources = [[sources[i]['NAME'], sources[i]['TYPE']] for i in range(len(sources))]

        # Cria nova base de dados com o nome do usuário
        # Migrar usuário 'exemplo' criará o db 'exemplo' com o único schema 'public' e dará todas as permissões para o usuário 'exemplo'
        if create_db:
            cur.execute(f'CREATE DATABASE {user}')
            cur.execute(f'GRANT USAGE ON DATABASE {user} TO {user}')
            cur.execute(f'ALTER DATABASE {user} OWNER TO {user}')
            self.write2log(f'Criado novo database {user}!')
            cur.close()
            self.pg_conn.close()
            self.pg_url = f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{user}"
            self.pg_user = user
            self.pg_password = password
            self.pg_database = user
            self.pg_conn = jaydebeapi.connect(self.pg_driver, self.pg_url, [user, password], self.pg_jar)
            self.write2log(f'Conectado no database {user} com novo usuário {user}')
            self.pg_schema = 'public'
        # Cria apenas o schema com o nome do usuário e insere no DB atual
        else:
            pg_schemas = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query='SELECT nspname FROM pg_namespace').load().collect()
            pg_schemas = [pg_schemas[i]['nspname'] for i in range(len(pg_schemas))]
            if user not in pg_schemas:
                cur.execute(f'CREATE SCHEMA {user}')
            cur.execute(f'GRANT USAGE ON SCHEMA {user} TO {user}')
            cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA {user} GRANT ALL PRIVILEGES ON TABLES TO {user}')
            cur.execute(f'ALTER SCHEMA {user} OWNER TO {user}')
            self.write2log(f'Criado schema {user} no database {self.pg_database}')
            self.pg_schema = user
            cur.close()
        self.no_backup = True
        self.etl_initialization(tables, sources)

    # Lista as tabelas disponiveis com a conexão estabelecida e permite o usuário escolher quais tabelas serão migradas
    def draw_table_selection_window(self, user):

        self.user = user

        for frame in self.winfo_children():
            frame.destroy()
        self.master.geometry('800x600')

        if self.ora_system_user:
            query = f"SELECT table_name FROM all_tables WHERE owner = '{user}'"
        else:
            query = f"SELECT table_name FROM user_tables"
        tables = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
        tables = [tables[i]['TABLE_NAME'] for i in range(len(tables))]

        # Seleção de tabelas
        ttk.Label(self, text='Tabelas disponiveis para migração\nSelecione as tabelas que deseja migrar:').pack(padx=20, pady=10)
        table_frame = ttk.Frame(self, height=300)
        table_frame.pack(fill='x', expand=True)

        table_scrollBar = tk.Scrollbar(table_frame, orient='vertical')
        table_scrollBar.pack(side='right', fill='y')

        table_listbox = tk.Listbox(table_frame, selectmode='multiple', exportselection=False, yscrollcommand=table_scrollBar.set)
        table_listbox.pack(padx=10, pady=10, expand=True, fill='both')

        for table in tables:
            table_listbox.insert(tk.END, table)

        table_scrollBar.config(command=table_listbox.yview)

        ttk.Button(self, text='Selecionar todos', command=lambda: table_listbox.select_set(0, tk.END)).pack(anchor='w', padx=20)

        s_mode = tk.StringVar()

        c1 = ttk.Radiobutton(self, text='Migrar todos os procedimentos relacionados', variable=s_mode, value='1')
        c1.pack()
        c2 = ttk.Radiobutton(self, text='Migrar todos os procedimentos na base de dados', variable=s_mode, value='2')
        c2.pack()
        c3 = ttk.Radiobutton(self, text='Selecionar os procedimentos manualmente', variable=s_mode, value='3')
        c3.pack()

        c1.invoke()

        lower_frame = ttk.Frame(self)
        lower_frame.pack(expand=True)
        ttk.Label(lower_frame, text="Nome do schema (Opicional, deixar vazio usará schema 'public')").pack(side='top', pady=20)
        self.schema_entry = ttk.Entry(lower_frame, width=30)
        self.schema_entry.pack()
        if self.ora_system_user:
            ttk.Button(lower_frame, text='Voltar', command=lambda:self.draw_schema_selection_window()).pack(side='left', padx=20, pady=20)
        else:
            ttk.Button(lower_frame, text='Voltar', command=lambda:self.draw_start_menu()).pack(side='left', padx=20, pady=20)
        ttk.Button(lower_frame, text='Prosseguir', command=lambda: self.check_existing_tables(table_listbox, s_mode)).pack(side='right', padx=20, pady=20)

    # Checa se as tabelas a serem migradas já existem no schema
    def check_existing_tables(self, table_listbox, s_mode):
        if len(table_listbox.curselection()) == 0:
            return
        self.pg_schema = self.schema_entry.get()
        if self.pg_schema == '':
            self.pg_schema = 'public'
        existing_tables = []
        tables = []
        pg_schemas = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query='SELECT nspname FROM pg_namespace').load().collect()
        pg_schemas = [pg_schemas[i]['nspname'] for i in range(len(pg_schemas))]
        # Checa se o schema existe no postgres
        if self.pg_schema not in pg_schemas:
            # Se o schema não existir, checa se o usuário tem permissão para criar schemas
            if not self.pg_superuser:
                query = f"SELECT usename AS grantee, datname, privilege_type \
                        FROM pg_database, aclexplode(datacl) a \
                        JOIN pg_user e \
                        ON a.grantee = e.usesysid \
                        WHERE e.usename = '{self.pg_user}' and datname = '{self.pg_database}'"
                perm = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
                perm = [perm[i]['privilege_type'] for i in range(len(perm))]
                if 'CREATE' not in perm:
                    message = f"Usuário {self.pg_user} não tem permissão para criar o schema {self.pg_schema}\nLogue com um usuário com as devidas permissões!"
                    self.message_window(message)
                    self.write2log('Migração cancelada por falta de permissões')
                    self.draw_start_menu()
                    return
            cur = self.pg_conn.cursor()
            self.write2log(f'Criando schema {self.pg_schema} na base de dados {self.pg_database}...')
            cur.execute(f'CREATE SCHEMA {self.pg_schema}')
            cur.close()
        # Se o schema existir, checa se o usuário tem permissão para criar tabelas
        elif not self.pg_superuser:
            query = f"SELECT usename AS grantee, nspname, privilege_type \
                        FROM pg_namespace, aclexplode(nspacl) a \
                        JOIN pg_user e \
                        ON a.grantee = e.usesysid \
                        WHERE e.usename = '{self.pg_user}' and nspname = '{self.pg_schema}'"
            perm = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
            perm = [perm[i]['privilege_type'] for i in range(len(perm))]
            if 'CREATE' not in perm:
                message = f'Usuário {self.pg_user} não tem permissão para criar tabelas no schema {self.pg_schema}!\nLogue com um usuário com as devidas permissôes!'
                self.message_window(message)
                self.write2log('Migração cancelada por falta de permissões')
                self.draw_start_menu()
                return
        query = f'''SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.pg_schema}' '''
        pg_tables = self.etl.read.format('jdbc').options(driver=self.pg_driver, user=self.pg_user, password=self.pg_password, url=self.pg_url, query=query).load().collect()
        pg_tables = [pg_tables[i]['table_name'] for i in range(len(pg_tables))]
        for i in table_listbox.curselection():
            tables.append(table_listbox.get(i))
        for table in tables:
            if table.lower() in pg_tables:
                existing_tables.append(table)
        if len(existing_tables) > 0:
            window = tk.Toplevel(self)
            window.geometry('600x400')
            ttk.Label(window, text='As seguintes tabelas já existem na base de dados alvo\nSelecione as tabelas que deseja substituir').pack(pady=10)
            list_frame = ttk.Frame(window, height=300)
            list_frame.pack(fill='x', expand=True)

            listbox_scrollBar = tk.Scrollbar(list_frame, orient='vertical')
            listbox_scrollBar.pack(side='right', fill='y')

            listbox = tk.Listbox(list_frame, selectmode='multiple', exportselection=False, yscrollcommand=listbox_scrollBar.set)
            listbox.pack(padx=10, pady=10, expand=True, fill='both')

            for table in existing_tables:
                listbox.insert(tk.END, table)

            listbox_scrollBar.config(command=listbox.yview)

            ttk.Button(window, text='Selecionar todos', command=lambda: listbox.select_set(0, tk.END)).pack(anchor='w')
            lower_frame = ttk.Frame(window)
            lower_frame.pack(side='bottom', fill='x')
            ttk.Button(lower_frame, text='Cancelar', command=lambda: window.destroy()).pack(side='left', padx=10, pady=10)
            ttk.Button(lower_frame, text='Prosseguir', command=lambda: [self.update_tables(tables, pg_tables, listbox, s_mode), window.destroy()]).pack(side='right', padx=10, pady=10)
        else:
            self.source_selection_window(tables, s_mode)

    # Remove tabelas existentes que usuário não quer substituir
    def update_tables(self, tables, pg_tables, listbox, s_mode):
        keep_tables = []
        for i in listbox.curselection():
            keep_tables.append(listbox.get(i))
        aux_table = deepcopy(tables)
        for table in aux_table:
            if table.lower() in pg_tables and table not in keep_tables:
                tables.remove(table)
        self.source_selection_window(tables, s_mode)

    # Idem com seleção de tabelas, mas agora para procedimentos guardados
    def source_selection_window(self, tables, s_mode):
        if s_mode.get() in ['1', '2']:
            window = tk.Toplevel(self)
            window.geometry('600x400')
            ttk.Label(window, text='Procedimentos sendo migrados:').pack(padx=20, pady=10)
            source_frame = ttk.Frame(window, height=300)
            source_frame.pack(fill='x', expand=True)

            source_scrollBar = tk.Scrollbar(source_frame, orient='vertical')
            source_scrollBar.pack(side='right', fill='y')

            textbox = tk.Text(source_frame, yscrollcommand=source_scrollBar.set, state='normal', height=10)
            textbox.pack(expand=True, fill='both')

            # Migra procedimentos relacionados
            if s_mode.get() == '1':
                for table in tables:
                    query = f"SELECT DISTINCT name, type FROM all_dependencies WHERE referenced_name = '{table}' AND owner = '{self.user}'"
                    sources = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
                    sources = [[sources[i]['NAME'], sources[i]['TYPE']] for i in range(len(sources))]
                for source in sources:
                    textbox.insert(tk.END, f'{source[0]}    {source[1]}\n')
            
            # Migra todos os procedimentos
            elif s_mode.get() == '2':
                query = f"SELECT DISTINCT name, type FROM all_dependencies WHERE owner = '{self.user}'"
                sources = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
                sources = [[sources[i]['NAME'], sources[i]['TYPE']] for i in range(len(sources))]
                for source in sources:
                    textbox.insert(tk.END, str(source) + '\n')

            source_scrollBar.config(command=textbox.yview)
            textbox.config(state='disabled')

            lower_frame = ttk.Frame(window)
            lower_frame.pack()
            ttk.Button(lower_frame, text='Cancelar', command=lambda: window.destroy()).pack(side='left', padx=20, pady=10)
            ttk.Button(lower_frame, text='Concordar', command=lambda: [self.etl_initialization(tables, sources), window.destroy()]).pack(side='right', padx=20, pady=10)

        # Seleciona manualmente procedimentos a serem migrados
        elif s_mode.get() == '3':
            window = tk.Toplevel(self)
            window.geometry('600x400')
            query = f"SELECT name, type FROM all_source where owner = '{self.user}' AND line = 1 UNION SELECT view_name, 'VIEW' FROM all_views where owner = '{self.user}'"
            sources = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
            sources = [[sources[i]['NAME'], sources[i]['TYPE']] for i in range(len(sources))]

            ttk.Label(window, text='Procedimentos disponiveis para migração\nSelecione quais deseja migrar:').pack(padx=20, pady=10)
            source_frame = ttk.Frame(window, height=300)
            source_frame.pack(fill='x', expand=True)

            source_scrollBar = tk.Scrollbar(source_frame, orient='vertical')
            source_scrollBar.pack(side='right', fill='y')

            source_listbox = tk.Listbox(source_frame, selectmode='multiple', yscrollcommand=source_scrollBar.set)
            source_listbox.pack(expand=True, fill='both')

            for src in sources:
                source_listbox.insert(tk.END, f"{src[0]} {src[1]}")

            source_scrollBar.config(command=source_listbox.yview)

            lower_frame = ttk.Frame(window)
            lower_frame.pack()
            ttk.Button(lower_frame, text='Cancelar', command=lambda: window.destroy()).pack(side='left', padx=20, pady=10)
            ttk.Button(lower_frame, text='Prosseguir', command=lambda: [self.etl_initialization(tables, source_listbox), window.destroy()]).pack(side='right', padx=20, pady=10)

    # Coleta os nomes dos objetos selecionados pelo usuário para iniciar a migração
    def etl_initialization(self, tables, sources):
        if len(tables) == 0 and len(sources) == 0:
            window = tk.Toplevel(self)
            ttk.Label(window, text='Nenhuma tabela ou procedimento selecionado\nRetornando ao menu inicial').pack(padx=10, pady=10)
            ttk.Button(window, text='Ok', command=window.destroy).pack(pady=10)
            self.wait_window(window)
            self.draw_start_menu()
            return
        if type(sources) != list:
            selected_sources = []
            for i in sources.curselection():
                split_src = sources.get(i).split()
                selected_sources.append([split_src[0], split_src[1]])
        else:
            selected_sources = sources
        outer_references = []
        dep_tables = set()
        # Detecta dependencias entre tabelas
        for table in tables:
            query = f"SELECT DISTINCT table_name, owner FROM all_cons_columns WHERE constraint_name in \
            (select r_constraint_name from all_constraints where constraint_type = 'R' and table_name = '{table}')"
            r_tables = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
            for i in range(len(r_tables)):
                r_owner = r_tables[i]['OWNER']
                r_name = r_tables[i]['TABLE_NAME']
                # checa se tabela a qual depende pertence a um schema que não é o usuário
                if r_owner != self.user:
                    r_tables.remove(r_tables[i])
                    outer_references.append([tables.pop(tables.index(table)), 'TABELA', r_owner, r_name, 'TABELA'])
            r_tables = [r_tables[i]['TABLE_NAME'] for i in range(len(r_tables))]
            for tab in r_tables:
                if tab not in tables:
                    dep_tables.add(tab)
        if sources:
            # Detecta dependencias entre procedimentos
            dep_sources = set()
            for src in selected_sources:
                query = f"SELECT DISTINCT referenced_name, referenced_type, referenced_owner FROM all_dependencies WHERE name = '{src[0]}' \
                    AND referenced_owner != 'PUBLIC' AND referenced_owner not like 'SYS%'"
                dependencies = self.etl.read.format('jdbc').options(driver=self.ora_driver, user=self.ora_user, password=self.ora_password, url=self.ora_url, query=query).load().collect()
                # Checa se objeto o qual depende pertence a um schema que não é o usuário
                for i in range(len(dependencies)):
                    r_owner = dependencies[i]['REFERENCED_OWNER']
                    r_type = dependencies[i]['REFERENCED_TYPE']
                    r_name = dependencies[i]['REFERENCED_NAME']
                    if r_owner != self.user:
                        dependencies.remove(dependencies[i])
                        outer_references.append(selected_sources.pop(selected_sources.index(src)))
                        outer_references[-1] = list(outer_references[-1])
                        outer_references[-1].append(r_owner)
                        outer_references[-1].append(r_name)
                        outer_references[-1].append(r_type)
                dependencies = [[dependencies[i]['REFERENCED_NAME'], dependencies[i]['REFERENCED_TYPE']] for i in range(len(dependencies))]
                # Detecta se procedimento depende de alguma tabela
                for dep in dependencies:
                    if dep[1] == 'TABLE':
                        if dep[0] not in tables:
                            dep_tables.add(dep[0])
                    elif dep not in selected_sources:
                        dep_sources.add(tuple(dep))
            dep_sources = list(dep_sources)
        dep_tables = list(dep_tables)
        if len(dep_tables) > 0 or len(dep_sources) > 0 or len(outer_references) > 0:
            self.draw_dep_window(dep_tables, tables, dep_sources, selected_sources, outer_references)
        else:
            self.begin_etl_session(tables, selected_sources)

    # Lista dependencias entre os objetos escolhidos e pede permissão para incluí-los à migração
    def draw_dep_window(self, dep_tables, sel_tables, dep_sources, sel_sources, outer_references):
        window = tk.Toplevel(self)
        ttk.Label(window, text='Algumas das tabelas/procedimentos selecionados dependem das seguintes tabelas/procedimentos\nPara prosseguir elas tambem serão migradas').pack(padx=10, pady=10)
        box_frame1 = ttk.Frame(window)
        box_frame1.pack(padx=10, pady=10)
        scrollBar = tk.Scrollbar(box_frame1, orient='vertical')
        scrollBar.pack(side='right', fill='y')
        textBox = tk.Text(box_frame1, yscrollcommand=scrollBar.set, state='normal', height=10)
        textBox.pack(pady=10)
        for table in dep_tables:
            textBox.insert(tk.END, table + f'    TABLE\n')
            sel_tables.append(table)
        for src in dep_sources:
            textBox.insert(tk.END, f'{src[0]}   {src[1]}\n')
            sel_sources.append(src)
        textBox.config(state='disabled')
        if len(outer_references) > 0:
            ttk.Label(window, text='Alguns objetos referenciam objetos em diferentes schemas\nEstes serão removidos e listados no arquivo para migração manual').pack(padx=10, pady=10)
            box_frame2 = ttk.Frame(window)
            box_frame2.pack(padx=10, pady=10)
            scrollBar2 = tk.Scrollbar(box_frame2, orient='vertical')
            scrollBar2.pack(side='right', fill='y')
            textBox2 = tk.Text(box_frame2, yscrollcommand=scrollBar2.set, state='normal', height=10)
            textBox2.pack(pady=10)
            for ref in outer_references:
                textBox2.insert(tk.END, ref + '\n')
            textBox2.config(state='disabled')
        lower_frame = ttk.Frame(window)
        lower_frame.pack()
        ttk.Button(lower_frame, text='Concordar', command=lambda: [self.begin_etl_session(sel_tables, sel_sources, outer_references), window.destroy()]).pack(side='right', padx=20, pady=10)
        ttk.Button(lower_frame, text='Cancelar', command=lambda: window.destroy()).pack(side='left', padx=20, pady=10)

    # Inicializa a sessão de etl e começa a migrar
    @threaded
    def begin_etl_session(self, tables, sources, outer_refs=[]):
        pg_conf = {'host': self.pg_host, 'port': self.pg_port,'user': self.pg_user, 'password': self.pg_password, 'database': self.pg_database}
        ora_conf = {'host': self.ora_host, 'port': self.ora_port, 'user': self.ora_user, 'password': self.ora_password, 'service': self.ora_service}
        if not self.no_backup:
            self.write2log(f'Criando backup do schema {self.pg_schema}')
            self.create_backup(self.pg_schema)
        self.write2log(f"migrando tabelas {tables} e sources {sources} do Oracle schema '{self.user}' para Postgres database {self.pg_database} schema '{self.pg_schema}'!")
        etl_session = ETL_session(self.master, pg_conf, ora_conf, self.user, tables, sources, self.etl, self.pg_jar, self.ora_jar, self.pg_schema)
        self.active_sessions.append(etl_session)
        # Começa a migração
        etl_session.start_etl()
        self.wait4ETL(etl_session, outer_refs)
        if self.ora_system_user:
            self.draw_schema_selection_window()
        else:
            self.draw_start_menu()

    # Espera processo terminar e restaura para o estado anterior em caso de falha
    @threaded
    def wait4ETL(self, session, outer_refs):
        # Sei que isso é feio mas não consegui fazer funcionar de outra forma
        # caso encontre um método mais elegante para esperar a janela, sinta-se livre para substituir
        while session.winfo_exists():
            pass
        if session._state == 'success':
            self.write2log('Migração concluída com sucesso!')
            if len(outer_refs) > 0:
                self.write_outer_refs(outer_refs)
        elif session._state == 'failed':
            if not self.no_backup:
                self.write2log('Migração falhou!\nRetornando base de dados para estado anterior...')
                self.restore_from_backup(self.pg_schema)
                self.write2log('Base de dados retornada ao último estado estável')
        self.active_sessions.remove(session)
        del session
        self.etl.stop()

    def write_outer_refs(self, outer_refs):
        OUTER_REF_FILE = os.path.join(APP_HOME, 'manual_migrations/outer_references.txt')
        with open(OUTER_REF_FILE, 'a') as file:
            data = datetime.datetime.strftime(datetime.datetime.now(), "%d/%m/%Y %H:%M:%S") + ': ' + data + '\n'
            file.write(data)
            file.write(f'Sessão ETL Oracle {self.user} -> Postgres {self.pg_schema}\n')
            file.write(('-' * 50) + '\n')
            for ref in outer_refs:
                file.write(f"User: {self.user}, Objeto: {ref[0]}, Tipo: {ref[1]} -> Referencia schema: {ref[2]}, Objeto: {ref[3]}, Tipo: {ref[4]}\n")
            file.write('\n\n')
            file.close()