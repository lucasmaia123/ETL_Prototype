Suporte: Oracle -> Postgresql

Features: Migra tabelas, relações, constraints, usuário, procedures, functions, triggers, views e faz tradução de plsql para plpgsql

Prerequisitos: Tenha Java e Python instalados no seu sistema

Como utilizar: 
Metodo 1: Utilizando o instalador
- Utilize algum terminal shell para executar o script install_app.sh
- O script criará o ambiente virtual e um executável
- Inicie o app pelo executável
- Não remova o executável da pasta, caso faça, o app não irá funcionar

Metodo 2: Direto pelo script
- Opcional: Crie um ambiente virtual python e o ative
- No diretório do app execute o comando 'pip install -r requirements.txt' para instalar dependencias
- Defina a variável de ambiente SPARK_HOME apontando para o modulo do pyspark instalado pelo pip
- Inicie o app com o comando 'python3 start_app.py'

Feito isto, caso o app não encontre o JDK automaticamente, aponte para o local do JDK na janela que aparecerá.
Siga as instruções na UI para prosseguir com o ETL.

TO DO: Adicionar suporte para migrar Postgresql -> Oracle
