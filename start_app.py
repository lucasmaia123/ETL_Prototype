from pyspark import SparkConf
import tkinter as tk
from tkinter import filedialog
import os
import sys
import pathlib
from code.gui import etl_UI

# Encontra o diretório do app
if getattr(sys, 'frozen', False):
    APP_HOME = os.path.dirname(sys.executable)
else:
    APP_HOME = os.path.dirname(os.path.abspath(__file__))
    APP_HOME = pathlib.Path(APP_HOME).parent

# Tenta encontrar o ambiente virtual
try:
    os.environ['VIRTUAL_ENV']
except:
    p = pathlib.Path(APP_HOME).glob('*.venv')
    for file in p:
        if file.is_dir():
            os.environ['VIRTUAL_ENV'] = os.path.abspath(file)
            break

root = tk.Tk()
window = None

# Tenta encontrar os drivers do PySpark
try:
    os.environ['SPARK_HOME']
except:
    try:
        os.environ['VIRTUAL_ENV']
        if not getattr(sys, 'frozen', False) and sys.prefix == sys.base_prefix:
            raise Exception
        os.environ['SPARK_HOME'] = os.path.join(os.environ['VIRTUAL_ENV'], 'lib/python3.11/site-packages/pyspark')
    except:
        window = tk.Frame(root)
        window.pack()
        tk.Label(window, text='Para executar o aplicativo fora do ambiente virtual, instale as dependencias\ne defina a variável de ambiente SPARK_HOME para apontar na instalaçao do pyspark!').pack(padx=10, pady=10)
        tk.Button(window, text='Ok', command=root.destroy).pack(padx=10, pady=10)

def get_jdk(path, output, window):
    if os.path.exists(os.path.join(path, 'bin/java')) or os.path.exists(os.path.join(path, 'bin/java.exe')):
        os.environ['JAVA_HOME'] = path
        window.destroy()
        app = etl_UI(root, conf, pg_jar, ora_jar)
    else:
        output.config(text='JDK não encontrado!')

# Configurações de conexão
conf = SparkConf().setAppName('ETLTest').setMaster('local').set('spark.driver.extraClassPath', os.path.join(APP_HOME, 'jdbc/*'))
ora_jar = os.path.join(APP_HOME, "jdbc/ojdbc11.jar")
pg_jar = os.path.join(APP_HOME, "jdbc/postgresql-42.7.3.jar")

def file_explorer(entry):
    filename = filedialog.askdirectory(initialdir=APP_HOME, title='Especifique o diretório do JDK')
    if len(filename) > 0:
        entry.delete(0, tk.END)
        entry.insert(tk.END, filename)

# Localização do JDK e drivers
try:
    if os.environ['JAVA_HOME']:
        app = etl_UI(root, conf, pg_jar, ora_jar)
    else:
        raise Exception
except:
    if not window:
        window = tk.Frame(root)
        window.pack()
        tk.Label(window, text='Por favor, digite o path para o diretório do JDK abaixo:').pack(pady=10)
        jdk_entry = tk.Entry(window, width=30)
        jdk_entry.pack()
        jdk_entry.insert(tk.END, "/lib/jvm/jdk-17-oracle-x64")
        browse = tk.Button(window, text='Procurar diretório', command=lambda:file_explorer(jdk_entry))
        browse.pack()
        button = tk.Button(window, text='prosseguir')
        button.pack(pady=10)
        label = tk.Label(window, text='')
        label.pack(pady=10)
        button.config(command=lambda:get_jdk(jdk_entry.get(), label, window))
os.environ['CLASSPATH'] = os.path.join(APP_HOME, "jdbc/*")

root.mainloop()
sys.exit(1)