#!/bin/bash

create_venv() {
   if ! command -v virtualenv &> /dev/null; then
      echo "virtualenv is not installed. Installing..."
      python3 -m pip install --user virtualenv --break-system-packages
      echo "virtualenv installation complete."
   fi

   local env_name=${1:-".venv"}

   if [ -d "$env_name" ]; then
      echo "Virtual environment '$env_name' already exists. Aborting."
      return 1
   fi

   virtualenv "$env_name"
   source "./$env_name/bin/activate"
   pip install -U pip

   if [ -f "requirements.txt" ]; then
      pip install -r ./requirements.txt
   fi

   if [ -f "setup.py" ]; then
      pip install -e .
   fi
}

install_app() {

   if ! command -v pyinstaller &> /dev/null; then
      echo "pyinstaller is not installed. Installing..."
      python3 -m pip install --user pyinstaller --break-system-packages
      echo "pyinstaller installation complete."
   fi

   pyinstaller -w --onefile --additional-hooks-dir=. start_app.py
   mv ./dist/start_app .
   rm -rf ./dist
   rm -rf ./build
   echo "Instalação concluída, utilize o executável criado para iniciar o app!"
}

create_venv "$2"
install_app
