# Python 3.6

## Instalación

- Instalar librerías requeridas:

  `$ sudo apt install build-essential libssl-dev zlib1g-dev`

  `$ sudo apt install libbz2-dev libreadline-dev libsqlite3-dev`

- Descargar y descomprimir el _tarball_:

  `$ wget https://www.python.org/ftp/python/3.6.4/Python-3.6.4.tgz`

  `$ tar xvf Python-3.6.4.tgz`

  `$ cd Python-3.6.4`

- Compilar el paquete e instalar sus componentes en el sistema:

  `# ./configure --enable-optimizations`

  `# make altinstall`

- Crear un link simbólico para `python3.6`:

  `# ln -s /usr/local/bin/python3.6 /usr/bin/python3.6`

- Pip

  `$ sudo apt install python-pip` o `python3-pip`

## Entorno virtuales

- Instalación de la herramienta _Virtualenv_:

  `$ sudo apt-get install virtualenv python-virtualenv`

- Crear entorno:

  `$ python3.6 -m venv <name-venv>`

- Activar el entorno virtual:

  `$ . venv/bin/activate`

## Secret key

- Generar _secret key_:

  ```python
  from django.utils.crypto import get_random_string

  chars = 'abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*(-_=+)'
  print(get_random_string(50, chars))
  ```
