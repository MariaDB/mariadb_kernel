Welcome! <3
The MariaDB Jupyter kernel is an Open Source project licensed under BSD-new.
Feel free to create new Issues for feature requests or bug reports.  
Code, tests, documentation contributions are welcome :-)  
Friendly advice or feedback about the project counts as a contribution too!

### Setting up the development environment

1. Download Miniconda
```
https://docs.conda.io/en/latest/miniconda.html
```
2. Install Miniconda
```
sh ./Miniconda3-latest-Linuxscript-x86_64.sh
```

3. Create a new conda environment
```
conda create -n maria_env python=3.7
```

4. Activate the new environment
```
# You should see the terminal prompt prefixed with (maria_env)
conda activate maria_env
```
5. Install Jupyterlab
```
conda install -c conda-forge jupyterlab
```
6. Clone the mariadb_kernel repository
```
git clone https://github.com/MariaDB/mariadb_kernel.git
```
7. Move into the repo folder and install our development packages
```
cd mariadb_kernel/
python3 -m pip install -r dev-requirements.txt
```
8. Build the kernel
```
python setup.py develop
```
9. Install the kernelspec so that JupyterLab can see the MariaDB kernel
```
python -m mariadb_kernel.install
```
You’re done now, you should be able to open JupyterLab and use your own local MariaDB Jupyter kernel!

Make sure you install MariaDB on your computer. If it’s not installed, download and install it from [mariadb.org](https://mariadb.org/download/) or install MariaDB via `apt` or your package manager of choice.

### Running the tests
```
pytest -v
```
Tests are also run by our CI bot when you submit your Pull Request

### Code formatting

We use [Black](https://black.readthedocs.io/en/stable/) in this repository for formatting the code.
We recommend you run this program on your changes before submitting a Pull Request,
it will be much easier for the reviewer to accept your contribution as fast as possible.

Usage:
```
black .
```

For getting a sense of how the kernel looks internally, we encourage you to check the [Main Components and Architecture](https://mariadb.com/kb/en/the-mariadb-jupyter-kernel-main-components-and-architecture/) kernel documentation page.
Feel free to ask questions on [Zulip](https://mariadb.zulipchat.com/#), we are here to guide you through your first contribution <3

### Adding a new magic command

Adding a new magic command is quite easy, we tried hard to make the components of the kernel as independent as possible so you can create a new magic command using minimum effort.
To add a new magic, you need to follow four steps:

1. Pick a creative name for your magic  
Let’s assume you chose to name it `%echo`

2. Create a new Python file containing the code of the magic command  
The new magic commands should be added in this directory:
`mariadb_kernel/maria_magics/`

```
> cat mariadb_kernel/maria_magics/echo.py
```

```python
“””This class implements the %echo magic command“””

help_text = “””
The %echo magic command prints the arguments you pass to it
“””

from mariadb_kernel.maria_magics.line_magic import LineMagic

# The class inherits LineMagic because it is a line magic command
# Cell magic commands have to inherit the CellMagic class
class Echo(LineMagic):
	def __init__(self, args):
		self.args = args
	def name(self):
		return ‘%echo’
	def help(self):
		return help_text
	def execute(self, kernel, data):
		message = { ‘name’: ‘stdout’, ‘text’: self.args}
		kernel.send_response(self.iopub_socket, ‘stream’, message)
```

3. Let the kernel know it should now support a new magic command  
Write two lines of code in `mariadb_kernel/maria_magics/supported_magics.py`:  
```python
# Import the class of your magic command
from mariadb_kernel.maria_magics.echo import Echo
```
```pyhon  
# Add a new entry in the dictionary returned by the get() function:
{
 ...,
 ”echo”: Echo
}
```
4. Build the project and test the new magic in JupyterLab!
