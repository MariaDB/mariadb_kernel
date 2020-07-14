# MariaDB Jupyter Kernel

`mariadb_kernel` is a Jupyter kernel that helps users interact with a MariaDB
deployment from Jupyter Lab or Notebook.

This kernel assumes an existing installation of MariaDB 10.5+ on the local machine (that is the kernel
assumes there is a `mariadb` executable in `PATH`).

If you don't have it already installed, you can download it from [here](https://mariadb.org/download/).

The project is not yet packaged for PyPi or conda-forge, so you'll need to install the kernel by hand.

# Quick installation
Assuming you already have Jupyter Lab installed, ideally in a `miniconda` environment,
all you need to do is:

1. Clone the repo:
```bash
git clone https://github.com/robertbindar/mariadb_kernel.git
```

2. Move into the repository dir and pip install the package:
```bash
pip install .
```

3. Install the Jupyter kernelspec so the kernel becomes visible for Jupyter
```bash
python -m mariadb_kernel.install
```

# Complete installation steps

1. Clone the repo:

```bash
git clone https://github.com/robertbindar/mariadb_kernel.git
```

2. Download and install [miniconda](https://docs.conda.io/en/latest/miniconda.html):
```bash
# After you downloaded the script run:
sh ./Miniconda3-latest-Linux-x86_64.sh
```
3. Create a new miniconda environment
```bash
conda create -n maria_env python=3.7
```
4. Activate the new env
```bash
# You should see the terminal prompt prefixed with (maria_env)
conda activate maria_env
```
5. Install Jupyter Lab
```bash
conda install -c conda-forge jupyterlab
```
6. Clone the repo and install the kernel:
```bash
git clone https://github.com/robertbindar/mariadb_kernel.git
pip install .
python -m mariadb_kernel.install
```

# Using the kernel
Once the kernel is installed, when you open Jupyter Lab, you should have the option to create a new `MariaDB` notebook.
<br />
<img src="https://raw.githubusercontent.com/MariaDB/mariadb_kernel/master/static/lab_open.png" width="400" height="130">
### Note
Please make sure there is a running instance of MariaDB Server before the notebook is created

### Configuration
By default the kernel tries to connect to a running server on `localhost`, port `3306` using the user `root` and `no password`.
You can change these settings by providing a `json` configuration file for the server.
This file should be stored either at:
1. `~/.jupyter/mariadb_config.json`
or
2. If you have overwritten the `JUPYTER_CONFIG_DIR` environment variable, the config file should be stored at `$JUPYTER_CONFIG_DIR/mariadb_config.json`

Config example:
```bash
cat ~/.jupyter/mariadb_config.json
{
    "user": "robert",
    "host": "localhost",
    "port": "3306",
    "password": "securepassword"
}
```

# Contributing
Please note this project is still in its very early stages, it should not be used in production environments,
but we are actively working to make it better, add more features, add CI infrastructure, tests, documentation and so on.

You can see [here](https://jira.mariadb.org/browse/MDBF-53?jql=project%20%3D%20MDBF%20AND%20labels%3Djupyter) the Jira tasks we follow for reaching
the first release of the project. Feel free to add new entries there for feature requests and bug reports.

Code or documentation contributions or warm friendly advices are welcome :-)
Thanks for testing the project and helping us make it better.

