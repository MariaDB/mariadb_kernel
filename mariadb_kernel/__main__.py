from ipykernel.kernelapp import IPKernelApp
from . import MariaDBKernel

IPKernelApp.launch_instance(kernel_class=MariaDBKernel)
