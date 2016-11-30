from setuptools import setup, find_packages

with open('requirements.txt') as req_txt:
    required = req_txt.read().splitlines()

setup(name='compranet',
    version='0.1.0',
    description='Tools to track changes in CompraNet',
    author='Jordan T. Bates',
    author_email='jtbates@gmail.com',
    license='MIT',
    packages=find_packages(),
    install_requires=required,
    entry_points={
        'console_scripts': [
            'compranet-cli=compranet.cli:cli',
        ],
    },
)
      
