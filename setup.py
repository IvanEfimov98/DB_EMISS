from setuptools import setup, find_packages

setup(
    name='db_emiss',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'arcticdb',
        'pandas',
        'pyarrow',
        'pyyaml',
        'requests',
        'openpyxl',
        'python-docx',
        'jinja2',
        'plotly',
        'matplotlib',
        'seaborn',
    ],
    author='Ivan Efimov',
    description='AI-powered generation of scientific works using EMISS data and DeepSeek',
    url='https://github.com/IvanEfimov98/DB_EMISS',
)