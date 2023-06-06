from setuptools import setup, find_packages

setup(
    name='pybarb',
    version='0.3.4',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'sqlalchemy',
        'plotly',
        'requests' 
    ],
    author='Simon Raper',
    author_email='info@barb.co.uk',
    description='A package to interact with the BARB API',
    url='https://github.com/coppeliaMLA/barb_api',
)
