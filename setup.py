import os
from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='openimis-be-merankabandi',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    license='GNU AGPL v3',
    description='The openIMIS Backend merankabandi reference module.',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://openimis.org/',
    author='anthbel',
    author_email='abelem@worldbank.org',
    install_requires=[
        'django',
        'django-db-signals',
        'djangorestframework',
        'weasyprint',
        'django_oauth_toolkit',
        'openimis-be-core',
        'django-two-factor-auth[phonenumberslite]',
        'PyPDF2'
    ],
    package_data={
        'merankabandi': ['fixtures/*', 'templates/*', 'static/*.*', 'static/merankabandi/*.*', 'management/commands/*'],
    },
    classifiers=[
        'Environment :: Web Environment',
        'Framework :: Django',
        'Framework :: Django :: 3.0',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
    ],
)
