from setuptools import setup

from tshistory_refinery import __version__


setup(name='tshistory_refinery',
      version=__version__,
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr, arnaud.campeas@pythonian.fr',
      packages=['tshistory_refinery'],
      zip_safe=False,
      install_requires=[
          'dbcache >= 0.6.1',
          'tshistory >= 0.22.2, < 0.23',
          'tsview >= 0.23.1, < 0.24',
          'tshistory_supervision >= 0.15.1',
          'tshistory_formula >= 0.18.2, < 0.19',
          'tshistory_xl >= 0.10.1, < 0.11',
          'rework >= 0.19.2, < 0.20',
          'rework_ui >= 0.18.1',
          'inireader',
          'webtest',
          'responses',
          'requests',
          'pml',
          'pygments',
          'python-icron',
      ],
      extras_require={
          'doc': [
            'sphinx == 8.2.1',
            'sphinx-rtd-theme == 3.0.2',
            'sphinx-autoapi == 3.6.0',
            'pydata-sphinx-theme == 0.16.1'
        ]},
      package_data={'tshistory_refinery': [
          'refinery_static/*',
          'templates/*',
          'schema.sql'
      ]},
      entry_points={
          'tshistory.subcommands': [
              'webstart=tshistory_refinery.cli:webstart',
              'init-db=tshistory_refinery.cli:initdb',
              'setup-tasks=tshistory_refinery.cli:setup_tasks',
          ],
          'tshistory.migrate.Migrator': [
              'migrator=tshistory_refinery.migrate:Migrator'
          ],
          'tshclass': [
              'tshclass=tshistory_refinery.tsio:timeseries'
          ],
          'httpclient': [
              'httpclient=tshistory_refinery.http:refinery_httpclient'
          ],
          'scrapers': [
              'scrapers=tshistory_refinery.scrap:SCRAPERS'
          ],
          'forceimports': [
              'forceimports=tshistory_refinery.search:IMPORTCALLBACK'
          ]
      },
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Scientific/Engineering',
          'Topic :: Software Development :: Version Control',
          'Topic :: Software Development :: Libraries :: Application Frameworks'
      ]
)
