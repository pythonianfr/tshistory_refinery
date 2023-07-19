from setuptools import setup

from tshistory_refinery import __version__


setup(name='tshistory_refinery',
      version=__version__,
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr, arnaud.campeas@pythonian.fr',
      packages=['tshistory_refinery'],
      zip_safe=False,
      install_requires=[
          'tshistory >= 0.18.0',
          'tsview >= 0.17.0',
          'tshistory_supervision >= 0.11.0',
          'tshistory_formula >= 0.14.0',
          'tshistory_xl >= 0.6.0',
          'rework >= 0.15.1',
          'rework_ui >= 0.13.0',
          'inireader',
          'pytest_sa_pg',
          'webtest',
          'responses',
          'requests',
          'pml',
          'pygments',
          'croniter',
      ],
      extras_require={
          'doc': [
            'sphinx == 4.5.0',
            'sphinx-rtd-theme',
            'sphinx-autoapi',
            'pydata-sphinx-theme'
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
              'migrate-to-cache=tshistory_refinery.cli:migrate_to_cache',
              'setup-tasks=tshistory_refinery.cli:setup_tasks',
              'list-series-locks=tshistory_refinery.cli:list_series_locks'
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
