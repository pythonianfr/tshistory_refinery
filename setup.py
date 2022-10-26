from setuptools import setup


setup(name='tshistory_refinery',
      version='0.5.0',
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr, arnaud.campeas@pythonian.fr',
      packages=['tshistory_refinery'],
      zip_safe=False,
      install_requires=[
          'tshistory >= 0.16.0',
          'tsview >= 0.15.0',
          'tshistory_supervision >= 0.10.1',
          'tshistory_formula >= 0.11.0',
          'tshistory_xl >= 0.4.1',
          'rework >= 0.15.1',
          'rework_ui >= 0.13.0',
          'inireader',
          'pytest_sa_pg',
          'webtest',
          'responses',
          'requests',
          'pml',
          'pygments',
          'croniter'
      ],
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
              'setup-tasks=tshistory_refinery.cli:setup_tasks'
          ],
      },
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Scientific/Engineering',
          'Topic :: Software Development :: Version Control'
      ]
)
