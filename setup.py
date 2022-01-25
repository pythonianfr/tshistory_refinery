from setuptools import setup


setup(name='tshistory_refinery',
      version='0.3.0',
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr, arnaud.campeas@pythonian.fr',
      packages=['tshistory_refinery'],
      install_requires=[
          'tshistory',
          'tsview',
          'tshistory_supervision',
          'tshistory_formula',
          'tshistory_editor',
          'rework',
          'rework_ui',
          'inireader',
          'pytest_sa_pg',
          'webtest',
          'responses',
          'requests',
          'pml',
          'pygments',
          'croniter'
      ],
      entry_points={
          'tshistory.subcommands': [
              'webstart=tshistory_refinery.cli:webstart',
              'init-db=tshistory_refinery.cli:initdb'
          ],
      }
)
