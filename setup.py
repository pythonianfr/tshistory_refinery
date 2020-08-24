from setuptools import setup


deps = [
    'pika',
    'pandas >= 1.0',
    'apscheduler',
    'tshistory',
    'tshistory_rest',
    'tshistory_client',
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
    'holidays',
    'pycountry',
]

setup(name='tshistory_refinery',
      version='0.1.0',
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr, arnaud.campeas@pythonian.fr',
      packages=['tshistory_refinery'],
      install_requires=deps,
      entry_points={
          'tshistory.subcommands': [
              'webstart=tshistory_refinery.cli:webstart',
              'init-db=tshistory_refinery.cli:initdb'
          ],
      }
)
