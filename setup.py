from pathlib import Path
import subprocess

from setuptools import setup
from setuptools.command.build_ext import build_ext


WORKING_DIR = Path(__file__).resolve().parent
STATIC_DIR = WORKING_DIR / 'tshistory_refinery' / 'refinery_static'


def compile_elm(edit_kind, src):
    """Compile elm component to JS"""
    src = WORKING_DIR / 'elm' / src
    out = STATIC_DIR / f'{edit_kind}_elm.js'
    cmd = f'elm make --optimize --output {out} {src}'
    subprocess.call(cmd, shell=True)


class ElmBuild(build_ext):
    """Build Elm components"""

    def run(self):
        compile_elm('cache', 'Cache.elm')
        super().run()


setup(name='tshistory_refinery',
      version='0.3.0',
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr, arnaud.campeas@pythonian.fr',
      packages=['tshistory_refinery'],
      zip_safe=False,
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
      package_data={'tshistory_refinery': [
          'refinery_static/*',
          'templates/*'
      ]},
      entry_points={
          'tshistory.subcommands': [
              'webstart=tshistory_refinery.cli:webstart',
              'init-db=tshistory_refinery.cli:initdb',
              'migrate-to-cache=tshistory_refinery.cli:migrate_to_cache',
              'setup-tasks=tshistory_refinery.cli:setup_tasks'
          ],
      },
      cmdclass={'build_ext': ElmBuild}
)
