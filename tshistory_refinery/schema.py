from pathlib import Path

from sqlhelp import sqlfile

from rework.schema import init as rework_init
from rework_ui.schema import init as rework_ui_init
from tshistory.schema import kvapi, tsschema
from tshistory_supervision.schema import supervision_schema
from tshistory_formula.schema import formula_schema
from tsview.schema import tsview_schema

from tshistory_refinery import __version__


CACHE_POLICY = Path(__file__).parent / 'schema.sql'


class refinery_schema(supervision_schema, formula_schema):

    def create(self, engine):
        super().create(engine)

        rework_init(engine)
        rework_ui_init(engine)

        with engine.begin() as cn:
            cn.execute(sqlfile(CACHE_POLICY, ns=self.namespace), _binary=False)

        tsschema(f'{self.namespace}-cache').create(engine)

        kvstore = kvapi.kvstore(str(engine.url), namespace=f'{self.namespace}-kvstore')
        kvstore.set('tshistory-refinery-version', __version__)

        tsview_schema(self.namespace).create(engine)
