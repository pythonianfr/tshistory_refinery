from pathlib import Path

from sqlhelp import sqlfile

from rework.schema import init as rework_init
from rework_ui.schema import init as rework_ui_init
from tshistory.schema import tsschema
from tshistory_supervision.schema import supervision_schema
from tshistory_formula.schema import formula_schema


CACHE_POLICY = Path(__file__).parent / 'schema.sql'


class refinery_schema(supervision_schema, formula_schema):

    def create(self, engine, reset=False, rework=False):
        super().create(engine, reset=reset)

        if rework:
            rework_init(engine, drop=reset)
            rework_ui_init(engine)

        with engine.begin() as cn:
            cn.execute(sqlfile(CACHE_POLICY, ns=self.namespace))

        tsschema(f'{self.namespace}-cache').create(engine)
