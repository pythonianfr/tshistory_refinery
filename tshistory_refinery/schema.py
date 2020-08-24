from rework.schema import init as rework_init
from rework_ui.schema import init as rework_ui_init
from tshistory.schema import tsschema
from tshistory_formula.schema import formula_schema



def init(engine, drop=False):
    tsschema('tsh').create(engine)
    tsschema('tsh-upstream').create(engine)
    formula_schema().create(engine)
    rework_init(engine, drop=drop)
    rework_ui_init(engine)
