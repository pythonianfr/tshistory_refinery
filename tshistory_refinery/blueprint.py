import io
import json
import traceback
from collections import defaultdict
from contextlib import redirect_stdout

import numpy as np
import pandas as pd
from flask import (
    Blueprint,
    jsonify,
    make_response,
    render_template,
    request,
    url_for
)
import sqlalchemy
from pml import HTML
from rework_ui.helper import argsdict as _args
from psyl.lisp import (
    parse as fparse,
    serialize,
)

from sqlhelp import select
from tshistory_formula import registry
from tsview.util import format_formula as pretty_formula

from tshistory_refinery import cache


def format_formula(formula):
    h = HTML()
    formatted = pretty_formula(
        formula
    )
    s = h.span(
        formatted,
        escape=False
    )
    return str(s)


def format_name(name):
    h = HTML()
    h.text(name)
    h.a('view', target='new', href=url_for('tsview.tsinfo', name=name))
    h.a('edit', target='new', href=url_for('tsview.tsformula', name=name))
    return str(h)


def format_metadata(meta):
    h = HTML()
    for key, val in sorted(meta.items()):
        if key == 'tzaware':
            with h.div() as d:
                d.span(f'{key} → ')
                d.span(str(val), klass='tzaware' if val else 'tznaive')
        else:
            h.div(f'{key} → {val}')
    return str(h)


def refinery_bp(tsa):
    engine = tsa.engine

    bp = Blueprint(
        'refinery',
        __name__,
        template_folder='templates',
        static_folder='refinery_static',
    )

    # extra formula handling

    @bp.route('/formulas')
    def formulas():
        pd.set_option('display.max_colwidth', None)
        fmt = {
            'name': format_name,
            'text': format_formula,
            'metadata': format_metadata
        }
        with engine.begin() as cn:
            return render_template(
                'bigtable.html',
                table=pd.read_sql_table(
                    'formula',
                    cn,
                    schema='tsh'
                ).drop(
                    labels='id',
                    axis=1
                ).to_html(
                    index=False,
                    classes='table table-striped table-bordered table-sm',
                    escape=False,
                    formatters=fmt
                )
            )

    def validate_formula(df_formula):
        errors = defaultdict(list)
        warnings = defaultdict(list)

        # conflicts with primary series are an error
        primaries = {
            name for name in np.unique(df_formula['name'])
            if tsa.type(name) == 'primary'
               and tsa.exists(name)
        }
        if primaries:
            errors['primary'] = sorted(primaries)

        # overriding an existing formula yields a warning
        formulas = {
            name for name in np.unique(df_formula['name'])
            if tsa.type(name) == 'formula'
               and tsa.exists(name)
        }
        if formulas:
            warnings['existing'] = sorted(formulas)

        # formula syntax error detection
        # and needed series
        uploadset = {
            row.name
            for row in df_formula.itertuples()
        }
        ok = set()
        syntax_error = set()
        missing = set()

        def exists(sname):
            if not tsa.exists(sname):
                if sname in registry.AUTO:
                    return True
                for op in ('cronos', 'meteo', 'pointconnect'):
                    if sname.startswith(op):
                        return True
                return False
            return True

        for row in df_formula.itertuples():
            try:
                parsed = fparse(row.text)
            except SyntaxError:
                syntax_error.add(row.name)
                continue

            needset = set(
                tsa.tsh.find_metas(tsa.engine, parsed)
            )
            # even if ok, the def might refer to the current
            # uploaded set, or worse ...
            newmissing = {
                needname
                for needname in needset
                if needname not in uploadset and not exists(needname)
            }
            missing |= newmissing
            if not newmissing:
                ok.add(row.name)

        if syntax_error:
            errors['syntax'] = sorted(syntax_error)

        if missing:
            errors['missing'] = sorted(missing)

        return errors, warnings

    @bp.route('/addformulas')
    def addformulas():
        return render_template(
            'formula_form.html'
        )

    @bp.route('/updateformulas', methods=['POST'])
    def updateformulas():
        if not request.files:
            return jsonify({'errors': ['Missing CSV file']})
        args = _args(request.form)
        stdout = io.StringIO()
        try:
            content = request.files['new_formulas.csv'].stream.read().decode("utf-8")
            stdout.write(content)
            stdout.seek(0)
            df_formula = pd.read_csv(stdout, dtype={'name': str, 'serie': str}, sep=',')

            errors, warnings = validate_formula(df_formula)
            if errors or not args.reallydoit:
                return jsonify({
                    'errors': errors,
                    'warnings': warnings
                })

            with redirect_stdout(stdout):
                for row in df_formula.itertuples():
                    tsa.register_formula(
                        row.name,
                        row.text,
                        reject_unknown=False
                    )

        except Exception:
            traceback.print_exc()
            h = HTML()
            return json.dumps({
                'crash': str(h(traceback.format_exc())),
                'output': stdout.getvalue().replace('\n', '<br>')
            })
        return jsonify({
            'output': stdout.getvalue().replace('\n', '<br>'),
            'crash': ''
        })

    @bp.route('/downloadformulas')
    def downloadformulas():
        formulas = pd.read_sql(
            'select name, text from tsh.formula',
            engine
        )
        df = formulas.sort_values(
            by=['name', 'text'],
            kind='mergesort'
        )
        df['text'] = df['text'].apply(lambda x: serialize(fparse(x)))
        response = make_response(
            df.to_csv(
                index=False,
                quotechar="'"
            ), 200
        )
        response.headers['Content-Type'] = 'text/json'
        return response

    @bp.route('/formulacache')
    def formulacache():
        return render_template('cache.html')

    # /formula
    # formula cache

    @bp.route('/policies')
    def cache_policies():
        q = select(
            'name', 'ready',
            'initial_revdate', 'from_date',
            'look_before', 'look_after',
            'revdate_rule', 'schedule_rule'
        ).table('tsh.cache_policy')

        return jsonify(
            [
                dict(item)
                for item in q.do(engine).fetchall()
            ]
        )

    @bp.route('/delete-policy/<name>', methods=['DELETE'])
    def delete_policy(name):
        with engine.begin() as cn:
            cn.execute(
                'delete from tsh.cache_policy '
                'where name = %(name)s',
                name=name
            )

            return make_response('', 204)

    class policy_args(_args):
        types = {
            'name': str,
            'initial_revdate': str,
            'look_before': str,
            'look_after': str,
            'revdate_rule': str,
            'schedule_rule': str
        }

    @bp.route('/create-policy', methods=['PUT'])
    def create_policy():
        args = policy_args(request.json)
        try:
            cache.new_policy(engine, **args)
        except ValueError as err:
            return make_response(str(err), 400)
        except TypeError:
            return make_response('Missing fields', 400)
        except sqlalchemy.exc.IntegrityError as err:
            return make_response(
                'A policy with identical parameters already exists',
                400
            )

        return make_response('', 201)

    @bp.route('/cacheable-formulas')
    def cacheable_formulas():
        with tsa.engine.begin() as cn:
            return jsonify(
                tsa.tsh.cacheable_formulas(cn)
            )

    # /formula cache

    return bp
