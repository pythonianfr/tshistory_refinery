import io
import json
import traceback
from collections import defaultdict
from contextlib import redirect_stdout
from operator import itemgetter

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
from pml import HTML
from rework_ui.helper import argsdict as _args
from psyl.lisp import (
    parse as fparse,
    serialize,
)

from sqlhelp import select
from tshistory_formula import registry
from tsview.util import format_formula as pretty_formula
from tsview.blueprint import homeurl

from tshistory_refinery import cache


def format_formula(formula, baseurl=''):
    h = HTML()
    formatted = pretty_formula(
        formula,
        baseurl
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


def refinery_bp(tsa, more_sections=None):
    engine = tsa.engine

    bp = Blueprint(
        'refinery',
        __name__,
        template_folder='templates',
        static_folder='refinery_static',
    )

    @bp.route('/')
    def welcome():
        title = 'Refinery cockpit'
        sections = {
            'Time series': {
                'Series Catalog': url_for('tsview.tssearch'),
                'Series Quick-View': url_for('tsview.home'),
                'Delete Series': url_for('tsview.tsdelete'),
            },
            'Formula': {
                'All Formulas': url_for('refinery.formulas'),
                'Upload New Formulas': url_for('refinery.addformulas'),
                'Edit a new Formula': url_for('tsview.tsformula'),
                'Edit the formula cache': url_for('tsview.formulacache'),
                'Formula operators documentation': url_for('tsview.formula_operators'),
            },
            'Tasks': {
                'Monitoring': url_for('reworkui.home')
            }
        }

        if more_sections is not None:
            sections.update(more_sections())

        return render_template(
            'summary.html',
            title=title,
            sections=sections
        )

    # extra formula handling

    @bp.route('/formulas')
    def formulas():
        pd.set_option('display.max_colwidth', None)
        fmt = {
            'name': format_name,
            'formula': lambda f: format_formula(f, baseurl=homeurl()),
            'metadata': format_metadata
        }
        with engine.begin() as cn:
            return render_template(
                'bigtable.html',
                table=pd.read_sql_query(
                    'select id, name, internal_metadata->>\'formula\' as formula '
                    'from tsh.registry '
                    'where internal_metadata->\'formula\' is not null '
                    'order by name',
                    cn
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
            'select name, internal_metadata->\'formula\' as text '
            'from tsh.registry '
            'where internal_metadata->\'formula\' is not null',
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

    # /formula
    # formula cache

    @bp.route('/policies')
    def cache_policies():
        q = select(
            'name', 'ready',
            'initial_revdate',
            'look_before', 'look_after',
            'revdate_rule', 'schedule_rule'
        ).table('tsh.cache_policy')

        out = [
            dict(item)
            for item in sorted(
                    q.do(engine).fetchall(),
                    key=itemgetter('name')
            )
        ]
        for item in out:
            item['active'] = cache.scheduled_policy(engine, item['name'])

        return jsonify(out)

    @bp.route('/delete-policy/<name>', methods=['DELETE'])
    def delete_policy(name):
        cache.delete_policy(engine, name)
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

    @bp.route('/validate-policy', methods=['PUT'])
    def validate_policy():
        args = policy_args(request.json)
        return jsonify(
            cache.validate_policy(**args)
        )

    @bp.route('/create-policy', methods=['PUT'])
    def create_policy():
        args = policy_args(request.json)
        try:
            cache.new_policy(engine, **args)
        except ValueError as err:
            return make_response(str(err), 400)
        except TypeError:
            return make_response('Missing fields', 400)

        return make_response('', 201)

    @bp.route('/edit-policy', methods=['PUT'])
    def edit_policy():
        args = policy_args(request.json)
        try:
            cache.edit_policy(engine, **args)
        except ValueError as err:
            return make_response(str(err), 400)
        except TypeError:
            return make_response('Missing fields', 400)

        return make_response('', 200)

    @bp.route('/schedule-policy', methods=['PUT'])
    def schedule_policy():
        args = policy_args(request.json)
        if cache.scheduled_policy(engine, args.name):
            return make_response('nothing changed', 200)

        cache.schedule_policy(engine, args.name)
        return make_response('', 201)

    @bp.route('/scheduled-policy')
    def scheduled_policy():
        args = policy_args(request.args)
        return jsonify(
            cache.scheduled_policy(engine, args.name)
        )

    @bp.route('/unschedule-policy', methods=['PUT'])
    def unschedule_policy():
        args = policy_args(request.json)
        if not cache.scheduled_policy(engine, args.name):
            return make_response('nothing changed', 200)

        cache.unschedule_policy(engine, args.name)
        return make_response('', 201)

    @bp.route('/cacheable-formulas')
    def cacheable_formulas():
        # remove me and use the api cache_free_series (allsources=False)
        with tsa.engine.begin() as cn:
            return jsonify(
                sorted(
                    tsa.tsh.cacheable_formulas(cn)
                )
            )


    @bp.route('/policy-series/<name>')
    def policy_series(name):
        return jsonify(
            sorted(
                cache.policy_series(engine, name)
            )
        )

    class set_policy_args(_args):
        types = {
            'seriesname': str,
            'policyname': str,
        }

    @bp.route('/set-series-policy', methods=['PUT'])
    def set_series_policy():
        args = set_policy_args(request.json)
        with engine.begin() as cn:
            cache.set_policy(
                cn,
                args.policyname,
                args.seriesname
            )
        return make_response('', 201)


    class unset_policy_args(_args):
        types = {
            'name': str
        }

    @bp.route('/unset-series-policy', methods=['PUT'])
    def unset_series_policy():
        args = unset_policy_args(request.json)
        with engine.begin() as cn:
            tsa.tsh.unset_cache_policy(cn, args.name)
        return make_response('', 204)

    # /formula cache

    return bp
