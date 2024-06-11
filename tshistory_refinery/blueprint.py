from operator import itemgetter
from pathlib import Path

import pandas as pd
from flask import (
    Blueprint,
    jsonify,
    make_response,
    render_template,
    request,
    url_for,
    send_from_directory,
)
from pml import HTML
from rework_ui.helper import argsdict as _args

from sqlhelp import select
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

    @bp.route('/favicon.ico')
    def favicon():
        return send_from_directory(
            Path(bp.root_path, 'refinery_static'),
            'arrow.ico',
            mimetype='image/vnd.microsoft.icon'
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


    # /formula
    # formula cache

    @bp.route('/policies')
    def cache_policies():
        q = select(
            'name',
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
