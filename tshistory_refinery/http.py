import json

from flask_restx import (
    inputs,
    Resource,
    reqparse
)

from tshistory.http.util import onerror
from tshistory.http.client import unwraperror
from tshistory_xl.http import (
    xl_httpapi,
    XLClient
)

from tshistory_refinery import cache


cp = reqparse.RequestParser()
cp.add_argument(
    'name',
    type=str,
    required=True,
    help='cache policy name'
)

newcp = cp.copy()
newcp.add_argument(
    'initial_revdate',
    type=str,
    required=True,
    help='initial revision date'
)
newcp.add_argument(
    'look_before',
    type=str,
    required=True,
    help='date expression to provide the refresh horizon on the left'
)
newcp.add_argument(
    'look_after',
    type=str,
    required=True,
    help='date expression to provide the refresh horizon on the right'
)
newcp.add_argument(
    'revdate_rule',
    type=str,
    required=True,
    help='cron rule for the revision date'
)
newcp.add_argument(
    'schedule_rule',
    type=str,
    required=True,
    help='cron rule to schedule the refresher'
)

deletecp = cp.copy()


def jsonlist(thing):
    return json.loads(thing)


mapcp = cp.copy()
mapcp.add_argument(
    'seriesnames',
    type=jsonlist,
    required=True,
    help='series list to associate with a cache policy'
)

unmapcp = reqparse.RequestParser()
unmapcp.add_argument(
    'seriesnames',
    type=jsonlist,
    required=True,
    help='series list to remove from their cache policy'
)



class refinery_httpapi(xl_httpapi):
    __slots__ = 'tsa', 'bp', 'api', 'nss', 'nsg'

    def routes(self):
        super().routes()

        tsa = self.tsa
        api = self.api
        nsc = self.nsc = self.api.namespace(
            'cache',
            description='Formula Cache Operations'
        )

        @nsc.route('/policy')
        class cache_policy(Resource):

            @api.expect(newcp)
            @onerror
            def put(self):
                args = newcp.parse_args()
                try:
                    tsa.new_cache_policy(
                        args.name,
                        args.initial_revdate,
                        args.look_before,
                        args.look_after,
                        args.revdate_rule,
                        args.schedule_rule
                    )
                except Exception as e:
                    api.abort(409, str(e))

                return '', 204


            @api.expect(newcp)
            @onerror
            def patch(self):
                args = newcp.parse_args()
                try:
                    tsa.edit_cache_policy(
                        args.name,
                        args.initial_revdate,
                        args.look_before,
                        args.look_after,
                        args.revdate_rule,
                        args.schedule_rule
                    )
                except Exception as e:
                    api.abort(409, str(e))

                return '', 204

            @api.expect(deletecp)
            @onerror
            def delete(self):
                args = deletecp.parse_args()
                tsa.delete_cache_policy(
                    args.name
                )

                return '', 204

        @nsc.route('/mapping')
        class policy_mapping(Resource):

            @api.expect(mapcp)
            @onerror
            def put(self):
                args = mapcp.parse_args()
                tsa.set_cache_policy(
                    args.name,
                    args.seriesnames
                )

                return '', 204

            @api.expect(unmapcp)
            @onerror
            def delete(self):
                args = unmapcp.parse_args()
                tsa.unset_cache_policy(
                    args.seriesnames
                )

                return '', 204

        @nsc.route('/cacheable')
        class cacheable_series(Resource):

            @onerror
            def get(self):
                return tsa.cache_free_series()

        @nsc.route('/policies')
        class cache_policy(Resource):

            @onerror
            def get(self):
                return tsa.cache_policies()

        @nsc.route('/policy-series')
        class cache_policy_series(Resource):

            @api.expect(cp)
            @onerror
            def get(self):
                args = cp.parse_args()
                return tsa.cache_policy_series(args.name)


class RefineryClient(XLClient):

    def __repr__(self):
        return f"refinery-http-client(uri='{self.uri}')"

    @unwraperror
    def new_cache_policy(
            self,
            name,
            initial_revdate,
            look_before,
            look_after,
            revdate_rule,
            schedule_rule):

        res = self.session.put(f'{self.uri}/cache/policy', data={
            'name': name,
            'initial_revdate': initial_revdate,
            'look_before': look_before,
            'look_after': look_after,
            'revdate_rule': revdate_rule,
            'schedule_rule': schedule_rule
        })

        if res.status_code == 409:
            raise ValueError(res.json()['message'])

        if res.status_code == 204:
            return

        return res

    @unwraperror
    def edit_cache_policy(
            self,
            name,
            initial_revdate,
            look_before,
            look_after,
            revdate_rule,
            schedule_rule):

        res = self.session.patch(f'{self.uri}/cache/policy', data={
            'name': name,
            'initial_revdate': initial_revdate,
            'look_before': look_before,
            'look_after': look_after,
            'revdate_rule': revdate_rule,
            'schedule_rule': schedule_rule
        })

        if res.status_code == 409:
            raise ValueError(res.json()['message'])

        if res.status_code == 204:
            return

        return res

    @unwraperror
    def delete_cache_policy(self, name):
        res = self.session.delete(f'{self.uri}/cache/policy', data={
            'name': name,
        })

        if res.status_code == 204:
            return

        return res

    @unwraperror
    def set_cache_policy(self, policyname, seriesnames):
        res = self.session.put(f'{self.uri}/cache/mapping', data={
            'name': policyname,
            'seriesnames': json.dumps(seriesnames)
        })
        if res.status_code == 204:
            return

        return res

    @unwraperror
    def unset_cache_policy(self, seriesnames):
        res = self.session.delete(f'{self.uri}/cache/mapping', data={
            'seriesnames': json.dumps(seriesnames)
        })
        if res.status_code == 204:
            return

        return res

    @unwraperror
    def cache_free_series(self):
        res = self.session.get(f'{self.uri}/cache/cacheable')
        if res.status_code == 200:
            return res.json()

        return res

    @unwraperror
    def cache_policies(self):
        res = self.session.get(f'{self.uri}/cache/policies')
        if res.status_code == 200:
            return res.json()

        return res

    @unwraperror
    def cache_policy_series(self, policyname):
        res = self.session.get(f'{self.uri}/cache/policy-series', params={
            'name': policyname
        })
        if res.status_code == 200:
            return res.json()

        return res
