import json

from flask_restx import (
    inputs,
    Resource,
    reqparse
)

from tshistory.http.util import (
    onerror,
    required_roles
)
from tshistory.http.client import unwraperror
from tshistory_xl.http_xl import (
    xl_httpapi,
    xl_httpclient
)


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

delete_cache = reqparse.RequestParser()
delete_cache.add_argument(
    'name',
    type=str,
    required=True,
    help='series name'
)

series_policy = reqparse.RequestParser()
series_policy.add_argument(
    'name',
    type=str,
    required=True,
    help='series name'
)


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

cacheable = reqparse.RequestParser()
cacheable.add_argument(
    'allsources',
    type=inputs.boolean,
    required=True,
    default=True,
    help='get the answer for all sources'
)

shc = reqparse.RequestParser()
shc.add_argument(
    'name',
    type=str,
    required=True,
    help='series name'
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

            @api.doc(
                responses={204: 'Success', 409: 'Error'},
                description="""Create a new cache policy

Cache policies control automated refreshing of formula series to improve query performance.

**Parameters:**
- name: policy name
- initial_revdate: initial revision date (ISO8601)
- look_before: time expression for left refresh horizon (e.g. "today", "today - 1 month")
- look_after: time expression for right refresh horizon (e.g. "today + 3 months")
- revdate_rule: cron expression for revision date scheduling
- schedule_rule: cron expression for cache refresh scheduling

**Returns:** empty body, status 204

**Errors:**
- 409: policy creation failed (e.g. duplicate name)

**Example:**
```
PUT /cache/policy
  name=daily-refresh&initial_revdate=2025-01-01T00:00:00Z
  &look_before=today - 1 month&look_after=today + 3 months
  &revdate_rule=0 2 * * *&schedule_rule=0 3 * * *
→ 204 Success
```
"""
            )
            @api.expect(newcp)
            @onerror
            @required_roles('admin', 'rw')
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


            @api.doc(
                responses={204: 'Success', 409: 'Error'},
                description="""Update an existing cache policy

**Parameters:**
- name: policy name
- initial_revdate: initial revision date (ISO8601)
- look_before: time expression for left refresh horizon
- look_after: time expression for right refresh horizon
- revdate_rule: cron expression for revision date scheduling
- schedule_rule: cron expression for cache refresh scheduling

**Returns:** empty body, status 204

**Errors:**
- 409: policy update failed (e.g. invalid cron expression)
"""
            )
            @api.expect(newcp)
            @onerror
            @required_roles('admin', 'rw')
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

            @api.doc(
                responses={204: 'Success'},
                description="""Delete a cache policy

**Parameters:**
- name: policy name

**Returns:** empty body, status 204

**Note:** Associated series will be unmapped from the policy before deletion.
"""
            )
            @api.expect(deletecp)
            @onerror
            @required_roles('admin', 'rw')
            def delete(self):
                args = deletecp.parse_args()
                tsa.delete_cache_policy(
                    args.name
                )

                return '', 204

        @nsc.route('/mapping')
        class policy_mapping(Resource):

            @api.doc(
                responses={204: 'Success'},
                description="""Associate series to a cache policy

**Parameters:**
- name: policy name
- seriesnames: JSON array of series names

**Returns:** empty body, status 204

**Example:**
```
PUT /cache/mapping
  name=daily-refresh&seriesnames=["temperature", "pressure", "humidity"]
→ 204 Success
```
"""
            )
            @api.expect(mapcp)
            @onerror
            @required_roles('admin', 'rw')
            def put(self):
                args = mapcp.parse_args()
                tsa.set_cache_policy(
                    args.name,
                    args.seriesnames
                )

                return '', 204

            @api.doc(
                responses={204: 'Success'},
                description="""Remove series from their cache policy

**Parameters:**
- seriesnames: JSON array of series names

**Returns:** empty body, status 204

**Example:**
```
DELETE /cache/mapping
  seriesnames=["temperature", "pressure"]
→ 204 Success
```
"""
            )
            @api.expect(unmapcp)
            @onerror
            @required_roles('admin', 'rw')
            def delete(self):
                args = unmapcp.parse_args()
                tsa.unset_cache_policy(
                    args.seriesnames
                )

                return '', 204

        @nsc.route('/cacheable')
        class cacheable_series(Resource):

            @api.doc(
                responses={200: 'Got content'},
                description="""Get list of series eligible for caching

Returns formula series that can be cached but are not yet associated with any cache policy.

**Parameters:**
- allsources: if true, include series from all sources; if false, only primary source (default: true)

**Returns:** list of series names

**Example:**
```
GET /cache/cacheable?allsources=true
→ ["wind-forecast", "temp-adjusted", "pressure-corrected"]
```
"""
            )
            @api.expect(cacheable)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = cacheable.parse_args()
                return tsa.cache_free_series(args.allsources)

        @nsc.route('/policies')
        class cache_policies(Resource):

            @api.doc(
                responses={200: 'Got content'},
                description="""Get list of all cache policies

**Returns:** list of policy definitions
```json
[
  {
    "name": "daily-refresh",
    "initial_revdate": "2025-01-01T00:00:00Z",
    "look_before": "today - 1 month",
    "look_after": "today + 3 months",
    "revdate_rule": "0 2 * * *",
    "schedule_rule": "0 3 * * *"
  }
]
```
"""
            )
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                return tsa.cache_policies()

        @nsc.route('/policy-series')
        class cache_policy_series(Resource):

            @api.doc(
                responses={200: 'Got content'},
                description="""Get series associated with a cache policy

**Parameters:**
- name: policy name

**Returns:** list of series names

**Example:**
```
GET /cache/policy-series?name=daily-refresh
→ ["temperature", "pressure", "humidity"]
```
"""
            )
            @api.expect(cp)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = cp.parse_args()
                return tsa.cache_policy_series(args.name)

        @nsc.route('/series-policy')
        class cache_series_policy(Resource):

            @api.doc(
                responses={200: 'Got content'},
                description="""Get the cache policy for a series

**Parameters:**
- name: series name

**Returns:** policy name string, or null if series has no cache policy

**Example:**
```
GET /cache/series-policy?name=temperature
→ "daily-refresh"
```
"""
            )
            @api.expect(series_policy)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = series_policy.parse_args()
                return tsa.cache_series_policy(args.name)

        @nsc.route('/series-has-cache')
        class series_has_cache(Resource):

            @api.doc(
                responses={200: 'Got content'},
                description="""Check if a series has cached data

**Parameters:**
- name: series name

**Returns:** boolean

**Example:**
```
GET /cache/series-has-cache?name=temperature
→ true
```
"""
            )
            @api.expect(shc)
            @onerror
            @required_roles('admin', 'rw', 'ro')
            def get(self):
                args = shc.parse_args()
                return tsa.has_cache(args.name)

            @api.doc(
                responses={204: 'Success'},
                description="""Delete cached data for a series

**Parameters:**
- name: series name

**Returns:** empty body, status 204

**Note:** This removes cached data but does not unmap the series from its cache policy.
"""
            )
            @api.expect(delete_cache)
            @onerror
            @required_roles('admin', 'rw')
            def delete(self):
                args = delete_cache.parse_args()
                tsa.delete_cache(args.name)
                return '', 204

        @nsc.route('/refresh-policy-now')
        class refresh_policy_now(Resource):

            @api.doc(
                responses={200: 'Got content'},
                description="""Trigger immediate cache refresh for a policy

Refreshes all series associated with a cache policy immediately, bypassing the scheduled refresh.

**Parameters:**
- name: policy name

**Returns:** task ID (integer)

**Example:**
```
PUT /cache/refresh-policy-now?name=daily-refresh
→ 12345
```

**Note:** The returned task ID can be used to monitor the refresh operation status.
"""
            )
            @api.expect(cp)
            @onerror
            @required_roles('admin', 'rw')
            def put(self):
                args = cp.parse_args()
                tid = tsa.refresh_series_policy_now(args.name)
                return tid, 200


class refinery_httpclient(xl_httpclient):
    index = 3

    def __repr__(self):
        return f"refinery-http-client(uri='{self.uri}')"

    @unwraperror
    def new_cache_policy(
            self,
            name: str,
            initial_revdate: str,
            look_before: str,
            look_after: str,
            revdate_rule: str,
            schedule_rule: str):

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
            name: str,
            initial_revdate: str,
            look_before: str,
            look_after: str,
            revdate_rule: str,
            schedule_rule: str):

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
    def delete_cache_policy(self, name: str):
        res = self.session.delete(f'{self.uri}/cache/policy', data={
            'name': name,
        })

        if res.status_code == 204:
            return

        return res

    @unwraperror
    def set_cache_policy(self, policyname: str, seriesnames: list[str]):
        res = self.session.put(f'{self.uri}/cache/mapping', data={
            'name': policyname,
            'seriesnames': json.dumps(seriesnames)
        })
        if res.status_code == 204:
            return

        return res

    @unwraperror
    def unset_cache_policy(self, seriesnames: list[str]):
        res = self.session.delete(f'{self.uri}/cache/mapping', data={
            'seriesnames': json.dumps(seriesnames)
        })
        if res.status_code == 204:
            return

        return res

    @unwraperror
    def cache_free_series(self, allsources: bool=True):
        res = self.session.get(f'{self.uri}/cache/cacheable', params={
            'allsources': allsources
        })
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
    def cache_policy_series(self, policyname: str):
        res = self.session.get(f'{self.uri}/cache/policy-series', params={
            'name': policyname
        })
        if res.status_code == 200:
            return res.json()

        return res

    @unwraperror
    def cache_series_policy(self, seriesname: str):
        res = self.session.get(f'{self.uri}/cache/series-policy', params={
            'name': seriesname
        })
        if res.status_code == 200:
            return res.json()

        return res

    @unwraperror
    def has_cache(self, seriesname: str):
        res = self.session.get(f'{self.uri}/cache/series-has-cache', params={
            'name': seriesname
        })
        if res.status_code == 200:
            return res.json()

        return res

    @unwraperror
    def delete_cache(self, seriesname: str):
        res = self.session.delete(f'{self.uri}/cache/series-has-cache', params={
            'name': seriesname
        })
        if res.status_code == 204:
            return

        return res

    @unwraperror
    def refresh_series_policy_now(self, policyname: str):
        res = self.session.put(f'{self.uri}/cache/refresh-policy-now', params={
            'name': policyname
        })
        if res.status_code == 200:
            return res.json()

        return res
