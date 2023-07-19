from tshistory.search import (
    _OPMAP,
    query,
    usym
)


# rendez-vous object
IMPORTCALLBACK = None


class hascachepolicy(query):

    def __expr__(self):
        return '(by.cache)'

    @classmethod
    def _fromtree(cls, _):
        return cls()

    def sql(self, namespace='tsh'):
        return (
            f'(internal_metadata -> \'formula\' is not null and '
            f'exists(select 1 from "{namespace}".cache_policy_series as cps where '
            f'       cps.series_id = reg.id)'
            f')',
            {}
        )


_OPMAP['by.cache'] = 'hascachepolicy'
