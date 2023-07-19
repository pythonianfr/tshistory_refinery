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

class cachepolicy(query):
    __slots__ = ('query',)

    def __init__(self, query: str):
        self.query = query

    def __expr__(self):
        return f'(by.cachepolicy "{self.query}")'

    @classmethod
    def _fromtree(cls, tree):
        return cls(tree[1])

    def sql(self, namespace='tsh'):
        vid = usym('name')
        return (
            f'(internal_metadata -> \'formula\' is not null and '
            f'exists(select 1 '
            f'       from "{namespace}".cache_policy_series as cps,  '
            f'            "{namespace}".cache_policy as cp'
            f'       where'
            f'       cps.series_id = reg.id and'
            f'       cps.cache_policy_id = cp.id and'
            f'       cp.name like %({vid})s)'
            f')',
            {vid: f'%%{self.query}%%'}
        )


_OPMAP['by.cache'] = 'hascachepolicy'
_OPMAP['by.cachepolicy'] = 'cachepolicy'
