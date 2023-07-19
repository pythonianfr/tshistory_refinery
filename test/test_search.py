from tshistory_refinery import search


def _serialize_roundtrip(searchobj):
    return search.query.fromexpr(searchobj.expr()).expr() == searchobj.expr()


def test_search():
    s0 = search.hascachepolicy()
    assert s0.expr() == '(by.cache)'
    assert _serialize_roundtrip(s0)

    s1 = search.cachepolicy('my-policy')
    assert s1.expr() == '(by.cachepolicy "my-policy")'
    assert _serialize_roundtrip(s1)
