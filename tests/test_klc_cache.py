from autoingest.app.klc import routes


def test_cached_returns_cached_value():
    routes._cache.clear()
    calls = []

    def fetch():
        calls.append(1)
        return ["value"]

    assert routes._cached(("key",), fetch) == ["value"]
    assert routes._cached(("key",), fetch) == ["value"]
    assert len(calls) == 1


def test_cached_refetches_after_ttl(monkeypatch):
    routes._cache.clear()
    calls = []

    def fetch():
        calls.append(1)
        return [len(calls)]

    assert routes._cached(("key",), fetch) == [1]
    monkeypatch.setattr(routes, "CACHE_TTL_SECONDS", 0)
    assert routes._cached(("key",), fetch) == [2]


def test_cache_evicts_oldest_entry(monkeypatch):
    routes._cache.clear()
    monkeypatch.setattr(routes, "CACHE_MAX_ENTRIES", 2)

    routes._cached(("first",), lambda: [1])
    routes._cached(("second",), lambda: [2])
    routes._cached(("third",), lambda: [3])

    assert ("first",) not in routes._cache
    assert ("second",) in routes._cache
    assert ("third",) in routes._cache
