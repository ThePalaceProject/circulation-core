# encoding: utf-8
from unittest.mock import MagicMock

import pytest

from ...model.hasfulltablecache import HasFullTableCache


class TestHasFullTableCache:
    @pytest.fixture()
    def mock_db(self):
        def mock():
            mock_db = MagicMock()
            mock_db._palace_cache = {}
            return mock_db

        return mock

    @pytest.fixture()
    def mock(self):
        mock = MagicMock()
        mock.id = "the only ID"
        mock.cache_key = MagicMock(return_value="the only cache key")
        return mock

    @pytest.fixture()
    def mock_class(self):
        return HasFullTableCache

    def test_get_cache(self, mock_db, mock_class):
        mock_db1 = mock_db()
        mock_db2 = mock_db()

        # Calling get_cache with two different database
        # sessions should return two different caches
        cache1 = mock_class.get_cache(mock_db1)
        cache2 = mock_class.get_cache(mock_db2)
        assert cache1 is not cache2

        # Each one is a CacheTuple instance
        assert isinstance(cache1, mock_class.CacheTuple)
        assert isinstance(cache2, mock_class.CacheTuple)

    def test_cache_insert(self, mock_db, mock_class, mock):
        db = mock_db()
        cache = mock_class.get_cache(db)
        mock_class._cache_insert(mock, cache)

        # Items are inserted in both the key and id cache
        assert cache.id[mock.id] == mock
        assert cache.key[mock.cache_key()] == mock

    def test_by_id(self, mock_db, mock_class, mock):
        db = mock_db()

        # Look up item using by_id
        item = mock_class.by_id(db, mock.id)
        cache = mock_class.get_cache(db)

        # Make sure statistics are kept
        assert cache.stats.misses == 1
        assert cache.stats.hits == 0
        assert len(cache.id) == 1
        assert len(cache.key) == 1
        # Item was queried from DB
        db.query.assert_called_once()

        # Lookup item again
        cached_item = mock_class.by_id(db, item.id)

        # Stats are updated
        assert cache.stats.misses == 1
        assert cache.stats.hits == 1
        assert len(cache.id) == 1
        assert len(cache.key) == 1
        # Item comes from cache
        assert item == cached_item
        db.query.assert_called_once()

    def test_by_cache_key_miss_triggers_create_function(
        self, mock_db, mock_class, mock
    ):

        db = mock_db()
        create_func = MagicMock(side_effect=lambda: (mock, True))
        created, is_new = mock_class.by_cache_key(db, mock.cache_key(), create_func)
        cache = mock_class.get_cache(db)

        # Item from create_func
        assert is_new is True
        assert created is mock
        create_func.assert_called_once()

        # Make sure statistics are kept
        assert cache.stats.misses == 1
        assert cache.stats.hits == 0
        assert len(cache.id) == 1
        assert len(cache.key) == 1

        # Item from cache
        cached_item, cached_is_new = mock_class.by_cache_key(
            db, mock.cache_key(), create_func
        )
        assert cached_is_new is False
        assert cached_item is created
        create_func.assert_called_once()

        # Make sure statistics are kept
        assert cache.stats.misses == 1
        assert cache.stats.hits == 1
        assert len(cache.id) == 1
        assert len(cache.key) == 1
