# encoding: utf-8
from unittest.mock import MagicMock, PropertyMock

import pytest

from ...model import ConfigurationSetting
from ...model.hasfulltablecache import HasFullTableCache
from ...testing import DatabaseTest


class TestHasFullTableCache:
    @pytest.fixture()
    def mock_db(self):
        def mock():
            mock_db = MagicMock()
            mock_db.info = {}
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
        cache_miss_hook = MagicMock(side_effect=lambda: (mock, True))
        created, is_new = mock_class.by_cache_key(db, mock.cache_key(), cache_miss_hook)
        cache = mock_class.get_cache(db)

        # Item from create_func
        assert is_new is True
        assert created is mock
        cache_miss_hook.assert_called_once()

        # Make sure statistics are kept
        assert cache.stats.misses == 1
        assert cache.stats.hits == 0
        assert len(cache.id) == 1
        assert len(cache.key) == 1

        # Item from cache
        cached_item, cached_is_new = mock_class.by_cache_key(
            db, mock.cache_key(), cache_miss_hook
        )
        assert cached_is_new is False
        assert cached_item is created
        cache_miss_hook.assert_called_once()

        # Make sure statistics are kept
        assert cache.stats.misses == 1
        assert cache.stats.hits == 1
        assert len(cache.id) == 1
        assert len(cache.key) == 1

    def test_warm_cache(self, mock_db, mock_class):
        item1 = MagicMock()
        type(item1).id = PropertyMock(return_value=1)
        item1.cache_key = MagicMock(return_value="key1")
        item2 = MagicMock()
        type(item2).id = PropertyMock(return_value=2)
        item2.cache_key = MagicMock(return_value="key2")

        def populate():
            return [item1, item2]

        db = mock_db()
        # Throw exception if we query database
        db.query.side_effect = Exception

        # Warm cache with items from populate
        mock_class.warm_cache(db, populate)
        cache = mock_class.get_cache(db)

        assert cache.stats.misses == 0
        assert cache.stats.hits == 0
        assert len(cache.id) == 2
        assert len(cache.key) == 2

        print(cache.id.keys())

        # Get item1 by key and id
        item1_by_id = mock_class.by_id(db, 1)
        assert item1_by_id is item1
        item1_by_key, item1_new = mock_class.by_cache_key(db, "key1", db.query)
        assert item1_by_key is item1
        assert item1_new is False

        assert cache.stats.misses == 0
        assert cache.stats.hits == 2

        # Get item2 by key and id
        item2_by_id = mock_class.by_id(db, 2)
        assert item2_by_id is item2
        item2_by_key, item2_new = mock_class.by_cache_key(db, "key2", db.query)
        assert item2_by_key is item2
        assert item2_new is False

        assert cache.stats.misses == 0
        assert cache.stats.hits == 4


class TestHasFullTableCache2(DatabaseTest):
    def test_cached_values_are_properly_updated(self):
        setting_key = "key"
        setting_old_value = "old value"
        setting_new_value = "new value"

        # First, let's create a ConfigurationSetting instance and save it in the database.
        setting = ConfigurationSetting(key=setting_key, _value=setting_old_value)
        self._db.add(setting)
        self._db.commit()

        # Let's save ConfigurationSetting's ID to find it later.
        setting_id = setting.id

        # Now let's fetch the configuration setting from the database and add it to the cache.
        db_setting1 = (
            self._db.query(ConfigurationSetting)
            .filter(ConfigurationSetting.key == setting_key)
            .one()
        )
        ConfigurationSetting.warm_cache(self._db, lambda: [db_setting1])

        # After, let's fetch it again and change its value.
        db_setting2 = (
            self._db.query(ConfigurationSetting)
            .filter(ConfigurationSetting.key == setting_key)
            .one()
        )
        db_setting2.value = setting_new_value

        # Now let's make sure that the cached value has also been updated.
        assert (
            ConfigurationSetting.by_id(self._db, setting_id)._value == setting_new_value
        )
