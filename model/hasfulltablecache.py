# encoding: utf-8
# HasFullTableCache

from collections import namedtuple
from types import SimpleNamespace
from typing import Callable, Dict, Hashable, Optional, Tuple

from sqlalchemy.orm import Session

from . import Base, get_one


class HasFullTableCache(object):
    CacheTuple = namedtuple("CacheTuple", ["id", "key", "stats"])

    """
    A mixin class for ORM classes that maintain an in-memory cache of
    items previously requested from the database table for performance reasons.

    Items in this cache are always maintained in the same database session.
    """

    def cache_key(self):
        raise NotImplementedError()

    @classmethod
    def _cache_insert(cls, obj: Base, cache: CacheTuple):
        """Cache an object for later retrieval."""
        key = obj.cache_key()
        id = obj.id
        cache.id[id] = obj
        cache.key[key] = obj

    @classmethod
    def _cache_lookup(
        cls,
        _db: Session,
        cache: CacheTuple,
        cache_name: str,
        cache_key: Hashable,
        lookup_hook: Callable,
    ) -> Tuple[Optional[Base], bool]:
        """Helper method used by both by_id and by_cache_key.

        Looks up `cache_key` in the `cache_name` property of `cache`, returning
        the item if its found, or calling `lookup_hook` and adding the item to
        the cache if its not. This method also updates our cache statistics to
        keep track of cache hits and misses.
        """
        lookup_cache = getattr(cache, cache_name)
        if cache_key in lookup_cache:
            cache.stats.hits += 1
            return lookup_cache[cache_key], False
        else:
            cache.stats.misses += 1
            obj, new = lookup_hook()
            if obj is not None:
                cls._cache_insert(obj, cache)
            return obj, new

    @classmethod
    def get_cache(cls, _db: Session):
        """Get cache from database session."""
        try:
            cache = _db._palace_cache
        except AttributeError:
            _db._palace_cache = cache = {}
        if cls.__name__ not in cache:
            cache[cls.__name__] = cls.CacheTuple(
                {}, {}, SimpleNamespace(hits=0, misses=0)
            )
        return cache[cls.__name__]

    @classmethod
    def by_id(cls, _db: Session, id: int) -> Optional[Base]:
        """Look up an item by its unique database ID."""
        cache = cls.get_cache(_db)

        def lookup_hook():
            return get_one(_db, cls, id=id), False

        obj, _ = cls._cache_lookup(_db, cache, "id", id, lookup_hook)
        return obj

    @classmethod
    def by_cache_key(
        cls, _db: Session, cache_key: Hashable, lookup_hook: Callable
    ) -> Tuple[Optional[Base], bool]:
        """Look up and item by its cache key."""
        cache = cls.get_cache(_db)
        return cls._cache_lookup(_db, cache, "key", cache_key, lookup_hook)
