#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午3:41
"""

from collections.abc import MutableMapping, MutableSequence
from typing import Dict, List, Optional, Tuple, Union

import aelog
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.database import Database
from pymongo.errors import (ConnectionFailure, DuplicateKeyError, InvalidName, PyMongoError)

from ._alchemy import AlchemyMixIn, BaseMongo, BasePagination, SessionMixIn
from .err import HttpError, MongoDuplicateKeyError, MongoError, MongoInvalidNameError

__all__ = ("AsyncMongo",)


class Pagination(BasePagination):
    """Internal helper class returned by :meth:`BaseQuery.paginate`.  You
    can also construct it from any other SQLAlchemy query object if you are
    working with other libraries.  Additionally it is possible to pass `None`
    as query object in which case the :meth:`prev` and :meth:`next` will
    no longer work.

    """

    def __init__(self, session: 'Session', name: str, page: int, per_page: int, total: int, items: List[Dict],
                 query_key: Dict, filter_key: Dict, sort: List[Tuple] = None):
        super().__init__(session, name, page, per_page, total, items, query_key, filter_key, sort)

    # noinspection PyProtectedMember
    async def prev(self, ) -> List[Dict]:
        """Returns a :class:`Pagination` object for the previous page."""
        _offset_clause = (self.page - 1 - 1) * self.per_page
        return await self.session._find_many(self.name, self.query_key, self.filter_key, self.per_page,
                                             _offset_clause, self.sort)

    # noinspection PyProtectedMember
    async def next(self, ) -> List[Dict]:
        """Returns a :class:`Pagination` object for the next page."""
        _offset_clause = (self.page - 1 + 1) * self.per_page
        return await self.session._find_many(self.name, self.query_key, self.filter_key, self.per_page,
                                             _offset_clause, self.sort)


class Session(SessionMixIn, object):
    """
    query session
    """

    def __init__(self, db: Database, message: Dict, msg_zh: str, max_per_page: int = None):
        """
            query session
        Args:
            db: db engine
            message: 消息提示
            msg_zh: 中文提示或者而英文提示
            max_per_page: 每页最大的数量
        """
        self.db: Database = db
        self.message: Dict = message
        self.msg_zh: str = msg_zh
        self.max_per_page: int = max_per_page

    async def _insert_one(self, name: str, document: Union[Dict, List[Dict]], insert_one: bool = True
                          ) -> Union[str, Tuple[str]]:
        """
        插入一个单独的文档
        Args:
            name:collection name
            document: document obj
            insert_one: insert_one insert_many的过滤条件，默认True
        Returns:
            返回插入的Objectid
        """
        try:
            if insert_one:
                result = await self.db.get_collection(name).insert_one(document)
            else:
                result = await self.db.get_collection(name).insert_many(document)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except DuplicateKeyError as e:
            raise MongoDuplicateKeyError("Duplicate key error, {}".format(e))
        except PyMongoError as err:
            aelog.exception("Insert one document failed, {}".format(err))
            raise HttpError(400, message=self.message[100][self.msg_zh], error=err)
        else:
            return str(result.inserted_id) if insert_one else (str(val) for val in result.inserted_ids)

    async def _insert_many(self, name: str, documents: List[Dict]) -> Tuple[str]:
        """
        批量插入文档
        Args:
            name:collection name
            documents: documents obj
        Returns:
            返回插入的Objectid列表
        """
        return await self._insert_one(name, documents, insert_one=False)

    async def _find_one(self, name: str, query_key: Dict, filter_key: Dict = None) -> Optional[Dict]:
        """
        查询一个单独的document文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
        Returns:
            返回匹配的document或者None
        """
        try:
            find_data = await self.db.get_collection(name).find_one(query_key, projection=filter_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Find one document failed, {}".format(err))
            raise HttpError(400, message=self.message[103][self.msg_zh], error=err)
        else:
            if find_data and find_data.get("_id", None) is not None:
                find_data["id"] = str(find_data.pop("_id"))
            return find_data

    async def _find_many(self, name: str, query_key: Dict, filter_key: Dict = None, limit: int = None,
                         skip: int = None, sort: List[Tuple] = None) -> List[Dict]:
        """
        批量查询documents文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
            limit: 限制返回的document条数
            skip: 从查询结果中调过指定数量的document
            sort: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        Returns:
            返回匹配的document列表
        """
        try:
            find_data = []
            cursor = self.db.get_collection(name).find(query_key, projection=filter_key, limit=limit, skip=skip,
                                                       sort=sort)
            # find_data = await cursor.to_list(None)
            async for doc in cursor:
                if doc.get("_id", None) is not None:
                    doc["id"] = str(doc.pop("_id"))
                find_data.append(doc)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Find many documents failed, {}".format(err))
            raise HttpError(400, message=self.message[104][self.msg_zh], error=err)
        else:
            return find_data

    async def _find_count(self, name: str, query_key: Dict) -> int:
        """
        查询documents的数量
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回匹配的document数量
        """
        try:
            return await self.db.get_collection(name).count(query_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Find many documents failed, {}".format(err))
            raise HttpError(400, message=self.message[104][self.msg_zh], error=err)

    async def _update_one(self, name: str, query_key: Dict, update_data: Dict, upsert: bool = False,
                          update_one: bool = True) -> Dict:
        """
        更新匹配到的一个的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
            update_one: update_one or update_many的匹配条件
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 1, "modified_count": 1, "upserted_id":"f"}
        """
        try:
            if update_one:
                result = await self.db.get_collection(name).update_one(query_key, update_data, upsert=upsert)
            else:
                result = await self.db.get_collection(name).update_many(query_key, update_data, upsert=upsert)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except DuplicateKeyError as e:
            raise MongoDuplicateKeyError("Duplicate key error, {}".format(e))
        except PyMongoError as err:
            aelog.exception("Update documents failed, {}".format(err))
            raise HttpError(400, message=self.message[101][self.msg_zh], error=err)
        else:
            return {"matched_count": result.matched_count, "modified_count": result.modified_count,
                    "upserted_id": str(result.upserted_id) if result.upserted_id else None}

    async def _update_many(self, name: str, query_key: Dict, update_data: Dict, upsert: bool = False) -> Dict:
        """
        更新匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 2, "modified_count": 2, "upserted_id":"f"}
        """
        return await self._update_one(name, query_key, update_data, upsert, update_one=False)

    async def _delete_one(self, name: str, query_key: Dict, delete_one: bool = True) -> int:
        """
        删除匹配到的一个的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            delete_one: delete_one delete_many的匹配条件
        Returns:
            返回删除的数量
        """
        try:
            if delete_one:
                result = await self.db.get_collection(name).delete_one(query_key)
            else:
                result = await self.db.get_collection(name).delete_many(query_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Delete documents failed, {}".format(err))
            raise HttpError(400, message=self.message[102][self.msg_zh], error=err)
        else:
            return result.deleted_count

    async def _delete_many(self, name: str, query_key: Dict) -> int:
        """
        删除匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return await self._delete_one(name, query_key, delete_one=False)

    async def _aggregate(self, name: str, pipline: List[Dict]) -> List[Dict]:
        """
        根据pipline进行聚合查询
        Args:
            name: collection name
            pipline: 聚合查询的pipeline,包含一个后者多个聚合命令
        Returns:
            返回聚合后的documents
        """
        result = []
        try:
            async for doc in self.db.get_collection(name).aggregate(pipline):
                if doc.get("_id", None) is not None:
                    doc["id"] = str(doc.pop("_id"))
                result.append(doc)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Aggregate documents failed, {}".format(err))
            raise HttpError(400, message=self.message[105][self.msg_zh], error=err)
        else:
            return result

    async def insert_many(self, name: str, documents: List[Dict]) -> Tuple[str]:
        """
        批量插入文档
        Args:
            name:collection name
            documents: documents obj
        Returns:
            返回插入的转换后的_id列表
        """
        if not isinstance(documents, MutableSequence):
            raise MongoError("insert many document failed, documents is not a iterable type.")
        documents = list(documents)
        for document in documents:
            if not isinstance(document, MutableMapping):
                raise MongoError("insert one document failed, document is not a mapping type.")
            self._update_doc_id(document)
        return await self._insert_many(name, documents)

    async def insert_one(self, name: str, document: Dict) -> str:
        """
        插入一个单独的文档
        Args:
            name:collection name
            document: document obj
        Returns:
            返回插入的转换后的_id
        """
        if not isinstance(document, MutableMapping):
            raise MongoError("insert one document failed, document is not a mapping type.")
        document = dict(document)
        return await self._insert_one(name, self._update_doc_id(document))

    async def find_one(self, name: str, query_key: Dict = None, filter_key: Dict = None) -> Optional[Dict]:
        """
        查询一个单独的document文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
        Returns:
            返回匹配的document或者None
        """
        return await self._find_one(name, self._update_query_key(query_key), filter_key=filter_key)

    # noinspection DuplicatedCode
    async def find_many(self, name: str, query_key: Dict = None, filter_key: Dict = None, per_page: int = 0,
                        page: int = 1, sort: List[Tuple] = None) -> Pagination:
        """
        批量查询documents文档,分页数据
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
            per_page: 每页数据的数量
            page: 查询第几页的数据
            sort: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        Returns:
            Returns a :class:`Pagination` object.
        """
        if self.max_per_page is not None:
            per_page = min(per_page, self.max_per_page)

        if page < 1:
            page = 1

        if per_page < 0:
            per_page = 20

        # 如果per_page为0,则证明要获取所有的数据，否则还是通常的逻辑
        if per_page != 0:
            _limit_clause = per_page
            _offset_clause = (page - 1) * per_page
        else:
            _limit_clause = None
            _offset_clause = None

        query_key = self._update_query_key(query_key)
        items = await self._find_many(name, query_key, filter_key=filter_key, limit=_limit_clause,
                                      skip=_offset_clause, sort=sort)

        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if page == 1 and len(items) < per_page:
            total = len(items)
        else:
            total = await self.find_count(name, query_key)

        return Pagination(self, name, page, per_page, total, items, query_key, filter_key, sort)

    async def find_all(self, name: str, query_key: Dict = None, filter_key: Dict = None,
                       sort: List[Tuple] = None) -> List[Dict]:
        """
        批量查询documents文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
            sort: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        Returns:
            返回匹配的document列表
        """
        return await self._find_many(name, self._update_query_key(query_key), filter_key=filter_key, sort=sort)

    async def find_count(self, name: str, query_key: Dict = None) -> int:
        """
        查询documents的数量
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回匹配的document数量
        """
        return await self._find_count(name, self._update_query_key(query_key))

    async def update_many(self, name: str, query_key: Dict, update_data: Dict, upsert: bool = False) -> Dict:
        """
        更新匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 2, "modified_count": 2, "upserted_id":"f"}
        """
        update_data = dict(update_data)
        return await self._update_many(name, self._update_query_key(query_key),
                                       self._update_update_data(update_data), upsert=upsert)

    async def update_one(self, name: str, query_key: Dict, update_data: Dict, upsert: bool = False) -> Dict:
        """
        更新匹配到的一个的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 1, "modified_count": 1, "upserted_id":"f"}
        """
        update_data = dict(update_data)
        return await self._update_one(name, self._update_query_key(query_key),
                                      self._update_update_data(update_data), upsert=upsert)

    async def delete_many(self, name: str, query_key: Dict) -> int:
        """
        删除匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return await self._delete_many(name, self._update_query_key(query_key))

    async def delete_one(self, name: str, query_key: Dict) -> int:
        """
        删除匹配到的一个的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return await self._delete_one(name, self._update_query_key(query_key))

    # noinspection DuplicatedCode
    async def aggregate(self, name: str, pipline: List[Dict], page: int = None, limit: int = None) -> List[Dict]:
        """
        根据pipline进行聚合查询
        Args:
            name: collection name
            pipline: 聚合查询的pipeline,包含一个后者多个聚合命令
            limit: 每页数据的数量
            page: 查询第几页的数据
        Returns:
            返回聚合后的documents
        """
        if not isinstance(pipline, MutableSequence):
            raise MongoError("Aggregate query failed, pipline arg is not a iterable type.")
        if page is not None and limit is not None:
            pipline.extend([{'$skip': (int(page) - 1) * int(limit)}, {'$limit': int(limit)}])
        return await self._aggregate(name, pipline)


class AsyncMongo(AlchemyMixIn, BaseMongo):
    """
    mongo 非阻塞工具类
    """

    def init_app(self, app, *, username: str = None, passwd: str = None, host: str = None, port: int = None,
                 dbname: str = None, pool_size: int = None, **kwargs):
        """
        mongo 实例初始化
        Args:
            app: app应用
            host:mongo host
            port:mongo port
            dbname: database name
            username: mongo user
            passwd: mongo password
            pool_size: mongo pool size
        Returns:

        """
        super().init_app(app, username=username, passwd=passwd, host=host, port=port, dbname=dbname,
                         pool_size=pool_size, **kwargs)
        
        self._verify_sanic_app()  # 校验APP类型是否正确

        @app.listener('before_server_start')
        async def open_connection(app_, loop):
            """

            Args:

            Returns:

            """
            self.bind_pool[None] = self._create_engine(
                host=self.host, port=self.port, username=self.username, passwd=self.passwd,
                pool_size=self.pool_size, dbname=self.dbname)

        @app.listener('after_server_stop')
        async def close_connection(app_, loop):
            """

            Args:

            Returns:

            """
            for _, engine in self.engine_pool.items():
                if engine:
                    engine.close()

    def _create_engine(self, host: str, port: int, username: str, passwd: str, pool_size: int,
                       dbname: str) -> Database:
        # host和port确定了mongodb实例,username确定了权限,其他的无关紧要
        engine_name = f"{host}_{port}_{username}"
        try:
            if engine_name not in self.engine_pool:
                self.engine_pool[engine_name] = AsyncIOMotorClient(
                    host, port, username=username, password=passwd, maxPoolSize=pool_size)
            db = self.engine_pool[engine_name].get_database(name=dbname)
        except ConnectionFailure as e:
            aelog.exception("Mongo connection failed host={} port={} error:{}".format(host, port, e))
            raise MongoError("Mongo connection failed host={} port={} error:{}".format(host, port, e))
        except InvalidName as e:
            aelog.exception("Invalid mongo db name {} {}".format(dbname, e))
            raise MongoInvalidNameError("Invalid mongo db name {} {}".format(dbname, e))
        except PyMongoError as err:
            aelog.exception("Mongo DB init failed! error: {}".format(err))
            raise MongoError("Mongo DB init failed!") from err
        else:
            return db

    @property
    def session(self, ) -> Session:
        """
        session default bind
        Args:

        Returns:

        """
        if None not in self.bind_pool:
            raise ValueError("Default bind is not exist.")
        if None not in self.session_pool:
            self.session_pool[None] = Session(self.bind_pool[None], self.message, self.msg_zh)
        return self.session_pool[None]

    def gen_session(self, bind: str) -> Session:
        """
        session bind
        Args:
            bind: engine pool one of connection
        Returns:

        """
        self._get_engine(bind)
        if bind not in self.session_pool:
            self.session_pool[bind] = Session(self.bind_pool[bind], self.message, self.msg_zh)
        return self.session_pool[bind]
