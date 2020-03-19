#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午3:56
"""

import atexit
from collections.abc import MutableMapping, MutableSequence
from typing import Dict, List, Optional, Tuple, Union

import aelog
from pymongo import MongoClient as MongodbClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, DuplicateKeyError, InvalidName, PyMongoError

from . import Query
from ._alchemy import AlchemyMixIn, BaseMongo, BasePagination, SessionMixIn
from ._err_msg import mongo_msg
from .err import HttpError, MongoDuplicateKeyError, MongoError, MongoInvalidNameError

__all__ = ("SyncMongo",)


class Pagination(BasePagination):
    """Internal helper class returned by :meth:`BaseQuery.paginate`.  You
    can also construct it from any other SQLAlchemy query object if you are
    working with other libraries.  Additionally it is possible to pass `None`
    as query object in which case the :meth:`prev` and :meth:`next` will
    no longer work.

    """

    def __init__(self, session, query: Query, total: int, items: List[Dict], query_key: Dict):
        super().__init__(session, query, total, items, query_key)

    # noinspection PyProtectedMember
    def prev(self, ) -> List[Dict]:
        """Returns a :class:`Pagination` object for the previous page."""
        self.page = self.page - 1
        _offset_clause = (self.page - 1) * self.per_page
        return self.session._find_many(self.cname, self.query_key, self.exclude_key, self.per_page,
                                       _offset_clause, self.sort)

    # noinspection PyProtectedMember
    def next(self, ) -> List[Dict]:
        """Returns a :class:`Pagination` object for the next page."""
        self.page = self.page + 1
        _offset_clause = (self.page - 1) * self.per_page
        return self.session._find_many(self.cname, self.query_key, self.exclude_key, self.per_page,
                                       _offset_clause, self.sort)


# noinspection PyProtectedMember
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
        self.db = db
        self.message = message
        self.msg_zh = msg_zh
        self.max_per_page: int = max_per_page

    def _insert_one(self, cname: str, document: Union[List[Dict], Dict], insert_one: bool = True
                    ) -> Union[Tuple[str], str]:
        """
        插入一个单独的文档
        Args:
            cname:collection name
            document: document obj
            insert_one: insert_one insert_many的过滤条件，默认True
        Returns:
            返回插入的Objectid
        """
        try:
            if insert_one:
                result = self.db.get_collection(cname).insert_one(document)
            else:
                result = self.db.get_collection(cname).insert_many(document)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(cname, e))
        except DuplicateKeyError as e:
            raise MongoDuplicateKeyError("Duplicate key error, {}".format(e))
        except PyMongoError as err:
            aelog.exception("Insert one document failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[100][self.msg_zh])
        else:
            return str(result.inserted_id) if insert_one else (str(val) for val in result.inserted_ids)

    def _insert_many(self, cname: str, document: List[Dict]) -> Tuple[str]:
        """
        批量插入文档
        Args:
            cname:collection name
            document: document obj
        Returns:
            返回插入的Objectid列表
        """
        return self._insert_one(cname, document, insert_one=False)

    def _find_one(self, cname: str, query_key: Dict, exclude_key: Dict = None) -> Optional[Dict]:
        """
        查询一个单独的document文档
        Args:
            cname: collection name
            query_key: 查询document的过滤条件
            exclude_key: 过滤返回值中字段的过滤条件
        Returns:
            返回匹配的document或者None
        """
        try:
            find_data = self.db.get_collection(cname).find_one(query_key, projection=exclude_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(cname, e))
        except PyMongoError as err:
            aelog.exception("Find one document failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[103][self.msg_zh])
        else:
            if find_data and find_data.get("_id", None) is not None:
                find_data["id"] = str(find_data.pop("_id"))
            return find_data

    def _find_many(self, cname: str, query_key: Dict, exclude_key: Dict = None, limit: int = None,
                   skip: int = None, sort: List[Tuple] = None) -> List[Dict]:
        """
        批量查询document文档
        Args:
            cname: collection name
            query_key: 查询document的过滤条件
            exclude_key: 过滤返回值中字段的过滤条件
            limit: 限制返回的document条数
            skip: 从查询结果中调过指定数量的document
            sort: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        Returns:
            返回匹配的document列表
        """
        try:
            find_data = []
            cursor = self.db.get_collection(cname).find(query_key, projection=exclude_key, limit=limit, skip=skip,
                                                        sort=sort)
            for doc in cursor:
                if doc.get("_id", None) is not None:
                    doc["id"] = str(doc.pop("_id"))
                find_data.append(doc)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(cname, e))
        except PyMongoError as err:
            aelog.exception("Find many document failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[104][self.msg_zh])
        else:
            return find_data

    def _find_count(self, cname: str, query_key: Dict) -> int:
        """
        查询document的数量
        Args:
            cname: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回匹配的document数量
        """
        try:
            return self.db.get_collection(cname).count(query_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(cname, e))
        except PyMongoError as err:
            aelog.exception("Find many document failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[104][self.msg_zh])

    def _update_one(self, cname: str, query_key: Dict, update_data: Dict, upsert: bool = False,
                    update_one: bool = True) -> Dict:
        """
        更新匹配到的一个的document
        Args:
            cname: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
            update_one: update_one or update_many的匹配条件
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 1, "modified_count": 1, "upserted_id":"f"}
        """
        try:
            if update_one:
                result = self.db.get_collection(cname).update_one(query_key, update_data, upsert=upsert)
            else:
                result = self.db.get_collection(cname).update_many(query_key, update_data, upsert=upsert)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(cname, e))
        except DuplicateKeyError as e:
            raise MongoDuplicateKeyError("Duplicate key error, {}".format(e))
        except PyMongoError as err:
            aelog.exception("Update document failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[101][self.msg_zh])
        else:
            return {"matched_count": result.matched_count, "modified_count": result.modified_count,
                    "upserted_id": str(result.upserted_id) if result.upserted_id else None}

    def _update_many(self, cname: str, query_key: Dict, update_data: Dict, upsert: bool = False) -> Dict:
        """
        更新匹配到的所有的document
        Args:
            cname: collection name
            query_key: 查询document的过滤条件
            update_data: 对匹配的document进行更新的document
            upsert: 没有匹配到document的话执行插入操作，默认False
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 2, "modified_count": 2, "upserted_id":"f"}
        """
        return self._update_one(cname, query_key, update_data, upsert, update_one=False)

    def _delete_one(self, cname: str, query_key: Dict, delete_one: bool = True) -> int:
        """
        删除匹配到的一个的document
        Args:
            cname: collection name
            query_key: 查询document的过滤条件
            delete_one: delete_one delete_many的匹配条件
        Returns:
            返回删除的数量
        """
        try:
            if delete_one:
                result = self.db.get_collection(cname).delete_one(query_key)
            else:
                result = self.db.get_collection(cname).delete_many(query_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(cname, e))
        except PyMongoError as err:
            aelog.exception("Delete document failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[102][self.msg_zh])
        else:
            return result.deleted_count

    def _delete_many(self, cname: str, query_key: Dict) -> int:
        """
        删除匹配到的所有的document
        Args:
            cname: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return self._delete_one(cname, query_key, delete_one=False)

    def _aggregate(self, cname: str, pipline: List[Dict]) -> List[Dict]:
        """
        根据pipline进行聚合查询
        Args:
            cname: collection name
            pipline: 聚合查询的pipeline,包含一个后者多个聚合命令
        Returns:
            返回聚合后的document
        """
        result = []
        try:
            for doc in self.db.get_collection(cname).aggregate(pipline):
                if doc.get("_id", None) is not None:
                    doc["id"] = str(doc.pop("_id"))
                result.append(doc)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(cname, e))
        except PyMongoError as err:
            aelog.exception("Aggregate document failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[105][self.msg_zh])
        else:
            return result

    def insert_many(self, query: Query) -> Tuple[str]:
        """
        批量插入文档
        Args:
            query: Query class
                cname:collection name
                document: document obj
        Returns:
            返回插入的转换后的_id列表
        """
        document: List[Dict] = query._insert_data
        if not isinstance(document, MutableSequence):
            raise MongoError("insert many document failed, document is not a iterable type.")
        for document_ in document:
            if not isinstance(document_, MutableMapping):
                raise MongoError("insert one document failed, document is not a mapping type.")
            self._update_doc_id(document_)
        return self._insert_many(query._cname, document)

    def insert_one(self, query: Query) -> str:
        """
        插入一个单独的文档
        Args:
            query: Query class
                cname:collection name
                document: document obj
        Returns:
            返回插入的转换后的_id
        """
        document: Dict = query._insert_data
        if not isinstance(document, MutableMapping):
            raise MongoError("insert one document failed, document is not a mapping type.")
        return self._insert_one(query._cname, self._update_doc_id(document))

    def find_one(self, query: Query) -> Optional[Dict]:
        """
        查询一个单独的document文档
        Args:
            query: Query class
                cname: collection name
                query_key: 查询document的过滤条件
                exclude_key: 过滤返回值中字段的过滤条件
        Returns:
            返回匹配的document或者None
        """
        return self._find_one(query._cname, self._update_query_key(query._query_key), exclude_key=query._exclude_key)

    # noinspection DuplicatedCode
    def find_many(self, query: Query) -> Pagination:
        """
        批量查询document文档
        Args:
            query: Query class
                cname: collection name
                query_key: 查询document的过滤条件
                exclude_key: 过滤返回值中字段的过滤条件
                per_page: 每页数据的数量
                page: 查询第几页的数据
                sort: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        Returns:
            Returns a :class:`Pagination` object.
        """

        query_key = self._update_query_key(query._query_key)
        items = self._find_many(query._cname, query_key, exclude_key=query._exclude_key, limit=query._limit_clause,
                                skip=query._offset_clause, sort=query._order_by)

        # No need to count if we're on the first page and there are fewer
        # items than we expected.
        if query._page == 1 and len(items) < query._per_page:
            total = len(items)
        else:
            total = self.find_count(query)

        return Pagination(self, query, total, items, query_key)

    def find_all(self, query: Query) -> List[Dict]:
        """
        批量查询document文档
        Args:
            query: Query class
                cname: collection name
                query_key: 查询document的过滤条件
                exclude_key: 过滤返回值中字段的过滤条件
                sort: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        Returns:
            返回匹配的document列表
        """
        return self._find_many(query._cname, self._update_query_key(query._query_key),
                               exclude_key=query._exclude_key, sort=query._order_by)

    def find_count(self, query: Query) -> int:
        """
        查询document的数量
        Args:
            query: Query class
                cname: collection name
                query_key: 查询document的过滤条件
        Returns:
            返回匹配的document数量
        """
        return self._find_count(query._cname, self._update_query_key(query._query_key))

    def update_many(self, query: Query) -> Dict:
        """
        更新匹配到的所有的document
        Args:
            query: Query class
                cname: collection name
                query_key: 查询document的过滤条件
                update_data: 对匹配的document进行更新的document
                upsert: 没有匹配到document的话执行插入操作，默认False
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 2, "modified_count": 2, "upserted_id":"f"}
        """
        return self._update_many(query._cname, self._update_query_key(query._query_key),
                                 self._update_update_data(query._update_data), upsert=query._upsert)

    def update_one(self, query: Query) -> Dict:
        """
        更新匹配到的一个的document
        Args:
            query: Query class
                cname: collection name
                query_key: 查询document的过滤条件
                update_data: 对匹配的document进行更新的document
                upsert: 没有匹配到document的话执行插入操作，默认False
        Returns:
            返回匹配的数量和修改数量的dict, eg:{"matched_count": 1, "modified_count": 1, "upserted_id":"f"}
        """
        return self._update_one(query._cname, self._update_query_key(query._query_key),
                                self._update_update_data(query._update_data), upsert=query._upsert)

    def delete_many(self, query: Query) -> int:
        """
        删除匹配到的所有的document
        Args:
            query: Query class
                cname: collection name
                query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return self._delete_many(query._cname, self._update_query_key(query._query_key))

    def delete_one(self, query: Query) -> int:
        """
        删除匹配到的一个的document
        Args:
            query: Query class
                cname: collection name
                query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return self._delete_one(query._cname, self._update_query_key(query._query_key))

    # noinspection DuplicatedCode
    def aggregate(self, query: Query) -> List[Dict]:
        """
        根据pipline进行聚合查询
        Args:
            query: Query class
                cname: collection name
                pipline: 聚合查询的pipeline,包含一个后者多个聚合命令
                per_page: 每页数据的数量
                page: 查询第几页的数据
        Returns:
            返回聚合后的document
        """
        pipline: List[Dict] = query._pipline
        if not isinstance(pipline, MutableSequence):
            raise MongoError("Aggregate query failed, pipline arg is not a iterable type.")
        if query._limit_clause is not None and query._per_page is not None:
            pipline.extend([{'$skip': query._limit_clause}, {'$limit': query._per_page}])
        return self._aggregate(query._cname, pipline)


class SyncMongo(AlchemyMixIn, BaseMongo):
    """
    mongo 工具类
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

        self._verify_flask_app()  # 校验APP类型是否正确

        @app.before_first_request
        def open_connection():
            """

            Args:

            Returns:

            """
            self.bind_pool[None] = self._create_engine(
                host=self.host, port=self.port, username=self.username, passwd=self.passwd,
                pool_size=self.pool_size, dbname=self.dbname)

        @atexit.register
        def close_connection():
            """
            释放mongo连接池所有连接
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
                self.engine_pool[engine_name] = MongodbClient(
                    host, port, username=username, password=passwd, maxPoolSize=pool_size)
            db = self.engine_pool[engine_name].get_database(name=dbname)
        except ConnectionFailure as e:
            aelog.exception(f"Mongo connection failed host={host} port={port} error:{str(e)}")
            raise MongoError(f"Mongo connection failed host={host} port={port} error:{str(e)}")
        except InvalidName as e:
            aelog.exception(f"Invalid mongo db name {dbname} {str(e)}")
            raise MongoInvalidNameError(f"Invalid mongo db name {dbname} {str(e)}")
        except PyMongoError as err:
            aelog.exception(f"Mongo DB init failed! error: {str(err)}")
            raise MongoError("Mongo DB init failed!") from err
        else:
            return db

    @property
    def query(self, ) -> Query:
        """

        Args:

        Returns:

        """
        return Query(self.max_per_page)

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
