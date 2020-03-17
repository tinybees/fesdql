#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 18-12-25 下午3:56
"""

import atexit
from collections.abc import MutableMapping, MutableSequence
from typing import Dict, List, NoReturn, Optional, Tuple, Union

import aelog
from pymongo import MongoClient as MongodbClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError, InvalidName, PyMongoError

from ._alchemy import AlchemyMixIn
from ._err_msg import mongo_msg
from .err import HttpError, MongoDuplicateKeyError, MongoError, MongoInvalidNameError
from .utils import _verify_message

__all__ = ("SyncMongo",)


class SyncMongo(AlchemyMixIn, object):
    """
    mongo 工具类
    """

    def __init__(self, app=None, *, username: str = "mongo", passwd: str = None, host: str = "127.0.0.1",
                 port: int = 27017, dbname: str = None, pool_size: int = 50, **kwargs):
        """
        mongo 工具类
        Args:
            app: app应用
            host:mongo host
            port:mongo port
            dbname: database name
            username: mongo user
            passwd: mongo password
            pool_size: mongo pool size
        """
        self.app = app
        self.client = None
        self.db = None
        self.username = username
        self.passwd = passwd
        self.host = host
        self.port = port
        self.dbname = dbname
        self.pool_size = pool_size
        self.message = kwargs.get("message", {})
        self.use_zh = kwargs.get("use_zh", True)
        self.msg_zh = None

        if app is not None:
            self.init_app(app, username=self.username, passwd=self.passwd, host=self.host, port=self.port,
                          dbname=self.dbname, pool_size=self.pool_size, **kwargs)

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
        """
        self.app = app
        self._verify_flask_app()  # 校验APP类型是否正确
        username = username or app.config.get("ECLIENTS_MONGO_USERNAME", None) or self.username
        passwd = passwd or app.config.get("ECLIENTS_MONGO_PASSWD", None) or self.passwd
        host = host or app.config.get("ECLIENTS_MONGO_HOST", None) or self.host
        port = port or app.config.get("ECLIENTS_MONGO_PORT", None) or self.port
        dbname = dbname or app.config.get("ECLIENTS_MONGO_DBNAME", None) or self.dbname
        pool_size = pool_size or app.config.get("ECLIENTS_MONGO_POOL_SIZE", None) or self.pool_size
        message = kwargs.get("message") or app.config.get("ECLIENTS_MONGO_MESSAGE", None) or self.message
        use_zh = kwargs.get("use_zh") or app.config.get("ECLIENTS_MONGO_MSGZH", None) or self.use_zh

        passwd = passwd if passwd is None else str(passwd)
        self.message = _verify_message(mongo_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"

        @app.before_first_request
        def open_connection():
            """

            Args:

            Returns:

            """
            self._create_db_conn(host, port, pool_size, username, passwd, dbname)

        @atexit.register
        def close_connection():
            """
            释放mongo连接池所有连接
            Args:

            Returns:

            """
            if self.client:
                self.client.close()

    # noinspection DuplicatedCode
    def init_engine(self, *, username: str = None, passwd: str = None, host: str = None, port: int = None,
                    dbname: str = None, pool_size: int = None, **kwargs):
        """
        mongo 实例初始化
        Args:
            host:mongo host
            port:mongo port
            dbname: database name
            username: mongo user
            passwd: mongo password
            pool_size: mongo pool size
        """
        username = username or self.username
        passwd = passwd or self.passwd
        host = host or self.host
        port = port or self.port
        dbname = dbname or self.dbname
        pool_size = pool_size or self.pool_size
        message = kwargs.get("message") or self.message
        use_zh = kwargs.get("use_zh") or self.use_zh

        passwd = passwd if passwd is None else str(passwd)
        self.message = _verify_message(mongo_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"

        # engine
        self._create_db_conn(host, port, pool_size, username, passwd, dbname)

        @atexit.register
        def close_connection():
            """
            释放mongo连接池所有连接
            Args:

            Returns:

            """
            if self.client:
                self.client.close()

    def _create_db_conn(self, host: str, port: int, pool_size: int, username: str, passwd: str, dbname: str
                        ) -> NoReturn:
        try:
            self.client = MongodbClient(host, port, maxPoolSize=pool_size, username=username, password=passwd)
            self.db = self.client.get_database(name=dbname)
        except ConnectionFailure as e:
            aelog.exception(f"Mongo connection failed host={host} port={port} error:{str(e)}")
            raise MongoError(f"Mongo connection failed host={host} port={port} error:{str(e)}")
        except InvalidName as e:
            aelog.exception(f"Invalid mongo db name {dbname} {str(e)}")
            raise MongoInvalidNameError(f"Invalid mongo db name {dbname} {str(e)}")
        except PyMongoError as err:
            aelog.exception(f"Mongo DB init failed! error: {str(err)}")
            raise MongoError("Mongo DB init failed!") from err

    def _insert_document(self, name: str, document: Union[List[Dict], Dict], insert_one: bool = True
                         ) -> Union[Tuple[str], str]:
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
                result = self.db.get_collection(name).insert_one(document)
            else:
                result = self.db.get_collection(name).insert_many(document)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except DuplicateKeyError as e:
            raise MongoDuplicateKeyError("Duplicate key error, {}".format(e))
        except PyMongoError as err:
            aelog.exception("Insert one document failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[100][self.msg_zh])
        else:
            return str(result.inserted_id) if insert_one else (str(val) for val in result.inserted_ids)

    def _insert_documents(self, name: str, documents: List[Dict]) -> Tuple[str]:
        """
        批量插入文档
        Args:
            name:collection name
            documents: documents obj
        Returns:
            返回插入的Objectid列表
        """
        return self._insert_document(name, documents, insert_one=False)

    def _find_document(self, name: str, query_key: Dict, filter_key: Dict = None) -> Optional[Dict]:
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
            find_data = self.db.get_collection(name).find_one(query_key, projection=filter_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Find one document failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[103][self.msg_zh])
        else:
            if find_data and find_data.get("_id", None) is not None:
                find_data["id"] = str(find_data.pop("_id"))
            return find_data

    def _find_documents(self, name: str, query_key: Dict, filter_key: Dict = None, limit: int = None,
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
            for doc in cursor:
                if doc.get("_id", None) is not None:
                    doc["id"] = str(doc.pop("_id"))
                find_data.append(doc)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Find many documents failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[104][self.msg_zh])
        else:
            return find_data

    def _find_count(self, name: str, query_key: Dict) -> int:
        """
        查询documents的数量
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回匹配的document数量
        """
        try:
            return self.db.get_collection(name).count(query_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Find many documents failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[104][self.msg_zh])

    def _update_document(self, name: str, query_key: Dict, update_data: Dict, upsert: bool = False,
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
                result = self.db.get_collection(name).update_one(query_key, update_data, upsert=upsert)
            else:
                result = self.db.get_collection(name).update_many(query_key, update_data, upsert=upsert)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except DuplicateKeyError as e:
            raise MongoDuplicateKeyError("Duplicate key error, {}".format(e))
        except PyMongoError as err:
            aelog.exception("Update documents failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[101][self.msg_zh])
        else:
            return {"matched_count": result.matched_count, "modified_count": result.modified_count,
                    "upserted_id": str(result.upserted_id) if result.upserted_id else None}

    def _update_documents(self, name: str, query_key: Dict, update_data: Dict, upsert: bool = False) -> Dict:
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
        return self._update_document(name, query_key, update_data, upsert, update_one=False)

    def _delete_document(self, name: str, query_key: Dict, delete_one: bool = True) -> int:
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
                result = self.db.get_collection(name).delete_one(query_key)
            else:
                result = self.db.get_collection(name).delete_many(query_key)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Delete documents failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[102][self.msg_zh])
        else:
            return result.deleted_count

    def _delete_documents(self, name: str, query_key: Dict) -> int:
        """
        删除匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return self._delete_document(name, query_key, delete_one=False)

    def _aggregate(self, name: str, pipline: List[Dict]) -> List[Dict]:
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
            for doc in self.db.get_collection(name).aggregate(pipline):
                if doc.get("_id", None) is not None:
                    doc["id"] = str(doc.pop("_id"))
                result.append(doc)
        except InvalidName as e:
            raise MongoInvalidNameError("Invalid collention name {} {}".format(name, e))
        except PyMongoError as err:
            aelog.exception("Aggregate documents failed, {}".format(err))
            raise HttpError(400, message=mongo_msg[105][self.msg_zh])
        else:
            return result

    def insert_documents(self, name: str, documents: List[Dict]) -> Tuple[str]:
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
        return self._insert_documents(name, documents)

    def insert_document(self, name: str, document: Dict) -> str:
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
        return self._insert_document(name, self._update_doc_id(document))

    def find_document(self, name: str, query_key: Dict = None, filter_key: Dict = None) -> Optional[Dict]:
        """
        查询一个单独的document文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
        Returns:
            返回匹配的document或者None
        """
        return self._find_document(name, self._update_query_key(query_key), filter_key=filter_key)

    def find_documents(self, name: str, query_key: Dict = None, filter_key: Dict = None, limit: int = 0,
                       page: int = 1, sort: List[Tuple] = None) -> List[Dict]:
        """
        批量查询documents文档
        Args:
            name: collection name
            query_key: 查询document的过滤条件
            filter_key: 过滤返回值中字段的过滤条件
            limit: 每页数据的数量
            page: 查询第几页的数据
            sort: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        Returns:
            返回匹配的document列表
        """
        skip = (int(page) - 1) * int(limit)
        return self._find_documents(name, self._update_query_key(query_key), filter_key=filter_key, limit=int(limit),
                                    skip=skip, sort=sort)

    def find_count(self, name: str, query_key: Dict = None) -> int:
        """
        查询documents的数量
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回匹配的document数量
        """
        return self._find_count(name, self._update_query_key(query_key))

    def update_documents(self, name: str, query_key: Dict, update_data: Dict, upsert: bool = False) -> Dict:
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
        return self._update_documents(name, self._update_query_key(query_key),
                                      self._update_update_data(update_data), upsert=upsert)

    def update_document(self, name: str, query_key: Dict, update_data: Dict, upsert: bool = False) -> Dict:
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
        return self._update_document(name, self._update_query_key(query_key),
                                     self._update_update_data(update_data), upsert=upsert)

    def delete_documents(self, name: str, query_key: Dict) -> int:
        """
        删除匹配到的所有的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return self._delete_documents(name, self._update_query_key(query_key))

    def delete_document(self, name: str, query_key: Dict) -> int:
        """
        删除匹配到的一个的document
        Args:
            name: collection name
            query_key: 查询document的过滤条件
        Returns:
            返回删除的数量
        """
        return self._delete_document(name, self._update_query_key(query_key))

    # noinspection DuplicatedCode
    def aggregate(self, name: str, pipline: List[Dict], page: int = None, limit: int = None) -> List[Dict]:
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
        return self._aggregate(name, pipline)
