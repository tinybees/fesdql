#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/3/1 下午3:51
"""
import atexit
import copy
from math import ceil
from typing import Dict, List, MutableMapping, MutableSequence, NoReturn, Tuple, Union

from bson import ObjectId
from bson.errors import BSONError
from marshmallow import Schema
from pymongo.database import Database

from ._err_msg import mongo_msg
from .err import ConfigError, FuncArgsError
from .query import Query
from .utils import _verify_message, under2camel

__all__ = ("BasePagination", "BaseMongo", "AlchemyMixIn", "SessionMixIn")


# noinspection PyProtectedMember
class BasePagination(object):
    """Internal helper class returned by :meth:`BaseQuery.paginate`.  You
    can also construct it from any other SQLAlchemy query object if you are
    working with other libraries.  Additionally it is possible to pass `None`
    as query object in which case the :meth:`prev` and :meth:`next` will
    no longer work.

    """

    def __init__(self, session, query: Query, total: int, items: List[Dict], query_key: Dict):
        #: the unlimited query object that was used to create this
        #: aiomongoclient object.
        self.session = session
        # collection name
        self.cname = query._cname
        #: the current page number (1 indexed)
        self.page: int = query._page
        #: the number of items to be displayed on a page.
        self.per_page: int = query._per_page
        #: the total number of items matching the query
        self.total: int = total
        #: the items for the current page
        self.items: List[Dict] = items
        # query key
        self.query_key: Dict = query_key
        # exclude key
        self.exclude_key: Dict = query._exclude_key
        # sort key
        self.sort: List[Tuple] = query._order_by

    @property
    def pages(self) -> int:
        """The total number of pages"""
        if self.per_page == 0:
            pages = 0
        else:
            pages = int(ceil(self.total / float(self.per_page)))
        return pages

    def prev(self, ) -> List[Dict]:
        """Returns a :class:`Pagination` object for the previous page."""
        raise NotImplementedError

    @property
    def prev_num(self) -> int:
        """Number of the previous page."""
        if not self.has_prev:
            return None
        return self.page - 1

    @property
    def has_prev(self) -> bool:
        """True if a previous page exists"""
        return self.page > 1

    async def next(self, ) -> List[Dict]:
        """Returns a :class:`Pagination` object for the next page."""
        raise NotImplementedError

    @property
    def has_next(self) -> bool:
        """True if a next page exists."""
        return self.page < self.pages

    @property
    def next_num(self) -> int:
        """Number of the next page"""
        if not self.has_next:
            return None
        return self.page + 1


class BaseMongo(object):
    """
    mongo 基类
    """

    def __init__(self, app=None, *, username: str = "mongo", passwd: str = None, host: str = "127.0.0.1",
                 port: int = 27017, dbname: str = None, pool_size: int = 50, **kwargs):
        """
        mongo 非阻塞工具类
        Args:
            app: app应用
            host:mongo host
            port:mongo port
            dbname: database name
            username: mongo user
            passwd: mongo password
            pool_size: mongo pool size
            fesdql_binds: binds config, eg:{"first":{"fesdql_mongo_host":"127.0.0.1",
                                        "fesdql_mongo_port":3306,
                                        "fesdql_mongo_username":"root",
                                        "fesdql_mongo_passwd":"",
                                        "fesdql_mongo_dbname":"dbname",
                                        "fesdql_mongo_pool_size":10}}

        """
        self.app = app
        self.engine_pool = {}  # engine pool
        self.bind_pool = {}  # bind engine pool
        self.session_pool = {}  # session pool
        # default bind connection
        self.username = username
        self.passwd = passwd
        self.host = host
        self.port = port
        self.dbname = dbname
        self.pool_size = pool_size
        # other info
        self.fesdql_binds: Dict = kwargs.get("fesdql_binds", {})  # binds config
        self.message = kwargs.get("message", {})
        self.use_zh = kwargs.get("use_zh", True)
        self.max_per_page = kwargs.get("max_per_page", None)
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
        Returns:

        """

        self.app = app

        self.username = username or app.config.get("FESDQL_MONGO_USERNAME", None) or self.username
        passwd = passwd or app.config.get("FESDQL_MONGO_PASSWD", None) or self.passwd
        self.host = host or app.config.get("FESDQL_MONGO_HOST", None) or self.host
        self.port = port or app.config.get("FESDQL_MONGO_PORT", None) or self.port
        self.dbname = dbname or app.config.get("FESDQL_MONGO_DBNAME", None) or self.dbname
        self.pool_size = pool_size or app.config.get("FESDQL_MONGO_POOL_SIZE", None) or self.pool_size

        message = kwargs.get("message") or app.config.get("FESDQL_MONGO_MESSAGE", None) or self.message
        use_zh = kwargs.get("use_zh") or app.config.get("FESDQL_MONGO_MSGZH", None) or self.use_zh

        self.fesdql_binds = kwargs.get("fesdql_binds") or app.config.get(
            "FESDQL_BINDS", None) or self.fesdql_binds
        self.verify_binds()

        self.passwd = passwd if passwd is None else str(passwd)
        self.message = _verify_message(mongo_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"
        self.max_per_page = kwargs.get("max_per_page", None) or self.max_per_page

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
        Returns:

        """
        username = username or self.username
        passwd = passwd or self.passwd
        host = host or self.host
        port = port or self.port
        dbname = dbname or self.dbname
        self.pool_size = pool_size or self.pool_size

        message = kwargs.get("message") or self.message
        use_zh = kwargs.get("use_zh") or self.use_zh

        self.fesdql_binds = kwargs.get("fesdql_binds") or self.fesdql_binds
        self.verify_binds()

        passwd = passwd if passwd is None else str(passwd)
        self.message = _verify_message(mongo_msg, message)
        self.msg_zh = "msg_zh" if use_zh else "msg_en"
        self.max_per_page = kwargs.get("max_per_page", None) or self.max_per_page

        # engine
        self.bind_pool[None] = self._create_engine(host=host, port=port, username=username, passwd=passwd,
                                                   pool_size=pool_size, dbname=dbname)

        @atexit.register
        def close_connection():
            """

            Args:

            Returns:

            """
            for _, engine in self.engine_pool.items():
                if engine:
                    engine.close()

    def _create_engine(self, host: str, port: int, username: str, passwd: str, pool_size: int,
                       dbname: str) -> Database:
        raise NotImplementedError

    def _get_engine(self, bind: str) -> NoReturn:
        """
        session bind
        Args:
            bind: engine pool one of connection
        Returns:

        """
        if bind not in self.fesdql_binds:
            raise ValueError("bind is not exist, please config it in the FESDQL_BINDS.")
        if bind not in self.bind_pool:
            bind_conf: Dict = self.fesdql_binds[bind]
            self.bind_pool[bind] = self._create_engine(
                host=bind_conf.get("fesdql_mongo_host"), port=bind_conf.get("fesdql_mongo_port"),
                username=bind_conf.get("fesdql_mongo_username"), passwd=bind_conf.get("fesdql_mongo_passwd"),
                pool_size=bind_conf.get("fesdql_mongo_pool_size") or self.pool_size,
                dbname=bind_conf.get("fesdql_mongo_dbname"))

    def verify_binds(self, ):
        """
        校验fesdql_binds
        Args:

        Returns:

        """
        if self.fesdql_binds:
            if not isinstance(self.fesdql_binds, dict):
                raise TypeError("fesdql_binds type error, must be Dict.")
            for bind_name, bind in self.fesdql_binds.items():
                if not isinstance(bind, dict):
                    raise TypeError(f"fesdql_binds config {bind_name} type error, must be Dict.")
                missing_items = []
                for item in ["fesdql_mongo_host", "fesdql_mongo_port", "fesdql_mongo_username",
                             "fesdql_mongo_passwd", "fesdql_mongo_dbname"]:
                    if item not in bind:
                        missing_items.append(item)
                if missing_items:
                    raise ConfigError(f"fesdql_binds config {bind_name} error, "
                                      f"missing {' '.join(missing_items)} config item.")


class AlchemyMixIn(object):
    """
    base alchemy
    """

    # noinspection PyUnresolvedReferences
    def _verify_sanic_app(self, ):
        """
        校验APP类型是否正确

        暂时只支持sanic框架
        Args:

        Returns:

        """

        try:
            from sanic import Sanic
        except ImportError as e:
            raise ImportError(f"Sanic import error {e}.")
        else:
            if not isinstance(self.app, Sanic):
                raise FuncArgsError("app type must be Sanic.")

    # noinspection PyUnresolvedReferences
    def _verify_flask_app(self, ):
        """
        校验APP类型是否正确

        暂时只支持flask框架
        Args:

        Returns:

        """

        try:
            from flask import Flask
        except ImportError as e:
            raise ImportError(f"Flask import error {e}.")
        else:
            if not isinstance(self.app, Flask):
                raise FuncArgsError("app type must be Flask.")

    @staticmethod
    def gen_schema(schema_cls: Schema, class_suffix: str = None, table_suffix: str = None,
                   table_name: str = None, field_mapping: Dict[str, str] = None,
                   schema_fields: Union[Tuple[str], List[str]] = None):
        """
        用于根据现有的schema生成新的schema类

        1.主要用于分表的查询和插入生成新的schema,这时候生成的schema和原有的schema一致,主要是类名和表明不同.
        2.映射字段主要用来处理同一个字段在不同的库中有不同的名称的情况
        3.生成新的schema类时的字段多少,如果字段比schema_cls类中的多,则按照schema_cls中的字段为准,
        如果字段比schema_cls类中的少,则以schema_fields中的为准
        Args:
            schema_cls: 要生成分表的schema类
            class_suffix: 新的schema类名的后缀,生成新的类时需要使用
            table_suffix: 新的table名的后缀,生成新的表名时需要使用
            table_name: 如果指定了table name则使用,否则使用schema_cls的table name
            field_mapping: 字段映射,字段别名,如果有字段别名则生成的别名按照映射中的别名来,
                           如果没有则按照schema_cls中的name来处理
            schema_fields: 生成新的schema类时的字段多少,如果字段比schema_cls类中的多,则按照schema_cls中的字段为准,
                    如果字段比schema_cls类中的少,则以schema_fields中的为准
        Returns:
            新生成的schema类
        """
        if not issubclass(schema_cls, Schema):
            raise ValueError("schema_cls must be Schema type.")

        if table_name is None:
            table_name = f"{getattr(schema_cls, '__tablename__', schema_cls.__name__.rstrip('Schema'))}"
        if class_suffix:
            class_name = f"{under2camel(table_name)}{class_suffix.capitalize()}Schema"
        else:
            class_name = f"{under2camel(table_name)}Schema"
        if table_suffix:
            table_name = f"{table_name}_{table_suffix}"

        if getattr(schema_cls, "_cache_class", None) is None:
            setattr(schema_cls, "_cache_class", {})

        schema_cls_ = getattr(schema_cls, "_cache_class").get(class_name, None)
        if schema_cls_ is None:
            attr_fields = {}
            field_mapping = {} if not isinstance(field_mapping, MutableMapping) else field_mapping
            schema_fields = tuple() if not isinstance(
                schema_fields, MutableSequence) else (*schema_fields, *field_mapping.keys())
            for attr_name, attr_field in getattr(schema_cls, "_declared_fields", {}).items():
                if schema_fields and attr_name not in schema_fields:
                    continue
                attr_field = copy.copy(attr_field)
                setattr(attr_field, "attribute", field_mapping.get(attr_name))
                attr_fields[attr_name] = attr_field
            schema_cls_ = type(class_name, (Schema,), {
                "__doc__": schema_cls.__doc__,
                "__tablename__": table_name,
                "__module__": schema_cls.__module__,
                **attr_fields})
            getattr(schema_cls, "_cache_class")[class_name] = schema_cls_

        return schema_cls_


class SessionMixIn(object):
    """
    session minin
    """

    @staticmethod
    def _update_update_data(update_data: Dict) -> Dict:
        """
        处理update data, 包装最常使用的操作
        Args:
            update_data: 需要更新的文档值
        Returns:
            返回处理后的update data
        """

        # $set用的比较多，这里默认做个封装
        if len(update_data) > 1:
            update_data = {"$set": update_data}
        else:
            operator, doc = update_data.popitem()
            pre_flag = operator.startswith("$")
            update_data = {"$set" if not pre_flag else operator: {operator: doc} if not pre_flag else doc}
        return update_data

    @staticmethod
    def _update_query_key(query_key: Dict) -> Dict:
        """
        更新查询的query
        Args:
            query_key: 查询document的过滤条件
        Returns:
            返回处理后的query key
        """
        query_key = dict(query_key) if query_key else {}
        try:
            for key, val in query_key.items():
                if isinstance(val, MutableMapping):
                    if key != "id":
                        query_key[key] = {key if key.startswith("$") else f"${key}": val for key, val in val.items()}
                    else:
                        query_key["_id"] = {
                            key if key.startswith("$") else f"${key}": [ObjectId(val) for val in val]
                            if "in" in key else val for key, val in query_key.pop(key).items()}
                else:
                    if key == "id":
                        query_key["_id"] = ObjectId(query_key.pop("id"))
        except BSONError as e:
            raise FuncArgsError(str(e))
        else:
            return query_key

    @staticmethod
    def _update_doc_id(document: Dict) -> Dict:
        """
        修改文档中的_id
        Args:
            document: document obj
        Returns:
            返回处理后的document
        """
        if "id" in document:
            try:
                document["_id"] = ObjectId(document.pop("id"))
            except BSONError as e:
                raise FuncArgsError(str(e))
        return document
