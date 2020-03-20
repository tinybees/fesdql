#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/3/1 上午12:00
"""
import inspect
from typing import (Dict, List, Tuple, Union)

from marshmallow import Schema

from .err import FuncArgsError

__all__ = ("Query",)


class BaseQuery(object):
    """
    查询
    """

    def __init__(self, ):
        """
            查询
        Args:

        """
        # collection name
        self._cname: str = None
        # 查询document的过滤条件
        self._query_key: Dict = None
        # 过滤返回值中字段的过滤条件
        self._exclude_key: Dict = None
        # 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
        self._order_by: List = None
        # 对匹配的document进行更新的document
        self._update_data: Dict = None
        # 没有匹配到document的话执行插入操作，默认False
        self._upsert: bool = False
        # 要插入的document obj
        self._insert_data: Union[List[Dict], Dict] = None
        # limit 每页的数量
        self._limit_clause: int = None
        # offset 要offset的数量
        self._offset_clause: int = None
        # aggregate 聚合查询的pipeline,包含一个后者多个聚合命令
        self._pipline: List[Dict] = None

    def where(self, **query_key) -> 'BaseQuery':
        """
        return basequery construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        query_key: 查询document的过滤条件,
            a dictionary specifying the query to be performed OR any other type to be used as
            the value for a query for ``"_id"``
        """
        if self._query_key is None:
            self._query_key = {}
        self._query_key.update(query_key)
        return self

    def collection(self, cclause: Union[Schema, str]) -> 'BaseQuery':
        """
        return basequery construct with the given expression added to
        its model clause.

        Arg:
            modelclause: Schema或者collection的名称
        """
        if inspect.isclass(cclause) and issubclass(cclause, Schema):
            cclause = getattr(cclause, "__tablename__", None)
            if cclause is None:
                raise FuncArgsError("cclause(Schema)中没有__tablename__属性")
        elif not isinstance(cclause, str):
            raise FuncArgsError("cclause只能为schema的子类或者字符串")

        self._cname = cclause
        return self

    table = collection

    def order_by(self, *clauses: Tuple[str, int]) -> 'BaseQuery':
        """
        return basequery with the given list of ORDER BY
        criterion applied.

        clauses: 排序方式，可以自定多种字段的排序，值为一个列表的键值对， eg:[('field1', pymongo.ASCENDING)]
            a list of (key, direction) pairs specifying the sort order for this query.
                eg: [('field1', pymongo.ASCENDING),
                    ('field2', pymongo.DESCENDING)]
        """
        if self._order_by is None:
            self._order_by = []
        for one_order in clauses:
            self._order_by.append(one_order)
        return self

    def upsert(self, upsert: bool = False) -> 'BaseQuery':
        """
        update upsert

        Args:
            upsert:  没有匹配到document的话执行插入操作，默认False,
                    If ``True``, perform an insert if no documents match the filter.
        Returns:
            返回更新的条数
        """
        self._upsert = upsert
        return self

    def exclude(self, **exclude) -> 'BaseQuery':
        """
        update upsert

        Args:
            exclude: 过滤返回值中字段的过滤条件,
                    Use a dict to exclude fields from the result,e.g. projection={'_id': False}
        Returns:
            返回更新的条数
        """
        if self._exclude_key is None:
            self._exclude_key = {}
        self._exclude_key.update(exclude)
        return self

    def aggregation(self, pipline) -> 'BaseQuery':
        """
        aggregation query

        Args:
            pipline: 包含聚合命令中的一个, a dict of aggregation pipeline stages
        Returns:
            返回更新的条数
        """
        if self._pipline is None:
            self._pipline = []
        self._pipline.append(pipline)
        return self


# noinspection PyProtectedMember
class Query(BaseQuery):
    """
    查询
    """

    def __init__(self, max_per_page: int = None):
        """

        Args:

        Returns:

        """
        # per page max count
        self.max_per_page: int = max_per_page
        #: the current page number (1 indexed)
        self._page = None
        #: the number of items to be displayed on a page.
        self._per_page = None
        # aggregation
        self._is_aggregation = None

        super().__init__()

    @classmethod
    def from_query(cls, **kwargs) -> 'Query':
        """
        生成query实例
        Args:
            cls
        Returns:

        """
        cls_instance = cls(max_per_page=kwargs.get("max_per_page"))
        cls_instance._cname = kwargs.get("cname")
        cls_instance._query_key = kwargs.get("query_key")
        # filter key
        cls_instance._exclude_key = kwargs.get("exclude_key")
        cls_instance._order_by = kwargs.get("order_by")
        # update
        cls_instance._update_data = kwargs.get("update_data")
        cls_instance._upsert = kwargs.get("upsert", False)
        # insert
        cls_instance._insert_data = kwargs.get("insert_data")
        # limit, offset
        cls_instance._limit_clause = kwargs.get("limit_clause")
        cls_instance._offset_clause = kwargs.get("offset_clause")
        # aggregate query
        cls_instance._pipline = kwargs.get("pipline")
        cls_instance._page = kwargs.get("page")
        #: the number of items to be displayed on a page.
        cls_instance._per_page = kwargs.get("per_page")
        return cls_instance

    def _verify_collection(self, ):
        """

        Args:

        Returns:

        """
        if self._cname is None:
            raise FuncArgsError("Query 对象中缺少collection name(cname)")

    def insert_query(self, insert_data: Union[List[Dict], Dict]) -> 'Query':
        """
        insert query
        Args:
            insert_data: 值类型Dict or List[Dict]
        Returns:
            Select object
        """
        self._verify_collection()
        self._insert_data = insert_data
        return self

    def update_query(self, update_data: Dict) -> 'Query':
        """
        update query

        Args:
            update_data: 值类型Dict or List[Dict]
        Returns:
            返回更新的条数
        """
        self._verify_collection()
        self._update_data = update_data
        return self

    def delete_query(self, ) -> 'Query':
        """
        delete query
        Args:
        Returns:
            返回删除的条数
        """
        self._verify_collection()
        return self

    def select_query(self, is_agg: bool = False) -> 'Query':
        """
        select query
        Args:
            is_agg: 是否为聚合查询
        Returns:
            返回匹配的数据或者None
        """
        self._verify_collection()
        self._is_aggregation = is_agg
        return self

    def paginate_query(self, *, page: int = 1, per_page: int = 20) -> 'Query':
        """
        If ``page`` or ``per_page`` are ``None``, they will be retrieved from
        the request query. If ``max_per_page`` is specified, ``per_page`` will
        be limited to that value. If there is no request or they aren't in the
        query, they default to 1 and 20 respectively.

        目前是改造如果limit传递为0，则返回所有的数据，这样业务代码中就不用更改了

        Args:
            page: page is less than 1, or ``per_page`` is negative.
            per_page: page or per_page are not ints.

            ``page`` and ``per_page`` default to 1 and 20 respectively.

        Returns:

        """
        self._verify_collection()
        if self.max_per_page is not None:
            per_page = min(per_page, self.max_per_page)

        if page < 1:
            page = 1

        if per_page < 0:
            per_page = 20

        self._page, self._per_page = page, per_page

        # 如果per_page为0,则证明要获取所有的数据，否则还是通常的逻辑
        if per_page != 0:
            self._limit_clause = per_page
            self._offset_clause = (page - 1) * per_page

        return self

    def sql(self, ) -> Dict:
        """
        generate dict

        Args:
        Returns:

        """
        result_sql = None

        if self._insert_data is not None:
            result_sql = {"cname": self._cname, "insert_data": self._insert_data, "max_per_page": self.max_per_page}
        elif self._update_data is not None:
            result_sql = {"cname": self._cname, "query_key": self._query_key, "update_data": self._update_data,
                          "upsert": self._upsert, "max_per_page": self.max_per_page}
        elif self._is_aggregation:
            result_sql = {"cname": self._cname, "pipline": self._pipline, "page": self._page,
                          "per_page": self._per_page, "max_per_page": self.max_per_page,
                          "limit_clause": self._limit_clause, "offset_clause": self._offset_clause}
        else:
            result_sql = {"cname": self._cname, "query_key": self._query_key, "exclude_key": self._exclude_key,
                          "page": self._page, "per_page": self._per_page, "order_by": self._order_by,
                          "max_per_page": self.max_per_page, "limit_clause": self._limit_clause,
                          "offset_clause": self._offset_clause}

        return result_sql
