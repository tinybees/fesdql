#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/3/1 下午3:51
"""
import copy
from typing import Dict, List, MutableMapping, MutableSequence, Tuple, Union

from bson import ObjectId
from bson.errors import BSONError
from marshmallow import Schema

from .err import FuncArgsError
from .utils import under2camel

__all__ = ("AlchemyMixIn",)


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
