#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/3/14 下午6:23

这里主要是对marshmallow的Field做简单更改,比如提示消息等等
"""

from marshmallow import fields

__all__ = (
    "fields",
)

# Field
fields.Field.default_error_messages = {
    "required": "必需的字段缺少数据.",
    "null": "字段不能为空(null).",
    "validator_failed": "无效的值.",
}

# Nested
fields.Nested.default_error_messages = {"type": "无效的类型."}

# List
fields.List.default_error_messages = {"invalid": "不是一个有效的列表(数组)."}

# Tuple
fields.Tuple.default_error_messages = {"invalid": "不是一个有效的元祖(数组)."}

# String
fields.String.default_error_messages = {
    "invalid": "不是一个有效的字符串.",
    "invalid_utf8": "不是一个有效的utf-8字符串."
}

# UUID
fields.UUID.default_error_messages = {"invalid_uuid": "不是一个有效的UUID类型."}

# Number
fields.Number.default_error_messages = {
    "invalid": "不是一个有效的Number类型.",
    "too_large": "Number类型值太大,越界.",
}

# Integer
fields.Integer.default_error_messages = {"invalid": "不是一个有效的整型(integer)."}

# Float
fields.Float.default_error_messages = {
    "special": "不允许特殊数值(nan或无穷大)."
}

# Decimal
fields.Decimal.default_error_messages = {
    "special": "不允许特殊数值(nan或无穷大)."
}

# Boolean
fields.Boolean.default_error_messages = {"invalid": "不是一个有效的boolean类型."}

# DateTime
fields.DateTime.default_error_messages = {
    "invalid": "不是一个有效的{obj_type}类型.",
    "invalid_awareness": "不是一个有效的{awareness} {obj_type}类型.",
    "format": '"{input}" 不能格式化为{obj_type}类型.',
}

# Time
fields.Time.default_error_messages = {
    "invalid": "不是一个有效的时间(time)类型.",
    "format": '"{input}" 不能格式化为时间(time)类型.',
}

# Date
fields.Date.default_error_messages = {
    "invalid": "不是一个有效的日期(date)类型.",
    "format": '"{input}" 不能格式化为日期(date)类型.',
}

# TimeDelta
fields.TimeDelta.default_error_messages = {
    "invalid": "不是一个有效的时间段(TimeDelta)类型.",
    "format": "{input!r} 不能格式化为时间段(TimeDelta)类型.",
}

# Mapping
fields.Mapping.default_error_messages = {"invalid": "不是一个有效的映射(mapping)类型."}

# Url
fields.Url.default_error_messages = {"invalid": "不是一个有效的URL类型."}

# Email
fields.Email.default_error_messages = {"invalid": "不是一个有效的email地址."}
