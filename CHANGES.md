## fesdql Changelog

###[1.0.1b3] - 2020-9-29

#### Changed
- 更改所有mypy提示的类型标注
- 更改同步异步分页limit和page的默认值

###[1.0.1b2] - 2020-9-18

#### Changed
- 更改部分类型标注
- 更改同步mongo初始化的时机,改为调用即初始化


###[1.0.1b1] - 2020-3-17

#### Added
- 整体重构,拆分aclients库和eclients中的mongo功能到此库中,减少安装包的依赖
- 增加生成分表schema功能，使得分表的使用简单高效
- 增加LRI和LRU缓存功能,方便使用
- 增加默认依赖marshmallow的功能,依靠marshmallow来进行数据的校验
- 覆盖marshmallow中的fields模块,更改其中所有类型的默认错误提示
- 增加混入类抽取异步和同步中有相同的功能的模块
- 增加查询的session功能把查询和session分开
- 增加Pagination类对于分页查询更简单，也更容易上手(sqlalchemy的写法)
- 增加多库多session的同时切换使用功能，提供对访问多个库的支持功能
- 配置增加fessql_binds用于多库的配置,并且增加配置校验功能
- 增加Query类所有的查询操作均在Query类中完成，session类只负责具体的查询
- Query类中增加生成增删改查SQL字符串语句的功能,方便jrpc调用
- Query类中增加生成增删改查SQL对象的功能,方便普通调用

#### Changed
- 优化所有代码中没有类型标注的地方,都改为typing中的类型标注
- 修改生成schema的功能适配字段映射,schema类字段增减等功能,适用于同一个schema适配不同的库表
- 同步和异步模块中所有的session查询都改为按照Query类进行查询
- 更改Pagination中上一页和下一页的功能
