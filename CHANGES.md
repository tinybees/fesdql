## fesdql Changelog

###[1.0.1b1] - 2020-3-17

#### Added
- 整体重构,拆分aclients库和eclients中的mongo功能到此库中,减少安装包的依赖
- 增加根据已有schema生成新的schema的功能
- 增加LRI和LRU缓存功能,方便使用
- 增加默认依赖marshmallow的功能,依靠marshmallow来进行数据的校验
- 覆盖marshmallow中的fields模块,更改其中所有类型的默认错误提示
- 增加混入类抽取异步和同步中有相同的功能的模块
