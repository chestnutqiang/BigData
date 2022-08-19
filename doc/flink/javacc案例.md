Calcite
-------

### Calcite 简介

是一个动态数据的管理框架，可以用来构建数据库系统的语法解析模块

*   不包含数据存储、数据处理等功能

*   可以通过编写 Adaptor 来扩展功能，以支持不同的数据处理平台

*   Flink SQL 使用并对其扩展以支持 SQL 语句的解析和验证

Calcite 提供了 SQL parser、SQL validation、Query optimizer、SQL generator 和 Data federator

### 查询的执行过程

