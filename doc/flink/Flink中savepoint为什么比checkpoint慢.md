

问题：作业每次手动停止做savepoint要5min，自动化checkpoint只需要秒级

1. savepoint是要比checkpoint多存一些内容吗？

2. savepoint为什么这么耗时？（在不保存savepoint的情况下，也是秒级停止）



答:

1. flink 1.13以前savepoint数据格式应该和checkpoint格式一样.
   你的作业是否开启了增量checkpoint, 如果作业开启增量checkpoint的话， checkpoint会快(只上传增量部分),
   savepoint会慢（需要上传全量数据）。

2. flink 1.13
   中统一了所有stateBackend的savepoint格式，因为savepoint时需要逐个遍历出state中的key-value数据，所以速度相比checkpoint也会慢很多。

3. flink 1.15中引入了native模式的savepoint[1],
   放开了savepoint格式限制，其速度应该类似于一次全量checkpoint。

[1]https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/savepoints/

[2]https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/ops/state/checkpoints_vs_savepoints/

