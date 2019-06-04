# dtc-mr_v0.2使用介绍
## 简介
`dtc-mr`为小时任务，主要是从hdfs中读取日志数据，判断/拼接及保存至es中。<br />
## 前期准备及部署
1. 次环境依赖与jdk>1.8+；
1. `dtc-mr`是在dtc用户下的；
2. 按照`deployment/elasticsearch`中的`es.json`的描述操作操作；
3. 进入到hbase用户下，新建表：

    ```sbtshell
       create 'mr_restule','f'
    ```
4. `/lib/common`下找到`elasticsearch-hadoop-6.7.1.jar`与`commons-httpclient-3.1.jar`put到hdfs的`/user/dtc/event/`路径下；<br />
5. 然后将安装包`dtc-0.1.--bin.tar.gz`解压至`software`目录下,并软链到`dtc`下,并增加环境变量；
6. 最后增加定时任务：
    ```sbtshell
    1 * * * * /home/dtc/software/dtc/bin/dtc.sh h start
    ```
7. 实时处理正在开发中...
