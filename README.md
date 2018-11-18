# 基于 Apache Spark 和 Elasticsearch 的可伸缩的推荐系统

本系统通过存储在 Elasticsearch 中的用户行为数据，使用 Spark 来训练一个协同过滤推荐模型，并将训练好的模型保存到 Elasticsearch，然后使用 Elasticsearch 通过该模型来提供实时推荐。以此达到离线训练，在线推荐的目的。

因为使用了 Elasticsearch 插件，系统除了能够提供个性化用户和类似条目推荐，还能够将推荐与搜索和内容过滤相结合。

本文主要描述了如下内容：

* 使用 Elasticsearch Spark 连接器将用户行为数据导入到 Elasticsearch 中并建立索引。
* 将行为数据加载到 Spark DataFrames 中，并使用 Spark 的机器学习库 (MLlib) 来训练一个协同过滤推荐系统模型。
* 将训练后的模型导出到 Elasticsearch 中。
* 使用一个自定义 Elasticsearch 插件，计算 _个性化用户_ 和 _类似条目_ 推荐，并将推荐与搜索和内容过滤相结合。

![架构图](doc/image/architecture.png)

## 操作流程
1. 将数据集加载到 Spark 中。
2. 使用 Spark DataFrame 操作清理该数据集，并将它加载到 Elasticsearch 中。
3. 使用 Spark MLlib，离线训练一个协同过滤推荐模型。
4. 将得到的模型保存到 Elasticsearch 中。
5. 使用 Elasticsearch 查询和一个自定义矢量评分插件，在线生成用户推荐。

## 包含的组件
* [Apache Spark](http://spark.apache.org/)：一个开源、快速、通用的集群计算系统。
* [Elasticsearch](http://elasticsearch.org)：开源搜索和分析引擎。
* [Elasticsearch Vector Scoring](https://github.com/MLnick/elasticsearch-vector-scoring)：A plugin allows you to score documents based on arbitrary raw vectors, using dot product or cosine similarity.

# 步骤
按照以下步骤部署所需的部件，并创建推荐服务。

1. [设置 Elasticsearch](#2-set-up-elasticsearch)
2. [下载 Elasticsearch Spark 连接器](#3-download-the-elasticsearch-spark-connector)
3. [下载 Apache Spark](#4-download-apache-spark)
4. [数据准备](#5-prepare-the-data)
5. [启动 Notebook](#6-launch-the-notebook)
6. [运行 Notebook](#7-run-the-notebook)

### 1.设置 Elasticsearch

此推荐系统目前依赖于 Elasticsearch 5.3.0。转到[下载页面](https://www.elastic.co/downloads/past-releases/elasticsearch-5-3-0)，下载适合您的系统的包。

如果在 Linux / Mac 上，可以下载 [TAR 归档文件](https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.3.0.tar.gz) 并使用以下命令进行解压：

```
$ wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.3.0.tar.gz
$ tar xfz elasticsearch-5.3.0.tar.gz
```

使用以下命令，将目录更改为新解压的文件夹：

```
$ cd elasticsearch-5.3.0
```

接下来安装 [Elasticsearch 矢量评分插件](https://github.com/MLnick/elasticsearch-vector-scoring)。运行以下命令（Elasticsearch 将为您下载插件文件）：

```
$ ./bin/elasticsearch-plugin install https://github.com/MLnick/elasticsearch-vector-scoring/releases/download/v5.3.0/elasticsearch-vector-scoring-5.3.0.zip
```

接下来，启动 Elasticsearch（在一个单独的终端窗口中这么做，使其保持正常运行）：

```
$ ./bin/elasticsearch
```

您会看到一些显示的启动日志。如果显示 `elasticsearch-vector-scoring-plugin` 则插件已成功加载：

```
$ ./bin/elasticsearch
[2017-09-08T15:58:18,781][INFO ][o.e.n.Node               ] [] initializing ...
...
[2017-09-08T15:58:19,406][INFO ][o.e.p.PluginsService     ] [2Zs8kW3] loaded plugin [elasticsearch-vector-scoring]
[2017-09-08T15:58:20,676][INFO ][o.e.n.Node               ] initialized
...
```

最后安装 Elasticsearch Python 客户端。运行以下命令（执行此命令时使用的终端窗口应该与运行 Elasticsearch 的终端窗口不同）：

```
$ pip install elasticsearch
```

如果系统安装了Anaconda，也可以使用conda install命令来安装。

### 2.下载 Elasticsearch Spark 连接器

[Elasticsearch Hadoop 项目](https://www.elastic.co/products/hadoop) 提供了 Elasticsearch 与各种兼容 Hadoop 的系统（包括 Spark）之间的连接器。该项目提供了一个 ZIP 文件供下载，其中包含所有这些连接器。此推荐系统需要将特定于 Spark 的连接器 JAR 文件放在类路径上。按照以下步骤设置连接器：

1.[下载](http://download.elastic.co/hadoop/elasticsearch-hadoop-5.3.0.zip) `elasticsearch-hadoop-5.3.0.zip` 文件，其中包含所有连接器。为此，可以运行：
```
$ wget http://download.elastic.co/hadoop/elasticsearch-hadoop-5.3.0.zip
```
2.运行以下命令来解压该文件：
```
$ unzip elasticsearch-hadoop-5.3.0.zip
```
3.Spark 连接器的 JAR 名为 `elasticsearch-spark-20_2.11-5.3.0.jar`，它将位于您解压上述文件所用的目录的 `dist` 子文件夹中。

### 3.下载 Apache Spark

本推荐系统适用于任何 Spark 2.x 版本，从[下载页面](http://spark.apache.org/downloads.html) 下载最新版 Spark（目前为 2.2.0）。运行以下命令来解压它：
```
$ tar xfz spark-2.2.0-bin-hadoop2.7.tgz
```

> *请注意，如果下载不同的版本，应该相应地调整上面使用的相关命令和其他地方。*

![下载 Apache Spark](doc/image/download-apache-spark.png)

Spark 的机器学习库 [MLlib](http://spark.apache.org/mllib)依赖于 [Numpy](http://www.numpy.org)，运行以下命令安装 Numpy：
```
$ pip install numpy
```

### 4.数据准备

推荐系统依赖于一个用户行为数据集，用户行为包括浏览、购买资源。如果只记录了购买行为，那么暂时就只以此行为构建数据集。

数据集中的每条数据是一个多元组，其中包括：
* userId，全局唯一的用户Id，这里的用户是指真实购买资源的广告主，而不是在系统中进行操作的代理商。
* itemId，全局唯一的资源Id，这里的资源是指可以展示广告的广告位。
* rating，用户评分，系统目前没有真实的用户评分行为，我们将用户的浏览、购买行为等价为评分。一次浏览为3分，一次购买为5分。
* timeStamp，时间戳，用户产生这次行为的时间，时间精确到秒。

数据文件的格式可以参考 [Movielens 数据集](https://grouplens.org/datasets/movielens/)，其中包含一组电影用户提供的评分和电影元数据。我们的数据集可以完全照此准备。

### 5.启动 Notebook

> 该 Notebook 应该适用于 Python 2.7 或 3.x（而且已在 2.7.11 和 3.6.1 上测试）

要运行该 Notebook，您需要在一个 Jupyter Notebook 中启动一个 PySpark 会话。如果没有安装 Jupyter，可以运行以下命令来安装它：
```
$ pip install jupyter
```

在启动 Notebook 时，记得在类路径上包含来自[第 3 步](#3-download-the-elasticsearch-spark-connector) 的 Elasticsearch Spark 连接器 JAR。

运行以下命令，以便在本地启动您的 PySpark Notebook 服务器。**要正确运行此命令，需要从您在[第 1 步](#1-clone-the-repo)克隆的 Code Pattern 存储库的基本目录启动该 Notebook**。如果您不在该目录中，请先通过 `cd` 命令进入该目录。

```
PYSPARK_DRIVER_PYTHON="jupyter" PYSPARK_DRIVER_PYTHON_OPTS="notebook" ../spark-2.2.0-bin-hadoop2.7/bin/pyspark --driver-memory 4g --driver-class-path ../../elasticsearch-hadoop-5.3.0/dist/elasticsearch-spark-20_2.11-5.3.0.jar
```

这会打开一个浏览器窗口，其中显示了 Code Pattern 文件夹内容。单击 `notebooks` 子文件夹，然后单击 `elasticsearch-spark-recommender.ipynb` 文件启动该 Notebook。

![启动 Notebook](doc/image/launch-notebook.png)

> _可选：_
>
> 要在推荐演示中显示图像，您需要访问 [The Movie Database API](https://www.themoviedb.org/documentation/api)。请按照[操作说明](https://developers.themoviedb.org/3/getting-started) 获取 API 密钥。您还需要使用以下命令安装 Python 客户端：
```
$ pip install tmdbsimple
```
>
> 不访问此 API 也能执行该演示，但不会显示图像（所以看起来不太美观！）。

### 6.运行该 Notebook

执行一个 Notebook 时，实际情况是，
按从上往下的顺序执行该 Notebook 中的每个代码单元。

可以选择每个代码单元，并在代码单元前面的左侧空白处添加一个标记。标记
格式为 `In [x]:`。根据 Notebook 的状态，`x` 可以是：

* 空白，表示该单元从未执行过。
* 一个数字，表示执行此代码步骤的相对顺序。
* 一个`*`，表示目前正在执行该单元。

可通过多种方式执行 Notebook 中的代码单元：

* 一次一个单元。
  * 选择该单元，然后在工具栏中按下 `Play` 按钮。也可以按下 `Shift+Enter` 来执行该单元并前进到下一个单元。
* 批处理模式，按顺序执行。
  * `Cell` 菜单栏中包含多个选项。例如，可以
    选择 `Run All` 运行 Notebook 中的所有单元，也可以选择 `Run All Below`，
    这将从当前选定单元下方的第一个单元开始执行，然后
    继续执行后面的所有单元。

![](doc/source/images/notebook-run-cells.png)

# 样本输出

`data/examples` 文件夹中的示例输出显示了运行 Notebook 后的完整输出。可以在[这里]()查看它。

> *备注：* 要查看代码和没有输出的精简单元，可以在 [Github 查看器中查看原始 Notebook](notebooks/elasticsearch-spark-recommender.ipynb)。

# 故障排除

* 错误：`java.lang.ClassNotFoundException: Failed to find data source: es.`

如果尝试在 Notebook 中将数据从 Spark 写入 Elasticsearch 时看到此错误，这意味着 Spark 在启动该 Notebook 时没有在类路径中找到 Elasticsearch Spark 连接器 (`elasticsearch-spark-20_2.11-5.3.0.jar`)。

  > 解决方案：首先启动[第 6 步](#6-launch-the-notebook) 中的命令，**确保从 Code Pattern 存储库的基本目录运行它**。

  > 如果这不起作用，可以尝试在启动 Notebook 时使用完全限定的 JAR 文件路径，比如：
  > `PYSPARK_DRIVER_PYTHON="jupyter" PYSPARK_DRIVER_PYTHON_OPTS="notebook" ../spark-2.2.0-bin-hadoop2.7/bin/pyspark --driver-memory 4g --driver-class-path /FULL_PATH/elasticsearch-hadoop-5.3.0/dist/elasticsearch-spark-20_2.11-5.3.0.jar`
  > 其中 `FULL_PATH` 是 _您解压 `elasticsearch-hadoop` ZIP 文件_ 所用的目录的完全限定（不是相对）路径。

* 错误：`org.elasticsearch.hadoop.EsHadoopIllegalStateException: SaveMode is set to ErrorIfExists and index demo/ratings exists and contains data.Consider changing the SaveMode`

如果尝试在 Notebook 中将数据从 Spark 写入 Elasticsearch 时看到此错误，这意味着您已将数据写入到相关索引（例如将评分数据写入到 `ratings` 索引中）。

  > 解决方案：尝试从下一个单元继续处理该 Notebook。也可以先删除所有索引，重新运行 Elasticsearch 命令来创建索引映射（参阅 Notebook 中的小节 *第 2 步：将数据加载到 Elasticsearch 中*）。

* 错误：`ConnectionRefusedError: [Errno 61] Connection refused`

尝试在 Notebook 中连接到 Elasticsearch 时可能看到此错误。这可能意味着您的 Elasticsearch 实例未运行。

 > 解决方案：在一个新终端窗口中，通过 `cd` 命令转到安装 Elasticsearch 的目录，运行 `./bin/elasticsearch` 来启动 Elasticsearch。

* 错误：`Py4JJavaError: An error occurred while calling o130.save.
: org.elasticsearch.hadoop.rest.EsHadoopNoNodesLeftException: Connection error (check network and/or proxy settings)- all nodes failed; tried [[127.0.0.1:9200]]`

尝试在 Notebook 中将数据从 Elasticsearch 读入 Spark 中（或将数据从 Spark 写入 Elasticsearch 中）时，可能看到此错误。这可能意味着您的 Elasticsearch 实例未运行。

 > 解决方案：在一个新终端窗口中，通过 `cd` 命令转到安装 Elasticsearch 的目录，运行 `./bin/elasticsearch` 来启动 Elasticsearch。

* 错误：`ImportError: No module named elasticsearch`

如果您遇到此错误，这意味着 Elasticsearch Python 客户端未安装，或者可能无法在 `PYTHONPATH` 上找到。

 > 解决方案：先尝试使用 `$ pip install elasticsearch` 安装该客户端（如果在 Python 虚拟环境中运行，比如 Conda 或 Virtualenv），或者使用 `$ sudo pip install elasticsearch`。
 > 如果这样不起作用，可以将您的 site-packages 文件夹添加到 Python 路径（比如在 Mac 上：`export PYTHONPATH=/Library/Python/2.7/site-packages` 用于 Python 2.7）。有关 Linux 上的另一个示例，参阅这个 [Stackoverflow 问题](https://stackoverflow.com/questions/7731947/add-module-to-pythonpath-nothing-works)。
 > _备注：_ 同一个通用解决方案适用于您可能遇到的任何其他模块导入错误。

 * 错误：`HTTPError: 401 Client Error: Unauthorized for url: https://api.themoviedb.org/3/movie/1893?api_key=...`

如果在测试 TMDb API 访问或生成推荐时，在您的 Notebook 中看到此错误，这意味着您已经安装了 `tmdbsimple` Python 包，但没有设置您的 API 密钥。

> 解决方案：按照[第 6 步](#6-launch-the-notebook) 末尾的操作说明来设置您的 TMDb 帐户并获取您的 API 密钥。然后将该密钥复制到 _第 1 步：准备数据_ 结束后 Notebook 单元中的 `tmdb.API_KEY = 'YOUR_API_KEY'` 行中（即将 `YOR_API_KEY` 替换为正确的密钥）。完成这一步后，执行该单元来测试您对 TMDb API 的访问。

# 链接

* [优酷上的演示](http://v.youku.com/v_show/id_XMzYwNjg4MjkwNA==.html)：观看视频。
* [站立会议视频演示](https://youtu.be/sa_Y488vj0M)：观看涵盖本 code pattern 的一些背景和技术细节的站立会议演示。
* [站立会议资料](https://www.slideshare.net/sparktc/spark-ml-meedup-pentreath-puget)：查看演示的幻灯片。
* [ApacheCon Big Data Europe 2016](http://events.linuxfoundation.org/sites/events/files/slides/ApacheBigDataEU16-NPentreath.pdf)：查阅该站立会议演示的一个扩展版本：
* [数据和分析](https://www.ibm.com/cloud/garage/content/architecture/dataAnalyticsArchitecture)：了解如何将本模式融入到数据和分析参考架构中。

# 了解更多信息

* **数据分析 Code Pattern**：喜欢本 Code Pattern 吗？了解我们的其他[数据分析 Code Pattern](https://developer.ibm.com/cn/technologies/data-science/)
* **AI 和数据 Code Pattern 播放清单**：收藏包含我们所有 Code Pattern 视频的[播放清单](http://i.youku.com/i/UNTI2NTA2NTAw/videos?spm=a2hzp.8244740.0.0)
* **Data Science Experience**：通过 IBM [Data Science Experience](https://datascience.ibm.com/) 掌握数据科学艺术
* **IBM Cloud 上的 Spark**：需要一个 Spark 集群？通过我们的 [Spark 服务](https://console.bluemix.net/catalog/services/apache-spark)，在 IBM Cloud 上创建多达 30 个 Spark 执行程序。

# 许可
[Apache 2.0](LICENSE)
