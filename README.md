# wiki-flink
本文参考官方文档，使用flink 提供的API，在flink集群上运行流式分析。

维基百科提供了一个IRC频道，对所有的wiki的编辑都会被记录下来。并计算每个用户在给定时间窗口内编辑的字节数。使用flink实现这个功能只需要几分钟，而且非常的简单，同时也为后面学习更复杂的程序打下基础。

开发环境：

- macOS High Sierra
- IntelliJ IDEA
- JDK 1.8

## 构建maven项目

引入相关依赖

```xml
 <properties>
        <flink.version>1.4.0</flink.version>
        <scala.version>2.11</scala.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-wikiedits_2.11</artifactId>
            <version>1.4.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka-0.8_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
    </dependencies>
```

## Flink程序

Flink程序的第一步是创建一个`StreamExecutionEnvironment` （如果是批处理则创建`ExecutionEnvironment`）。这可以用来设置执行参数，并创建来自外部系统的读取源。

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
```

然后创建一个从维基百科 IPC 日志的读取源。

```java
DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
```

我想要获取每个用户在一段时间内窗口中添加或删除的字节数(比如：5秒)。为此，必须得先keying stream by userName，可以通过`KeySelector`达到目的。

```java
KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
    .keyBy(new KeySelector<WikipediaEditEvent, String>() {
        @Override
        public String getKey(WikipediaEditEvent event) {
            return event.getUser();
        }
    });
```

在无限元素流上，计算集合时，正如前文提到的，需要Windows。这里我会计算每5秒钟汇总编辑字节的总和。

```java
DataStream<Tuple2<String, Long>> result = keyedEdits
    .timeWindow(Time.seconds(5))
    .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
        @Override
        public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
            acc.f0 = event.getUser();
            acc.f1 += event.getByteDiff();
            return acc;
        }
    });
```

然后我们将数据写入kafka

```java
result
    .map(new MapFunction<Tuple2<String,Long>, String>() {
        @Override
        public String map(Tuple2<String, Long> tuple) {
            return tuple.toString();
        }
    })
    .addSink(new FlinkKafkaProducer08<>("localhost:9092", "wiki-result", new SimpleStringSchema()));
```



## 运行程序

如何运行这个程序呢？当然你可以本地直接启kafka，然后运行main方法。但是这里，我想把程序丢到flink集群中去处理。

### 本地安装flink

可以参考：[flink安装入门，快速开始](http://blog.csdn.net/lisi1129/article/details/54846789?locationNum=2&fps=1)

官网下载flink以后：

```shell
$ cd my/flink/directory
$ bin/start-local.sh
```

即可启动flink

### 本地安装kafka

可以参考：[kafka安装与启动](http://orchome.com/6)

```shell
#在kafka的根目录下
#启动zk
bin/zookeeper-server-start.sh config/zookeeper.properties
#启动kafka
bin/kafka-server-start.sh config/server.properties &
#创建topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic wiki-result
```

### 在flink集群上运行程序

注意哈，要把maven程序打包成可运行的jar，而不是一个简单的jar

需要添加这么一段代码在pom文件之中

```xml
 <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>2.5.5</version>
                <configuration>
                    <archive>
                        <manifest>
                            <addClasspath>true</addClasspath>
                            <classpathPrefix>lib/</classpathPrefix>
                            <mainClass>com.keozhao.WikipediaAnalysis</mainClass>
                        </manifest>
                    </archive>
                    <descriptorRefs>
                        <descriptorRef>
                            jar-with-dependencies
                        </descriptorRef>
                    </descriptorRefs>
                </configuration>
            </plugin>
```

然后打包的时候不执行mvn package 而执行maven-assembly

可以参考 [maven打包成可执行的jar](http://blog.csdn.net/h70614959/article/details/41697189)

然后我们将程序放到flink里执行

```shell
#进入你flink的文件夹
cd my/flink/directory
#运行jar
bin/flink run -c wikiedits.WikipediaAnalysis path/to/wikiedits-0.1.jar
```

可以看到运行结果：

![](https://ws4.sinaimg.cn/large/006tNc79gy1fnct75k2ehj31kw0f6h4x.jpg)

可以访问http://localhost:8081，看到集群的资源状况，与job情况

可以在kafka的目录下执行指令：

```shell
#查看（消费）flink写入的数据
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic wiki-test --from-beginning
```

![](https://ws3.sinaimg.cn/large/006tNc79gy1fnctf0g7n1j317w182agj.jpg)

可以看到源源不断的数据，这是维基百科的编辑流状态。