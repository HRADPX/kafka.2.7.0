Apache Kafka
=================
See our [web site](https://kafka.apache.org) for details on the project.

You need to have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

We build and test Apache Kafka with Java 8, 11 and 15. We set the `release` parameter in javac and scalac
to `8` to ensure the generated binaries are compatible with Java 8 or higher (independently of the Java version
used for compilation).

Scala 2.13 is used by default, see below for how to use a different Scala version or all of the supported Scala versions.

### Build a jar and run it ###
    ./gradlew jar

Follow instructions in https://kafka.apache.org/quickstart

### Build source jar ###
    ./gradlew srcJar

### Build aggregated javadoc ###
    ./gradlew aggregatedJavadoc

### Build javadoc and scaladoc ###
    ./gradlew javadoc
    ./gradlew javadocJar # builds a javadoc jar for each module
    ./gradlew scaladoc
    ./gradlew scaladocJar # builds a scaladoc jar for each module
    ./gradlew docsJar # builds both (if applicable) javadoc and scaladoc jars for each module

### Run unit/integration tests ###
    ./gradlew test # runs both unit and integration tests
    ./gradlew unitTest
    ./gradlew integrationTest
    
### Force re-running tests without code change ###
    ./gradlew cleanTest test
    ./gradlew cleanTest unitTest
    ./gradlew cleanTest integrationTest

### Running a particular unit/integration test ###
    ./gradlew clients:test --tests RequestResponseTest

### Running a particular test method within a unit/integration test ###
    ./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
    ./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testMetadataUpdateWaitTime

### Running a particular unit/integration test with log4j output ###
Change the log4j setting in either `clients/src/test/resources/log4j.properties` or `core/src/test/resources/log4j.properties`

    ./gradlew clients:test --tests RequestResponseTest

### Specifying test retries ###
By default, each failed test is retried once up to a maximum of five retries per test run. Tests are retried at the end of the test task. Adjust these parameters in the following way:

    ./gradlew test -PmaxTestRetries=1 -PmaxTestRetryFailures=5
    
See [Test Retry Gradle Plugin](https://github.com/gradle/test-retry-gradle-plugin) for more details.

### Generating test coverage reports ###
Generate coverage reports for the whole project:

    ./gradlew reportCoverage -PenableTestCoverage=true

Generate coverage for a single module, i.e.: 

    ./gradlew clients:reportCoverage -PenableTestCoverage=true
    
### Building a binary release gzipped tar ball ###
    ./gradlew clean releaseTarGz

The above command will fail if you haven't set up the signing key. To bypass signing the artifact, you can run:

    ./gradlew clean releaseTarGz -x signArchives

The release file can be found inside `./core/build/distributions/`.

### Building auto generated messages ###
Sometimes it is only necessary to rebuild the RPC auto-generated message data when switching between branches, as they could
fail due to code changes. You can just run:
 
    ./gradlew processMessages processTestMessages

### Cleaning the build ###
    ./gradlew clean

### Running a task with one of the Scala versions available (2.12.x or 2.13.x) ###
*Note that if building the jars with a version other than 2.13.x, you need to set the `SCALA_VERSION` variable or change it in `bin/kafka-run-class.sh` to run the quick start.*

You can pass either the major version (eg 2.12) or the full version (eg 2.12.7):

    ./gradlew -PscalaVersion=2.12 jar
    ./gradlew -PscalaVersion=2.12 test
    ./gradlew -PscalaVersion=2.12 releaseTarGz

### Running a task with all the scala versions enabled by default ###

Invoke the `gradlewAll` script followed by the task(s):

    ./gradlewAll test
    ./gradlewAll jar
    ./gradlewAll releaseTarGz

### Running a task for a specific project ###
This is for `core`, `examples` and `clients`

    ./gradlew core:jar
    ./gradlew core:test

Streams has multiple sub-projects, but you can run all the tests:

    ./gradlew :streams:testAll

### Listing all gradle tasks ###
    ./gradlew tasks

### Building IDE project ####
*Note that this is not strictly necessary (IntelliJ IDEA has good built-in support for Gradle projects, for example).*

    ./gradlew eclipse
    ./gradlew idea

The `eclipse` task has been configured to use `${project_dir}/build_eclipse` as Eclipse's build directory. Eclipse's default
build directory (`${project_dir}/bin`) clashes with Kafka's scripts directory and we don't use Gradle's build directory
to avoid known issues with this configuration.

### Publishing the jar for all version of Scala and for all projects to maven ###
    ./gradlewAll uploadArchives

Please note for this to work you should create/update `${GRADLE_USER_HOME}/gradle.properties` (typically, `~/.gradle/gradle.properties`) and assign the following variables

    mavenUrl=
    mavenUsername=
    mavenPassword=
    signing.keyId=
    signing.password=
    signing.secretKeyRingFile=

### Publishing the streams quickstart archetype artifact to maven ###
For the Streams archetype project, one cannot use gradle to upload to maven; instead the `mvn deploy` command needs to be called at the quickstart folder:

    cd streams/quickstart
    mvn deploy

Please note for this to work you should create/update user maven settings (typically, `${USER_HOME}/.m2/settings.xml`) to assign the following variables

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                           https://maven.apache.org/xsd/settings-1.0.0.xsd">
    ...                           
    <servers>
       ...
       <server>
          <id>apache.snapshots.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
       </server>
       <server>
          <id>apache.releases.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
        </server>
        ...
     </servers>
     ...


### Installing the jars to the local Maven repository ###
    ./gradlewAll install

### Building the test jar ###
    ./gradlew testJar

### Determining how transitive dependencies are added ###
    ./gradlew core:dependencies --configuration runtime

### Determining if any dependencies could be updated ###
    ./gradlew dependencyUpdates

### Running code quality checks ###
There are two code quality analysis tools that we regularly run, spotbugs and checkstyle.

#### Checkstyle ####
Checkstyle enforces a consistent coding style in Kafka.
You can run checkstyle using:

    ./gradlew checkstyleMain checkstyleTest

The checkstyle warnings will be found in `reports/checkstyle/reports/main.html` and `reports/checkstyle/reports/test.html` files in the
subproject build directories. They are also printed to the console. The build will fail if Checkstyle fails.

#### Spotbugs ####
Spotbugs uses static analysis to look for bugs in the code.
You can run spotbugs using:

    ./gradlew spotbugsMain spotbugsTest -x test

The spotbugs warnings will be found in `reports/spotbugs/main.html` and `reports/spotbugs/test.html` files in the subproject build
directories.  Use -PxmlSpotBugsReport=true to generate an XML report instead of an HTML one.

### Common build options ###

The following options should be set with a `-P` switch, for example `./gradlew -PmaxParallelForks=1 test`.

* `commitId`: sets the build commit ID as .git/HEAD might not be correct if there are local commits added for build purposes.
* `mavenUrl`: sets the URL of the maven deployment repository (`file://path/to/repo` can be used to point to a local repository).
* `maxParallelForks`: limits the maximum number of processes for each task.
* `ignoreFailures`: ignore test failures from junit
* `showStandardStreams`: shows standard out and standard error of the test JVM(s) on the console.
* `skipSigning`: skips signing of artifacts.
* `testLoggingEvents`: unit test events to be logged, separated by comma. For example `./gradlew -PtestLoggingEvents=started,passed,skipped,failed test`.
* `xmlSpotBugsReport`: enable XML reports for spotBugs. This also disables HTML reports as only one can be enabled at a time.
* `maxTestRetries`: the maximum number of retries for a failing test case.
* `maxTestRetryFailures`: maximum number of test failures before retrying is disabled for subsequent tests.
* `enableTestCoverage`: enables test coverage plugins and tasks, including bytecode enhancement of classes required to track said
coverage. Note that this introduces some overhead when running tests and hence why it's disabled by default (the overhead
varies, but 15-20% is a reasonable estimate).

### Dependency Analysis ###

The gradle [dependency debugging documentation](https://docs.gradle.org/current/userguide/viewing_debugging_dependencies.html) mentions using the `dependencies` or `dependencyInsight` tasks to debug dependencies for the root project or individual subprojects.

Alternatively, use the `allDeps` or `allDepInsight` tasks for recursively iterating through all subprojects:

    ./gradlew allDeps

    ./gradlew allDepInsight --configuration runtime --dependency com.fasterxml.jackson.core:jackson-databind

These take the same arguments as the builtin variants.

### Running system tests ###

See [tests/README.md](tests/README.md).

### Running in Vagrant ###

See [vagrant/README.md](vagrant/README.md).

### Contribution ###

Apache Kafka is interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * https://kafka.apache.org/contributing.html


Kafka 参数
  配置文件放在 Kafka 目录下的 config 目录中，主要是 server.properties 文件。

1.常规
  zookeeper.connect: zookeeper 集群地址，可以是多个，用逗号分开（一组 hostname/path 列表， hostname 是 zk 的机器名或 IP，port 是 zk 的端口，/path 是可选的 zk 的路径，如果不指定，默认是根路径）
  log.dirs: Kafka 把所有消息都保存在磁盘上，存放这些数据的目录通过 log.dirs 指定，可以使用多路径，使用逗号分隔。如果是多路径，Kafka 会根据 "最少使用" 原则，把同一个分区的日志片段保存在同一个路径下，会往拥有最少数据分区的路径新增分区。



2.生产者
 已发送但是没有接收响应的请求数量，默认是 5
 消息发送等待的最大时间
 消息失败重试的最大等待时间，默认是 100ms

 message.max.bytes:
 max.message.bytes:


3.broker
  auto.create.topic.enable: 是否允许自动创建主题。如果设为 true，那么生产者、消费者、请求元数据时发现主题不存在时，就会自动创建。
  delete.topic.enable: 删除主题配置，默认 true
  num.recovery.threads.per.data.dir: 每个数据目录用于日志恢复启动和关闭的线程数量。因为这些线程只是服务器启动（正常启动和崩溃重启）和关闭时会用到，所以完全可以设置大量的线程来达到并行的目的。注意，这个参数指的是每个日志目录的线程数。
  replica.lag.time.max.ms: 备份副本没有发送任何获取请求或者至少这次没有消耗到主副本日志结束偏移量



主题配置
  num.partitions: 每个新建主题的分区个数，只能增加不能减少。
  log.retention.hours: 日志保存时间，默认为 7 天。超过这个时间会清理数据。bytes 和 minutes 无论哪个先达到都会触发，与此类似还有 log.retention.minutes 和 log.retention.ms，如果都设置的话，优先使用具有最小值的那个。（提示：时间保留数据是通过检查磁盘上日志文件的最后修改时间来实现的，也就是最后修改时间是日志片段的关闭时间，也就是文件里最后一个消息的时间戳）
  log.retention.bytes: topic 每个分区的最大文件大小，一个 topic 的大小限制 = 分区数 * log.retention.bytes。-1 表示没有大小限制。log.retention.minutes 和 log.retention.bytes 任意一个达到要求，都会执行删除。如果是 log.retention.bytes 先达到了，则是删除多出来的部分数据，一般不推荐使用最大文件删除策略，而是推荐文件过期删除策略。
  log.segment.bytes: 分区的日志是存储在某个目录下的多个文件中的，这些文件将分区的日志切分成一段一段的，称为日志片段，这个属性就是日志片段的大小，默认是 1G。当日志片段到达这个大小后，就会关闭当前文件，并创建一个新的日志分段，被关闭的文件就开始等待过期。
  如果一个主题每天只接受 100MB 的数据，那么根据默认设置，需要 10 天才能填满一个文件，而且因为日志片段在关闭之前，消息是不会过期的，所以如果 log.retention.hours 保持默认值的话，那么这个日志片段需要 17 天才过期。
  message.max.bytes: 表示一个服务器能够接收处理的消息的最大字节数，注意这个值生产者和消费者必须设置一致，且不要太于 fetch.message.max.bytes 属性的值（消费者能读取的最大消息，这个值应该大于等于 message.max.bytes）。该值的默认值是 1000000 字节，大概 900KB～1MB。如果启动压缩，判断压缩后的值。这个值的大小对性能影响很大，值越大，网络的IO的时间越长，还会增加磁盘写入的大小。
  Kafka 设计的初衷是迅速处理短小的消息，一般 10K 大小的消息吞吐性能最好。



4.消费者
  session.timeout.ms: 消费者向服务端发送心跳的最大间隔，默认是 10s，如果超过这个时间间隔，协调者会将消费者移除消费组，并触发一次 rebalance。
  heartbeat.interval.ms: 消费者心跳间隔，默认3s。
  rebalance.timeout.ms: rebalance 开始后，每个消费者加入消费组的最大允许时间。默认是60s，
  max.poll.interval.ms:当使用消费组管理时两次调用 poll 方法的最大间隔，这个参数设置了消费者消费消息的最大空闲时间。如果超过这个时间，消费者将被视为消息失败，并触发 rebalance。
  fetch.min.bytes: 消费者一次拉取请求拉取的最小字节数，如果数据不足，请求会等待直到有足够多的数据量。默认1字节，表示只要有1个字节数据可用或请求在等待数据达到超时，就会立即返回。设置这个值大于1可以稍微提高服务器的吞吐，但是会带来一定的延迟。
  fetch.max.bytes: 消费者一次拉取请求拉取的最大字节数，消费者也是按照批次拉取消息的，如果某个分区的一个记录批次超过这个值，也会返回以确保消费者能够正常消费。所以这个值不是一个绝对的。


疑问：
（1）Kafka 主副本在备份副本拉取到数据后就更新主副本里维护的远程副本的 LEO，如果这个时候主副本返回备份副本的数据失败了，这个时候备份副本的 LEO 和主副本维护的 LEO 就不一致了？











