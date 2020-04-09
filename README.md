# BigDataExp

本项目为南京大学计算机系2019学年大数据综合实验小组代码共享仓库。**每次实验前务必先拉取最新版本的仓库。**

## 目录
- 配置SSH
- 配置Git分支
- 用IDEA开发Hadoop
  - 配置IDEA
  - 打包jar包
- 用Shell连接远程集群执行任务
- 更新日志


## 配置SSH

*本配置为RedHat或CentOS下配置SSH方法*

1. 安装git

```shell
sudo yum install git
```

2. 配置git

```shell
git config --global user.name "YourName"
git config --global user.email "YourEmail"
git config --global push.default matching #如果你执行 git push 但没有指定分支，它将 push 所有你本地的分支到远程仓库中对应匹配的分支
git config --list # 查看配置结果
```

3. 生成SSH公钥并检查

```shell
ssh-keygen -t rsa -C "YourEmail"
cd ~/.ssh
```

4. 将id_rsa.pub中所有内容复制到Github设置

```shell
cat id_rsa.pub
```

5. 验证修改

```shell
ssh -T git@github.com
```

出现

```shell
Hi ....! You've successfully authenticated, but GitHub does not provide shell access.
```

提示说明成功

## 配置Git分支

1. 新建自己的分支

```shell
git checkout -b yourbranchname
```

2. 修改后本地提交，有重大更新时可写更新日志

```shell
git add *
git commit -m "Update something"
```

3.  提交前先拉取最新版本

```shell
git pull origin master:master
git merge master
```

4. 将修改提交到远端

```shell
git push origin yourbranchname:master
```

## 用IDEA开发Hadoop
### 配置IDEA

1. 主要参考网页：[Hadoop: Intellij结合Maven本地运行和调试MapReduce程序 (无需搭载Hadoop和HDFS环境)](https://www.polarxiong.com/archives/Hadoop-Intellij%E7%BB%93%E5%90%88Maven%E6%9C%AC%E5%9C%B0%E8%BF%90%E8%A1%8C%E5%92%8C%E8%B0%83%E8%AF%95MapReduce%E7%A8%8B%E5%BA%8F-%E6%97%A0%E9%9C%80%E6%90%AD%E8%BD%BDHadoop%E5%92%8CHDFS%E7%8E%AF%E5%A2%83.html)

2. 需要额外配置源加速maven框架内依赖的下载速度，打开~/.m2文件夹，配置settings.xml文件：

```shell
cd ~/.m2
vim settings.xml
```

配置如下：

```xml
<?xml version="1.0"?>
<settings>
  <mirrors>
        <mirror>
            <id>alimaven</id>
            <name>aliyun maven</name>
            <url>http://maven.aliyun.com/nexus/content/groups/public/</url>
            <mirrorOf>central</mirrorOf>
        </mirror>
  </mirrors>
  <profiles>
    <profile>
       <id>nexus</id>
        <repositories>
            <repository>
                <id>nexus</id>
                <name>local private nexus</name>
                <url>http://maven.oschina.net/content/groups/public/</url>
                <releases>
                    <enabled>true</enabled>
                </releases>
                <snapshots>
                    <enabled>false</enabled>
                </snapshots>
            </repository>
        </repositories>

        <pluginRepositories>
            <pluginRepository>
            <id>nexus</id>
            <name>local private nexus</name>
            <url>http://maven.oschina.net/content/groups/public/</url>
            <releases>
                <enabled>true</enabled>
            </releases>
            <snapshots>
                <enabled>false</enabled>
            </snapshots>
            </pluginRepository>
        </pluginRepositories>
    </profile>
  </profiles>
</settings>

```

3. 另外网址中给出的pom.xml可以用如下内容替换：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>InvertedIndex</artifactId>
    <version>1.0-SNAPSHOT</version>
    <repositories>
        <repository>
            <id>apache</id>
            <url>http://maven.apache.org</url>
        </repository>
    </repositories>

    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-core</artifactId>
            <version>1.2.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.7.7</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.7.7</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.7</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

其中有几个数字需要说明：版本1.2.1是core的最新版，2.7.7是hadoop版本，1.8是jdk版本

4. 其余按照网址内配置。若有其它需要，直接修改pom.xml即可

### 打包jar包

按照网页[IDEA 开发hadoop项目配置及打包](https://blog.csdn.net/a377987399/article/details/80510776)下“二、IDEA开发hadoop项目打包”操作即可

**注意！不包含依赖的打包方法：**

- 第2步“……选择JAR,再选择第二个选项”应该更改为“……选择JAR,再选择第一个选项”，即不包含依赖打包
- 接着，需要将本工程的compile output作为Module加入jar包内
- 最后点击右下角Apply，OK完成设置
- 直接进入第5步“点击Build按钮，选择Build Artifacts...”。

## 用vscode开发Hadoop

### 插件依赖

微软的Java Extension Pack，自动安装六个其他常用Java插件。

### 参数配置

注意，不要使用带有`.classpath`或`.project`的目录，否则会使`settings.json`实效。在`.vscode/settings.json`中使用以下配置：
```json
{
    "java.project.referencedLibraries": [
        "lib/**/*.jar",
        "/opt/hadoop/**/*.jar"
    ]
}
```

### 运行调试

启用Debugger for Java插件之后，`main`函数上方出现Run和Debug按钮，点击即可。

### 导出jar包

vscode暂时不支持此功能，需要手动打包：

1. 声明环境变量，添加Hadoop的包
```shell
export CLASSPATH=`hadoop classpath`:$CLASSPATH
```

2. 编译，将`src`文件夹的`.java`文件全部编译，`-d .`指定编译结果放在当前目录，即`bin`目录
```shell
cd bin
javac ../src/*.java -d .
```

3. 打包，注意当前必须是`bin`目录，否则打包结构错误，无法在Hadoop平台运行，参数`c`用于命令创建档案，`f`用于指定档案名
```shell
jar cf invertedindex.jar Inverted*.class
```

## 用Shell连接远程集群执行任务

1. 进入本地待上传文件的目录
2. 将文件用scp命令上传到远程服务器，这里以lab2的TFIDF.jar文件为例：

```shell
scp TFIDF.jar 2020st42@114.212.190.95:/home/2020st42/lab2/TFIDF.jar
```

服务器内即使不存在lab2目录，该命令也可以直接建立该目录并把本地的TFIDF.jar文件直接写入服务器指定路径

3. ssh登录远端服务器：

```shell
ssh 2020st42@114.212.190.95
```

密码即为登陆密码，登录成功后可以用linux文件系统的命令检查刚刚上传的文件是否存在

4. 以lab2的TFIDF任务为例，进入lab2目录，直接向集群用命令提交任务即可：

```shell
cd lab2/
hadoop jar TFIDF.jar TFIDF /data/exercise_2 /user/2020st42/lab2/TFIDF_output
```

本次输入的数据文件在/data/exercise_2目录下，输出到lab2/TFIDF_output目录下

5. 检查输出文件夹和其中一个输出文件：

```shell
hdfs dfs -ls /user/2020st42/lab2/TFIDF_output
hdfs dfs -cat /user/2020st42/lab2/TFIDF_output/part-r-00000
```

其余输出文件同理可以查看

## 更新日志

> 2020.4.4
> > 创建项目，新增README和实验工程存放的文件夹
>
> 2020.4.6
> > 完成Lab2的基础功能和选做1
> >
> > 新增IDEA配置MapReduce开发环境及打包说明
>
> 2020.4.7
> > 完成Lab2的选做2
>
> 2020.4.8
> > 新增用Shell连接远程集群执行任务的方法
>
> 2020.4.9
> > 完善Lab2注释
> >
> > 新增vscode开发Hadoop方法