# BigDataExp

本项目为南京大学计算机系2019学年大数据综合实验小组代码共享仓库。

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

## 分支设置

1. 新建自己的分支

```shell
git checkout -c yourbranchname
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

## IDEA配置

1. 主要参考网址：[Hadoop: Intellij结合Maven本地运行和调试MapReduce程序 (无需搭载Hadoop和HDFS环境)](https://www.polarxiong.com/archives/Hadoop-Intellij%E7%BB%93%E5%90%88Maven%E6%9C%AC%E5%9C%B0%E8%BF%90%E8%A1%8C%E5%92%8C%E8%B0%83%E8%AF%95MapReduce%E7%A8%8B%E5%BA%8F-%E6%97%A0%E9%9C%80%E6%90%AD%E8%BD%BDHadoop%E5%92%8CHDFS%E7%8E%AF%E5%A2%83.html)

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

其中有几个数字需要说明：版本1.2.1是最新版，2.7.7是hadoop版本，1.8是jdk版本

4. 其余按照网址内配置。若有其它需要，直接修改pom.xml即可

## 更新日志

> 2020.4.4
> > 创建项目，添加README和实验工程存放的文件夹
>
> 2020.4.6
> > 完成Lab2的基础功能和选做1
> > 新增IDEA配置MapReduce开发环境的说明
