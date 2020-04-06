# Lab2

## 目录说明
* src: 源代码
* bin: 编译结果
* input: 输入目录，挑选了三篇小说
* output: 基本功能输出目录
* sorted: 选做1输出目录

## 运行方法
1. 环境变量
```shell
export CLASSPATH=`hadoop classpath`:$CLASSPATH
```

2. 编译
```shell
cd bin
javac ../src/*.java -d .
```

3. 打包
```shell
jar cf invertedindex.jar Inverted*.class
jar cf sort.jar Sort*.class
```

4. 运行
```shell
hdfs dfs -mkdir -p /user/$USER/Lab2
hdfs dfs -put ../input Lab2/input
hadoop jar invertedindex.jar InvertedIndex Lab2/input Lab2/output
hadoop jar sort.jar Sort Lab2/output Lab2/sorted
```

5. 查看
```shell
cd ..
hdfs dfs -get Lab2/output output
hdfs dfs -get Lab2/sorted sorted
tail output/*
tail sorted/*
```

## 注意事项
* 编译需要`hadoop`的`jar`包，通过`CLASSPATH`导入
* 运行`hadoop jar`之前必须删除输出目录