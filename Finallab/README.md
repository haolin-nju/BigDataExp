# README

## 任务一：Preprocess

因为这个程序需要三个参数，所以额外注明在此。

### 本地执行方法

- Artifact选择：Application

- Program arguments: input/People_List_unique.txt input/wuxia_novels output/

- Main class: preprocess

### 集群执行方法

- 执行命令

```shell
hadoop jar Preprocess.jar Preprocess /MP_Data/task2/People_List_unique.txt /MP_Data/task2/wuxia_novels /user/username/finallab/preprocess_output
```

## 任务二：Cooccurrance

本地执行方法不再赘述

### 集群执行方法

- 执行命令

```shell
hadoop jar cooccur.jar cooccur /user/username/finallab/preprocess_output/ /user/username/finallab/cooccur_output
```

## 任务三：Normalization

本地执行方法不再赘述

### 集群执行方法

- 执行命令

```shell
hadoop jar Normalization.jar Normalization /user/username/finallab/cooccur_output /user/username/finallab/normalization_output
```

## 任务四：PageRank

本地执行方法不再赘述

### 集群执行方法

- 执行命令

```shell
hadoop jar PageRank.jar PageRankDriver /user/username/finallab/normalization_output /user/username/finallab/pagerank_output
```

## PageRank对比实验：Centrality

该程序同样需要三个参数，因此也额外注明在此。

### 本地执行方法

- Artifact选择：Application

- Program arguments: input/cooccur_output input/normal_output output/

- Main class: CentralityDriver

### 集群执行方法

- 执行命令

```shell
hadoop jar Centrality.jar CentralityDriver /user/username/finallab/cooccur_output /user/username/finallab/normalization_output /user/username/finallab/centrality_output
```

## 任务五：LabelProp

本地执行方法不再赘述

### 集群执行方法

- 执行命令

```shell
hadoop jar LabelProp.jar LabelProp /user/username/finallab/normalization_output /user/username/finallab/labelprop_output
```

### Python脚本执行方法

- `check.py`，输出正确率，需要待验证目录，例如

```shell
python3 check.py output
```

- `process.py`，输出`Gephi`所需格式顶点表格和边表格，输入是点目录（任务五）和边目录（任务二），例如

```shell
python3 process.py output ../Cooccurrance/output
```

