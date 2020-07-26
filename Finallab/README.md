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
hadoop jar preprocess.jar preprocess /MP_Data/task2/People_List_unique.txt /MP_Data/task2/wuxia_novels /user/2020st42/finallab/preprocess_output
```

## 任务二：CoOccurrance

本地执行方法不再赘述

### 集群执行方法

- 执行命令

```shell
hadoop jar cooccur.jar cooccur /user/2020st42/finallab/preprocess_output/ /user/2020st42/finallab/cooccur_output
```

## 任务三：Normalization

本地执行方法不再赘述

### 集群执行方法

- 执行命令

```shell
hadoop jar Normalization.jar Normalization /user/2020st42/finallab/cooccur_output /user/2020st42/finallab/normalization_output
```

## 任务四：PageRank

本地执行方法不再赘述

### 集群执行方法

- 执行命令

```shell
hadoop jar PageRank.jar PageRankDriver /user/2020st42/finallab/normalization_output /user/2020st42/finallab/pagerank_output
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
hadoop jar Centrality.jar CentralityDriver /user/2020st42/finallab/cooccur_output /user/2020st42/finallab/normalization_output /user/2020st42/finallab/centrality_output
```

## 任务五：LabelProp

本地执行方法不再赘述

### 集群执行方法

- 执行命令

```shell
hadoop jar LabelProp.jar LabelProp /user/2020st42/finallab/normalization_output /user/2020st42/finallab/labelprop_output
```

## 任务六：TODO
