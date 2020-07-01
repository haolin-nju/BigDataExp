# README

## Preprocess

因为这个程序需要三个参数，所以额外注明在此。

### 本地执行方法

- Application

- Program arguments: input/People_List_unique.txt input/wuxia_novels output/

- Main class: preprocess

### 集群执行方法

- 提交jar包到2020st42@114.212.190.95后，进入

```shell
/home/2020st42/finallab/
```

这个目录

- 执行命令

```shell
hadoop jar preprocess.jar preprocess /MP_Data/task2/People_List_unique.txt /MP_Data/task2/wuxia_novels /user/2020st42/finallab/preprocess_output
```

## CoOccurrance

本地执行方法不再赘述，下述集群执行方法

```shell
hadoop jar cooccur.jar cooccur /user/2020st42/finallab/preprocess_output/ /user/2020st42/finallab/cooccur_output
```

