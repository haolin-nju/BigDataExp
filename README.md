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
git switch -c yourbranchname
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

## 更新日志

> 2020.4.4
> > 创建项目，添加README和实验工程存放的文件夹

> 2020.4.6
> > 完成Lab2的基础功能和选做1

