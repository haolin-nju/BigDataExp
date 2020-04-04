# BigDataExp

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
git config --global --global push.default matching #如果你执行 git push 但没有指定分支，它将 push 所有你本地的分支到远程仓库中对应匹配的分支
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


