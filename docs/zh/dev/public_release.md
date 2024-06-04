公开发布手册
=====================

::: warning
公开发布是指部署到 Maven Central. 只有核心项目成员才有权限发布到公开仓库.
:::

## 设置 GPG

在 macOS 上安装
```shell script
brew install gnupg
```

如果没有秘钥, 生成一个秘钥
```shell script
gpg --full-generate-key
```

公开公有秘钥
```shell script
gpg --send-keys [key-id] --keyserver hkp://subkeys.pgp.net
```

查看秘钥
```shell script
gpg --list-keys
```

## 设置 Sonatype 认证

编辑并将下面的配置放入 `~/.m2/settings.xml`

```xml
<servers>
  <server>
    <id>sonatype-nexus-snapshots</id>
    <username>{username}</username>
    <password>{password}</password>
  </server>
  <server>
    <id>sonatype-nexus-staging</id>
    <username>{username}</username>
    <password>{password}</password>
  </server>
</servers>
```

## 切出分支

只有在发布**特性版本**时, 你才需要从 master 分支上切出分支.

```shell script
git checkout -b 2.6
```

## 切换发布版本

```shell script
mvn versions:set -DgenerateBackupPoms=false -DnewVersion=2.6.0
git commit -am '(release) prepare release v2.6.0-rc0'
```

## 打包并部署到 Sonatype

```shell script
mvn clean deploy -DskipTests -Prelease -Pscala-2.11 -Pspark-2.4
mvn clean deploy -DskipTests -Prelease -Pscala-2.12 -Pspark-3.0
```

## 发布

进入 [Sonatype 仓库](https://oss.sonatype.org/#stagingRepositories)

1. 验证资源文件
2. 关闭仓库
3. 发布到 Maven Central

## 在 GitHub 上声明

编辑 [GitHub 发布页](https://github.com/timeplus-io/timeplus-native-jdbc/releases)

## 切换开发版本

```shell script
mvn versions:set -DgenerateBackupPoms=false -DnewVersion=2.7.0-SNAPSHOT
git commit -am '(release) prepare for next development iteration'
```