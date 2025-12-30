# Docker 镜像加速器配置指南

## Windows Docker Desktop 配置

### 步骤 1：打开 Docker Desktop 设置

1. 右键点击系统托盘中的 Docker 图标
2. 选择 **Settings**（设置）

### 步骤 2：配置镜像加速器

1. 在左侧菜单选择 **Docker Engine**
2. 在 JSON 配置中添加 `registry-mirrors`：

```json
{
  "builder": {
    "gc": {
      "defaultKeepStorage": "20GB",
      "enabled": true
    }
  },
  "experimental": false,
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://hub-mirror.c.163.com",
    "https://mirror.baidubce.com"
  ]
}
```

### 步骤 3：应用并重启

1. 点击 **Apply & Restart**
2. 等待 Docker 重启完成

### 步骤 4：验证配置

```cmd
docker info | findstr "Registry Mirrors"
```

应该能看到配置的镜像地址。

## 国内常用镜像加速器

### 1. 中科大镜像
```
https://docker.mirrors.ustc.edu.cn
```

### 2. 网易镜像
```
https://hub-mirror.c.163.com
```

### 3. 百度云镜像
```
https://mirror.baidubce.com
```

### 4. 阿里云镜像（需要注册）

1. 访问 [阿里云容器镜像服务](https://cr.console.aliyun.com/)
2. 登录后点击 **镜像加速器**
3. 复制你的专属加速地址（格式：`https://xxxxx.mirror.aliyuncs.com`）
4. 添加到 Docker Engine 配置中

### 5. 腾讯云镜像（需要注册）

1. 访问 [腾讯云容器镜像服务](https://cloud.tencent.com/document/product/1141/50332)
2. 获取专属加速地址
3. 添加到配置中

## 配置多个镜像源

可以同时配置多个镜像源，Docker 会自动选择最快的：

```json
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://hub-mirror.c.163.com",
    "https://mirror.baidubce.com",
    "https://xxxxx.mirror.aliyuncs.com"
  ]
}
```

## 验证配置是否生效

### 方法 1：查看 Docker 信息

```cmd
docker info
```

查找 `Registry Mirrors` 部分，应该能看到配置的镜像地址。

### 方法 2：测试拉取镜像

```cmd
docker pull python:3.12.8-slim
```

如果速度明显提升，说明配置成功。

## 如果仍然无法连接

### 方案 1：使用代理

如果有代理服务器，可以在 Docker Desktop 中配置：

1. Settings → Resources → Proxies
2. 配置 HTTP/HTTPS 代理

### 方案 2：使用 VPN

连接 VPN 后再尝试构建镜像。

### 方案 3：手动拉取基础镜像

```cmd
# 使用国内镜像源手动拉取
docker pull registry.cn-hangzhou.aliyuncs.com/acs/python:3.12.8-slim

# 然后修改 Dockerfile 使用这个镜像
```

## 临时解决方案

如果无法配置镜像加速器，可以临时修改 Dockerfile：

```dockerfile
# 使用阿里云镜像
FROM registry.cn-hangzhou.aliyuncs.com/acs/python:3.12.8-slim
```

或者使用其他国内镜像源。

## 常见问题

### Q: 配置后仍然很慢？

A: 
1. 尝试使用不同的镜像源
2. 检查网络连接
3. 使用阿里云或腾讯云的专属加速器（通常更快）

### Q: 如何知道哪个镜像源最快？

A: 可以逐个测试，或者使用网络测速工具测试各个镜像源的延迟。

### Q: 配置后需要重启 Docker 吗？

A: 是的，配置后需要点击 "Apply & Restart" 重启 Docker。

