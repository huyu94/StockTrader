# Docker 镜像拉取问题解决方案

## 问题描述

构建镜像时出现错误：
```
failed to resolve source metadata for docker.io/library/python:3.12.8-slim: 
failed to do request: Head "https://docker.mirrors.ustc.edu.cn/...": EOF
```

这通常是因为：
1. Docker 镜像源配置问题
2. 网络连接不稳定
3. 镜像源服务不可用

## 解决方案

### 方案 1：修改 Docker Desktop 镜像源配置（推荐）

1. **打开 Docker Desktop**
2. **进入设置**：
   - 点击右上角 ⚙️ 设置图标
   - 或右键系统托盘图标 → Settings

3. **配置镜像源**：
   - 左侧菜单选择 **Docker Engine**
   - 在 JSON 配置中添加或修改 `registry-mirrors`：

```json
{
  "registry-mirrors": [
    "https://docker.m.daocloud.io",
    "https://dockerproxy.com",
    "https://docker.nju.edu.cn",
    "https://docker.mirrors.sjtug.sjtu.edu.cn"
  ]
}
```

4. **应用并重启**：
   - 点击 **Apply & Restart**
   - 等待 Docker Desktop 重启完成

5. **重新构建**：
```bash
docker-compose build
```

### 方案 2：使用官方源（如果网络允许）

如果可以直接访问 Docker Hub，可以临时禁用镜像源：

1. **打开 Docker Desktop 设置**
2. **Docker Engine** → 删除或注释掉 `registry-mirrors`
3. **应用并重启**
4. **重新构建**

### 方案 3：手动拉取镜像

如果镜像源配置有问题，可以手动拉取：

```bash
# 尝试从官方源拉取
docker pull python:3.12.8-slim

# 或者指定镜像源
docker pull docker.m.daocloud.io/library/python:3.12.8-slim

# 拉取成功后，重新构建
docker-compose build
```

### 方案 4：使用国内镜像源直接拉取

```bash
# 使用 DaoCloud 镜像源
docker pull docker.m.daocloud.io/library/python:3.12.8-slim

# 或者使用阿里云镜像源（需要先配置）
docker pull registry.cn-hangzhou.aliyuncs.com/library/python:3.12.8-slim

# 拉取后重新构建
docker-compose build
```

## 推荐的镜像源列表

### 国内镜像源（按推荐顺序）

1. **DaoCloud**（推荐）
   ```
   https://docker.m.daocloud.io
   ```

2. **Docker Proxy**
   ```
   https://dockerproxy.com
   ```

3. **南京大学镜像**
   ```
   https://docker.nju.edu.cn
   ```

4. **上海交大镜像**
   ```
   https://docker.mirrors.sjtug.sjtu.edu.cn
   ```

5. **中科大镜像**（当前使用的，可能不稳定）
   ```
   https://docker.mirrors.ustc.edu.cn
   ```

### 完整配置示例

Docker Desktop → Settings → Docker Engine：

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
    "https://docker.m.daocloud.io",
    "https://dockerproxy.com",
    "https://docker.nju.edu.cn"
  ]
}
```

## 验证配置

配置完成后，验证是否生效：

```bash
# 查看 Docker 信息
docker info | grep -A 10 "Registry Mirrors"

# 应该能看到配置的镜像源列表
```

## 如果仍然失败

### 1. 检查网络连接

```bash
# 测试镜像源连接
curl -I https://docker.m.daocloud.io

# 测试 Docker Hub 连接
curl -I https://hub.docker.com
```

### 2. 清除 Docker 缓存

```bash
# 清除构建缓存
docker builder prune

# 重新构建（不使用缓存）
docker-compose build --no-cache
```

### 3. 使用代理（如果有）

如果使用代理，需要在 Docker Desktop 中配置：
- Settings → Resources → Proxies
- 配置 HTTP/HTTPS 代理

## 快速修复步骤总结

1. ✅ 打开 Docker Desktop 设置
2. ✅ 进入 Docker Engine
3. ✅ 添加或修改 `registry-mirrors` 配置
4. ✅ 应用并重启 Docker Desktop
5. ✅ 运行 `docker-compose build` 重新构建

## 测试镜像拉取

配置完成后，测试是否能正常拉取：

```bash
# 测试拉取 Python 镜像
docker pull python:3.12.8-slim

# 如果成功，会显示：
# 3.12.8-slim: Pulling from library/python
# ...
# Status: Downloaded newer image for python:3.12.8-slim
```

