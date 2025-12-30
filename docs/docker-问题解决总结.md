# Docker 构建问题解决总结

## 遇到的问题

### 1. 无法从 Docker Hub 拉取基础镜像

**错误现象**：
```
ERROR: failed to solve: python:3.12.8-slim: failed to resolve source metadata
failed to do request: Head "https://docker.mirrors.ustc.edu.cn/...": EOF
```

**原因**：
- 网络连接不稳定，无法访问 Docker Hub
- 镜像加速器配置未生效

### 2. 尝试使用国内镜像源失败

- 阿里云镜像：`registry.cn-hangzhou.aliyuncs.com/acs/python:3.12.8-slim` - **不存在**
- 腾讯云镜像：`ccr.ccs.tencentyun.com/library/python:3.12.8-slim` - **需要认证**

## 解决方案

### 最终方案：配置 Docker 镜像加速器

1. **配置镜像加速器**（Docker Desktop → Settings → Docker Engine）：
```json
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://hub-mirror.c.163.com",
    "https://mirror.baidubce.com"
  ]
}
```

2. **重启 Docker Desktop**（必须！）

3. **手动拉取测试**：
```cmd
docker pull python:3.12.8-slim
```

4. **构建镜像**：
```cmd
docker build -t stocktrader:latest .
```

5. **导出镜像**：
```cmd
docker save -o stocktrader-latest.tar stocktrader:latest
```

## 关键要点

✅ **镜像加速器配置后必须重启 Docker**  
✅ **使用官方 Dockerfile，通过镜像加速器访问 Docker Hub**  
✅ **分步验证：先拉取基础镜像，再构建完整镜像**

## 最终结果

- ✅ 基础镜像成功拉取
- ✅ 项目镜像成功构建（约 1.7 GB）
- ✅ 镜像成功导出为 `stocktrader-latest.tar`

## 保留的关键文件

- `Dockerfile` - 主 Dockerfile
- `docker-build-export.bat` - 构建并导出镜像
- `docker-run-dev.bat` - 开发模式运行（挂载代码）
- `docker-run-prod.bat` - 生产模式运行（代码在镜像中）
- `docker-compose.yml` - Docker Compose 配置
- `docker-compose.nas.yml` - NAS 配置

## 已删除的文件

- ❌ `Dockerfile.cn` - 备用 Dockerfile（不再需要）
- ❌ `docker-build-export-cn.bat` - 国内镜像源脚本（不再需要）
- ❌ `docker-pull-base.bat` - 测试脚本（不再需要）

