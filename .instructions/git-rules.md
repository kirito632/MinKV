---
inclusion: always
role: Senior Git Assistant
---

#  AI 执行指令 (AI MUST FOLLOW)
1. **自动起名**：当你创建分支时，必须根据任务类型自动建议符合“分支命名规范”的名称。
2. **预撰写提交**：在执行 `git commit` 前，必须先在聊天框展示你生成的“提交信息”，并在末尾询问：“是否以此信息进行提交？”。
3. **安全自检**：在执行 `git commit` 前，必须自主调用 `git status` 和 `git diff`，确认没有误删代码或遗留调试信息（如 printf/cout）。
4. **拒绝执行**：如果当前分支处于冲突状态或工作区不干净且未执行 stash，请停止操作并提示我。

# Git工作流规范

> **目标**: 规范化代码管理，体现工程化能力  
> **适用**: 个人项目、团队协作、开源贡献

## 🌿 **分支管理策略**

### **1. 分支命名规范**
```bash
# 功能开发
feature/user-authentication
feature/redis-cache-optimization

# 问题修复  
fix/memory-leak-in-session
fix/race-condition-clearSession

# 重构优化
refactor/stability-optimization
refactor/performance-improvement

# 发布分支
release/v1.0.0
release/v2.1.0

# 热修复
hotfix/critical-security-patch
```

### **2. 分支工作流**
```bash
# 1. 从main创建功能分支
git checkout main
git pull origin main
git checkout -b feature/new-feature

# 2. 开发和提交
git add .
git commit -m "feat: implement user authentication"

# 3. 推送到远程
git push origin feature/new-feature

# 4. 创建Pull Request
# 5. 代码审查通过后合并到main
# 6. 删除功能分支
git branch -d feature/new-feature
```

## 📝 **提交信息规范 (Conventional Commits)**

### **1. 提交格式**
```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### **2. 类型 (type)**
- **feat**: 新功能
- **fix**: 修复bug
- **docs**: 文档更新
- **style**: 代码格式化（不影响功能）
- **refactor**: 重构（既不是新功能也不是修复）
- **perf**: 性能优化
- **test**: 添加或修改测试
- **chore**: 构建过程或辅助工具的变动

### **3. 提交示例**

#### **日常开发 (推荐)**
```bash
# ✅ 简洁明了，一句搞定
git commit -m "fix(session): fix race condition in ClearSession"
git commit -m "feat(auth): add JWT token validation"
git commit -m "perf(cache): optimize vector operations with SIMD"
git commit -m "docs: update install guide for v1.0"
git commit -m "style: fix indentation in redis_handler.cpp"
git commit -m "test: add unit tests for user authentication"
```

#### **重要功能 (可选详细说明)**
```bash
# ✅ Header + 1-2句话说明
git commit -m "feat(auth): implement JWT token validation

Added middleware to verify tokens and handle expiration.
Includes basic unit tests for valid/invalid tokens."

git commit -m "fix: solve race condition in ClearSession method

Extended mutex scope to ensure atomic find+erase operation.
Eliminated TOCTOU vulnerability and improved performance."

git commit -m "perf(cache): implement SIMD-optimized vector operations

Use AVX2 instructions for 3x performance improvement.
Benchmark: 79万QPS → 237万QPS, P99: 4.2μs → 1.46μs"
```

### 🚀 AI 提交增强
- **perf 类型要求**：如果是 `perf` 类型，你必须尝试从之前的对话记录中提取性能数据（如 QPS 从 X 到 Y），并写入提交信息的 Body 部分。
- **feat/fix 要求**：必须在提交信息中简要说明“为什么这么改”，而不仅仅是“改了什么”。


## 🔍 **代码审查规范**

### **1. Pull Request模板**
```markdown
## 📋 变更描述
简要描述本次变更的内容和目的

## 🎯 解决的问题
- 修复了什么bug？
- 实现了什么功能？
- 优化了什么性能？

## 🧪 测试情况
- [ ] 单元测试通过
- [ ] 集成测试通过
- [ ] 性能测试通过
- [ ] 手动测试验证

## 📊 性能影响
- QPS变化：从 X 提升到 Y
- 延迟变化：P99从 X 降低到 Y
- 内存使用：减少/增加 X%

## 🔗 相关Issue
Closes #123
Fixes #456

## 📸 截图/日志
（如果适用）

## ✅ 检查清单
- [ ] 代码遵循项目编码规范
- [ ] 添加了必要的测试
- [ ] 更新了相关文档
- [ ] 没有引入新的编译警告
- [ ] 通过了所有CI检查
```

### **2. 代码审查要点**
```bash
# 审查前自检
git diff --check  # 检查空白字符问题
git log --oneline -10  # 检查提交历史

# 常见审查项目
- 内存泄漏检查
- 线程安全检查
- 异常安全检查
- 性能影响评估
- 代码可读性
- 测试覆盖率
```

## 🏷️ **版本标签管理**

### **1. 语义化版本 (SemVer)**
```
MAJOR.MINOR.PATCH

MAJOR: 不兼容的API变更
MINOR: 向后兼容的功能新增
PATCH: 向后兼容的问题修复
```

### **2. 标签创建**
```bash
# 创建带注释的标签
git tag -a v1.0.0 -m "Release version 1.0.0

Features:
- High-performance KV storage engine
- SIMD-optimized vector operations
- Redis-compatible protocol

Performance:
- 237万QPS throughput
- P99 latency: 1.46μs
- Memory usage: <100MB"

# 推送标签
git push origin v1.0.0
git push origin --tags
```

## 🤖 **自动化工作流**

### **1. GitHub Actions配置**
```yaml
# .github/workflows/ci.yml
name: CI/CD Pipeline

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup C++
      uses: actions/setup-cpp@v1
      with:
        compiler: gcc-11
        
    - name: Build
      run: |
        mkdir build
        cd build
        cmake -DCMAKE_BUILD_TYPE=Release ..
        make -j$(nproc)
        
    - name: Run Tests
      run: |
        cd build
        ctest --output-on-failure
        
    - name: Performance Benchmark
      run: |
        cd build
        ./benchmark --iterations=1000000
        
    - name: Memory Check
      run: |
        valgrind --leak-check=full ./build/tests
```

### **2. 提交钩子 (Git Hooks)**
```bash
#!/bin/sh
# .git/hooks/pre-commit

# 检查代码格式
if ! command -v clang-format &> /dev/null; then
    echo "clang-format not found, skipping format check"
else
    # 格式化所有C++文件
    find . -name "*.cpp" -o -name "*.h" | xargs clang-format -i
fi

# 运行快速测试
echo "Running quick tests..."
cd build && make test-quick

if [ $? -ne 0 ]; then
    echo "Tests failed, commit aborted"
    exit 1
fi

echo "Pre-commit checks passed"
```

## 📚 **最佳实践**

### **1. 提交频率**
- **小而频繁**: 每个提交解决一个具体问题
- **原子性**: 每个提交都是完整的、可编译的
- **逻辑性**: 相关的变更放在同一个提交中

### **2. 分支策略**
- **短生命周期**: 功能分支尽快合并
- **定期同步**: 经常从main分支拉取更新
- **清理分支**: 合并后及时删除功能分支

### **3. 冲突解决**
```bash
# 解决合并冲突
git checkout feature-branch
git rebase main

# 解决冲突后
git add .
git rebase --continue

# 强制推送（谨慎使用）
git push --force-with-lease origin feature-branch
```
---

### 🛡️ 开源贡献安全规范
1. **最小改动原则**：严禁修改非任务相关的代码行，严禁格式化整个文件（除非有明确要求）。
2. **测试驱动**：添加测试代码后，**必须手动执行一遍项目构建和测试命令**，确保输出为 `PASS` 后再进入 Git 流程。
3. **预检 Diff**：在 Commit 前，请对比当前修改与 `origin/main` 的差异，如果发现意外删除了原有代码，请立即撤销并修复。