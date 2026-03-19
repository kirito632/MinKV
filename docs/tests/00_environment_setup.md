# 通用环境搭建

> 所有测试共用此环境，每次竞价实例重启后需重新执行。

---

## 服务器信息

- 平台：腾讯云竞价实例
- 用户名：`ubuntu`
- IP：每次重启后变化，登录前去控制台确认当前公网 IP

---

## 1. 上传代码（本地执行）

```bash
# 替换 <server_ip> 为当前 IP
scp -r MinKV/ ubuntu@<server_ip>:~/MinKV/
```

---

## 2. SSH 登录

```bash
ssh ubuntu@<server_ip>
```

---

## 3. 安装依赖（服务器上执行）

```bash
sudo apt update
sudo apt install -y cmake g++ make
```

验证版本：

```bash
cmake --version   # 需要 >= 3.10
g++ --version     # 需要 >= 9，支持 C++17
```

---

## 4. 通用编译（各测试有自己的编译命令，此处仅供参考）

```bash
cd ~/MinKV
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_FLAGS="-O2 -march=native -mavx2 -mfma" ..
make -j$(nproc)
```

---

## 5. 保存结果（测试完后本地执行）

竞价实例重启后数据丢失，测试完立即把结果拉回本地：

```bash
# 替换文件名和 IP
scp ubuntu@<server_ip>:~/MinKV/<result_file> ./
```
