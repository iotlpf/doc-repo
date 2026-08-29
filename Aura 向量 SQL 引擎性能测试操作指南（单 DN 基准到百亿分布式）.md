# Aura 向量 SQL 引擎性能测试操作指南（单 DN 基准到百亿分布式）

# 文档概述

## 适用对象

本文档面向需要对 Aura 向量 SQL 引擎进行性能测试的测试工程师。即使你之前没有接触过向量数据库、Ray 分布式框架或 Lance 数据格式，按照本文档的步骤操作，也可以独立完成从环境搭建、单 DN 基准测试、分布式验证到百亿规模规划的全流程。

## 测试目标

1. **单 DN 基准测试（核心）**：在扩展到百亿分布式规模之前，先对单个 DN（Data Node，即一个 Ray Actor）处理 Lance Segment 的性能进行精细化测试，确定单 DN 的最优资源配置、数据承载量和并发参数。

2. **分布式验证**：基于单 DN 的最优配置，验证多 DN 协同检索的线性扩展性和分布式召回率。

3. **百亿规模规划**：根据单 DN 性能数据，推算百亿级数据所需的 DN 数量、资源总量和预期性能。

4. **业界对标**：将测试结果与 FAISS、Qdrant、Weaviate、Milvus、pgvector 等业界主流方案进行直观对比，评估 Aura 的竞争力。

## 核心概念速查

|术语|通俗解释|
|---|---|
|Aura|基于 Ray 构建的分布式向量处理引擎，通过 SQL 接口对外提供向量检索服务。|
|CN（Compute Node）|计算节点，负责接收和解析 SQL 语句，生成查询计划，协调和调度多个 DN 执行分布式计算，并汇总各 DN 的返回结果。|
|DN（Data Node）|数据节点，实际执行向量检索计算的工作单元。每个 DN 是一个 Ray Actor，负责处理一个或多个 Lance Segment。单 DN 性能是整个系统扩展性的基础。|
|Ray|开源分布式计算框架，Aura 运行在 Ray 之上。CN 和 DN 都是 Ray 上的 Actor，Ray 负责资源调度、Actor 生命周期管理和节点间通信。|
|Ray Actor|Ray 中的有状态计算单元。每个 DN 就是一个 Ray Actor，拥有独立的内存空间和计算资源，可以持续接收查询请求并返回结果。|
|Lance|一种现代化的列式数据格式，专为 AI/ML 工作负载设计，支持高性能随机访问和向量搜索，天然适配对象存储。Aura 的向量数据以 Lance 格式存储在 OBS 上。|
|Segment|Lance 数据的逻辑分片单位。一个数据集由多个 Segment 组成，每个 Segment 可以独立被一个 DN 加载和处理。Segment 是 DN 数据分配的基本单位。|
|Fragment|Segment 内部的数据文件单位。一个 Segment 包含多个 Fragment，每个 Fragment 是一个独立的数据文件，存储一定行数的向量数据。默认每个 Fragment 约 100 万行。|
|OBS|对象存储服务（Object Storage Service），Aura 的 Lance 数据文件持久化存储在 OBS 上。DN 在查询时从 OBS 加载所需的 Segment 数据到内存。|
|向量检索|给定一个查询向量，在海量向量中找出距离最近（最相似）的 Top K 个。类似"以图搜图"。|
|召回率（Recall@K）|近似检索返回的 Top K 结果中，有多少比例与精确检索（暴力搜索）的标准答案一致。例如 Recall@10 = 0\.95 表示平均有 9\.5 个结果是对的。|
|P50 / P95 / P99|延迟的百分位数。P99 = 10ms 表示 99% 的查询延迟不超过 10ms，比平均值更能反映真实体验。|
|QPS|每秒查询数（Queries Per Second），衡量系统吞吐量的核心指标。|

---

# Aura 系统架构说明

## 整体架构

Aura 引擎采用计算存储分离的分布式架构，运行在 Ray 集群之上，数据以 Lance 格式存储在 OBS 对象存储中。架构分为三层：

|层级|组件|职责|
|---|---|---|
|接入层|CN（Compute Node）|接收客户端 SQL 请求 → 解析 SQL 生成查询计划 → 根据数据分布将查询拆分到多个 DN → 收集各 DN 局部 Top\-K 结果 → 合并排序得到全局 Top\-K → 返回客户端|
|计算层|DN（Data Node）|每个 DN 是一个 Ray Actor，负责加载和处理一个或多个 Lance Segment。接收 CN 下发的查询请求 → 在本地 Segment 内执行向量检索 → 返回局部 Top\-K 结果给 CN|
|存储层|OBS \+ Lance|向量数据以 Lance 格式持久化存储在 OBS 对象存储上。Lance 格式支持高性能随机访问，DN 按需从 OBS 加载 Segment 到内存进行计算|

## 查询执行流程

一次向量检索 SQL 的完整执行流程如下：

1. 客户端向 CN 发送 SQL 查询语句（如 `SELECT id FROM table ORDER BY vector <-> query_vec LIMIT 10`）。

2. CN 解析 SQL，识别向量检索操作，确定涉及的表和 Segment 分布。

3. CN 生成分布式查询计划，将查询向量广播给所有负责该表 Segment 的 DN。

4. 每个 DN（Ray Actor）接收到查询后，在自己负责的 Segment 内执行向量检索，返回局部 Top\-K（通常 K' = K × 安全系数，如 K × 3）。

5. CN 收集所有 DN 的局部结果，进行全局合并排序，取最终 Top\-K 返回客户端。

**为什么先测单 DN？** 分布式系统的整体性能 = 单 DN 性能 × DN 数量 × 扩展效率。如果单 DN 性能没有调优到最优，盲目增加 DN 数量只会浪费资源。单 DN 基准测试是百亿规模规划的基石——只有知道一个 DN 能扛多少数据、多少 QPS、需要多少资源，才能准确推算百亿规模需要多少台机器。

## Lance 数据组织与 DN 映射

|层级|说明|与 DN 的关系|
|---|---|---|
|Dataset（表）|一张向量表，对应一个 Lance Dataset|由 CN 统一管理，分布到多个 DN|
|Segment|数据的逻辑分片，一个 Dataset 包含多个 Segment|**DN 分配的基本单位**，一个 DN 负责一个或多个 Segment|
|Fragment|Segment 内的物理数据文件，一个 Segment 包含多个 Fragment|DN 加载 Segment 时实际读取的文件单位，默认约 100 万行/Fragment|
|Row（行）|单条向量数据，包含向量列和可选的标量列|检索的最小单位|

Lance 官方建议：10 亿行以内，默认每个 Fragment 约 100 万行效果良好；超过 10 亿行后，可以将 Fragment 增大到约 1 亿行以减少 Fragment 数量。Segment 的大小则根据 DN 的内存和计算能力来规划。

---

# 测试环境准备

## 硬件配置建议

### 单 DN 基准测试环境

单 DN 测试的目的是找到一个 DN 的最优配置，建议在一台独立的物理机或独占虚拟机上进行，避免资源争抢影响测试结果。

|资源|推荐配置|说明|
|---|---|---|
|CPU|16 核以上（如 Intel Xeon / AMD EPYC）|向量检索是 CPU 密集型，核数影响单 DN 内的并行度。建议开启超线程。|
|内存|64 GB \~ 256 GB|决定单个 DN 能承载多大的 Segment。索引需要全部放入内存才能达到最佳性能。建议至少能放下 1000 万 × 128 维向量的索引（约 5\~10 GB）。|
|磁盘|NVMe SSD，500 GB\+|用于本地缓存 Lance 数据。虽然数据在 OBS 上，但 DN 会将热点 Segment 缓存到本地磁盘。|
|网络|10 Gbps\+|从 OBS 加载 Segment 数据时网络带宽是瓶颈。建议测试机与 OBS 在同一可用区。|

### 分布式测试环境

|规模|节点数|每节点配置|说明|
|---|---|---|---|
|小型验证|3 节点|16C64G|1 个 CN \+ 2 个 DN，验证分布式流程正确性|
|中型验证|5\~9 节点|16C64G|1 个 CN \+ 4\~8 个 DN，验证线性扩展性|
|大型验证|17\~33 节点|32C128G|1 个 CN \+ 16\~32 个 DN，模拟亿级到十亿级|
|百亿规划|100\+ 节点|32C256G|基于单 DN 数据推算，实际部署需评估网络和调度开销|

## 软件依赖

- 操作系统：Linux（推荐 Ubuntu 22\.04 LTS）

- Python 3\.9\+（Ray 和 Lance 的 Python 绑定）

- Ray 2\.x（分布式计算框架，Aura 运行时）

- pylance（Lance 格式 Python 库，用于数据准备和验证）

- Aura 引擎（CN 和 DN 服务）

- OBS SDK（用于上传和管理 Lance 数据）

- psutil（资源监控）、numpy（数据处理）、h5py（读取 ann\-benchmarks 数据集）

```bash
pip3 install ray[default] pylance psutil numpy h5py tqdm obsws-python
```

## OBS 配置

测试数据存储在 OBS 上，需要提前配置好 OBS 访问凭证。建议创建一个专用的测试 Bucket。

```bash
export OBS_ENDPOINT="obs.cn-east-xxx.myhuaweicloud.com"
export OBS_ACCESS_KEY="your-access-key"
export OBS_SECRET_KEY="your-secret-key"
export OBS_BUCKET="aura-vector-bench"
export OBS_REGION="cn-east-xxx"
```

```bash
mkdir -p ~/aura_bench/{datasets,lance_data,scripts,results,logs}
cd ~/aura_bench
```

## Ray 集群启动

### 单节点模式（用于单 DN 基准测试）

```bash
# 启动 Ray head 节点，指定可用资源
ray start --head --port=6379 \
  --num-cpus=16 \
  --num-gpus=0 \
  --object-store-memory=10000000000 \
  --dashboard-host=0.0.0.0

# 验证 Ray 状态
ray status
# 查看 Dashboard: http://localhost:8265
```

### 多节点模式（用于分布式测试）

```bash
# 在头节点执行
ray start --head --port=6379 --num-cpus=16 --dashboard-host=0.0.0.0
# 记录输出中的 ray://head-ip:6379 地址
```

```bash
# 在每个 worker 节点执行
ray start --address="ray://head-ip:6379" --num-cpus=16
# 验证所有节点已加入
ray status
```

---

# 开源数据集与 Lance 数据准备

## 常用开源数据集

|数据集|向量数|维度|距离|类型|单 DN 测试用途|
|---|---|---|---|---|---|
|SIFT1M|100 万|128|L2|图像|入门调试、功能验证、小 Segment 测试|
|GIST1M|100 万|960|L2|图像|高维向量测试，验证内存占用|
|SIFT10M|1000 万|128|L2|图像|**单 DN 基准测试主用数据集**，适中规模|
|Deep10M|1000 万|96|L2|图像|低维高吞吐测试|
|SIFT100M|1 亿|128|L2|图像|大 Segment 压力测试，单 DN 上限探索|
|Cohere10M|1000 万|768|Cosine|文本|文本嵌入场景，RAG 业务模拟|
|Deep1B|10 亿|96|L2|图像|分布式验证，多 DN 协同|

## 数据集下载

```python
import os
import urllib.request

DATASETS = {
    "sift-128-euclidean": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
    "gist-960-euclidean": "https://ann-benchmarks.com/gist-960-euclidean.hdf5",
    "deep-image-96-euclidean": "https://ann-benchmarks.com/deep-image-96-euclidean.hdf5",
    "glove-100-angular": "https://ann-benchmarks.com/glove-100-angular.hdf5",
}

def download(name, save_dir="datasets"):
    os.makedirs(save_dir, exist_ok=True)
    url = DATASETS[name]
    save_path = os.path.join(save_dir, f"{name}.hdf5")
    if os.path.exists(save_path):
        print(f"[跳过] {save_path} 已存在")
        return save_path
    print(f"[下载] {name} ...")
    urllib.request.urlretrieve(url, save_path)
    print(f"[完成] {save_path}")
    return save_path

if __name__ == "__main__":
    import sys
    download(sys.argv[1] if len(sys.argv) > 1 else "sift-128-euclidean")
```

## 将数据集转换为 Lance 格式

下载的 HDF5 数据集需要转换为 Lance 格式才能被 Aura 引擎使用。转换时需要规划好 Segment 和 Fragment 的大小。

```python
import h5py
import numpy as np
import lance
import pyarrow as pa
import os

def hdf5_to_lance(hdf5_path, lance_path, rows_per_fragment=1_000_000,
                   vector_col="vector", id_col="id"):
    """
    将 ann-benchmarks HDF5 数据集转换为 Lance 格式
    - hdf5_path: 输入 HDF5 文件路径
    - lance_path: 输出 Lance 数据集路径
    - rows_per_fragment: 每个 Fragment 的行数，默认 100 万
    """
    with h5py.File(hdf5_path, "r") as f:
        # ann-benchmarks 格式：train 或 base 是底库向量
        key = "base" if "base" in f else "train"
        vectors = f[key][:]
        n, dim = vectors.shape
        print(f"读取数据: {n} 条, {dim} 维")

    # 构建 Arrow Table
    ids = pa.array(np.arange(n, dtype=np.int64))
    # Lance 存储向量为 fixed-size list
    vector_array = pa.FixedSizeListArray.from_arrays(
        vectors.flatten(), dim
    )
    table = pa.table({id_col: ids, vector_col: vector_array})

    # 写入 Lance，指定 Fragment 大小
    os.makedirs(os.path.dirname(lance_path) if os.path.dirname(lance_path) else ".", exist_ok=True)
    ds = lance.write_dataset(
        table, lance_path,
        max_rows_per_file=rows_per_fragment,  # Fragment 大小
        max_rows_per_group=rows_per_fragment,
    )
    print(f"Lance 数据集已写入: {lance_path}")
    print(f"  版本数: {ds.versions}")
    print(f"  Fragment 数: {len(ds.get_fragments())}")
    print(f"  总行数: {ds.count_rows()}")
    return ds

if __name__ == "__main__":
    import sys
    hdf5_to_lance(sys.argv[1], sys.argv[2],
                   rows_per_fragment=int(sys.argv[3]) if len(sys.argv) > 3 else 1_000_000)
```

```bash
# 将 SIFT1M 转换为 Lance，每个 Fragment 50 万行（适合小 Segment 测试）
python3 scripts/hdf5_to_lance.py datasets/sift-128-euclidean.hdf5 lance_data/sift1m 500000

# 将 SIFT10M 转换为 Lance，每个 Fragment 100 万行（标准配置）
python3 scripts/hdf5_to_lance.py datasets/sift-128-euclidean_10m.hdf5 lance_data/sift10m 1000000
```

## Lance Segment 规划

Segment 是 DN 数据分配的基本单位。Segment 的大小直接影响：

- **内存占用**：DN 需要将 Segment 的索引加载到内存。Segment 越大，内存占用越高。

- **查询延迟**：Segment 越大，单次检索的计算量越大，延迟越高。

- **加载时间**：DN 启动或故障恢复时从 OBS 加载 Segment 的时间。

- **并发度**：Segment 数量决定了可以并行的 DN 数量。Segment 太小会导致调度开销增大。

|Segment 规模|向量数|128维索引大小\(估\)|Fragment数|适用场景|
|---|---|---|---|---|
|小 Segment|100 万|\~0\.5 GB|1\~2|功能验证、低延迟场景、高并发小查询|
|标准 Segment|500 万|\~2\.5 GB|5|**推荐起点**，平衡延迟和调度开销|
|大 Segment|1000 万|\~5 GB|10|高吞吐批处理、DN 资源充足时|
|超大 Segment|5000 万|\~25 GB|50|探索单 DN 上限，需大内存机器|

**单 DN 测试的核心变量之一就是 Segment 大小。**建议从 100 万开始，逐步增大到 5000 万，找到单 DN 在延迟、吞吐和资源之间的最佳平衡点。这个平衡点就是分布式部署时每个 DN 的标准数据承载量。

## 上传 Lance 数据到 OBS

```python
import os
from obs import ObsClient

def upload_lance_to_obs(local_path, obs_prefix, bucket, endpoint, ak, sk):
    """递归上传 Lance 数据集目录到 OBS"""
    client = ObsClient(access_key_id=ak, secret_access_key=sk, server=endpoint)
    uploaded = 0
    for root, dirs, files in os.walk(local_path):
        for fname in files:
            local_file = os.path.join(root, fname)
            rel_path = os.path.relpath(local_file, local_path)
            obs_key = f"{obs_prefix}/{rel_path}"
            resp = client.putFile(bucket, obs_key, local_file)
            if resp.status < 300:
                uploaded += 1
                if uploaded % 10 == 0:
                    print(f"  已上传 {uploaded} 个文件")
            else:
                print(f"  上传失败: {obs_key}, status={resp.status}")
    print(f"上传完成: 共 {uploaded} 个文件, OBS 前缀: {obs_prefix}")
    client.close()

if __name__ == "__main__":
    upload_lance_to_obs(
        local_path="lance_data/sift10m",
        obs_prefix="vector-bench/sift10m",
        bucket=os.environ["OBS_BUCKET"],
        endpoint=os.environ["OBS_ENDPOINT"],
        ak=os.environ["OBS_ACCESS_KEY"],
        sk=os.environ["OBS_SECRET_KEY"],
    )
```

---

# 单 DN 性能基准测试（核心）

**本章是整个测试的核心。**单 DN 基准测试的目标是回答三个问题：\(1\) 一个 DN 分配多少 CPU/内存最合适？\(2\) 一个 DN 处理多大的 Segment 性能最优？\(3\) 并发参数（DN 内并行度、查询并发数）调到多少时 QPS 最高且延迟可控？这三个问题的答案直接决定百亿规模的资源规划。

## 测试矩阵设计

单 DN 测试采用控制变量法，每次只改变一个维度，其他维度保持固定。测试矩阵如下：

|测试维度|取值范围|默认值|测试目的|
|---|---|---|---|
|Segment 大小|1M / 5M / 10M / 20M / 50M|10M|找到单 DN 最优数据承载量|
|DN CPU 核数|4 / 8 / 16 / 32|16|找到 CPU 资源与性能的性价比拐点|
|DN 内存|16G / 32G / 64G / 128G|64G|确认索引是否能全量入内存，评估 OOM 阈值|
|查询并发数|1 / 2 / 4 / 8 / 16 / 32 / 64|8|找到 QPS 饱和点和延迟拐点|
|Top K|1 / 10 / 50 / 100|10|评估 K 值对延迟和召回的影响|
|索引参数\(ef\_search\)|10 / 20 / 50 / 100 / 200 / 500|100|绘制召回率\-延迟权衡曲线|
|向量维度|96 / 128 / 768 / 1536|128|评估高维向量对性能的影响|

## 单 DN 启动与配置

### 通过 Ray Actor 启动单个 DN

```python
import ray
import time

# Aura DN Actor 的资源声明示例（根据实际 Aura API 调整）
@ray.remote(num_cpus=16, memory=64 * 1024 * 1024 * 1024)
class DataNodeActor:
    def __init__(self, obs_path, segment_id, index_params=None):
        """
        初始化 DN，从 OBS 加载指定 Segment
        - obs_path: Lance 数据集在 OBS 上的路径
        - segment_id: 要加载的 Segment ID
        - index_params: 索引参数字典，如 {"type": "hnsw", "m": 16, "ef_construction": 200}
        """
        self.obs_path = obs_path
        self.segment_id = segment_id
        self.index_params = index_params or {}
        self.load_start = time.time()
        # --- 以下为伪代码，替换为实际 Aura DN 初始化逻辑 ---
        # self.dataset = lance.dataset(obs_path)
        # self.segment = self.dataset.get_segments()[segment_id]
        # self.index = build_index(self.segment, **index_params)
        self.load_time = time.time() - self.load_start
        print(f"DN 加载 Segment {segment_id} 完成, 耗时 {self.load_time:.2f}s")

    def search(self, query_vector, k=10, ef_search=100):
        """执行向量检索，返回 (ids, distances, latency_ms)"""
        start = time.perf_counter()
        # --- 替换为实际 Aura DN 检索逻辑 ---
        # ids, distances = self.index.search(query_vector, k, ef=ef_search)
        ids, distances = [], []
        latency = (time.perf_counter() - start) * 1000
        return ids, distances, latency

    def get_stats(self):
        """返回 DN 统计信息"""
        return {
            "segment_id": self.segment_id,
            "load_time_s": self.load_time,
            "index_params": self.index_params,
        }

def start_dn(obs_path, segment_id, num_cpus=16, memory_gb=64, index_params=None):
    """启动一个 DN Actor 并等待加载完成"""
    print(f"启动 DN: CPU={num_cpus}, 内存={memory_gb}GB, Segment={segment_id}")
    dn = DataNodeActor.options(
        num_cpus=num_cpus,
        memory=memory_gb * 1024 * 1024 * 1024
    ).remote(obs_path, segment_id, index_params)
    stats = ray.get(dn.get_stats.remote())
    print(f"DN 就绪: {stats}")
    return dn
```

### 资源配置测试方法

测试不同 CPU/内存配置对单 DN 性能的影响时，通过 Ray Actor 的 `num_cpus` 和 `memory` 参数控制：

```python
import ray
import numpy as np

def test_resource_config(obs_path, segment_id, query_vectors, groundtruth,
                         cpu_configs=[4, 8, 16, 32], memory_gb=64, k=10):
    """遍历不同 CPU 配置，测试单 DN 性能"""
    results = []
    for cpus in cpu_configs:
        print(f"\n=== 测试 CPU={cpus} ===")
        dn = start_dn(obs_path, segment_id, num_cpus=cpus, memory_gb=memory_gb)
        # 延迟测试
        latency_stats = bench_latency(dn, query_vectors[:1000], k=k)
        # QPS 测试
        qps_stats = bench_qps(dn, query_vectors, k=k, duration=30)
        # 召回率测试
        recall_stats = bench_recall(dn, query_vectors, groundtruth, k=k)
        results.append({
            "cpu": cpus,
            "memory_gb": memory_gb,
            **latency_stats, **qps_stats, **recall_stats,
        })
        # 销毁 DN 释放资源
        ray.kill(dn)
    return results
```

## Segment 加载性能测试

DN 启动时从 OBS 加载 Segment 到内存并构建索引，这个过程的耗时直接影响冷启动时间和故障恢复速度。

```python
import time
import ray

def bench_segment_load(obs_path, segment_sizes=[1_000_000, 5_000_000, 10_000_000],
                       num_cpus=16, memory_gb=64):
    """测试不同大小 Segment 的加载时间和内存占用"""
    results = []
    for size in segment_sizes:
        print(f"\n=== 测试 Segment 大小: {size:,} ===")
        start = time.time()
        dn = start_dn(obs_path, segment_id=0, num_cpus=num_cpus, memory_gb=memory_gb)
        load_time = time.time() - start
        # 获取 Ray Actor 的内存占用
        actor_info = ray.state.actors()
        memory_usage = None
        for aid, info in actor_info.items():
            if info.get("Class Name") == "DataNodeActor":
                memory_usage = info.get("Memory", {}).get("used", 0) / (1024**3)
        stats = ray.get(dn.get_stats.remote())
        results.append({
            "segment_size": size,
            "load_time_s": load_time,
            "memory_usage_gb": memory_usage,
            "index_build_time_s": stats.get("load_time_s"),
        })
        print(f"  加载时间: {load_time:.2f}s, 内存占用: {memory_usage:.2f}GB")
        ray.kill(dn)
    return results
```

## 单查询延迟测试

```python
import time
import numpy as np

def bench_latency(dn, query_vectors, k=10, ef_search=100, warmup=100):
    """逐条查询，统计延迟分布（P50/P95/P99）"""
    latencies = []
    # Warmup
    for i in range(min(warmup, len(query_vectors))):
        ray.get(dn.search.remote(query_vectors[i], k=k, ef_search=ef_search))
    # 正式测试
    for vec in query_vectors:
        _, _, latency = ray.get(dn.search.remote(vec, k=k, ef_search=ef_search))
        latencies.append(latency)
    latencies = np.array(latencies)
    stats = {
        "k": k, "ef_search": ef_search, "n_queries": len(latencies),
        "p50_ms": float(np.percentile(latencies, 50)),
        "p95_ms": float(np.percentile(latencies, 95)),
        "p99_ms": float(np.percentile(latencies, 99)),
        "mean_ms": float(np.mean(latencies)),
        "min_ms": float(np.min(latencies)),
        "max_ms": float(np.max(latencies)),
    }
    print(f"  延迟: P50={stats['p50_ms']:.2f}ms, P95={stats['p95_ms']:.2f}ms, "
          f"P99={stats['p99_ms']:.2f}ms")
    return stats
```

## 并发 QPS 测试

并发测试是单 DN 调优的关键。通过逐步增加并发查询数，找到 QPS 的饱和点——即 QPS 不再随并发数线性增长的拐点。超过这个点后，增加并发只会导致延迟飙升。

```python
import time
import ray
import numpy as np
from concurrent.futures import ThreadPoolExecutor

def bench_qps(dn, query_vectors, k=10, ef_search=100,
              concurrency_levels=[1, 2, 4, 8, 16, 32, 64], duration=30):
    """
    测试不同并发度下的 QPS 和延迟
    使用线程池模拟多个客户端同时向同一个 DN 发查询
    """
    results = []
    for concurrency in concurrency_levels:
        print(f"\n--- 并发数: {concurrency} ---")
        latencies = []
        total_queries = 0
        stop = False

        def worker():
            nonlocal total_queries
            idx = 0
            while not stop:
                vec = query_vectors[idx % len(query_vectors)]
                _, _, latency = ray.get(dn.search.remote(vec, k=k, ef_search=ef_search))
                latencies.append(latency)
                total_queries += 1
                idx += 1

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker) for _ in range(concurrency)]
            time.sleep(duration)
            stop = True
            for f in futures:
                f.result()

        latencies = np.array(latencies)
        qps = total_queries / duration
        stats = {
            "concurrency": concurrency,
            "k": k, "ef_search": ef_search,
            "total_queries": total_queries,
            "qps": float(qps),
            "mean_latency_ms": float(np.mean(latencies)),
            "p50_ms": float(np.percentile(latencies, 50)),
            "p95_ms": float(np.percentile(latencies, 95)),
            "p99_ms": float(np.percentile(latencies, 99)),
        }
        results.append(stats)
        print(f"  QPS={qps:.1f}, P50={stats['p50_ms']:.2f}ms, "
              f"P99={stats['p99_ms']:.2f}ms")
    return results
```

**如何判断最优并发数？** 绘制"并发数\-QPS"曲线和"并发数\-P99延迟"曲线。最优并发数是 QPS 曲线开始变平（饱和）且 P99 延迟仍在可接受范围内（如 \< 50ms）的最大并发数。超过这个点后 QPS 增长缓慢但延迟急剧上升，属于资源浪费。

## 召回率测试

```python
import numpy as np
import ray

def compute_recall(predicted_ids, groundtruth_ids, k):
    """计算 Recall@K：预测结果与标准答案的交集比例"""
    pred = predicted_ids[:, :k]
    gt = groundtruth_ids[:, :k]
    recalls = [len(set(pred[i]) & set(gt[i])) / k for i in range(len(pred))]
    return float(np.mean(recalls))

def bench_recall(dn, query_vectors, groundtruth_ids, k_values=[1, 10, 50, 100],
                 ef_search=100):
    """测试不同 K 值和 ef_search 下的召回率"""
    max_k = max(k_values)
    n_query = len(query_vectors)
    all_predicted = np.zeros((n_query, max_k), dtype=np.int64)
    for i, vec in enumerate(query_vectors):
        ids, _, _ = ray.get(dn.search.remote(vec, k=max_k, ef_search=ef_search))
        all_predicted[i, :len(ids)] = ids
    results = {}
    for k in k_values:
        recall = compute_recall(all_predicted, groundtruth_ids, k)
        results[f"recall@{k}"] = recall
        print(f"  Recall@{k}: {recall:.4f} ({recall*100:.2f}%)")
    return results
```

## 索引参数调优（召回率\-延迟权衡）

```python
def tune_hnsw_params(dn, query_vectors, groundtruth_ids,
                     ef_search_values=[10, 20, 50, 100, 200, 500, 1000],
                     k=10, concurrency=8):
    """遍历不同 ef_search，记录召回率和延迟，绘制权衡曲线"""
    results = []
    for ef in ef_search_values:
        print(f"\n--- ef_search = {ef} ---")
        latency_stats = bench_latency(dn, query_vectors[:1000], k=k, ef_search=ef)
        recall_stats = bench_recall(dn, query_vectors, groundtruth_ids,
                                     k_values=[k], ef_search=ef)
        results.append({
            "ef_search": ef,
            "p50_ms": latency_stats["p50_ms"],
            "p99_ms": latency_stats["p99_ms"],
            f"recall@{k}": recall_stats[f"recall@{k}"],
        })
    print(f"\n{'ef_search':>10} {'p50(ms)':>10} {'p99(ms)':>10} {'recall@10':>12}")
    for r in results:
        print(f"{r['ef_search']:>10} {r['p50_ms']:>10.2f} {r['p99_ms']:>10.2f} "
              f"{r[f'recall@{k}']:>12.4f}")
    return results
```

## 资源占用监控

```python
import psutil
import time
import ray
import threading

class DNResourceMonitor:
    """监控 DN Actor 所在进程的 CPU 和内存"""
    def __init__(self, interval=1.0):
        self.interval = interval
        self.stop_event = threading.Event()
        self.records = []

    def _monitor(self):
        while not self.stop_event.is_set():
            # 获取 Ray 相关进程的资源占用
            total_cpu = 0
            total_mem = 0
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmd = " ".join(proc.info.get('cmdline') or [])
                    if 'ray' in cmd.lower() or 'aura' in cmd.lower():
                        total_cpu += proc.cpu_percent(interval=None)
                        total_mem += proc.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            self.records.append({
                "timestamp": time.time(),
                "cpu_percent": total_cpu,
                "memory_gb": total_mem / (1024**3),
            })
            time.sleep(self.interval)

    def start(self):
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join()
        if not self.records:
            return {}
        cpus = [r["cpu_percent"] for r in self.records]
        mems = [r["memory_gb"] for r in self.records]
        return {
            "cpu_avg": float(sum(cpus)/len(cpus)),
            "cpu_max": float(max(cpus)),
            "memory_avg_gb": float(sum(mems)/len(mems)),
            "memory_max_gb": float(max(mems)),
        }
```

## 单 DN 完整测试执行脚本

```python
"""
单 DN 性能基准测试主脚本
按顺序执行：Segment加载 → 延迟测试 → 并发QPS测试 → 召回率测试 → 参数调优
所有结果保存为 JSON，方便后续分析和对比
"""
import ray
import json
import time
import numpy as np
import h5py

def load_query_and_gt(hdf5_path, n_query=10000):
    """从 HDF5 加载查询向量和 Ground Truth"""
    with h5py.File(hdf5_path, "r") as f:
        queries = f["test"][:n_query]
        gt = f["neighbors"][:n_query]
    return queries, gt

def run_full_benchmark(obs_path, segment_id, hdf5_path, output_path,
                        num_cpus=16, memory_gb=64, k=10):
    ray.init(address="auto", ignore_reinit_error=True)
    queries, gt = load_query_and_gt(hdf5_path)
    all_results = {"config": {"num_cpus": num_cpus, "memory_gb": memory_gb, "k": k}}

    # 1. 启动 DN，记录加载时间
    print("=" * 60)
    print("Step 1: 启动 DN 并加载 Segment")
    load_start = time.time()
    dn = start_dn(obs_path, segment_id, num_cpus=num_cpus, memory_gb=memory_gb)
    all_results["segment_load_time_s"] = time.time() - load_start

    # 2. 单查询延迟测试
    print("\n" + "=" * 60)
    print("Step 2: 单查询延迟测试")
    monitor = DNResourceMonitor(interval=0.5)
    monitor.start()
    all_results["latency"] = bench_latency(dn, queries, k=k)
    all_results["resource_latency_test"] = monitor.stop()

    # 3. 并发 QPS 测试
    print("\n" + "=" * 60)
    print("Step 3: 并发 QPS 测试")
    monitor = DNResourceMonitor(interval=0.5)
    monitor.start()
    all_results["qps"] = bench_qps(dn, queries, k=k, duration=30)
    all_results["resource_qps_test"] = monitor.stop()

    # 4. 召回率测试
    print("\n" + "=" * 60)
    print("Step 4: 召回率测试")
    all_results["recall"] = bench_recall(dn, queries, gt, k_values=[1, 10, 50, 100])

    # 5. 参数调优
    print("\n" + "=" * 60)
    print("Step 5: HNSW 参数调优")
    all_results["hnsw_tuning"] = tune_hnsw_params(dn, queries, gt, k=k)

    # 6. 不同 Top K 测试
    print("\n" + "=" * 60)
    print("Step 6: 不同 Top K 延迟测试")
    all_results["topk_latency"] = []
    for k_val in [1, 10, 50, 100]:
        stats = bench_latency(dn, queries[:1000], k=k_val)
        stats["k"] = k_val
        all_results["topk_latency"].append(stats)

    # 保存结果
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(f"\n测试完成，结果已保存到: {output_path}")

    ray.kill(dn)
    return all_results

if __name__ == "__main__":
    run_full_benchmark(
        obs_path="obs://aura-vector-bench/vector-bench/sift10m",
        segment_id=0,
        hdf5_path="datasets/sift-128-euclidean.hdf5",
        output_path="results/single_dn_sift10m_16c64g.json",
        num_cpus=16, memory_gb=64, k=10,
    )
```

---

# 分布式性能测试

## 从单 DN 到多 DN

单 DN 基准测试确定最优配置后，进入分布式验证阶段。分布式测试的核心目标是验证：

1. **线性扩展性**：DN 数量从 1 增加到 N 时，QPS 是否按比例增长（理想情况是 N 倍）。

2. **分布式召回率**：CN 汇总各 DN 局部结果后，全局 Top\-K 的召回率是否与单 DN（全量数据）一致。

3. **CN 调度开销**：CN 解析 SQL、分发查询、汇总结果的开销是否可忽略。

4. **网络通信开销**：DN 之间、CN 与 DN 之间的通信是否成为瓶颈。

## 分布式测试配置

|测试项|DN 数量|数据总量|验证目标|
|---|---|---|---|
|基线|1|10M|单 DN 最优配置下的性能基线|
|2 DN|2|20M|验证 2 倍扩展效率，CN 汇总逻辑正确性|
|4 DN|4|40M|验证 4 倍扩展效率|
|8 DN|8|80M|验证 8 倍扩展效率，接近亿级|
|16 DN|16|160M|验证 16 倍扩展，多节点网络开销|
|32 DN|32|320M|大规模验证，十亿级前置验证|

## 分布式查询执行（通过 CN 发送 SQL）

```python
import time
import numpy as np

def bench_distributed_latency(cn_client, query_vectors, k=10, warmup=100):
    """通过 CN 发送 SQL 查询，统计端到端延迟（含 CN 调度 + DN 计算 + 结果汇总）"""
    latencies = []
    # Warmup
    for i in range(min(warmup, len(query_vectors))):
        vec_str = "[" + ",".join(f"{v:.6f}" for v in query_vectors[i]) + "]"
        sql = f"SELECT id FROM vector_table ORDER BY vector <-> '{vec_str}' LIMIT {k}"
        cn_client.execute(sql)
    # 正式测试
    for vec in query_vectors:
        vec_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
        sql = f"SELECT id FROM vector_table ORDER BY vector <-> '{vec_str}' LIMIT {k}"
        start = time.perf_counter()
        result = cn_client.execute(sql)
        latency = (time.perf_counter() - start) * 1000
        latencies.append(latency)
    latencies = np.array(latencies)
    return {
        "p50_ms": float(np.percentile(latencies, 50)),
        "p95_ms": float(np.percentile(latencies, 95)),
        "p99_ms": float(np.percentile(latencies, 99)),
        "mean_ms": float(np.mean(latencies)),
    }

def bench_distributed_scalability(cn_client_factory, dn_counts, query_vectors,
                                   k=10, duration=30):
    """测试不同 DN 数量下的 QPS，计算扩展效率"""
    results = []
    for n_dn in dn_counts:
        print(f"\n=== DN 数量: {n_dn} ===")
        cn = cn_client_factory(n_dn)  # 启动/配置 n_dn 个 DN 的集群
        # 并发 QPS 测试
        from concurrent.futures import ThreadPoolExecutor
        latencies = []
        total = 0
        stop = False
        def worker():
            nonlocal total
            idx = 0
            while not stop:
                vec = query_vectors[idx % len(query_vectors)]
                vec_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
                sql = f"SELECT id FROM t ORDER BY vector <-> '{vec_str}' LIMIT {k}"
                start = time.perf_counter()
                cn.execute(sql)
                latencies.append((time.perf_counter() - start) * 1000)
                total += 1
                idx += 1
        with ThreadPoolExecutor(max_workers=n_dn * 2) as ex:
            futures = [ex.submit(worker) for _ in range(n_dn * 2)]
            time.sleep(duration)
            stop = True
            for f in futures: f.result()
        latencies = np.array(latencies)
        qps = total / duration
        # 扩展效率 = 当前QPS / (单DN QPS × n_dn)
        baseline_qps = results[0]["qps"] if results else qps  # 第一个是单DN
        efficiency = qps / (baseline_qps * n_dn) * 100 if results else 100
        stats = {
            "n_dn": n_dn, "qps": float(qps),
            "p50_ms": float(np.percentile(latencies, 50)),
            "p99_ms": float(np.percentile(latencies, 99)),
            "scalability_efficiency": float(efficiency),
        }
        results.append(stats)
        print(f"  QPS={qps:.1f}, P50={stats['p50_ms']:.2f}ms, 扩展效率={efficiency:.1f}%")
    return results
```

## 分布式召回率验证

分布式检索中，每个 DN 只返回局部 Top\-K，CN 汇总后取全局 Top\-K。需要验证全局召回率是否等于（或接近）全量数据在单 DN 上的召回率。

```python
import numpy as np

def verify_distributed_recall(cn_client, query_vectors, groundtruth_full,
                               n_dn, k=10, local_k_factor=3):
    """
    验证分布式召回率
    - groundtruth_full: 全量数据的精确检索 Ground Truth
    - local_k_factor: 每个 DN 返回的局部 Top-K 数量 = k × local_k_factor
    """
    n_query = len(query_vectors)
    predicted = np.zeros((n_query, k), dtype=np.int64)
    for i, vec in enumerate(query_vectors):
        vec_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
        sql = f"SELECT id FROM t ORDER BY vector <-> '{vec_str}' LIMIT {k}"
        rows = cn_client.execute(sql)
        for j, row in enumerate(rows):
            predicted[i, j] = row[0]
    # 计算全局召回率
    recall = compute_recall(predicted, groundtruth_full, k)
    print(f"分布式召回率 (DN={n_dn}, K={k}): {recall:.4f}")
    # 如果召回率偏低，可能需要增大 local_k_factor
    if recall < 0.95:
        print(f"  ⚠️ 召回率偏低，建议增大 DN 局部返回数量 (当前 factor={local_k_factor})")
    return recall
```

---

# 从百万到百亿的扩展路线

## 渐进式扩展阶段

|阶段|数据规模|DN 数量|每DN数据|预估总资源|测试重点|
|---|---|---|---|---|---|
|Stage 0|100 万|1|1M|16C64G ×1|功能验证，SQL 语法正确性|
|Stage 1|1000 万|1|10M|16C64G ×1|**单 DN 基准测试**，确定最优配置|
|Stage 2|5000 万|1\~5|10M|16C64G ×5|单 DN 上限探索 \+ 小规模分布式验证|
|Stage 3|1 亿|10|10M|16C64G ×10|分布式线性扩展性验证|
|Stage 4|10 亿|100|10M|16C64G ×100|十亿级验证，网络和调度开销评估|
|Stage 5|100 亿|1000|10M|16C64G ×1000|百亿级规划（基于单 DN 数据推算）|

**百亿规模资源推算方法：**假设单 DN 基准测试得出最优配置为 16C64G、承载 1000 万向量、QPS=500、P99=10ms、Recall@10=95%。那么 100 亿向量需要 100 亿 / 1000 万 = 1000 个 DN，总资源 = 16000 核 CPU \+ 64000 GB 内存。理论总 QPS = 500 × 1000 × 扩展效率（如 80%）= 400000 QPS。实际部署还需考虑 CN 节点、故障冗余（建议 \+20%）和网络设备。

## Segment 与 DN 映射规划

|总数据量|Segment大小|Segment数|DN数|说明|
|---|---|---|---|---|
|1000 万|10M|1|1|单 DN 全量加载|
|1 亿|10M|10|10|每个 DN 一个 Segment|
|10 亿|10M|100|100|每个 DN 一个 Segment，10 节点 × 每节点 10 DN|
|100 亿|10M|1000|1000|100 节点 × 每节点 10 DN，或 50 节点 × 每节点 20 DN|

---

# 测试 SQL 语句与操作命令

## 建表与数据导入

```sql
-- 创建向量表，指定向量列和维度
CREATE TABLE IF NOT EXISTS vector_table (
    id BIGINT PRIMARY KEY,
    vector VECTOR(128) NOT NULL,
    category INT,
    created_at TIMESTAMP
)
STORAGE FORMAT lance
LOCATION 'obs://aura-vector-bench/vector-bench/sift10m';

-- 查看表信息
DESCRIBE vector_table;
SHOW CREATE TABLE vector_table;
```

```sql
-- HNSW 索引
CREATE INDEX idx_hnsw ON vector_table USING hnsw (vector)
WITH (metric = 'l2', m = 16, ef_construction = 200);

-- IVF 索引（适合大规模）
CREATE INDEX idx_ivf ON vector_table USING ivf (vector)
WITH (metric = 'l2', nlist = 4096, nprobe = 10);

-- 查看索引
SHOW INDEX FROM vector_table;
```

## 查询语句

```sql
-- Top-10 向量检索
SELECT id, vector <-> '[0.1,0.2,...]' AS distance
FROM vector_table
ORDER BY vector <-> '[0.1,0.2,...]'
LIMIT 10;

-- 带标量过滤的检索
SELECT id, category, vector <-> '[...]' AS distance
FROM vector_table
WHERE category = 5
ORDER BY vector <-> '[...]'
LIMIT 10;
```

```sql
-- 设置 HNSW 查询时的搜索宽度
SET hnsw.ef_search = 100;
SET hnsw.ef_search = 500;  -- 更高召回，更慢

-- 设置 IVF 查询时扫描的桶数
SET ivf.nprobe = 10;
SET ivf.nprobe = 50;

-- 查看当前参数
SHOW VARIABLES LIKE '%ef_search%';
SHOW VARIABLES LIKE '%nprobe%';
```

```sql
-- 查看查询计划，确认 CN 如何分发到 DN
EXPLAIN
SELECT id FROM vector_table
ORDER BY vector <-> '[...]'
LIMIT 10;

-- 详细执行计划（含各 DN 预估成本）
EXPLAIN ANALYZE
SELECT id FROM vector_table
ORDER BY vector <-> '[...]'
LIMIT 10;
```

## Ray 集群管理命令

```bash
# 查看集群状态（节点数、资源使用）
ray status

# 查看 Actor 列表（DN 实例）
ray list actors

# 查看 Dashboard
# 默认地址: http://head-node-ip:8265

# 关闭 Ray 集群
ray stop

# 查看 Ray 日志
ls /tmp/ray/session_latest/logs/
tail -f /tmp/ray/session_latest/logs/python-core-driver-*.log
```

## OBS 数据管理命令

```bash
# 列出 Bucket 中的 Lance 数据
obsutil ls obs://aura-vector-bench/vector-bench/ -r

# 查看 Lance 数据集大小
obsutil du obs://aura-vector-bench/vector-bench/sift10m

# 下载 Lance 数据到本地（用于验证）
obsutil cp obs://aura-vector-bench/vector-bench/sift10m ./lance_data/sift10m -r -f
```

---

# 指标采集与计算方法

## 核心指标定义

|指标|单位|定义与计算方法|
|---|---|---|
|P50 / P95 / P99 延迟|ms|将所有查询延迟从小到大排序，取第 50%/95%/99% 分位的值。使用 numpy\.percentile\(latencies, 99\) 计算。|
|QPS|查询/秒|固定时间窗口内完成的查询总数 / 时间秒数。并发测试中持续运行 duration 秒，统计总查询数。|
|Recall@K|比例\(0\~1\)|近似检索返回的 Top\-K 与精确检索 Ground Truth 的交集大小 / K，对所有查询取平均。公式：mean\(\|pred\_topk ∩ gt\_topk\| / K\)。|
|Segment 加载时间|s|从 DN Actor 初始化开始到 Segment 数据从 OBS 加载完成、索引构建完毕的总耗时。|
|索引构建时间|s|Segment 数据加载到内存后，构建 HNSW/IVF 索引的耗时。|
|内存占用|GB|DN Actor 进程在查询稳定状态下的 RSS（常驻内存），包含向量数据、索引结构和运行时开销。|
|CPU 利用率|%|查询期间 DN 进程的 CPU 使用率，平均值和峰值。100% 表示用完一个核，1600% 表示用完 16 核。|
|OBS 读取量|GB|Segment 加载期间从 OBS 下载的数据总量，可通过 OBS 监控或网络流量统计获取。|
|扩展效率|%|分布式测试中，N 个 DN 的实际 QPS / \(单 DN QPS × N\) × 100%。理想值 100%，低于 80% 说明存在显著的调度或网络开销。|
|错误率|%|查询失败数 / 总查询数 × 100%。长稳测试中重点关注。|

## 指标采集注意事项

- **必须做 Warmup**：正式测试前先跑 100\~500 条查询，让 Segment 数据进入内存缓存、JIT 编译完成，否则前几条查询延迟会异常高。

- **延迟和 QPS 分开测**：单查询延迟测试用串行（并发=1），QPS 测试用多线程并发。不要在并发测试中统计单查询延迟作为"延迟指标"。

- **召回率必须伴随性能**：任何延迟或 QPS 数据都必须同时标注对应的 Recall@K，否则数据无意义——可以通过降低 ef\_search 来获得任意低的延迟。

- **记录冷启动和热启动**：DN 刚启动后的第一次查询（冷启动）和持续查询中的延迟（热启动）差异可能很大，分别记录。

- **资源监控独立进程**：资源监控（CPU/内存）应在独立线程或进程中运行，避免影响被测查询的延迟统计。

---

# 业界对比与竞争力分析

本章提供业界主流向量检索方案在标准数据集上的公开性能数据作为对比基准。将 Aura 单 DN 测试结果填入对比表，可以直观评估 Aura 的性能水平和竞争力。

## SIFT1M（128维，L2）单节点业界基准

以下数据来自公开论文和官方基准测试，硬件配置为 16\~32 核 CPU、64GB 内存级别。

|系统/方案|QPS|P50\(ms\)|P99\(ms\)|Recall@10|数据来源|
|---|---|---|---|---|---|
|FAISS \(HNSW\)|\~866|\< 2|\~3|\~95%|arXiv 2608\.12812|
|Qdrant|\~216|4\.55|\~8\.2|\~95%|arXiv 2608\.12812|
|Weaviate \(HNSW\)|\~5639|2\.80|4\.43|97\.24%|Weaviate 官方基准 \(ef=96,M=16\)|
|pgvector \(HNSW\)|\~154|\~6|\~12|\~95%|arXiv 2608\.12812|
|HNSW 纯库 \(M=16,ef=100\)|\~2000\+|\~0\.5|\~1\.0|96%|ann\-benchmarks\.com|
|**Aura 单 DN \(待填\)**|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|本次测试|

## DBPedia OpenAI 1M（1536维，Cosine）业界基准

高维文本嵌入场景，更接近 RAG 业务实际。

|系统|QPS|P50\(ms\)|P95\(ms\)|P99\(ms\)|Precision@10|
|---|---|---|---|---|---|
|Qdrant|1238|3\.54|4\.95|8\.62|99%|
|Weaviate|1142|4\.99|7\.16|11\.33|97%|
|Elasticsearch|717|22\.10|72\.53|135\.68|98%|
|Milvus|219|393\.31|441\.32|576\.65|99%|
|**Aura 单 DN \(待填\)**|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|

## 大规模数据业界基准

|系统|数据集|规模|延迟\(ms\)|QPS|内存|Recall@10|
|---|---|---|---|---|---|---|
|sqlite\-vec \(int8\)|SIFT1B|10亿|8\.3|120\.5|14\.2GB|99\.2%|
|Milvus 2\.2 standalone|SIFT10M|1000万|P99=127|7522|—|—|
|阿里云 PolarDB \(IVF\-PQ\)|DINOv2|100亿|—|\~170K/s写入|—|—|
|Ray\+Lance\+OSS \(检索仅\)|—|—|30\.2\~85\.5|—|—|—|
|**Aura 单 DN \(待填\)**|SIFT10M|1000万|\_\_\_\_|\_\_\_\_|\_\_\_\_GB|\_\_\_\_%|

## 竞争力评估维度

|评估维度|业界优秀水平|Aura 达标判断|
|---|---|---|
|单查询延迟 \(SIFT1M, Recall@10≥95%\)|P50 \< 5ms, P99 \< 10ms|Aura P50 \_\_\_\_ms, P99 \_\_\_\_ms → \_\_\_\_（达标/不达标）|
|单 DN QPS \(SIFT1M, 16核\)|\> 500 QPS（全功能数据库），\>2000（纯检索库）|Aura \_\_\_\_ QPS → \_\_\_\_|
|召回率 \(ef\_search=100\)|Recall@10 ≥ 95%|Aura \_\_\_\_% → \_\_\_\_|
|**单 DN 数据承载量**|1000万向量/16C64G（主流水平）|Aura 最优 \_\_\_\_万/\_\_\_\_C\_\_\_\_G → \_\_\_\_|
|**分布式扩展效率**|8 DN 时 ≥ 80%|Aura \_\_\_\_% → \_\_\_\_|
|Segment 冷加载时间|1000万向量 \< 30s（含索引构建）|Aura \_\_\_\_s → \_\_\_\_|
|内存效率|索引大小 ≤ 原始向量大小 × 1\.5|Aura 原始\_\_\_\_GB, 索引\_\_\_\_GB, 比率\_\_\_\_ → \_\_\_\_|

---

# 测试结果记录表格

## 表一：测试环境信息

|项目|配置/值|
|---|---|
|测试日期|\_\_\_\_年\_\_月\_\_日|
|Aura 引擎版本|\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_|
|Ray 版本|\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_|
|Lance 版本|\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_|
|CPU 型号 / 核数|\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ / \_\_\_\_ 核|
|内存大小|\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ GB|
|磁盘类型|NVMe SSD / SATA SSD / HDD|
|网络带宽|\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ Gbps|
|OBS 区域 / Bucket|\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ / \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_|
|测试人员|\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_|

## 表二：单 DN 资源配置对比（SIFT10M，K=10，ef\_search=100）

|CPU核数|内存\(GB\)|P50\(ms\)|P99\(ms\)|QPS|Recall@10|CPU峰值\(%\)|内存峰值\(GB\)|
|---|---|---|---|---|---|---|---|
|4|32|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_|\_\_\_\_|
|8|32|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_|\_\_\_\_|
|16|64|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_|\_\_\_\_|
|32|128|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_|\_\_\_\_|

## 表三：单 DN Segment 大小对比（16C64G，K=10，ef\_search=100）

|Segment大小|加载时间\(s\)|P50\(ms\)|P99\(ms\)|QPS|Recall@10|内存峰值\(GB\)|
|---|---|---|---|---|---|---|
|100 万|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_|
|500 万|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_|
|1000 万|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_|
|2000 万|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_|
|5000 万|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_|

## 表四：单 DN 并发 QPS 测试（最优配置，K=10，ef\_search=100）

|并发数|总查询数|QPS|P50\(ms\)|P95\(ms\)|P99\(ms\)|错误数|
|---|---|---|---|---|---|---|
|1|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|2|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|4|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|8|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|16|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|32|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|64|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_|

## 表五：HNSW 参数调优（最优配置，K=10）

|ef\_search|Recall@10|P50\(ms\)|P99\(ms\)|QPS|
|---|---|---|---|---|
|10|\_\_\_\_%|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|20|\_\_\_\_%|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|50|\_\_\_\_%|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|100|\_\_\_\_%|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|200|\_\_\_\_%|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|500|\_\_\_\_%|\_\_\_\_|\_\_\_\_|\_\_\_\_|
|1000|\_\_\_\_%|\_\_\_\_|\_\_\_\_|\_\_\_\_|

## 表六：不同 Top K 性能（最优配置，ef\_search=100）

|Top K|P50\(ms\)|P99\(ms\)|QPS|Recall@K|备注|
|---|---|---|---|---|---|
|1|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%||
|10|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|业务常用|
|50|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%||
|100|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|RAG 重排序场景|

## 表七：分布式扩展性测试（每 DN 最优配置）

|DN数|数据总量|QPS|P50\(ms\)|P99\(ms\)|Recall@10|扩展效率|
|---|---|---|---|---|---|---|
|1|10M|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|100%（基线）|
|2|20M|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_%|
|4|40M|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_%|
|8|80M|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_%|
|16|160M|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_%|
|32|320M|\_\_\_\_|\_\_\_\_|\_\_\_\_|\_\_\_\_%|\_\_\_\_%|

## 表八：业界对比汇总（SIFT1M，Recall@10≈95%）

|系统|QPS|P50\(ms\)|P99\(ms\)|Recall@10|vs Aura|
|---|---|---|---|---|---|
|FAISS \(HNSW\)|\~866|\<2|\~3|\~95%|\_\_\_\_|
|Qdrant|\~216|4\.55|\~8\.2|\~95%|\_\_\_\_|
|Weaviate|\~5639|2\.80|4\.43|97\.24%|\_\_\_\_|
|pgvector|\~154|\~6|\~12|\~95%|\_\_\_\_|
|**Aura 单 DN**|**\_\_\_\_**|**\_\_\_\_**|**\_\_\_\_**|**\_\_\_\_%**|基线|

---

# 常见问题与注意事项

## 测试前检查清单

* [ ] Ray 集群已启动，所有节点状态正常（ray status）

* [ ] OBS 访问凭证已配置，Bucket 可读写

* [ ] Lance 数据已上传到 OBS，Segment 数量和大小符合规划

* [ ] 查询向量和 Ground Truth 已准备好

* [ ] 测试机与 OBS 在同一可用区，网络延迟低

* [ ] 关闭其他占用 CPU/内存的进程

* [ ] Aura CN 和 DN 服务已部署，版本号已记录

* [ ] 先用 100 万小数据验证 SQL 语法和查询正确性

## 常见问题

### Q1：DN 从 OBS 加载 Segment 非常慢怎么办？

1. 确认测试机与 OBS 在同一可用区/区域，跨区域访问延迟会高很多。

2. 检查网络带宽是否打满，可用 iftop 或 nload 监控。

3. 增大 Fragment 大小减少文件数量，降低 OBS 请求次数。

4. 启用 OBS 加速器或 CDN 缓存（如阿里云 OSS 加速器可将检索延迟从 85ms 降到 30ms）。

5. 考虑本地磁盘缓存：DN 首次加载后将 Segment 缓存到本地 NVMe，后续查询直接读本地。

### Q2：单 DN QPS 上不去，CPU 利用率低怎么办？

1. 增加查询并发数，直到 CPU 利用率达到 70\~80%。

2. 检查 DN 内部是否有串行瓶颈（如全局锁），如果有需要联系开发优化。

3. 确认 Ray Actor 的 num\_cpus 设置是否正确，是否被限制了核数。

4. 检查是否是 OBS I/O 瓶颈（加载阶段）还是计算瓶颈（查询阶段）。

5. 尝试增大 DN 的 CPU 核数，看 QPS 是否线性增长。

### Q3：分布式扩展效率低（远低于 80%）怎么办？

1. 检查 CN 是否成为瓶颈：CN 的 CPU 利用率是否打满？CN 汇总结果的耗时占比多少？

2. 检查网络通信：DN 返回的局部结果数据量是否过大？可以减小每个 DN 返回的局部 K 值。

3. 检查 Ray 调度开销：Actor 之间的消息传递延迟是否过高？

4. 确认数据是否均匀分布到各 DN，是否有数据倾斜导致某些 DN 过载。

5. 逐步增加 DN 数量（1→2→4→8），找到扩展效率开始下降的拐点。

### Q4：召回率偏低怎么办？

1. 单 DN 召回率低：增大 ef\_search（HNSW）或 nprobe（IVF），确认距离度量类型与数据集一致。

2. 分布式召回率低但单 DN 正常：检查 CN 汇总逻辑。每个 DN 返回的局部 Top\-K 数量是否足够？建议局部 K = 全局 K × 3（安全系数）。

3. 确认 Ground Truth 的 ID 映射正确：Lance 数据中的行 ID 是否与 HDF5 中的索引一致。

4. 检查是否有 Segment 遗漏：CN 的查询计划是否覆盖了所有 Segment。

### Q5：DN 内存不足（OOM）怎么办？

1. 减小 Segment 大小，让每个 DN 承载更少数据。

2. 使用量化压缩（如 PQ、SQ）减小索引内存占用。

3. 增大 DN 的内存分配（Ray Actor memory 参数）。

4. 确认是否有内存泄漏：长时间运行后内存是否持续增长？如果是，记录为 bug。

5. 使用磁盘索引模式（如 DiskANN），将部分索引放在磁盘上。

## 关键注意事项

**1\. 单 DN 测试是基础，不要跳过。**百亿规模的性能 = 单 DN 性能 × DN 数量 × 扩展效率。单 DN 没有调优好就上分布式，只会放大问题，浪费资源。

**2\. 控制变量，一次只改一个参数。**测试资源配置时固定 Segment 大小，测试 Segment 大小时固定资源配置。否则无法判断性能变化的原因。

**3\. 所有性能数据必须标注召回率。**可以通过降低 ef\_search 获得任意低的延迟，因此延迟/QPS 数据必须伴随对应的 Recall@K 才有意义。

**4\. 区分冷启动和热启动。**DN 刚启动时 Segment 不在内存中，第一次查询需要从 OBS 加载，延迟会很高。正式性能测试必须在 Warmup 后进行，冷启动数据单独记录。

**5\. 记录完整配置确保可复现。**测试报告必须包含：Aura/Ray/Lance 版本、CPU/内存型号、OBS 区域、Segment 大小、Fragment 数量、索引参数（M, ef\_construction, ef\_search）、查询并发数。缺少任何一项都可能导致结果不可复现。

**6\. 业界对比要公平。**与其他系统对比时，尽量使用相同的数据集、相同的硬件配置、相同的召回率水平。如果硬件不同，应在对比表中注明硬件差异，避免不公平比较。

---

# 附录：参考资源

- [ANN Benchmarks — 业界最权威的向量检索基准网站](https://ann-benchmarks.com/)

- [ann\-benchmarks GitHub — 可复现的基准测试代码](https://github.com/erikbern/ann-benchmarks)

- [Lance 官方网站 — 列式数据格式文档](https://lance.org/)

- [Lance Performance Guide — Fragment/Row Group 大小调优](https://github.com/lance-format/lance/blob/main/docs/src/guide/performance.md)

- [Ray 官方文档 — 分布式计算框架](https://docs.ray.io/)

- [VectorDBBench Leaderboard — 向量数据库性能排行榜](https://zilliz.com.cn/vdbbench-leaderboard)

- [向量数据库系统综合评测论文（FAISS/Qdrant/Weaviate/pgvector 对比）](https://arxiv.org/pdf/2608.12812)

- [TEXMEX — SIFT1B/Deep1B 大规模数据集下载](http://corpus-texmex.irisa.fr/)

- [Big ANN Benchmarks — NeurIPS 十亿级向量检索竞赛](https://big-ann-benchmarks.com/)

本文档中的 Python 脚本为通用模板，其中 DN Actor 的初始化和检索方法需要替换为实际 Aura 引擎的 API。SQL 语句语法也请根据 Aura 引擎的实际支持情况进行调整。如有疑问，请联系 Aura 引擎开发团队确认 API 细节。

> （注：部分内容可能由 AI 生成）
