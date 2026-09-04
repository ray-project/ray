# Go/Python/Java/C++ Runtime 对比测试（设计 Spec）

> 状态：已确认（用户批准：功能对等 + 基础性能；同一集群各语言 driver 分别提交；报告 + 脚本都入 docs/）
> 目标仓库：`/home/10353800/Desktop/learning/code/05-开源项目/ray`（branch `feat/go-runtime`）
> 基址：`36bd5d7e0d`（当前 HEAD）

## 1. 目标

验证迁移后的 Go Runtime 与 Ray 的 Python / Java / C++ runtime 在**核心功能上对等**，并给出**基础性能**对比数据（单任务延迟、吞吐、冷启动）。产出可复现的 driver 脚本 + 对比报告，全部提交到开源 repo 的 `docs/go-runtime-comparison/`。

## 2. 范围

### 2.1 功能对等（同一批负载，4 语言都跑通且结果正确）

| 负载 | 描述 | 对等断言 |
|---|---|---|
| Echo 任务 | `Remote(add)(i, j)`，N 个并发任务 | 全部结果 == i+j |
| 对象往返 | `put` 大对象（>100KB，触发 by-ref）→ `get` | 数据一致 |
| Actor | 有状态 actor，多次方法调用累加 | 计数器正确 |

### 2.2 基础性能指标

- **单任务延迟**：一次 `Remote(...).Call` → `Get` 的端到端耗时（预热后取中位数/均值）。
- **吞吐**：连续提交 N 个任务的总耗时 → tasks/sec。
- **冷启动**：首个任务耗时（含 worker 启动、函数注册、序列化框架初始化）。

### 2.3 明确不做

- 不做跨语言序列化互操作（Go↔Python 传对象等）——那是另一份 spec。
- 不做内存/CPU 画像分析，只报端到端基础指标。
- 不迁移 M2 组件（event/autoscaler/logmonitor）。

## 3. 环境与执行

- **集群**：一个 fresh Ray 集群（本 fork），`pip install -e` 源码安装。
- **提交方式**：Python / Java / C++ / Go 各写一个 driver，向同一集群提交相同负载。Go driver 走迁移后的 runtime（raygo + userfuncs 插件）。
- **重复**：每个负载跑 3 轮取稳定值，避免偶发抖动。
- **记录**：每轮记录时间戳、结果正确性、失败信息到 `results/` 下的 JSON/CSV。

## 4. 交付物（入 commit）

`docs/go-runtime-comparison/`：
- `README.md` — 对比方法、环境、结论表。
- `drivers/{python,java,cpp,go}/` — 各语言 driver 源码 + 构建说明。
- `run_all.sh` — 依次跑 4 语言 driver 并汇总结果。
- `results/` — 样例输出（跑一次的实际结果）。

## 5. 验证

- 每个 driver 单跑通过（功能对等断言全绿）。
- `run_all.sh` 在同一集群上完整跑通，产出 4 语言对齐的结果表。
- 报告明确注明环境（机器、Ray 版本、语言版本）与 caveat（不同语言序列化框架/worker 模型不同，指标仅作量级参考）。

## 6. 风险

- Java/C++ driver 依赖各自构建产物与运行环境，若 fork 未提供则降级为"Python + Go"核心对比并记录。
- 性能指标受机器负载影响，用多次取中位数缓解，报告标注。
