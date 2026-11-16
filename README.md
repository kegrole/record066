# shardfetch

纯 Python 标准库实现的分片并发抓取器：把 URL 列表按分片分给多个工作线程，
用独立于工人数的全局信号量限制在途请求，支持按 URL 重试、优雅停止，
并输出与输入序号严格对齐的 JSON 报告。不依赖任何第三方库。

## 目录结构

- `shardfetch/sharding.py`：确定性分片，保留每个 URL 的全局输入序号。
- `shardfetch/worker.py`：工作线程、`BoundedSemaphore` 全局并发限制、在途峰值统计。
- `shardfetch/retry.py`：单 URL 的 HTTP 抓取与有界重试（`urllib`）。
- `shardfetch/stop.py`：线程安全的停止事件，可由代码或 `SIGINT`/`SIGTERM` 触发。
- `shardfetch/merge.py`：按输入序号校验并排序合并结果。
- `shardfetch/report.py`：聚合 `total/succeeded/failed/skipped/peak_concurrency/retries`。
- `shardfetch/cli.py`、`shardfetch/__main__.py`：命令行入口与本地 mock 子进程管理。
- `demo/mock_site.py`：固定端口 8099 的确定性 mock 服务，可直接启动也可 import。
- `demo/urls.txt`：60 条本地 URL，其中 3 条永久 500、2 条永久超时。
- `tests/test_shardfetch.py`：覆盖全部验收口径的 `unittest`。

## 分片策略

`make_shards(items, workers)` 把输入切成 `workers` 个连续分片，每条元素携带
`(全局输入序号, URL)`。分片数量恒等于工人数（尾部可能为空分片），因此后续
合并完全不依赖线程完成顺序：每条记录都带着自己的输入序号，合并时校验序号必须
是连续的 `0..N-1` 且与输入一一对应，再按序号排序。

## 全局并发上限如何保证

工人数（分片数）与并发上限是两个独立参数，`--concurrency` 默认等于 `--workers`。
所有工作线程共享一个 `ConcurrencyLimiter`（内部为 `threading.BoundedSemaphore`），
每次实际 HTTP 请求前后成对 `acquire/release`，并用锁维护在途计数与观测峰值。
因此无论开多少个工作线程，服务端同一时刻看到的在途请求都不会超过该上限；
mock 服务端也独立统计自己的在途峰值，测试同时断言两侧峰值。

## 重试语义

`--retries N` 表示每条 URL 最多重试 N 次，即总尝试次数 `N+1`。任何传输错误、
超时或 HTTP 错误状态都会触发下一次尝试；超过上限后记为 `failed`，该 URL 的
`retries` 恰好为 N，报告里的 `retries` 是所有 URL 重试次数之和。单条 URL 失败
只影响它自己，成功与失败分开计数。超时由服务端挂起端点配合很小的客户端
`--timeout`（如 0.2 秒）触发，不做真实的长时间等待。

## 优雅停止语义

停止信号有两个来源：进程收到 `SIGINT` 或 `SIGTERM`，以及外部直接调用
`StopEvent.stop()` / `ShardFetcher.stop()`。信号置位后：

- 工作线程在开始下一条 URL 前检查停止事件，已置位则不再开始任何新任务；
- 已经在执行的 URL（含其重试）跑完并正常落盘；
- 尚未开始的 URL 记为 `skipped`，且服务端对这些 URL 的请求计数为 0；
- 报告恒满足 `succeeded + failed + skipped == total`。

## 报告字段

`--out` 指向的 JSON 包含：

- `total`：输入 URL 总数。
- `succeeded`：成功 URL 数。
- `failed`：重试用尽后仍失败的 URL 数。
- `skipped`：停止信号后从未开始的 URL 数。
- `peak_concurrency`：客户端侧观测到的在途请求峰值（受并发上限约束）。
- `retries`：所有 URL 累计重试次数（不含首次尝试）。
- `results`：按输入序号 `0..total-1` 排列的逐条结果。

## 本地演示

CLI 会在抓取前自动以子进程方式拉起与 `--urls` 文件同目录下的 `mock_site.py`，
抓取结束（或停止）后关闭它，全程不访问外网：

```bash
python -m shardfetch.cli --urls demo/urls.txt --workers 4 --out out/report.json
```

可选参数 `--concurrency` 单独设置全局并发上限（与 `--workers` 独立），
`--retries`、`--timeout`、`--no-mock` 同样可用。stdout 会打印一行摘要：

```
total=60 succeeded=55 failed=5 skipped=0 peak_concurrency=4 retries=15
```

也可以单独启动 mock 服务：

```bash
python demo/mock_site.py
```

## 运行测试

```bash
python -m unittest test_shardfetch -v
# 或
python -m unittest tests.test_shardfetch -v
```
