# 判读册核验 · 测试说明（gb-0041）

所有脚本从仓库根执行，使用同一个 Python 环境（`backend/requirements.txt` + `httpx`）。每个脚本自带 `DATABASE_URL` 指向 `/tmp` 下的独立 SQLite 文件，不触碰 Compose 的 MySQL 卷；`test_packets_http.py` 用内存假队列替代 Redis。

```bash
python -m pip install -r backend/requirements.txt httpx
cd test-artifacts/gb-0041
python test_algorithm_oracle.py
python test_packets_integration.py
python test_packets_http.py
python test_packets_tamper.py
python test_packets_concurrency.py
python test_packets_full_training.py
python test_packets_hash_order.py
python test_packets_scale.py          # 20k/50k/100k，约 60s
```

| 脚本 | 覆盖 | 通过判据 |
|---|---|---|
| `test_algorithm_oracle.py` | 独立流式复算 vs 生产 `algorithm.py`（sklearn） | 80 组随机类别分布/折数/平滑系数下，归折、预测、后验全部一致（后验差 < 1e-9） |
| `test_packets_integration.py` | 乱序分片、重试去重、同位置异值隔离、首处分歧、断点恢复、10 万条 | 六种情形逐一断言；10 万条核对通过且峰值内存平稳 |
| `test_packets_http.py` | 真实 FastAPI 路由与权限 | 访客写入 403、负责人入库、重复入库幂等、审计含四类事件 |
| `test_packets_tamper.py` | 封包后改动已入库样本 | 结构阶段被 `rejected`，错误指向内容哈希不符 |
| `test_packets_concurrency.py` | 三个 worker 线程抢同一本 | 仅一个 `verified`，其余 `busy`，结果完整不交错 |
| `test_packets_full_training.py` | 每折训练集 | 2 万条下每折训练件数 = 总数 − 该折验证件数 |
| `test_packets_hash_order.py` | 封包内容哈希 | 顺序到货与乱序到货+重试的两册，内容哈希完全一致 |
| `test_packets_scale.py` | 固定内存 | 20k→100k 峰值内存不随件数增长 |

## 关键验收口径

- **样本次序**：位置 1..N 连续；封包时校验连续并冻结 SHA256。
- **研究参数**：建册时经 `validate_parameters` 归一化并冻结，后续分片不能改动。
- **乱序分片可补**：任何顺序收件，缺位在详情接口列出（`missing_preview`）。
- **重试不重复收件**：`shard_key` 自然键 + 分片内容哈希；相同重试返回 `duplicate`，不同内容 `409`。
- **同位置异值隔离**：不覆盖，写 `judgement_conflicts`；未处理不可封包。
- **独立复算**：`services/verification.py` 不导入 sklearn，复现 `StratifiedKFold(shuffle=True, random_state=42)`；每折以包内全部训练样本拟合。
- **首处分歧**：归折→预测→后验依次比对，首个不符即停，记录位置/样本/来件值/复算值。
- **固定内存**：四阶段全流式，检查点落库；峰值与件数无关。
- **断点恢复**：阶段与每 1000 件检查点；令牌+租约保证并发不混版。
- **整册入库**：逐件全通过后由负责人入库；同一 `book_key` 仅一册，重复送达返回已存在。
