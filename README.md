### 使用说明（辛普森一家爬虫）

- 安装依赖（PowerShell）：
```bash
pip install -r requirements.txt
```

- 配置起始URL与参数：编辑 `crawler_config.yaml`。
  - `start_urls` 默认包含“辛普森一家”搜索结果第1-2页，可自行扩展。
  - `rate_limit` 控制限速；`headers.user_agent` 可设为固定字符串或 `auto` 随机UA。
  - `output` 决定JSON/CSV/SQLite输出路径。
  - `extract_player: true` 将在首个播放页尝试解析播放器对象 `player_aaaa`。

- 运行：
```bash
python main.py
```

- 输出：
  - JSON 行文本：`output/simpsons.jsonl`
  - CSV：`output/simpsons.csv`
  - SQLite：`output/simpsons.db`，表名 `simpsons`

- 说明：
  - 解析流程：搜索结果 → 详情页 → 播放列表（`#playlist a`）→ 播放页 `player_aaaa` 对象（若存在）。
  - 若需遍历所有播放页，可在 `main.py` 中将仅解析首个播放页调整为遍历 `item.play_pages`。
  - 若站点开启风控（验证码、WAF），请开启 `proxies` 并降低 `requests_per_second`。
