# Changelog

所有重要变更记录（格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)）。

## [0.1.5] - 2026-08-28

### Added

- **QQ 专用上下文压缩 preset**（解决腾讯云带宽告警根因）：QQ 会话改挂插件内置的 `qq` agent preset（`presets/qq/agent.cordis.yml`，standard 的副本），compaction 阈值从 0.8 → 0.3、保留 0.16 → 0.1（deepseek-v4-flash 上下文窗口 1M token 时，每次 LLM 调用上传体积从 ~160K-800K 收敛到 ~100K-300K，约 2.5x 下降）。**只影响 QQ 会话**：服务器上原生启动的 dsh/dst 及 dsh-web/dsh-tui 仍用框架自带 standard preset，行为不变。
- **preset 自动安装**：配置 `presetAutoInstall`（默认 true），插件启动时把仓库内 `presets/qq/` 同步到 `$DSH_HOME/.agent-presets/qq/`（内容哈希比对，幂等；安装后 0444 防 loader 回写），会话仍走 `agentPresets.mount()` 官方通道（保留 standing mount / 子代理 composeFrom / mount 审计）。
- `agentPreset` 默认值改为 `qq`（v0.1.5 起开箱即用，无需手工部署 preset 文件）。

### Changed

- 冒烟测试新增 preset 自安装用例（安装/幂等/只读/源变更重装），共 122 项。

## [0.1.4] - 2026-08-28

### Added

- **QQ Markdown 消息支持**（借鉴 Hermes `markdown_support`）：配置 `markdown`（默认 true）开启后，C2C/群聊发送 `msg_type: 2` + `markdown.content`，QQ 客户端原生渲染表格/标题/加粗等。
- **markdown 权限缺失自动回退**：收到 40034127（无 markdown 模板权限）时自动降级为纯文本（`msg_type: 0`）重发，不影响可用性。
- 键盘消息（审批按钮）强制纯文本（QQ 不允许 markdown+键盘混用）；频道消息保持 `msg_type: 1`。
- 冒烟测试新增 6 个用例（markdown 消息体 / 纯文本 / 键盘强制文本 / 40034127 回退），共 114 项全通过。

## [0.1.3] - 2026-08-27

### Added

- **关闭码分类处理**（借鉴 Hermes gateway/platforms/qqbot/adapter.py）：4004 → 刷新 token 后重连；4006/4007/4009 → 清 session 全新 identify；4008 → 退避 60s；**4914/4915（机器人下线/封禁）→ 停止重连**（避免无限重连与 PM2 重启循环）。
- **快速断连检测**：连接建立后 <5s 内断开连续 ≥3 次 → 判定配置/权限/凭据问题，停止重连（Hermes MAX_QUICK_DISCONNECT_COUNT）。
- **重连次数上限**（100 次）与**连续失败后强制重取 gateway**（网关地址可能轮换）。
- **token 刷新去重锁**：并发请求共享同一 in-flight 刷新 promise（Hermes asyncio.Lock + 双检）；`invalidateToken()` 支持 4004 后强制刷新。
- **发送通用重试**：网络错误/5xx → 指数退避重试（1s→2s→4s，最多 3 次）；永久错误（11255 等）不重试。
- **全局发送节流**：配置项 `interMessageDelayMs`（默认 300ms），出站分块间主动间隔（Hermes 主动频控思路）。
- **入站消息去重**：配置项 `dedupWindowMs`（默认 300s），同一消息 id 窗口内重复推送只处理一次（Hermes DEDUP_WINDOW_SECONDS）。
- **空闲会话 TTL 清理**：配置项 `sessionTtlMs`（默认 0 = 禁用），每 10 分钟清理空闲会话/作者/频控记录的内存项（磁盘历史保留，可懒恢复）。
- **结构化事件日志**：`log.event(name, fields)` 输出一行 JSON（ws_ready / ws_resumed / ws_fatal / watchdog_exit / bot_started / bot_stopped），便于 grep 排障。

### Changed

- token 刷新定时器改原生 `setInterval`（脱离 ctx.timer，与传输层一致，免疫 timer 服务异常）。
- `new WebSocket` 同步异常兜底（不再让重连定时器回调抛未捕获异常）。

### Fixed（2026-08-27 代码审计）

- **高危：重连定时器句柄误当函数调用**——`reconnectTimer?.()` 对原生 `setTimeout` 句柄做函数调用会抛 `TypeError`（实测），重连定时器未清空时二次调度会崩进程；改为 `clearTimeout()`（新增 `clearReconnectTimer()`，5 处）。
- **审批超时 / 出站 flush 定时器脱离 ctx.timer**：`approval.js` 审批超时与 `outbound.js` flush/max 定时器改用原生 `setTimeout`（原依赖 `ctx.timer`，timer 服务异常时审批永久挂起、回复永不 flush——2026-08-27 事故同类隐患）。
- **ACL 默认改 fail-closed**：`allowFrom` / `groupAllowFrom` 默认 `['*']` → `[]`（默认不配即全拒；本机配置显式指定白名单，不受影响）。
- **pruneIdle 修正**：空闲会话清理只释放 live agent 句柄、**保留映射**（原实现删除映射会导致用户回来时新建与磁盘旧会话同 id 的会话，sessionId 冲突、历史错乱）；并补 `outbound.pruneIdle` 清理 turnHasContent 等残留。
- **永久错误码集合去重**（`40034127 + 0` 冗余项）。

## [0.1.2] - 2026-08-27

### Added

- 断连看门狗（配置项 `disconnectWatchdogMs`，默认 300000ms = 5 分钟，0 = 禁用）：WS 掉线后超过阈值仍未恢复连接时，主动 `process.exit(1)`，由 pm2 自动重启拉起。针对 2026-08-27 事故（WS 掉线后进程存活但 0 连接、0 日志、重连静默失效）设计。

### Changed

- 心跳 / 断线重连 / 看门狗定时器全部改用原生 `setInterval` / `setTimeout`（`stop()` 统一清理），不再依赖 cordis `ctx.timer`，避免 timer 服务异常导致重连与心跳同时静默失效。

## [0.1.1] - 2026-08-16

### Fixed

- 清理验证文档中的 AppID / openid 示例凭据，避免幻影凭据入库。
- 版本号升至 0.1.1。

## [0.1.0] - 2026-08-16

### Added

- QQ 官方机器人（q.qq.com，公域）接入：私聊 / 群聊 @ / 频道 @ 与完整 agent 对话。
- WS 长连接传输（心跳 / 断线重连 / RESUME / op9 重 identify）。
- 会话隔离：每个用户/群独立 session（`qq:` 前缀），`/new` 开启新会话。
- 完整 agent 能力：工具 / 记忆 / 子代理 / 文件系统，经 `agentPresets.mount` 挂 standard preset。
- 审批桥：QQ 内联按钮（✅ 允许一次 / ⭐ 始终允许 / ❌ 拒绝）+ `/revoke` 撤销，点击者身份校验。
- 斜杠命令：`/help /ping /me /new /approve /always /revoke`。
- 白名单 fail-closed（私聊 / 群组分列）+ 频率限制（30 条/60s 滑动窗口）。
- 出站卫生：回复合并窗口、超长分段、剥离 dsh 内部标签、错误兜底。
- 凭据域集成：凭据写入 dsh 凭据域（不落 patch 明文），监听 `credentials/updated` 自动启动。
- 冒烟测试 `scripts/smoke-test.mjs`（mock dsh 运行时，无真实平台依赖）。