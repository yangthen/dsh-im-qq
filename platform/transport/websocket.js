/**
 * QQ WS 网关（当前默认 transport，设计文档 §5 / §7）。
 *
 * 协议要点（均已核实）：
 *   - op10(hello) 收 heartbeat_interval → op2 identify {token:'QQBot xxx', intents, shard:[0,1]}
 *   - 心跳 op1 携带 last_sequence；op11(HEARTBEAT_ACK) 确认
 *   - op7(RECONNECT)：可 RESUME（带 session_id + seq 恢复）
 *   - op9(INVALID_SESSION)：session 失效，必须全新 identify（不带 session_id）
 *   - 服务器约每 30min 主动关 WS（close code 4009 "Session timed out"）= 正常行为，自动恢复
 *   - intents：PUBLIC_GUILD_MESSAGES(1<<30) + USER_MESSAGE(1<<25) + INTERACTION_CREATE(1<<26)
 *   - 断线重连：指数退避（1s → 2s → 4s → … → 上限 30s）
 *
 * 运行环境：Node 22（全局 WebSocket，undici 实现）。心跳/重连/看门狗均用原生定时器
 * （stop() 统一清理；脱离 ctx.timer 以免疫 timer 服务挂掉导致的重连静默失效，2026-08-27 事故）。
 *
 * v0.1.3（借鉴 Hermes gateway/platforms/qqbot/adapter.py）：
 *   - 关闭码分类：4004→刷新 token 重连；4006/4007/4009→清 session 全新 identify；
 *     4008→退避 60s；4914/4915→机器人下线/封禁，停止重连（避免无限重连/重启循环）；
 *   - 快速断连检测：连接后 <5s 内断开连续 ≥3 次 → 判定配置/权限问题，停止重连；
 *   - 重连次数上限：超过后停止重连并明确报错（看门狗为瞬时故障兜底）；
 *   - 连续失败后强制重取 gateway（QQ 网关地址可能轮换）；
 *   - new WebSocket 同步异常兜底（不再让定时器回调抛未捕获异常）。
 */

const OP = {
  DISPATCH: 0,
  HEARTBEAT: 1,
  IDENTIFY: 2,
  PRESENCE: 3,
  RESUME: 6,
  RECONNECT: 7,
  INVALID_SESSION: 9,
  HELLO: 10,
  HEARTBEAT_ACK: 11,
}

/** 频道/群/单聊/按钮回调所需全部事件。 */
const INTENTS = (1 << 30) | (1 << 25) | (1 << 26)

const RECONNECT_BASE_MS = 1000
const RECONNECT_MAX_MS = 30_000
/** 心跳超时判定：超过 2 个心跳周期未收到 ack 视为断线。 */
const HEARTBEAT_ACK_TIMEOUT_FACTOR = 2
/** 断连看门狗：原生 setInterval 检查间隔（不依赖 ctx.timer）。 */
const WATCHDOG_CHECK_MS = 30_000
/** 断连看门狗：WS 断连超过该时长仍未能恢复 → 主动退出进程，由 pm2 重启拉起（默认 5 分钟）。 */
const WATCHDOG_DISCONNECT_MS = 300_000

// ── v0.1.3 借鉴 Hermes 的常量 ──
/** 快速断连判定：连接建立后不足该时长即断开，记为一次"快速断连"。 */
const QUICK_DISCONNECT_THRESHOLD_MS = 5_000
/** 连续快速断连上限：超过即判定为配置/权限问题，停止重连（Hermes MAX_QUICK_DISCONNECT_COUNT=3）。 */
const MAX_QUICK_DISCONNECT_COUNT = 3
/** 重连次数上限：连续失败超过该次数后停止重连（Hermes MAX_RECONNECT_ATTEMPTS=100）。 */
const MAX_RECONNECT_ATTEMPTS = 100
/** 连续失败多少次后强制重取 gateway（网关地址可能轮换）。 */
const GATEWAY_REFRESH_EVERY_ATTEMPTS = 5
/** 4008 频控退避时长。 */
const CLOSE_4008_BACKOFF_MS = 60_000

/** QQ 网关关闭码含义（Hermes adapter.py close-code 分类）。 */
const CLOSE_CODE = {
  TOKEN_INVALID: 4004, // token 失效 → 刷新后重连
  SESSION_INVALID: [4006, 4007, 4009], // session 失效 → 清 session 全新 identify
  RATE_LIMITED: 4008, // 频控 → 退避 60s
  BOT_OFFLINE: 4914, // 机器人下线/沙箱 → 停止重连
  BOT_BANNED: 4915, // 机器人被封禁 → 停止重连
}

export class QQWebSocketTransport {
  /**
   * @param {object} opts
   * @param {object} opts.qqapi    QQApi 实例（token / gateway）
   * @param {object} opts.logger   makeLogger 产物
   * @param {object} opts.timer    ctx.timer（保留字段，定时器已改用原生实现）
   * @param {(dispatch: object) => void} opts.onEvent  每个 op0 dispatch 事件（含 t/d/s）
   * @param {number} [opts.watchdogMs]  断连看门狗阈值 ms（默认 300000；0 = 禁用）
   */
  constructor({ qqapi, logger, timer, onEvent, watchdogMs }) {
    this.qqapi = qqapi
    this.log = logger
    this.timer = timer
    this.onEvent = onEvent

    this.ws = null
    this.sequence = null // 最近收到的服务端 seq（dispatch 的 s）
    this.sessionId = null // READY 后赋值，RESUME 用
    this.resumable = false // op9 后置 false（必须重 identify）
    this.heartbeatIntervalMs = 0
    this.heartbeatTimer = null
    this.lastAckAt = 0
    this.awaitingAck = false
    this.reconnectAttempt = 0
    this.reconnectTimer = null
    // 断连看门狗（2026-08-27 事故：WS 掉线后重连/心跳静默失效，进程活着但 0 连接、0 日志）。
    // 刻意用原生 setInterval，脱离 ctx.timer —— 即使 cordis timer 服务挂掉，看门狗仍能触发，
    // 超时未恢复连接就主动退出，让 pm2 重启整个进程自愈。
    this.watchdogMs = watchdogMs ?? WATCHDOG_DISCONNECT_MS
    this.lastConnectedAt = 0
    this.watchdogTimer = null
    this.stopped = false

    // ── v0.1.3：快速断连 / 重连上限 / 致命状态 ──
    this.fatal = false // 致命错误（封禁/下线/配置问题）→ 停止重连（也不触发看门狗退出，避免 PM2 重启循环）
    this.lastConnectAt = 0 // 最近一次连接建立时间（快速断连判定）
    this.connectedOnce = false // 是否曾成功建立过连接
    this.quickDisconnectCount = 0
  }

  /** 清理重连定时器：原生 setTimeout 句柄必须 clearTimeout（不能当作函数调用，否则抛 TypeError）。 */
  clearReconnectTimer() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  /** 启动：取 token + gateway → 建立连接 + 启动 token 刷新。失败 30s 后自动重试。 */
  async start() {
    if (this.stopped || this.fatal) return
    this.qqapi.startTokenRefresh()
    this.lastConnectedAt = Date.now() // 看门狗从启动起计时（含从未连上的情况）
    this.startWatchdog()
    try {
      await this.qqapi.getAccessToken()
      const url = await this.qqapi.getGateway()
      this.connect(url)
    } catch (err) {
      this.log.error('transport 启动失败，30s 后重试:', err?.message)
      this.clearReconnectTimer()
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null
        this.start()
      }, RECONNECT_MAX_MS)
      this.reconnectTimer.unref?.()
    }
  }

  connect(url) {
    if (this.stopped || this.fatal) return
    // 未传 url 且本地无缓存 → 先取 gateway 再连（gateway 可能已失效/轮换）
    if (!url && !this.qqapi.gatewayUrl) {
      this.qqapi
        .getGateway()
        .then((freshUrl) => this.connect(freshUrl))
        .catch((err) => {
          this.log.error('获取 gateway 失败，稍后重连:', err?.message)
          this.scheduleReconnect()
        })
      return
    }
    const target = url || this.qqapi.gatewayUrl
    this.log.info(`连接 WS 网关… (attempt ${this.reconnectAttempt + 1})`)
    let ws
    try {
      ws = new WebSocket(target)
    } catch (err) {
      // ⚠️ 同步异常兜底：new WebSocket 抛错时走重连而非让定时器回调抛未捕获异常（v0.1.3）
      this.log.error('创建 WebSocket 失败:', err?.message)
      this.scheduleReconnect()
      return
    }
    this.ws = ws
    this.lastConnectAt = Date.now()

    ws.onopen = () => {
      this.log.info('WS 已连接，等待 hello(op10)…')
      this.lastConnectedAt = Date.now()
      this.connectedOnce = true
    }

    ws.onmessage = (ev) => {
      let msg
      try {
        msg = JSON.parse(ev.data)
      } catch {
        this.log.warn('无法解析 WS 帧:', String(ev.data).slice(0, 200))
        return
      }
      this.handleMessage(msg)
    }

    ws.onclose = (ev) => {
      this.log.warn(`WS 关闭 code=${ev.code} reason=${ev.reason || ''}`)
      if (this.ws === ws) this.ws = null
      this.stopHeartbeat()
      this.handleClose(ev.code, ev.reason)
    }

    ws.onerror = (err) => {
      this.log.error('WS 错误:', err?.message || err)
    }
  }

  /**
   * 关闭码分类处理（v0.1.3，借鉴 Hermes adapter.py 的 close-code taxonomy）：
   *   - 4004       → token 失效，强制刷新后重连
   *   - 4006/4007/4009 → session 失效/周期断连，清 session 后重连（RESUME→全新 identify）
   *   - 4008       → 频控，退避 60s
   *   - 4914/4915  → 机器人下线/封禁，停止重连（避免无限重连 + PM2 重启循环）
   *   - 其他       → 快速断连检测（<5s 连续 3 次 = 配置/权限问题，停止）+ 指数退避重连
   */
  handleClose(code, reason) {
    if (this.stopped || this.fatal) return

    if (code === CLOSE_CODE.TOKEN_INVALID) {
      this.log.info('code=4004（token 失效），刷新 token 后重连')
      this.qqapi.invalidateToken?.()
      this.scheduleReconnect()
      return
    }
    if (CLOSE_CODE.SESSION_INVALID.includes(code)) {
      if (code === 4009) this.log.info('code=4009（Session timed out）：服务器周期断连，自动恢复')
      else this.log.warn(`code=${code}（session 失效），清 session 后全新 identify`)
      this.resumable = false
      this.sessionId = null
      this.sequence = null
      this.scheduleReconnect()
      return
    }
    if (code === CLOSE_CODE.RATE_LIMITED) {
      this.log.warn('code=4008（频控），60s 后重连')
      this.scheduleReconnect(CLOSE_4008_BACKOFF_MS)
      return
    }
    if (code === CLOSE_CODE.BOT_OFFLINE || code === CLOSE_CODE.BOT_BANNED) {
      const label = code === CLOSE_CODE.BOT_BANNED ? '机器人被封禁' : '机器人下线/沙箱'
      this.setFatal(`${label}（code=${code}${reason ? `, reason=${reason}` : ''}）`)
      return
    }

    // 其他关闭码：快速断连检测（疑似配置/权限/凭据错误，避免无意义重连）
    if (this.connectedOnce && this.lastConnectAt > 0) {
      const upFor = Date.now() - this.lastConnectAt
      if (upFor < QUICK_DISCONNECT_THRESHOLD_MS) {
        this.quickDisconnectCount += 1
        this.log.warn(`快速断连 ${this.quickDisconnectCount}/${MAX_QUICK_DISCONNECT_COUNT}（连接仅存活 ${upFor}ms）`)
        if (this.quickDisconnectCount >= MAX_QUICK_DISCONNECT_COUNT) {
          this.setFatal(`连续 ${MAX_QUICK_DISCONNECT_COUNT} 次快速断连（疑似配置/权限/凭据问题），停止重连`)
          return
        }
      } else {
        this.quickDisconnectCount = 0
      }
    }
    this.scheduleReconnect()
  }

  /** 致命错误：停止重连 + 停看门狗（避免 PM2 无限重启循环）。需人工修复（凭据/配置/权限）后重启生效。 */
  setFatal(message) {
    this.fatal = true
    this.log.error(`[fatal] ${message} —— 停止重连；请检查配置/凭据/机器人状态后重启`)
    this.log.event('ws_fatal', { reason: message })
    this.stopHeartbeat()
    this.clearReconnectTimer()
    this.reconnectTimer = null
    if (this.watchdogTimer) {
      clearInterval(this.watchdogTimer)
      this.watchdogTimer = null
    }
  }

  handleMessage(msg) {
    switch (msg.op) {
      case OP.HELLO: {
        this.heartbeatIntervalMs = msg.d?.heartbeat_interval ?? 41_000
        this.log.debug('hello: heartbeat_interval =', this.heartbeatIntervalMs)
        this.startHeartbeat()
        this.identify()
        break
      }
      case OP.DISPATCH: {
        this.sequence = msg.s
        if (msg.t === 'READY') {
          this.sessionId = msg.d?.session_id ?? null
          this.resumable = true
          this.reconnectAttempt = 0 // 连接稳定，重置退避
          this.quickDisconnectCount = 0
          this.lastConnectedAt = Date.now()
          this.log.info('WS READY, session_id =', this.sessionId?.slice(0, 8) + '…')
          this.log.event('ws_ready', { sessionId: this.sessionId?.slice(0, 8) })
        } else if (msg.t === 'RESUMED') {
          this.resumable = true
          this.reconnectAttempt = 0
          this.quickDisconnectCount = 0
          this.lastConnectedAt = Date.now()
          this.log.info('WS RESUMED（会话恢复成功）')
          this.log.event('ws_resumed')
        }
        if (this.onEvent) this.onEvent(msg)
        break
      }
      case OP.HEARTBEAT_ACK: {
        this.awaitingAck = false
        this.lastAckAt = Date.now()
        break
      }
      case OP.RECONNECT: {
        // op7：服务器要求重连，可 RESUME
        this.log.warn('收到 op7(reconnect)，主动重连（将尝试 RESUME）')
        this.resumable = true
        this.ws?.close(4000, 'op7 reconnect')
        break
      }
      case OP.INVALID_SESSION: {
        // op9：session 失效，必须全新 identify（不能带 session_id）
        this.log.warn('收到 op9(invalid session)，session 失效，将全新 identify')
        this.resumable = false
        this.sessionId = null
        this.sequence = null
        this.ws?.close(4000, 'op9 invalid session')
        break
      }
      default:
        this.log.debug('未处理 op:', msg.op)
    }
  }

  /** 心跳循环：每 heartbeat_interval 发 op1；超过 2 周期未 ack 判定断线重连。原生 setInterval（不依赖 ctx.timer）。 */
  startHeartbeat() {
    this.stopHeartbeat()
    this.awaitingAck = false
    this.lastAckAt = Date.now()
    this.heartbeatTimer = setInterval(() => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return
      if (this.awaitingAck && Date.now() - this.lastAckAt > this.heartbeatIntervalMs * HEARTBEAT_ACK_TIMEOUT_FACTOR) {
        this.log.warn('心跳超时（未收到 ack），强制重连')
        this.ws.close(4000, 'heartbeat timeout')
        return
      }
      this.awaitingAck = true
      try {
        this.ws.send(JSON.stringify({ op: OP.HEARTBEAT, d: this.sequence ?? null }))
      } catch (err) {
        this.log.error('心跳发送失败', err?.message)
      }
    }, this.heartbeatIntervalMs)
    this.heartbeatTimer.unref?.()
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
  }

  /** identify / resume 二选一：resumable 且有 session_id 时走 RESUME，否则全新 IDENTIFY。 */
  identify() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return
    const token = `QQBot ${this.qqapi.token}`
    if (this.resumable && this.sessionId) {
      this.log.info('发起 RESUME(seq=' + this.sequence + ')')
      this.ws.send(JSON.stringify({
        op: OP.RESUME,
        d: { token, session_id: this.sessionId, seq: this.sequence ?? null },
      }))
    } else {
      this.log.info('发起 IDENTIFY（全新会话）')
      this.ws.send(JSON.stringify({
        op: OP.IDENTIFY,
        d: { token, intents: INTENTS, shard: [0, 1] },
      }))
    }
  }

  /**
   * 断线重连：指数退避（1s → 2s → … → 30s 上限）。原生 setTimeout（不依赖 ctx.timer）。
   * v0.1.3：支持强制退避时长（4008 频控 60s）；连续失败达到上限后停止重连；
   * 每 GATEWAY_REFRESH_EVERY_ATTEMPTS 次失败强制重取 gateway（地址可能轮换）。
   */
  scheduleReconnect(forcedDelayMs = 0) {
    if (this.stopped || this.fatal) return
    this.clearReconnectTimer()

    // 重连次数上限（Hermes MAX_RECONNECT_ATTEMPTS=100）：达到后停止，交由看门狗/人工处理
    if (this.reconnectAttempt >= MAX_RECONNECT_ATTEMPTS) {
      this.log.error(
        `重连次数已达上限（${MAX_RECONNECT_ATTEMPTS} 次），停止重连；看门狗将在 ${Math.round(this.watchdogMs / 1000)}s 后退出进程（若仍断连）`,
      )
      return
    }

    // 连续失败若干次后强制刷新 gateway（地址可能轮换）
    if (this.reconnectAttempt > 0 && this.reconnectAttempt % GATEWAY_REFRESH_EVERY_ATTEMPTS === 0) {
      this.log.warn(`连续 ${this.reconnectAttempt} 次失败，强制重取 gateway`)
      this.qqapi.clearGateway?.()
    }

    const delay =
      forcedDelayMs > 0
        ? forcedDelayMs
        : Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** Math.min(this.reconnectAttempt, 10))
    this.reconnectAttempt += 1
    this.log.info(`${(delay / 1000).toFixed(0)}s 后重连… (attempt ${this.reconnectAttempt}/${MAX_RECONNECT_ATTEMPTS})`)
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.connect()
    }, delay)
    this.reconnectTimer.unref?.()
  }

  /** 断连看门狗：原生 setInterval（不依赖 ctx.timer），看门狗自身不会因 timer 服务挂掉而失效。 */
  startWatchdog() {
    if (this.stopped || this.fatal || this.watchdogMs <= 0 || this.watchdogTimer) return
    this.log.info(`[watchdog] 断连看门狗已启用：${Math.round(this.watchdogMs / 1000)}s 内未恢复连接将主动退出（交由 pm2 重启）`)
    this.watchdogTimer = setInterval(() => {
      if (this.stopped || this.fatal) return
      const connected = this.ws && this.ws.readyState === WebSocket.OPEN
      if (connected) {
        this.lastConnectedAt = Date.now()
        return
      }
      if (this.lastConnectedAt <= 0) return
      const downFor = Date.now() - this.lastConnectedAt
      if (downFor >= this.watchdogMs) {
        this.log.error(
          `[watchdog] QQ WS 已断连 ${Math.round(downFor / 1000)}s 且未能自动重连（阈值 ${Math.round(this.watchdogMs / 1000)}s），主动退出进程`,
        )
        this.log.event('watchdog_exit', { downForSeconds: Math.round(downFor / 1000), thresholdSeconds: Math.round(this.watchdogMs / 1000) })
        // 阻止退出前再调度重连/心跳，并立即清理所有定时器
        this.stopped = true
        this.stopHeartbeat()
        this.clearReconnectTimer()
        this.reconnectTimer = null
        clearInterval(this.watchdogTimer)
        this.watchdogTimer = null
        setTimeout(() => process.exit(1), 500)?.unref?.() // 留 500ms 让日志落盘到 pm2
      }
    }, WATCHDOG_CHECK_MS)
    this.watchdogTimer.unref?.()
  }

  /** 插件卸载/停止：关闭连接、清所有定时器（含看门狗）。 */
  stop() {
    this.stopped = true
    this.stopHeartbeat()
    this.clearReconnectTimer()
    this.reconnectTimer = null
    if (this.watchdogTimer) {
      clearInterval(this.watchdogTimer)
      this.watchdogTimer = null
    }
    try {
      this.ws?.close(4000, 'dsh-im-qq stop')
    } catch {
      /* ignore */
    }
    this.ws = null
  }
}
