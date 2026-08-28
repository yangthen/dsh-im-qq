/**
 * QQ 专用 agent preset 的自安装（v0.1.5）。
 *
 * dsh-im-qq 把 QQ 会话的上下文压缩策略以 agent preset 的形式随仓库分发
 * （presets/qq/），并在插件启动时自动同步到 harness 的用户预设目录
 * （$DSH_HOME/.agent-presets/qq/，即 agent-presets 的 USER_PRESET_DIR）。
 * 会话随后仍走 agentPresets.mount() 官方通道挂载 —— 保留 standing mount、
 * 子代理 composeFrom、mount 审计等机制，不绕开框架。
 *
 * 同步策略：
 *   - 内容哈希比对，文件缺失或内容变化才覆盖（幂等，重复启动零写入）；
 *   - 安装后 chmod 0444：任何 loader 回写路径都写不进去（standing mount
 *     本身已是 no-op write，这是双保险）；
 *   - 失败 fail-soft：只记录日志，不阻断插件启动（与本插件整体策略一致）。
 */

import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))

/** 仓库内随包分发的 preset 目录（presets/qq/）。 */
export const BUNDLED_PRESET_DIR = join(__dirname, '..', 'presets', 'qq')

/** 要安装的文件：agent.cordis.yml 必需，preset.yml 是展示元数据（可选）。 */
const PRESET_FILES = ['agent.cordis.yml', 'preset.yml']

/** dsh 用户根目录（与 dsh 启动器一致：$DSH_HOME 优先，兜底 ~/.dsh）。 */
export function dshHomeDir() {
  return process.env.DSH_HOME || join(homedir(), '.dsh')
}

/** 展开路径中的 ~（agent-presets discovery 同样会 expandHomePath）。 */
function expandHome(p) {
  return typeof p === 'string' && p.startsWith('~/') ? join(homedir(), p.slice(2)) : p
}

/**
 * 将内置 qq preset 同步到用户预设根目录。
 *
 * @param {object}  ctx    插件上下文（读取 ctx.agentPresets.roots 取权威用户根）
 * @param {object}  logger 插件 logger（log.info / log.error）
 * @returns {string} 安装目标目录（供日志/诊断）
 */
export function installBundledPreset(ctx, logger) {
  // 权威用户根：agent-presets 服务已解析的 roots 中 trust=user 的那一个；
  // 拿不到（服务未就绪/被 mock）时回退 $DSH_HOME/.agent-presets。
  let userRoot = null
  try {
    const roots = ctx?.agentPresets?.roots
    userRoot = (Array.isArray(roots) ? roots : []).find((r) => r?.trust === 'user')?.path
  } catch {
    /* 服务异常走回退 */
  }
  const base = userRoot ? expandHome(userRoot) : join(dshHomeDir(), '.agent-presets')
  const targetDir = resolve(base, 'qq')

  const installed = []
  for (const name of PRESET_FILES) {
    const src = join(BUNDLED_PRESET_DIR, name)
    if (!existsSync(src)) continue // 该文件未随包分发则跳过（如精简安装）
    const dst = join(targetDir, name)
    const srcBuf = readFileSync(src)
    if (existsSync(dst) && readFileSync(dst).equals(srcBuf)) continue // 幂等
    mkdirSync(targetDir, { recursive: true })
    writeFileSync(dst, srcBuf)
    try {
      chmodSync(dst, 0o444) // 只读：防 loader 回写（POSIX）
    } catch {
      /* 非 POSIX 平台忽略 */
    }
    installed.push(name)
  }

  if (installed.length > 0) {
    logger?.info(`已同步内置 qq preset 到 ${targetDir}（${installed.join(', ')}）`)
  }
  return targetDir
}
