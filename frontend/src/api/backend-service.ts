import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  EntryRow,
  LineageVersionDetail,
  OverviewResult,
  PageResult,
  ReplayValidateResult,
  ReplayVerificationResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 指标血缘版本 & 回放验证 ----

async function readJson<T>(path: string, init?: RequestInit, message = '数据读取失败'): Promise<T> {
  const response = await request(path, init)
  if (!response.ok) {
    let detail = message
    try {
      const payload = (await response.json()) as { detail?: string }
      if (payload?.detail) detail = payload.detail
    } catch {
      // 保持默认提示
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export async function fetchLineageVersions(
  metric: string,
  currentOnly = false,
): Promise<{ items: Record<string, unknown>[]; total: number }> {
  const query = new URLSearchParams({ metric })
  if (currentOnly) query.set('current_only', 'true')
  return await readJson(`/api/metric-lineage/versions?${query}`, undefined, '血缘版本读取失败')
}

export async function fetchLineageVersionDetail(versionId: string): Promise<LineageVersionDetail> {
  return await readJson(
    `/api/metric-lineage/versions/${encodeURIComponent(versionId)}`,
    undefined,
    '血缘版本详情读取失败',
  )
}

export async function refreshLineage(payload: {
  metric_name: string
  source_batch?: string
  checkpoint_id?: string
  topology_name?: string
}): Promise<ActionResult> {
  return await readJson(
    '/api/metric-lineage/refresh',
    { method: 'POST', body: JSON.stringify(payload) },
    '血缘刷新失败',
  )
}

export async function confirmLineageVersion(versionId: string): Promise<ActionResult> {
  return await readJson(
    '/api/metric-lineage/confirm',
    { method: 'POST', body: JSON.stringify({ version_id: versionId }) },
    '血缘版本确认失败',
  )
}

export async function fetchReplayValidation(
  metric?: string,
  checkpointId?: string,
): Promise<ReplayValidateResult> {
  const query = new URLSearchParams()
  if (metric) query.set('metric', metric)
  if (checkpointId) query.set('checkpoint_id', checkpointId)
  return await readJson(
    `/api/replay-engine/validate?${query}`,
    undefined,
    '回放引擎数据核对失败',
  )
}

export async function createReplayTask(payload: {
  job_name?: string
  checkpoint_id: string
  topology?: string
  mode?: string
  metric_name?: string
  lineage_version_id?: string
  execute?: boolean
}): Promise<ActionResult> {
  return await readJson(
    '/api/replay-engine/validate',
    { method: 'POST', body: JSON.stringify(payload) },
    '回放任务创建失败',
  )
}

export async function executeReplayTask(taskId: number): Promise<ActionResult> {
  return await readJson(
    `/api/replay-engine/${taskId}/execute`,
    { method: 'POST' },
    '回放任务执行失败',
  )
}

export async function fetchReplayHistory(
  metric?: string,
): Promise<{ items: Record<string, unknown>[]; total: number }> {
  const query = metric ? `?metric=${encodeURIComponent(metric)}` : ''
  return await readJson(
    `/api/replay-engine/history${query}`,
    undefined,
    '回放历史读取失败',
  )
}

export async function fetchReplayVerification(
  metric?: string,
): Promise<ReplayVerificationResult> {
  const query = metric ? `?metric=${encodeURIComponent(metric)}` : ''
  return await readJson(
    `/api/replay-engine/verification${query}`,
    undefined,
    '回放验证结果读取失败',
  )
}
