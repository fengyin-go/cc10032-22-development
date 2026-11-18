/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean | null | undefined
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
  entry?: Record<string, unknown> | null
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

// ---- 指标血缘版本 ----

export type LineageVersion = {
  version_id: string
  metric: string
  version: number
  path_version: string
  path_hash: string
  source_batch: string
  checkpoint_id: string
  checkpoint_snapshot_ts: number | null
  sources: string[]
  operators: string[]
  outputs: string[]
  operator_chain: string
  topology: string
  status: string
  confirmed: boolean
  current: boolean
  operator_missing: string[]
  path_changes: PathChange[]
  verification_status?: string
  verified_by_replay?: string | null
  created_at: number
}

export type PathChange = {
  field: string
  operator: string
  change: string
}

export type ReplayReport = {
  report_id: string
  task_id: number
  task_name: string
  metric: string
  lineage_version_id: string
  path_version: string
  source_batch: string
  checkpoint_id: string
  checkpoint_snapshot_ts: number | null
  operator_missing: string[]
  path_changes: PathChange[]
  checkpoint_missing_operators: string[]
  result: string
  status: string
  superseded: boolean
  current_path_version: string | null
  created_at: number
  replay_verification?: {
    all_passed: boolean
    expected: number
    found: number
    missing_operators: string[]
  }
}

export type ReplayTaskSummary = {
  task_id: number
  任务名称: string
  源检查点: string
  checkpoint_snapshot_ts: number | null
  血缘版本ID: string
  路径版本: string
  结果验证: string
  验证报告ID: string
  status: string
  operator_missing?: string[]
  path_changes?: PathChange[]
}

export type LineageVersionDetail = {
  version: LineageVersion
  lineage: EntryRow | null
  checkpoint: Record<string, unknown> | null
  drift: {
    operator_missing: string[]
    path_changes: PathChange[]
    path_changed: boolean
    live_operator_chain?: string
  }
  constant: boolean
  reports: ReplayReport[]
  replay_history: ReplayTaskSummary[]
  all_versions: {
    version_id: string
    path_version: string
    source_batch: string
    checkpoint_id: string
    status: string
    confirmed: boolean
    current: boolean
    operator_chain: string
    created_at: number
  }[]
}

export type ReplayValidateResult = {
  ok: boolean
  metric: string | null
  current_version: LineageVersion | null
  versions: LineageVersion[]
  checkpoints: {
    checkpoint_id: string
    config: string
    snapshot_ts: number | null
    operators: string[]
    external_path: string
  }[]
  replay_tasks: ReplayTaskSummary[]
  reports: ReplayReport[]
  lineage_snapshot: Record<string, number>
  replay_snapshot: Record<string, number>
}

export type ReplayVerificationResult = {
  metric: string | null
  versions: LineageVersion[]
  replay_tasks: ReplayTaskSummary[]
  reports: ReplayReport[]
}
