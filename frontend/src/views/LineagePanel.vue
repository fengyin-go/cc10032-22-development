<template>
  <section class="lineage-panel" data-testid="lineage-panel">
    <header class="panel-head">
      <div>
        <h3>当前路径版本</h3>
        <p class="page-desc">
          每条血缘记录绑定来源批次与路径版本；回放任务只能引用已确认血缘，并固定创建时点的版本。
        </p>
      </div>
      <div class="panel-actions">
        <label class="inline-field">
          <span>指标</span>
          <select v-model="metric" data-testid="lineage-metric" @change="reloadAll">
            <option v-for="name in metricOptions" :key="name" :value="name">{{ name }}</option>          </select>
        </label>
        <label class="inline-field">
          <span>检查点</span>
          <select v-model="checkpointId" data-testid="lineage-checkpoint">
            <option value="">不使用检查点</option>
            <option v-for="cp in checkpoints" :key="cp.checkpoint_id" :value="cp.checkpoint_id">
              {{ cp.checkpoint_id }}（{{ cp.config || '检查点' }}）
            </option>
          </select>
        </label>
        <button class="btn primary" data-testid="lineage-refresh" type="button" :disabled="loading" @click="refresh">
          刷新血缘
        </button>
      </div>
    </header>

    <p v-if="message" class="panel-message" :class="{ error: messageIsError }" data-testid="lineage-message">
      {{ message }}
    </p>

    <div class="lineage-grid">
      <div class="lineage-col">
        <h4>路径版本台账</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>路径版本</th>
              <th>来源批次</th>
              <th>检查点</th>
              <th>状态</th>
              <th>确认</th>
              <th>当前</th>
              <th>回放验证</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="item in versions"
              :key="item.version_id"
              :data-testid="`lineage-version-${item.version_id}`"
              :class="{ 'row-active': item.version_id === selectedVersionId }"
              @click="selectVersion(item.version_id)"
            >
              <td>{{ item.path_version }}</td>
              <td>{{ item.source_batch }}</td>
              <td>{{ item.checkpoint_id || '—' }}</td>
              <td>{{ item.status }}</td>
              <td>{{ item.confirmed ? '已确认' : '未确认' }}</td>
              <td>{{ item.current ? '当前' : '历史' }}</td>
              <td>{{ item.verification_status || '待验证' }}</td>
            </tr>
            <tr v-if="!versions.length">
              <td colspan="7" class="empty-state">该指标还没有路径版本，先刷新血缘生成。</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="lineage-col">
        <h4>血缘路径</h4>
        <p v-if="lineageStatus && lineageStatus !== '正常'" class="panel-message error" data-testid="lineage-status-warning">
          当前血缘状态：{{ lineageStatus }}（算子链已调整，请刷新并确认新的路径版本后再回放）
        </p>
        <div v-if="detail" class="dag-wrap" data-testid="lineage-dag">
          <svg class="dag-svg" :viewBox="`0 0 ${dagWidth} ${dagHeight}`" role="img">
            <defs>
              <marker id="lineage-arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
                <path d="M0,0 L6,3 L0,6 Z" fill="#94a3b8" />
              </marker>
            </defs>
            <g v-for="edge in dagEdges" :key="edge.key">
              <line
                class="dag-edge"
                :x1="edge.x1"
                :y1="edge.y1"
                :x2="edge.x2"
                :y2="edge.y2"
                marker-end="url(#lineage-arrow)"
              />
            </g>
            <g v-for="node in dagNodes" :key="node.key" class="dag-node">
              <rect
                :x="node.x"
                :y="node.y"
                width="150"
                height="44"
                rx="8"
                :fill="node.fill"
                stroke="#cbd5e1"
              />
              <text :x="node.x + 10" :y="node.y + 19">{{ node.title }}</text>
              <text :x="node.x + 10" :y="node.y + 34" class="node-sub">{{ node.subtitle }}</text>
            </g>
          </svg>
        </div>
        <dl v-if="detail" class="dag-detail">
          <div>
            <dt>路径版本</dt>
            <dd>{{ detail.version.path_version }} · {{ detail.version.path_hash }}</dd>
          </div>
          <div>
            <dt>来源批次</dt>
            <dd>{{ detail.version.source_batch }}</dd>
          </div>
          <div>
            <dt>检查点</dt>
            <dd>{{ detail.version.checkpoint_id || '未绑定' }}</dd>
          </div>
          <div>
            <dt>路径核对</dt>
            <dd>{{ detail.constant ? '与实时路径一致' : '实时路径已变化' }}</dd>
          </div>
          <div>
            <dt>算子缺失</dt>
            <dd>{{ detail.drift.operator_missing.length ? detail.drift.operator_missing.join('、') : '无' }}</dd>
          </div>
          <div>
            <dt>操作</dt>
            <dd class="row-actions">
              <button
                class="link"
                data-testid="lineage-confirm"
                type="button"
                :disabled="loading || !detail.version || detail.version.confirmed"
                @click="confirmVersion"
              >
                确认版本
              </button>
              <button
                class="link"
                data-testid="lineage-create-replay"
                type="button"
                :disabled="loading || !detail.version.confirmed"
                @click="createReplay"
              >
                以此为版本创建回放
              </button>
            </dd>
          </div>
        </dl>
      </div>
    </div>

    <div v-if="driftChanges.length" class="drift-block" data-testid="lineage-drift">
      <h4>路径变化</h4>
      <p v-for="(change, index) in driftChanges" :key="index" class="drift-item">
        {{ change.field }}：{{ change.operator }}（{{ change.change }}）
      </p>
    </div>

    <div class="lineage-grid">
      <div class="lineage-col">
        <h4>回放历史</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>任务</th>
              <th>检查点</th>
              <th>检查点时点</th>
              <th>路径版本</th>
              <th>结果验证</th>
              <th>验证报告</th>
              <th>状态</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="task in replayTasks" :key="task.task_id" :data-testid="`replay-task-${task.task_id}`">
              <td>{{ task.任务名称 }}</td>
              <td>{{ task.源检查点 }}</td>
              <td>{{ formatTs(task.checkpoint_snapshot_ts) }}</td>
              <td>{{ task.路径版本 }}</td>
              <td>{{ task.结果验证 }}</td>
              <td>
                <button v-if="task.验证报告ID" class="link" type="button" @click="selectReport(task.验证报告ID)">
                  {{ task.验证报告ID }}
                </button>
                <span v-else>—</span>
              </td>
              <td>{{ task.status }}</td>
            </tr>
            <tr v-if="!replayTasks.length">
              <td colspan="7" class="empty-state">该指标还没有回放任务。</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="lineage-col">
        <h4>回放验证报告</h4>
        <div v-if="selectedReport" class="report-card" data-testid="replay-report">
          <p><strong>{{ selectedReport.report_id }}</strong> · {{ selectedReport.status }}</p>
          <p class="page-desc">
            指标 {{ selectedReport.metric }} · 路径版本 {{ selectedReport.path_version }} ·
            来源批次 {{ selectedReport.source_batch }}
          </p>
          <p>使用检查点：{{ selectedReport.checkpoint_id }}（{{ formatTs(selectedReport.checkpoint_snapshot_ts) }}）</p>
          <p>
            算子缺失：
            {{ selectedReport.operator_missing.length ? selectedReport.operator_missing.join('、') : '无' }}
          </p>
          <p>
            路径变化：
            <span v-if="!selectedReport.path_changes.length">无</span>
            <span v-else>
              {{ selectedReport.path_changes.map((item) => `${item.field}:${item.operator}`).join('；') }}
            </span>
          </p>
          <p v-if="selectedReport.superseded" class="error-text">
            回放结束前出现了新的当前路径版本 {{ selectedReport.current_path_version }}，
            本次结果只对锁定版本有效。
          </p>
        </div>
        <p v-else class="page-desc">点击回放历史里的报告编号查看算子缺失、路径变化和检查点。</p>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  confirmLineageVersion as confirmVersionRequest,
  createReplayTask,
  fetchLineageVersionDetail,
  fetchLineageVersions,
  fetchReplayHistory,
  fetchReplayValidation,
  fetchReplayVerification,
  refreshLineage,
} from '@/api/backend-service'
import type {
  LineageVersion,
  LineageVersionDetail,
  ReplayReport,
  ReplayTaskSummary,
} from '@/data/types'

const props = defineProps<{ metrics: string[] }>()

const metric = ref(props.metrics[0] ?? '')
const metricOptions = computed(() =>
  props.metrics.includes(metric.value) || !metric.value
    ? props.metrics
    : [metric.value, ...props.metrics],
)
const versions = ref<LineageVersion[]>([])
const selectedVersionId = ref('')
const detail = ref<LineageVersionDetail | null>(null)
const checkpoints = ref<{ checkpoint_id: string; config: string }[]>([])
const checkpointId = ref('')
const replayTasks = ref<ReplayTaskSummary[]>([])
const reports = ref<ReplayReport[]>([])
const selectedReport = ref<ReplayReport | null>(null)
const loading = ref(false)
const message = ref('')
const messageIsError = ref(false)

watch(
  () => props.metrics,
  async (names) => {
    if (!metric.value && names.length) {
      metric.value = names[0]
      await reloadAll()
    }
  },
)

function say(text: string, isError = false) {
  message.value = text
  messageIsError.value = isError
}

async function reloadAll() {
  if (!metric.value) return
  loading.value = true
  try {
    const [payload, validation] = await Promise.all([
      fetchLineageVersions(metric.value),
      fetchReplayValidation(metric.value),
    ])
    versions.value = payload.items as unknown as LineageVersion[]
    checkpoints.value = validation.checkpoints
    if (!checkpointId.value && validation.checkpoints.length) {
      checkpointId.value = validation.checkpoints[0].checkpoint_id
    }
    const current = versions.value.find((item) => item.current)
    const target = selectedVersionId.value || current?.version_id || versions.value[0]?.version_id
    if (target) {
      await selectVersion(target)
    } else {
      detail.value = null
    }
    await loadReplayHistory()
  } catch (error) {
    say(error instanceof Error ? error.message : '血缘数据读取失败', true)
  } finally {
    loading.value = false
  }
}

async function selectVersion(versionId: string) {
  selectedVersionId.value = versionId
  try {
    detail.value = await fetchLineageVersionDetail(versionId)
    selectedReport.value = detail.value.reports.at(-1) ?? null
  } catch (error) {
    say(error instanceof Error ? error.message : '血缘版本详情读取失败', true)
  }
}

async function loadReplayHistory() {
  const payload = await fetchReplayHistory(metric.value)
  replayTasks.value = payload.items as unknown as ReplayTaskSummary[]
  const verification = await fetchReplayVerification(metric.value)
  reports.value = verification.reports
}

async function refresh() {
  if (!metric.value) return
  loading.value = true
  try {
    const result = await refreshLineage({
      metric_name: metric.value,
      checkpoint_id: checkpointId.value || undefined,
    })
    say(result.message, !result.ok)
    selectedVersionId.value = ''
    await reloadAll()
  } catch (error) {
    say(error instanceof Error ? error.message : '血缘刷新失败', true)
  } finally {
    loading.value = false
  }
}

async function confirmVersion() {
  if (!detail.value) return
  loading.value = true
  try {
    const result = await confirmVersionRequest(detail.value.version.version_id)
    say(result.message, !result.ok)
    await reloadAll()
  } catch (error) {
    say(error instanceof Error ? error.message : '版本确认失败', true)
  } finally {
    loading.value = false
  }
}

async function createReplay() {
  if (!detail.value || !checkpointId.value) {
    say('请先选择一个检查点', true)
    return
  }
  loading.value = true
  try {
    const result = await createReplayTask({
      checkpoint_id: checkpointId.value,
      metric_name: metric.value,
      lineage_version_id: detail.value.version.version_id,
      execute: true,
    })
    say(result.message, !result.ok)
    await reloadAll()
  } catch (error) {
    say(error instanceof Error ? error.message : '回放任务创建失败', true)
  } finally {
    loading.value = false
  }
}

function selectReport(reportId: string) {
  const found = detail.value?.reports.find((item) => item.report_id === reportId)
  if (found) {
    selectedReport.value = found
    return
  }
  selectedReport.value = reports.value.find((item) => item.report_id === reportId) ?? null
  if (!selectedReport.value) {
    say(`未找到报告 ${reportId}，请刷新后重试`, true)
  }
}

const driftChanges = computed(() => detail.value?.drift.path_changes ?? [])
const lineageStatus = computed(() => {
  const current = versions.value.find((item) => item.current)
  return current?.status ?? ''
})

const dagWidth = 940
const dagHeight = 250

const dagNodes = computed(() => {
  if (!detail.value) return []
  const version = detail.value.version
  const nodes: { key: string; title: string; subtitle: string; x: number; y: number; fill: string }[] = []
  const columns = [
    { items: version.sources, label: '源端', fill: '#e0f2fe' },
    { items: version.operators, label: '算子', fill: '#ecfdf5' },
    { items: version.outputs, label: '输出', fill: '#fef3c7' },
  ]
  columns.forEach((column, columnIndex) => {
    column.items.forEach((item, itemIndex) => {
      nodes.push({
        key: `${columnIndex}-${itemIndex}-${item}`,
        title: item,
        subtitle: column.label,
        x: 20 + columnIndex * 310,
        y: 40 + itemIndex * 64,
        fill: column.fill,
      })
    })
  })
  return nodes
})

const dagEdges = computed(() => {
  const edges: { key: string; x1: number; y1: number; x2: number; y2: number }[] = []
  const grouped = [
    dagNodes.value.filter((node) => node.subtitle === '源端'),
    dagNodes.value.filter((node) => node.subtitle === '算子'),
    dagNodes.value.filter((node) => node.subtitle === '输出'),
  ]
  for (let column = 0; column < grouped.length - 1; column += 1) {
    const left = grouped[column]
    const right = grouped[column + 1]
    if (!left.length || !right.length) continue
    left.forEach((source, index) => {
      const target = right[Math.min(index, right.length - 1)]
      edges.push({
        key: `${column}-${index}`,
        x1: source.x + 150,
        y1: source.y + 22,
        x2: target.x,
        y2: target.y + 22,
      })
    })
  }
  return edges
})

function formatTs(value: number | null | undefined) {
  if (!value) return '—'
  const date = new Date(value * 1000)
  return date.toLocaleString('zh-CN', { hour12: false })
}

async function focusMetric(name: string, section: 'versions' | 'replay' = 'versions') {
  if (!name) return
  if (metric.value !== name) {
    metric.value = name
    selectedVersionId.value = ''
  }
  await reloadAll()
  say(section === 'replay' ? `已载入「${name}」的回放核对结果` : `已载入「${name}」的路径版本`)
}

defineExpose({ focusMetric, reloadAll })

onMounted(reloadAll)
</script>
