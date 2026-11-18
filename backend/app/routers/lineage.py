"""跨模块业务接口：指标血缘版本、回放验证报告与检查点驱动的回放任务。

统一资源路由（app/routers/resource.py）负责各模块的通用列表/登记/动作；
这里只放需要跨模块协作的入口，路径都挂在固定的二级前缀下，避免与
`/api/{module}/{entry_id}` 的动态段冲突（entry_id 只接受整数）。

接口一览：
  GET  /api/metric-lineage/versions              血缘版本台账（可按指标/当前版本过滤）
  GET  /api/metric-lineage/versions/{version_id} 单个路径版本详情 + 回放验证报告
  POST /api/metric-lineage/refresh               检查点驱动的血缘刷新（幂等）
  POST /api/metric-lineage/confirm               复核确认血缘版本
  GET  /api/replay-engine/validate               回放引擎数据流核对
  POST /api/replay-engine/validate               依据已确认血缘 + 检查点创建回放任务
  POST /api/replay-engine/{task_id}/execute      执行回放并生成验证报告
  GET  /api/replay-engine/history                回放历史（含验证报告摘要）
  GET  /api/replay-engine/verification           按指标汇总版本与验证结果
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.schemas import ActionResult
from app.services.metric_lineage import MetricLineageService
from app.services.replay_engine import ReplayEngineService

router = APIRouter(prefix="/api", tags=["跨模块业务"])

lineage_service = MetricLineageService()
replay_service = ReplayEngineService()


class LineageRefreshPayload(BaseModel):
    metric_name: str
    source_batch: str | None = None
    checkpoint_id: str | None = None
    topology_name: str | None = None


class LineageConfirmPayload(BaseModel):
    version_id: str


class ReplayValidatePayload(BaseModel):
    job_name: str | None = None
    checkpoint_id: str
    topology: str = ""
    mode: str = "full_replay"
    metric_name: str | None = None
    lineage_version_id: str | None = None
    execute: bool = False


def _result(payload: dict[str, Any], *, message: str) -> ActionResult:
    """把跨模块服务的明细结果包进统一动作回执，明细放 entry。"""
    return ActionResult(
        ok=bool(payload.get("ok")),
        message=str(payload.get("error") or message),
        entry=payload if payload.get("ok") else None,
    )


# ---- 指标血缘版本 ----

@router.get("/metric-lineage/versions", response_model=dict)
def list_lineage_versions(
    metric: str | None = Query(default=None),
    current_only: bool = Query(default=False),
) -> dict[str, Any]:
    versions = lineage_service.list_versions(metric, current_only=current_only)
    return {
        "items": versions,
        "total": len(versions),
        "metric": metric,
        "current_only": current_only,
    }


@router.get("/metric-lineage/versions/{version_id}", response_model=dict)
def get_lineage_version(version_id: str) -> dict[str, Any]:
    detail = lineage_service.version_detail(version_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"血缘版本 {version_id} 不存在")
    return detail


@router.post("/metric-lineage/refresh", response_model=ActionResult)
def refresh_lineage(payload: LineageRefreshPayload) -> ActionResult:
    result = lineage_service.refresh_lineage(
        payload.metric_name,
        source_batch=payload.source_batch,
        checkpoint_id=payload.checkpoint_id,
        topology_name=payload.topology_name,
    )
    if not result.get("ok"):
        return _result(result, message="血缘刷新失败")
    message = (
        f"血缘复用当前版本 {result['path_version']}（批次 {result['source_batch']}）"
        if result.get("idempotent")
        else f"血缘已刷新至 {result['path_version']}（批次 {result['source_batch']}）"
    )
    return _result(result, message=message)


@router.post("/metric-lineage/confirm", response_model=ActionResult)
def confirm_lineage_version(payload: LineageConfirmPayload) -> ActionResult:
    result = lineage_service.confirm_version(payload.version_id)
    return _result(result, message="血缘版本已确认")


# ---- 回放引擎 ----

@router.get("/replay-engine/validate", response_model=dict)
def validate_replay_inputs(
    metric: str | None = Query(default=None),
    checkpoint_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """核对回放引擎的数据来源：当前血缘版本、可用检查点、可复用的验证报告。"""
    versions = lineage_service.list_versions(metric)
    current = lineage_service.current_version(metric) if metric else None
    checkpoints = []
    for cp in replay_service.list_checkpoints():
        if checkpoint_id and str(cp.get("checkpoint_id")) != str(checkpoint_id):
            continue
        checkpoints.append(cp)
    tasks = [
        task for task in replay_service.list_replay_history()
        if metric is None or task.get("指标名称") == metric
    ]
    return {
        "ok": True,
        "metric": metric,
        "current_version": current,
        "versions": versions,
        "checkpoints": checkpoints,
        "replay_tasks": tasks,
        "reports": replay_service.lineage_verification_history(metric)["reports"],
        "lineage_snapshot": lineage_service.domain_snapshot(),
        "replay_snapshot": replay_service.domain_snapshot(),
    }


@router.post("/replay-engine/validate", response_model=ActionResult)
def create_replay_from_lineage(payload: ReplayValidatePayload) -> ActionResult:
    job_name = payload.job_name or f"回放_{payload.checkpoint_id}_{payload.metric_name or 'lineage'}"
    result = replay_service.create_replay_job(
        job_name=job_name,
        checkpoint_id=payload.checkpoint_id,
        topology_name=payload.topology,
        mode=payload.mode,
        metric_name=payload.metric_name,
        lineage_version_id=payload.lineage_version_id,
    )
    if not result.get("ok"):
        return _result(result, message="回放任务创建失败")

    if payload.execute:
        executed = replay_service.execute_replay(result["task_id"])
        result["execution"] = executed
        result["ok"] = bool(executed.get("ok"))
        if not executed.get("ok"):
            result["error"] = executed.get("error")

    return _result(
        result,
        message=(
            f"回放任务 {result['task_id']} 已创建并固定血缘版本 "
            f"{result.get('lineage_version', '')}"
        ),
    )


@router.post("/replay-engine/{task_id}/execute", response_model=ActionResult)
def execute_replay(task_id: int) -> ActionResult:
    result = replay_service.execute_replay(task_id)
    return _result(result, message=f"回放任务 {task_id} 已执行")


@router.get("/replay-engine/history", response_model=dict)
def replay_history(metric: str | None = Query(default=None)) -> dict[str, Any]:
    history = replay_service.list_replay_history()
    if metric:
        history = [item for item in history if item.get("指标名称") == metric]
    return {"items": history, "total": len(history), "metric": metric}


@router.get("/replay-engine/verification", response_model=dict)
def replay_verification(metric: str | None = Query(default=None)) -> dict[str, Any]:
    return replay_service.lineage_verification_history(metric)
