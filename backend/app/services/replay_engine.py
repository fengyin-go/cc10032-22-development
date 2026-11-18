"""回放引擎领域服务：消费检查点数据执行数据回放与恢复验证。

数据流: checkpoint_mgr（检查点管理）→ replay_engine（回放引擎）→ restore_plan（故障恢复）
核心逻辑: 检查点快照回放、状态恢复验证、位点对齐、回放进度管理。

版本化增强：
- 创建回放任务时读取指标血缘的当前版本，只允许引用「已确认」的路径版本；
- 任务创建与血缘刷新并发时，按锁内快照时点固定血缘版本（_lineage_pinned_at），
  回放结束后不会把结果写回一个更新过的路径；
- 执行回放后生成回放验证报告，记录算子缺失、路径变化和使用到的检查点；
- 页面状态最终停靠到一个验证过的当前版本：仅当该版本仍是当前版本且验证通过时，
  才把血缘版本标记为 verified。
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.metric_lineage import MetricLineageService
from app.store import store


class ReplayEngineService(BaseModuleService):
    MODULE = "replay_engine"
    ENTITY = "回放任务"
    LIST_FIELDS = ["任务名称", "源检查点", "目标拓扑", "回放模式", "起始位点", "结果验证", "进度"]
    REQUIRED_FIELDS = ["任务名称", "源检查点", "回放模式"]
    STATUS_ORDER = ["草稿", "待回放", "回放中", "已完成", "验证失败"]
    ACTION_RULES = {"启动回放": "待回放", "暂停回放": "回放中", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = ["暂停回放"]

    def __init__(self) -> None:
        self.lineage = MetricLineageService()

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    @staticmethod
    def _checkpoint(checkpoint_id: str) -> dict[str, Any] | None:
        for row in store.rows("checkpoint_mgr"):
            if str(row.get("checkpoint_id") or row.get("id")) == str(checkpoint_id):
                return row
        return None

    @staticmethod
    def list_checkpoints() -> list[dict[str, Any]]:
        """列出可被回放任务引用的检查点（已启用且带有快照）。"""
        checkpoints = []
        for row in store.rows("checkpoint_mgr"):
            if row.get("status") != "已启用":
                continue
            checkpoint_id = row.get("checkpoint_id") or row.get("id")
            if not checkpoint_id:
                continue
            states = row.get("operator_states") or row.get("state") or {}
            checkpoints.append({
                "checkpoint_id": str(checkpoint_id),
                "config": row.get("配置名称", ""),
                "snapshot_ts": row.get("snapshot_ts") or row.get("timestamp"),
                "operators": list(states) if isinstance(states, dict) else [],
                "external_path": row.get("外部化路径", ""),
            })
        # 带算子快照的检查点排在前面：没有快照的配置无法支撑回放验证。
        checkpoints.sort(
            key=lambda cp: (not cp["operators"], not cp["snapshot_ts"], cp["checkpoint_id"])
        )
        return checkpoints

    def _resolve_version(
        self,
        metric_name: str | None,
        lineage_version_id: str | None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """确定要引用的血缘版本；只接受已确认版本。"""
        if lineage_version_id:
            version = store.get_lineage_version(lineage_version_id)
            if version is None:
                return None, f"血缘版本「{lineage_version_id}」不存在"
            if metric_name and version.get("metric") != metric_name:
                return None, (
                    f"血缘版本「{lineage_version_id}」属于指标「{version.get('metric')}」，"
                    f"与指标「{metric_name}」不一致"
                )
            metric_name = metric_name or version.get("metric")
        else:
            if not metric_name:
                return None, "缺少指标名称或血缘版本"
            version = self.lineage.current_version(metric_name)
            if version is None:
                return None, f"指标「{metric_name}」尚无可回放的血缘版本，请先刷新血缘"

        drift = self.lineage.evaluate_version_drift(version)
        if drift["operator_missing"]:
            return None, (
                f"血缘{version.get('path_version')}存在算子缺失："
                f"{'、'.join(drift['operator_missing'])}"
            )
        if drift["path_changed"]:
            return None, (
                f"血缘{version.get('path_version')}之后实时路径已变化，"
                "请先刷新并确认新的路径版本"
            )
        if not version.get("confirmed"):
            return None, (
                f"血缘{version.get('path_version')}尚未确认，回放任务只能引用已确认血缘"
            )
        return version, None

    # ------------------------------------------------------------------
    # 创建回放任务
    # ------------------------------------------------------------------

    def create_replay_job(
        self,
        job_name: str,
        checkpoint_id: str,
        topology_name: str,
        mode: str = "full_replay",
        metric_name: str | None = None,
        lineage_version_id: str | None = None,
    ) -> dict[str, Any]:
        """基于检查点与已确认血缘版本创建回放任务。

        血缘刷新与回放创建同时发生时，两者共用 store 的同一把锁：
        先拿到锁的一方决定快照时点，回放任务固定创建当时的血缘版本。
        """
        cp_row = self._checkpoint(checkpoint_id)
        if cp_row is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}
        if cp_row.get("status") != "已启用":
            return {"ok": False, "error": f"检查点「{checkpoint_id}」未启用"}

        topo_found = any(
            r.get("拓扑名称") == topology_name for r in store.rows("task_topology")
        )
        if not topo_found and topology_name:
            return {"ok": False, "error": f"拓扑配置「{topology_name}」不存在"}

        with store.lock():
            # 锁内读取血缘：刷新若已发生，则固定刷新后的版本；否则固定刷新前的版本。
            version, error = self._resolve_version(metric_name, lineage_version_id)
            if error:
                return {"ok": False, "error": error}
            assert version is not None

            with store.transaction(self.MODULE, include_lineage=True, include_reports=True):
                task_id = max(
                    (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
                ) + 1
                replay_task = {
                    "id": task_id,
                    "任务名称": job_name,
                    "源检查点": str(checkpoint_id),
                    "目标拓扑": topology_name,
                    "回放模式": mode,
                    "起始位点": cp_row.get("snapshot_ts")
                    or cp_row.get("timestamp")
                    or time.time() - 3600,
                    "结果验证": "待验证",
                    "进度": "0%",
                    "指标名称": version["metric"],
                    "血缘版本ID": version["version_id"],
                    "路径版本": version.get("path_version", ""),
                    "来源批次": version.get("source_batch", ""),
                    "验证报告ID": "",
                    "status": "待回放",
                    "pending": True,
                    "abnormal": False,
                    "_checkpoint_snapshot": copy.deepcopy(cp_row),
                    "_checkpoint_snapshot_ts": cp_row.get("snapshot_ts")
                    or cp_row.get("timestamp"),
                    "_lineage_snapshot": copy.deepcopy(version),
                    "_lineage_pinned_at": time.time(),
                    "_created_at": time.time(),
                }
                store.rows(self.MODULE).append(replay_task)

            return {
                "ok": True,
                "task_id": task_id,
                "task_name": job_name,
                "checkpoint_id": checkpoint_id,
                "topology": topology_name,
                "mode": mode,
                "metric": version["metric"],
                "lineage_version_id": version["version_id"],
                "lineage_version": version.get("path_version", ""),
                "source_batch": version.get("source_batch", ""),
                "pinned_at": replay_task["_lineage_pinned_at"],
            }

    # ------------------------------------------------------------------
    # 执行回放与验证报告
    # ------------------------------------------------------------------

    def execute_replay(self, task_id: int) -> dict[str, Any]:
        """执行回放任务：检查点状态快照回放 + 血缘核对 + 结果验证。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "error": "回放任务不存在"}

        if task.get("status") not in ("待回放", "回放中"):
            return {"ok": False, "error": f"任务状态为「{task.get('status')}」, 不可回放"}

        task["status"] = "回放中"

        cp_snapshot = task.get("_checkpoint_snapshot", {})
        state_data = cp_snapshot.get(
            "state", cp_snapshot.get("operator_states", {}),
        )

        replay_results = []
        if isinstance(state_data, dict):
            for op_name, op_state in state_data.items():
                state_entry = {
                    "id": max(
                        (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
                    ) + 1,
                    "配置名称": f"replay_{op_name}_{task_id}",
                    "后端类型": "memory",
                    "状态键模式": f"replay://{op_name}",
                    "TTL时长": "3600",
                    "检查点间隔": "30",
                    "增量模式": "full",
                    "清理策略": "after_replay",
                    "status": "已生效",
                    "pending": True,
                    "abnormal": False,
                    "_replay_task_id": task_id,
                    "_replay_state": copy.deepcopy(op_state),
                    "_created_at": time.time(),
                }
                store.rows("state_store").append(state_entry)
                replay_results.append({
                    "operator": op_name,
                    "state_keys": len(op_state) if isinstance(op_state, dict) else 0,
                    "restored_to": "state_store",
                })

        verification = self._verify_replay_results(task_id, replay_results)
        with store.lock():
            report = self._build_replay_report(task, verification, replay_results)
            task["结果验证"] = verification["status"]
            task["进度"] = "100%"
            task["status"] = "已完成" if verification["all_passed"] else "验证失败"
            task["_replay_results"] = replay_results
            task["验证报告ID"] = report["report_id"]
            task["_verification"] = copy.deepcopy(verification)
            if task["status"] == "验证失败":
                task["abnormal"] = True
            self._settle_lineage_version(task, report)

        return {
            "ok": True,
            "task_id": task_id,
            "restored_operators": len(replay_results),
            "replay_results": replay_results,
            "verification": verification,
            "report": copy.deepcopy(report),
        }

    def _verify_replay_results(
        self, task_id: int, results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """验证回放结果的完整性：检查 state_store 中是否正确落入了回放状态。"""
        restored_ops = set(r["operator"] for r in results)
        found_in_store = set()
        for row in store.rows("state_store"):
            if row.get("_replay_task_id") == task_id:
                cfg_name = row.get("配置名称", "")
                parts = cfg_name.split("_")
                if len(parts) >= 3:
                    found_in_store.add(parts[1])

        missing = restored_ops - found_in_store
        all_passed = len(missing) == 0
        return {
            "status": "passed" if all_passed else "partial",
            "all_passed": all_passed,
            "expected": len(restored_ops),
            "found": len(found_in_store),
            "missing_operators": list(missing),
        }

    def _build_replay_report(
        self,
        task: dict[str, Any],
        verification: dict[str, Any],
        replay_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """生成回放验证报告：写清算子缺失、路径变化和使用的检查点。"""
        version = store.get_lineage_version(str(task.get("血缘版本ID") or ""))
        if version is None:
            version = task.get("_lineage_snapshot") or {}

        drift = (
            self.lineage.evaluate_version_drift(version)
            if version
            else {"operator_missing": [], "path_changes": [], "path_changed": False}
        )

        # 检查点内容与血缘算子链是否对得上：检查点缺失的算子同样记为算子缺失。
        checkpoint = self._checkpoint(str(task.get("源检查点") or "")) or {}
        checkpoint_ops = set(
            str(op) for op in (
                checkpoint.get("operator_states") or checkpoint.get("state") or {}
            )
        )
        lineage_ops = {str(op) for op in (version.get("operators") or [])}
        checkpoint_missing = sorted(lineage_ops - checkpoint_ops) if checkpoint_ops else []

        operator_missing = sorted(
            set(drift.get("operator_missing") or []) | set(checkpoint_missing)
        )
        path_changes = list(drift.get("path_changes") or [])
        if drift.get("path_changed"):
            path_changes.append({
                "field": "路径指纹",
                "operator": version.get("path_hash", ""),
                "change": "path_changed",
            })

        # 先确认“锁定版本”与任务时点一致，再看执行期间实时路径是否继续变化。
        pinned_version = str(task.get("血缘版本ID") or "")
        current_version = self.lineage.current_version(
            str(task.get("指标名称") or version.get("metric") or "")
        )
        superseded = bool(
            current_version
            and str(current_version.get("version_id")) != pinned_version
        )
        if superseded:
            path_changes.append({
                "field": "当前路径版本",
                "operator": str(current_version.get("path_version")),
                "change": "superseded_during_replay",
            })

        report_id = "rr_" + hashlib.sha256(
            f"{task.get('id')}_{task.get('血缘版本ID')}_{time.time()}".encode()
        ).hexdigest()[:16]

        passed = (
            verification.get("all_passed", False)
            and not operator_missing
            and not path_changes
        )
        report = {
            "report_id": report_id,
            "task_id": task.get("id"),
            "task_name": task.get("任务名称", ""),
            "metric": task.get("指标名称") or version.get("metric", ""),
            "lineage_version_id": task.get("血缘版本ID", ""),
            "path_version": version.get("path_version", task.get("路径版本", "")),
            "source_batch": version.get("source_batch", task.get("来源批次", "")),
            "checkpoint_id": task.get("源检查点", ""),
            "checkpoint_snapshot_ts": task.get("_checkpoint_snapshot_ts"),
            "topology": task.get("目标拓扑", ""),
            "lineage_pinned_at": task.get("_lineage_pinned_at"),
            "superseded": superseded,
            "current_path_version": (
                current_version.get("path_version") if current_version else None
            ),
            "operator_missing": operator_missing,
            "path_changes": path_changes,
            "checkpoint_missing_operators": checkpoint_missing,
            "replay_verification": copy.deepcopy(verification),
            "restored_operators": [r["operator"] for r in replay_results],
            "lineage_operator_chain": version.get("operator_chain", ""),
            "result": "passed" if passed else "failed",
            "status": "已验证" if passed else "验证失败",
            "created_at": time.time(),
        }
        store.save_replay_report(report)
        version_record = store.get_lineage_version(report["lineage_version_id"])
        if version_record is not None:
            if passed:
                version_record["verification_status"] = "passed"
                version_record["verified_by_replay"] = report_id
            else:
                version_record["verification_status"] = "failed"
                version_record["verified_by_replay"] = report_id
            version_record["last_verification"] = {
                "report_id": report_id,
                "task_id": task.get("id"),
                "result": report["result"],
                "operator_missing": operator_missing,
                "path_changes": path_changes,
            }
            store.save_lineage_version(version_record)
        return report

    def _settle_lineage_version(
        self, task: dict[str, Any], report: dict[str, Any],
    ) -> None:
        """把页面状态停靠到一个验证过的当前版本。

        只有验证通过、且该版本仍是当前版本时才落 verified；否则保持当前版本
        的 status（变更中/断链），避免把过期路径显示成已验证。
        """
        version = store.get_lineage_version(report["lineage_version_id"])
        if version is None:
            return
        version["verification_status"] = report["result"]
        version["verified_by_replay"] = report["report_id"]
        if report["result"] == "passed" and version.get("current"):
            version["status"] = "正常"
            version["confirmed"] = True
            version["confirmed_at"] = version.get("confirmed_at") or time.time()
            version["verified_at"] = time.time()
        else:
            # 回放失败或版本已被新路径取代：状态由实时核对决定，不落 verified。
            version["verified_at"] = None
            if version.get("current"):
                drift = self.lineage.evaluate_version_drift(version)
                if drift["operator_missing"]:
                    version["status"] = "断链"
                elif drift["path_changed"] or report.get("superseded"):
                    version["status"] = "变更中"
                version["confirmed"] = False
        store.save_lineage_version(version)
        self.lineage._sync_row(version["metric"], version)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def list_replay_history(self) -> list[dict[str, Any]]:
        """列出所有回放任务历史记录及恢复方案关联。"""
        history = []
        for task in store.rows(self.MODULE):
            entry = {
                "task_id": task.get("id"),
                "任务名称": task.get("任务名称", ""),
                "源检查点": task.get("源检查点", ""),
                "checkpoint_snapshot_ts": task.get("_checkpoint_snapshot_ts"),
                "回放模式": task.get("回放模式", ""),
                "指标名称": task.get("指标名称", ""),
                "血缘版本ID": task.get("血缘版本ID", ""),
                "路径版本": task.get("路径版本", ""),
                "结果验证": task.get("结果验证", "待验证"),
                "验证报告ID": task.get("验证报告ID", ""),
                "进度": task.get("进度", "0%"),
                "status": task.get("status", ""),
            }
            for rp in store.rows("restore_plan"):
                if task.get("目标拓扑", "") in str(rp.get("任务范围", "")):
                    entry["linked_restore_plan"] = rp.get("方案名称", "")
                    break
            report = store.get_replay_report(str(task.get("验证报告ID") or ""))
            if report:
                entry["operator_missing"] = list(report.get("operator_missing") or [])
                entry["path_changes"] = list(report.get("path_changes") or [])
            history.append(entry)
        return history

    def lineage_verification_history(self, metric: str | None = None) -> dict[str, Any]:
        """按指标或版本汇总回放验证结果，供血缘详情页核对。"""
        versions = self.lineage.list_versions(metric)
        tasks = [
            task for task in store.rows(self.MODULE)
            if metric is None or task.get("指标名称") == metric
        ]
        reports = []
        for task in tasks:
            report = store.get_replay_report(str(task.get("验证报告ID") or ""))
            if report is None:
                task_reports = store.replay_reports(task_id=task.get("id"))
                report = task_reports[-1] if task_reports else None
            if report is not None and report not in reports:
                reports.append(report)
        reports.sort(key=lambda r: r.get("created_at", 0))
        return {
            "metric": metric,
            "versions": versions,
            "replay_tasks": [
                {
                    "task_id": t.get("id"),
                    "任务名称": t.get("任务名称", ""),
                    "源检查点": t.get("源检查点", ""),
                    "checkpoint_snapshot_ts": t.get("_checkpoint_snapshot_ts"),
                    "血缘版本ID": t.get("血缘版本ID", ""),
                    "路径版本": t.get("路径版本", ""),
                    "结果验证": t.get("结果验证", "待验证"),
                    "验证报告ID": t.get("验证报告ID", ""),
                    "status": t.get("status", ""),
                }
                for t in tasks
            ],
            "reports": reports,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，附加回放成功率与血缘绑定情况。"""
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        completed = sum(1 for t in rows if t.get("status") == "已完成")
        failed = sum(1 for t in rows if t.get("status") == "验证失败")
        total_done = completed + failed
        base["replay_success_rate"] = completed / max(total_done, 1)
        base["completed_replays"] = completed
        base["failed_replays"] = failed
        base["version_bound_tasks"] = sum(1 for t in rows if t.get("血缘版本ID"))
        base["reports"] = len(store.replay_reports())
        base["pinned_tasks"] = sum(1 for t in rows if t.get("_lineage_pinned_at"))
        return base
