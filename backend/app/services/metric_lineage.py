"""指标血缘领域服务：追踪实时指标从数据源到最终输出的全链路血缘关系。

在原有“一次刷新只覆盖一段路径文本”的基础上补齐版本能力：

- 每条血缘记录都绑定「来源批次 + 路径版本」，刷新不再直接改写旧路径；
- 每个路径版本写入 store 的版本台账，历史版本可回看、可对照回放验证报告；
- 刷新由检查点驱动时可记录检查点快照时点，供回放引擎复现；
- 同一路径重复刷新（含并发重试）复用同一个路径版本，不会出现两个当前版本；
- 刷新失败走 store.transaction 回滚，保留上一份完整版本；
- 读取时校验算子链与实时模块的差异，算子缺失/路径变化会把旧血缘从「正常」改为
  「变更中」或「断链」，不再出现算子链调整后旧血缘仍显示正常的问题。
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

STATUS_NORMAL = "正常"
STATUS_CHANGING = "变更中"
STATUS_BROKEN = "断链"


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


class MetricLineageService(BaseModuleService):
    MODULE = "metric_lineage"
    ENTITY = "血缘记录"
    LIST_FIELDS = ["记录ID", "指标名称", "上游源端", "中间算子", "输出目标", "更新时间", "版本号"]
    REQUIRED_FIELDS = ["记录ID", "指标名称", "上游源端"]
    STATUS_ORDER = [STATUS_NORMAL, STATUS_CHANGING, STATUS_BROKEN]
    ACTION_RULES = {"刷新血缘": STATUS_NORMAL, "标记断链": STATUS_CHANGING}
    NEGATIVE_ACTIONS = ["标记断链"]

    # ------------------------------------------------------------------
    # 路径采集与版本标识
    # ------------------------------------------------------------------

    @staticmethod
    def _names(module: str, field: str, *, usable_only: bool = False) -> list[str]:
        rows = sorted(store.rows(module), key=lambda r: _as_int(r.get("id", 0)))
        names: list[str] = []
        for row in rows:
            name = str(row.get(field) or "").strip()
            if not name:
                continue
            if usable_only and row.get("status") == "已下线":
                continue
            if name not in names:
                names.append(name)
        return names

    def _topology(self, topology_name: str | None) -> dict[str, Any] | None:
        if not topology_name:
            return None
        for row in store.rows("task_topology"):
            if row.get("拓扑名称") == topology_name:
                return row
        return None

    def _path_parts(
        self, metric_name: str, topology_name: str | None = None,
    ) -> dict[str, Any]:
        """从实时模块采集当前路径，作为刷新时的路径快照。"""
        sources = self._names("stream_source", "源端名称", usable_only=True)[:3]
        outputs = self._names("sink_output", "配置名称")[:3]

        topology = self._topology(topology_name)
        chain_raw = str((topology or {}).get("算子链", "") or "")
        chain = [part.strip() for part in chain_raw.replace("->", "→").split("→") if part.strip()]
        if chain:
            operators = chain[:5]
        else:
            operators = self._names("calc_operator", "算子名称")[:5]
            if not operators:
                operators = self._names("window_config", "策略名称")[:5]

        return {
            "metric": metric_name,
            "sources": sources,
            "operators": operators,
            "outputs": outputs,
            "topology": topology_name or "",
            "operator_chain": " → ".join(operators),
        }

    @staticmethod
    def _path_hash(parts: dict[str, Any]) -> str:
        payload = json.dumps(
            {
                "sources": parts.get("sources", []),
                "operators": parts.get("operators", []),
                "outputs": parts.get("outputs", []),
                "topology": parts.get("topology", ""),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def _current_version(self, metric: str) -> dict[str, Any] | None:
        current = store.lineage_versions(metric, current_only=True)
        return current[-1] if current else None

    def current_version(self, metric: str) -> dict[str, Any] | None:
        """公开的当前版本读取口，供回放引擎等下游模块消费。"""
        return self._current_version(metric)

    @staticmethod
    def _version_id(
        metric: str, batch: str, path_hash: str, checkpoint_id: str | None,
    ) -> str:
        raw = f"{metric}|{batch}|{path_hash}|{checkpoint_id or ''}"
        return "lv_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def _resolve_batch(
        self,
        metric: str,
        path_hash: str,
        checkpoint_id: str | None,
        source_batch: str | None,
    ) -> str:
        if source_batch and str(source_batch).strip():
            return str(source_batch).strip()
        current = self._current_version(metric)
        if (
            current
            and current.get("path_hash") == path_hash
            and str(current.get("checkpoint_id") or "") == str(checkpoint_id or "")
        ):
            # 路径与检查点都没变时沿用原批次：重复刷新落到同一个版本上。
            return str(current.get("source_batch") or "")
        return f"batch_{metric}_{int(time.time() * 1000)}"

    # ------------------------------------------------------------------
    # 差异检测
    # ------------------------------------------------------------------

    def _operator_exists(self, operator: str) -> bool:
        if not operator:
            return True
        for row in store.rows("calc_operator"):
            if row.get("算子名称") == operator:
                return True
        for row in store.rows("window_config"):
            if row.get("策略名称") == operator:
                return True
        for row in store.rows("task_topology"):
            chain = str(row.get("算子链", "")).replace("->", "→").split("→")
            if operator in {part.strip() for part in chain if part.strip()}:
                return True
        return False

    def _operator_missing(self, operators: list[str]) -> list[str]:
        return [op for op in operators if not self._operator_exists(op)]

    @staticmethod
    def _path_changes(
        previous: dict[str, Any] | None, parts: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if not previous:
            return []
        changes: list[dict[str, Any]] = []
        for field, label in (
            ("sources", "上游源端"),
            ("operators", "中间算子"),
            ("outputs", "输出目标"),
        ):
            old_list = list(previous.get(field) or [])
            new_list = list(parts.get(field) or [])
            for item in old_list:
                if item not in new_list:
                    changes.append({"field": label, "operator": item, "change": "removed"})
            for item in new_list:
                if item not in old_list:
                    changes.append({"field": label, "operator": item, "change": "added"})
        if str(previous.get("topology") or "") != str(parts.get("topology") or ""):
            changes.append({
                "field": "任务拓扑",
                "operator": parts.get("topology") or previous.get("topology") or "",
                "change": "topology_changed",
            })
        return changes

    def evaluate_version_drift(self, version: dict[str, Any]) -> dict[str, Any]:
        """对照实时模块核对某个版本，返回算子缺失与路径变化。"""
        if not version.get("drift_check", True):
            return {"operator_missing": [], "path_changes": [], "path_changed": False}
        live_parts = self._path_parts(
            str(version.get("metric", "")), version.get("topology") or None,
        )
        live_hash = self._path_hash(live_parts)
        missing = self._operator_missing(list(version.get("operators") or []))
        changes = self._path_changes(version, live_parts)
        return {
            "operator_missing": missing,
            "path_changes": changes,
            "path_changed": live_hash != version.get("path_hash"),
            "live_operator_chain": live_parts.get("operator_chain", ""),
        }

    # ------------------------------------------------------------------
    # 版本写入与行投影
    # ------------------------------------------------------------------

    def _set_current(self, metric: str, version: dict[str, Any]) -> None:
        for other in store.lineage_versions(metric):
            if other is not version and other.get("current"):
                other["current"] = False
                other["superseded_at"] = time.time()
                other["superseded_by"] = version.get("version_id", "")
        version["current"] = True

    def _sync_row(self, metric: str, version: dict[str, Any]) -> dict[str, Any] | None:
        """把版本投影到列表行；只有当前版本才能改写列表行。

        历史版本被确认、标记断链或回放验证时，不能把列表页拉回旧路径。
        """
        rows = store.rows(self.MODULE)
        row = next((r for r in rows if r.get("指标名称") == metric), None)
        if not version.get("current"):
            return row
        record_id = f"lineage_{metric}"
        payload: dict[str, Any] = {
            "记录ID": record_id,
            "指标名称": metric,
            "上游源端": " → ".join(version.get("sources") or []),
            "中间算子": version.get("operator_chain", ""),
            "输出目标": " → ".join(version.get("outputs") or []),
            "更新时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "版本号": version.get("path_version", ""),
            "来源批次": version.get("source_batch", ""),
            "路径版本": version.get("path_version", ""),
            "血缘版本ID": version.get("version_id", ""),
            "路径指纹": version.get("path_hash", ""),
            "检查点ID": version.get("checkpoint_id", "") or "",
            "status": version.get("status", STATUS_NORMAL),
            "pending": version.get("status") == STATUS_CHANGING,
            "abnormal": version.get("status") == STATUS_BROKEN,
        }
        if row is None:
            payload["id"] = max(
                (_as_int(r.get("id", 0)) for r in rows), default=0
            ) + 1
            rows.append(payload)
            return payload
        row.update(payload)
        return row

    def _persist_version(
        self,
        *,
        metric: str,
        parts: dict[str, Any],
        source_batch: str,
        checkpoint_id: str | None,
        checkpoint_snapshot_ts: float | None,
        origin: str,
        drift_check: bool = True,
    ) -> tuple[dict[str, Any], bool]:
        """在锁内写入一个路径版本；返回 (版本, 是否新建)。"""
        path_hash = self._path_hash(parts)
        version_id = self._version_id(metric, source_batch, path_hash, checkpoint_id)

        existing = store.get_lineage_version(version_id)
        if existing is not None:
            # 同一指标、同一批次、同一路径、同一检查点：重复刷新复用原版本。
            self._set_current(metric, existing)
            self._sync_row(metric, existing)
            return existing, False

        previous = self._current_version(metric)
        # 种子基线版本只有路径文本、没有实时核对能力，不参与“路径变化”比较；
        # 第一次真实刷新建立基线，之后再变化才算路径变更。
        previous_path = (
            previous
            if previous
            and previous.get("drift_check")
            and previous.get("path_hash") != path_hash
            else None
        )
        missing = self._operator_missing(list(parts.get("operators") or [])) if drift_check else []
        changes = self._path_changes(previous_path, parts) if drift_check else []

        if missing:
            status = STATUS_BROKEN
        elif changes:
            status = STATUS_CHANGING
        else:
            status = STATUS_NORMAL

        version_no = store.next_lineage_version_no(metric)
        version = {
            "version_id": version_id,
            "metric": metric,
            "version": version_no,
            "path_version": f"v{version_no}",
            "path_hash": path_hash,
            "source_batch": source_batch,
            "checkpoint_id": checkpoint_id or "",
            "checkpoint_snapshot_ts": checkpoint_snapshot_ts,
            "sources": list(parts.get("sources") or []),
            "operators": list(parts.get("operators") or []),
            "outputs": list(parts.get("outputs") or []),
            "operator_chain": parts.get("operator_chain", ""),
            "topology": parts.get("topology", ""),
            "status": status,
            "confirmed": status == STATUS_NORMAL and drift_check,
            "current": True,
            "origin": origin,
            "drift_check": drift_check,
            "operator_missing": missing,
            "path_changes": changes,
            "created_at": time.time(),
            "confirmed_at": time.time() if status == STATUS_NORMAL else None,
            "verified_by_replay": None,
            "verification_status": "待验证",
        }
        store.save_lineage_version(version)
        self._set_current(metric, version)
        self._sync_row(metric, version)
        return version, True

    # ------------------------------------------------------------------
    # 刷新 / 确认 / 标记断链
    # ------------------------------------------------------------------

    def refresh_lineage(
        self,
        metric_name: str,
        *,
        source_batch: str | None = None,
        checkpoint_id: str | None = None,
        topology_name: str | None = None,
    ) -> dict[str, Any]:
        """刷新指标血缘：生成或复用路径版本，并绑定来源批次与检查点。"""
        metric = str(metric_name or "").strip()
        if not metric:
            return {"ok": False, "error": "指标名称不能为空"}

        checkpoint_ts: float | None = None
        cp_id = str(checkpoint_id).strip() if checkpoint_id else None
        if cp_id:
            cp_row = None
            for row in store.rows("checkpoint_mgr"):
                if str(row.get("checkpoint_id") or row.get("id")) == cp_id:
                    cp_row = row
                    break
            if cp_row is None:
                return {"ok": False, "error": f"检查点「{cp_id}」不存在"}
            if cp_row.get("status") != "已启用":
                return {"ok": False, "error": f"检查点「{cp_id}」未启用"}
            checkpoint_ts = cp_row.get("snapshot_ts") or cp_row.get("timestamp")

        try:
            with store.transaction(
                self.MODULE, "checkpoint_mgr", include_lineage=True,
            ):
                parts = self._path_parts(metric, topology_name)
                path_hash = self._path_hash(parts)
                batch = self._resolve_batch(metric, path_hash, cp_id, source_batch)
                if not batch:
                    batch = f"batch_{metric}_{int(time.time() * 1000)}"
                version, created = self._persist_version(
                    metric=metric,
                    parts=parts,
                    source_batch=batch,
                    checkpoint_id=cp_id,
                    checkpoint_snapshot_ts=checkpoint_ts,
                    origin="refresh",
                )
                row = next(
                    (r for r in store.rows(self.MODULE) if r.get("指标名称") == metric),
                    None,
                )
                replay_reports = store.replay_reports(version_id=version["version_id"])
                return {
                    "ok": True,
                    "idempotent": not created,
                    "version": copy.deepcopy(version),
                    "version_id": version["version_id"],
                    "path_version": version["path_version"],
                    "source_batch": version["source_batch"],
                    "checkpoint_id": version.get("checkpoint_id", ""),
                    "operator_missing": list(version.get("operator_missing") or []),
                    "path_changes": list(version.get("path_changes") or []),
                    "lineage": copy.deepcopy(row) if row else None,
                    "reports": copy.deepcopy(replay_reports),
                }
        except Exception as exc:  # 事务失败：回滚后仍保留上一份完整版本
            try:
                retained = self._current_version(metric)
            except Exception:
                retained = None
            return {
                "ok": False,
                "error": f"血缘刷新失败，已保留原版本：{exc}",
                "retained": retained,
            }

    def refresh_lineage_from_path(
        self,
        metric_name: str,
        *,
        sources: list[str],
        operators: list[str],
        outputs: list[str],
        topology_name: str | None = None,
        checkpoint_id: str | None = None,
        source_batch: str | None = None,
        origin: str = "pipeline",
    ) -> dict[str, Any]:
        """按调用方给定的路径段写入版本（管道链路追踪等非页面入口）。

        与页面刷新共用同一套批次/版本台账，避免绕过版本直接覆盖路径文本。
        """
        metric = str(metric_name or "").strip()
        if not metric:
            return {"ok": False, "error": "指标名称不能为空"}
        parts = {
            "metric": metric,
            "sources": [str(item) for item in sources if str(item).strip()],
            "operators": [str(item) for item in operators if str(item).strip()],
            "outputs": [str(item) for item in outputs if str(item).strip()],
            "topology": topology_name or "",
            "operator_chain": " → ".join(
                str(item) for item in operators if str(item).strip()
            ),
        }
        try:
            with store.transaction(
                self.MODULE, "checkpoint_mgr", include_lineage=True,
            ):
                path_hash = self._path_hash(parts)
                batch = self._resolve_batch(metric, path_hash, checkpoint_id, source_batch)
                if not batch:
                    batch = f"batch_{metric}_{int(time.time() * 1000)}"
                version, created = self._persist_version(
                    metric=metric,
                    parts=parts,
                    source_batch=batch,
                    checkpoint_id=checkpoint_id,
                    checkpoint_snapshot_ts=None,
                    origin=origin,
                    drift_check=False,
                )
                row = next(
                    (r for r in store.rows(self.MODULE) if r.get("指标名称") == metric),
                    None,
                )
                return {
                    "ok": True,
                    "idempotent": not created,
                    "version": copy.deepcopy(version),
                    "lineage": copy.deepcopy(row) if row else None,
                }
        except Exception as exc:
            return {"ok": False, "error": f"血缘路径写入失败，已保留原版本：{exc}"}

    def refresh_all_lineages(self) -> dict[str, Any]:
        """批量刷新所有已登记指标的血缘版本。"""
        metrics = {
            str(row.get("指标名称") or "").strip()
            for row in store.rows(self.MODULE)
            if str(row.get("指标名称") or "").strip()
        }
        refreshed: list[str] = []
        failed: list[dict[str, Any]] = []
        for metric in sorted(metrics):
            result = self.refresh_lineage(metric)
            if result.get("ok"):
                refreshed.append(metric)
            else:
                failed.append({"metric": metric, "error": result.get("error")})
        return {
            "ok": not failed,
            "refreshed": len(refreshed),
            "metrics": refreshed,
            "failed": failed,
        }

    def confirm_version(self, version_id: str) -> dict[str, Any]:
        """复核确认一个版本；确认后回放任务才允许引用。"""
        version = store.get_lineage_version(version_id)
        if version is None:
            return {"ok": False, "error": f"血缘版本「{version_id}」不存在"}
        if not version.get("current"):
            return {
                "ok": False,
                "error": "该版本已被更新的路径版本取代，请确认当前版本",
                "current_version_id": (self._current_version(version["metric"]) or {}).get(
                    "version_id"
                ),
            }

        try:
            with store.transaction(self.MODULE, include_lineage=True):
                drift = self.evaluate_version_drift(version)
                if drift["operator_missing"]:
                    version["status"] = STATUS_BROKEN
                    version["confirmed"] = False
                    self._sync_row(version["metric"], version)
                    return {
                        "ok": False,
                        "error": "版本存在算子缺失，不能确认",
                        "operator_missing": drift["operator_missing"],
                    }
                if drift["path_changed"]:
                    version["status"] = STATUS_CHANGING
                    version["confirmed"] = False
                    version["path_changes"] = drift["path_changes"]
                    self._sync_row(version["metric"], version)
                    return {
                        "ok": False,
                        "error": "实时路径已变化，请先刷新血缘再确认",
                        "path_changes": drift["path_changes"],
                    }
                version["status"] = STATUS_NORMAL
                version["confirmed"] = True
                version["confirmed_at"] = time.time()
                version["path_changes"] = []
                store.save_lineage_version(version)
                self._set_current(version["metric"], version)
                row = self._sync_row(version["metric"], version)
                return {
                    "ok": True,
                    "version": copy.deepcopy(version),
                    "lineage": copy.deepcopy(row),
                }
        except Exception as exc:
            return {"ok": False, "error": f"确认失败，已保留原版本：{exc}"}

    def mark_version_broken(self, version_id: str) -> dict[str, Any]:
        version = store.get_lineage_version(version_id)
        if version is None:
            return {"ok": False, "error": f"血缘版本「{version_id}」不存在"}
        if not version.get("current"):
            return {"ok": False, "error": "只能标记当前路径版本断链"}
        version["status"] = STATUS_BROKEN
        version["confirmed"] = False
        version["confirmed_at"] = None
        store.save_lineage_version(version)
        self._sync_row(version["metric"], version)
        return {"ok": True, "version": copy.deepcopy(version)}

    def mark_current_broken(self, metric: str) -> dict[str, Any]:
        current = self._current_version(metric)
        if current is None:
            row = next(
                (r for r in store.rows(self.MODULE) if r.get("指标名称") == metric),
                None,
            )
            if row is None:
                return {"ok": False, "error": f"血缘记录「{metric}」不存在"}
            row["status"] = STATUS_BROKEN
            row["abnormal"] = True
            row["pending"] = False
            return {"ok": True, "lineage": row}
        return self.mark_version_broken(current["version_id"])

    # ------------------------------------------------------------------
    # 读取与健康同步
    # ------------------------------------------------------------------

    def _bootstrap_unbound_rows(self) -> None:
        """示例数据里的旧血缘只有路径文本：首次读取时补一份只读基线版本。

        基线版本标记 drift_check=False，不参与实时模块核对，作为“历史版本”保留，
        等真正刷新时才生成与实时路径绑定的新版本。
        """
        with store.lock():
            for row in store.rows(self.MODULE):
                metric = str(row.get("指标名称") or "").strip()
                if not metric or row.get("来源批次") or store.lineage_versions(metric):
                    continue
                version = {
                    "version_id": f"baseline_{_as_int(row.get('id', 0))}",
                    "metric": metric,
                    "version": 0,
                    "path_version": str(row.get("版本号") or "基线版本"),
                    "path_hash": hashlib.sha256(
                        f"baseline|{metric}".encode("utf-8")
                    ).hexdigest()[:16],
                    "source_batch": f"seed_batch_{_as_int(row.get('id', 0))}",
                    "checkpoint_id": "",
                    "checkpoint_snapshot_ts": None,
                    "sources": [
                        part.strip()
                        for part in str(row.get("上游源端") or "").split("→")
                        if part.strip()
                    ],
                    "operators": [
                        part.strip()
                        for part in str(row.get("中间算子") or "").split("→")
                        if part.strip()
                    ],
                    "outputs": [
                        part.strip()
                        for part in str(row.get("输出目标") or "").split("→")
                        if part.strip()
                    ],
                    "operator_chain": str(row.get("中间算子") or ""),
                    "topology": "",
                    "status": str(row.get("status") or STATUS_NORMAL),
                    "confirmed": False,
                    "current": True,
                    "origin": "seed",
                    "drift_check": False,
                    "operator_missing": [],
                    "path_changes": [],
                    "created_at": time.time(),
                    "confirmed_at": None,
                    "verified_by_replay": None,
                    "verification_status": "待验证",
                }
                store.save_lineage_version(version)
                row["来源批次"] = version["source_batch"]
                row["路径版本"] = version["path_version"]
                row["血缘版本ID"] = version["version_id"]
                row["路径指纹"] = version["path_hash"]
                row["检查点ID"] = ""

    def sync_health(self) -> dict[str, Any]:
        """读取前核对实时路径，发现算子缺失或路径变化时纠正旧状态。"""
        self._bootstrap_unbound_rows()
        updated: list[str] = []
        for version in store.lineage_versions(current_only=True):
            if not version.get("drift_check", True):
                continue
            drift = self.evaluate_version_drift(version)
            if drift["operator_missing"]:
                new_status = STATUS_BROKEN
            elif drift["path_changed"]:
                new_status = STATUS_CHANGING
            else:
                continue
            if version.get("status") != new_status or version.get("confirmed"):
                version["status"] = new_status
                version["confirmed"] = False
                version["operator_missing"] = drift["operator_missing"]
                version["path_changes"] = drift["path_changes"]
                version["_drift_detected_at"] = time.time()
                store.save_lineage_version(version)
                self._sync_row(version["metric"], version)
                updated.append(version["version_id"])
        return {"ok": True, "updated": updated, "count": len(updated)}

    def list_versions(
        self,
        metric: str | None = None,
        *,
        current_only: bool = False,
    ) -> list[dict[str, Any]]:
        self.sync_health()
        return copy.deepcopy(store.lineage_versions(metric, current_only=current_only))

    def version_detail(self, version_id: str) -> dict[str, Any] | None:
        self.sync_health()
        version = store.get_lineage_version(version_id)
        if version is None:
            return None
        drift = self.evaluate_version_drift(version)
        checkpoint = None
        if version.get("checkpoint_id"):
            checkpoint = next(
                (
                    row for row in store.rows("checkpoint_mgr")
                    if str(row.get("checkpoint_id") or row.get("id")) == str(version["checkpoint_id"])
                ),
                None,
            )
        row = next(
            (r for r in store.rows(self.MODULE) if r.get("指标名称") == version["metric"]),
            None,
        )
        reports = store.replay_reports(version_id=version_id)
        return {
            "version": copy.deepcopy(version),
            "lineage": copy.deepcopy(row) if row else None,
            "checkpoint": copy.deepcopy(checkpoint) if checkpoint else None,
            "drift": drift,
            "constant": drift["operator_missing"] == [] and not drift["path_changed"],
            "reports": copy.deepcopy(reports),
            "replay_history": [
                self._replay_brief(task) for task in store.rows("replay_engine")
                if str(task.get("血缘版本ID") or "") == str(version_id)
            ],
            "all_versions": [
                self._version_brief(item) for item in store.lineage_versions(version["metric"])
            ],
        }

    @staticmethod
    def _version_brief(version: dict[str, Any]) -> dict[str, Any]:
        return {
            "version_id": version.get("version_id"),
            "path_version": version.get("path_version"),
            "source_batch": version.get("source_batch"),
            "checkpoint_id": version.get("checkpoint_id", ""),
            "status": version.get("status"),
            "confirmed": bool(version.get("confirmed")),
            "current": bool(version.get("current")),
            "operator_chain": version.get("operator_chain", ""),
            "created_at": version.get("created_at"),
        }

    @staticmethod
    def _replay_brief(task: dict[str, Any]) -> dict[str, Any]:
        return {
            "task_id": task.get("id"),
            "任务名称": task.get("任务名称", ""),
            "源检查点": task.get("源检查点", ""),
            "checkpoint_snapshot_ts": task.get("_checkpoint_snapshot_ts"),
            "血缘版本ID": task.get("血缘版本ID", ""),
            "路径版本": task.get("路径版本", ""),
            "结果验证": task.get("结果验证", "待验证"),
            "验证报告ID": task.get("验证报告ID", ""),
            "status": task.get("status", ""),
        }

    # ------------------------------------------------------------------
    # 复用基类：列表读取前同步健康状态，动作走版本化刷新
    # ------------------------------------------------------------------

    def new_entry(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记血缘记录时立即绑定一个初始路径版本与来源批次。"""
        entry = super().new_entry(values)
        entry["记录ID"] = str(values.get("记录ID") or f"lineage_{entry['id']}")
        entry["来源批次"] = str(values.get("来源批次") or f"batch_{entry['记录ID']}_0")
        entry["路径版本"] = "v0"
        entry["血缘版本ID"] = ""
        entry["路径指纹"] = ""
        entry["检查点ID"] = ""
        return entry

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        with store.lock():
            entry = self.new_entry(values)
            store.rows(self.MODULE).append(entry)
        return entry, []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        self.sync_health()
        return super().list_entries(
            filters=filters, keyword=keyword, status=status, page=page, size=size,
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        metric = str(entry.get("指标名称") or "")

        if action == "刷新血缘":
            checkpoint_id = None
            for cp in store.rows("checkpoint_mgr"):
                if cp.get("checkpoint_id") and cp.get("status") == "已启用":
                    checkpoint_id = str(cp["checkpoint_id"])
                    break
            result = self.refresh_lineage(metric, checkpoint_id=checkpoint_id)
            if not result.get("ok"):
                return None, str(result.get("error") or "血缘刷新失败")
            row = next(
                (r for r in store.rows(self.MODULE) if r.get("指标名称") == metric),
                entry,
            )
            return row, f"血缘已刷新至{result.get('path_version')}（批次 {result.get('source_batch')}）"

        if action == "标记断链":
            result = self.mark_current_broken(metric)
            if not result.get("ok"):
                return None, str(result.get("error") or "标记断链失败")
            row = next(
                (r for r in store.rows(self.MODULE) if r.get("指标名称") == metric),
                entry,
            )
            return row, "血缘已标记断链"

        return super().run_action(entry_id, action)

    def domain_snapshot(self) -> dict[str, Any]:
        self.sync_health()
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        versions = store.lineage_versions()
        base["normal"] = sum(1 for r in rows if r.get("status") == STATUS_NORMAL)
        base["changing"] = sum(1 for r in rows if r.get("status") == STATUS_CHANGING)
        base["broken"] = sum(1 for r in rows if r.get("status") == STATUS_BROKEN)
        base["version_count"] = len(versions)
        base["current_versions"] = sum(1 for v in versions if v.get("current"))
        base["confirmed_versions"] = sum(1 for v in versions if v.get("confirmed"))
        base["bound_records"] = sum(1 for r in rows if r.get("来源批次") and r.get("路径版本"))
        return base
