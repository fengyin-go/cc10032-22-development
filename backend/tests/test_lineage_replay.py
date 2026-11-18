"""指标血缘版本化 + 回放验证测试。

覆盖题位22的关键约束：
  - 血缘记录与来源批次、路径版本绑定；
  - 同一路径重复/并发刷新只产生一个当前版本（幂等）；
  - 算子链调整后旧血缘不再显示正常；
  - 回放任务只能引用已确认血缘，并固定快照时点的版本；
  - 回放验证报告记录算子缺失、路径变化和使用的检查点；
  - 事务失败回滚后保留上一份完整版本。
"""
from __future__ import annotations

import copy
import threading

import pytest

from app.seed import SEED_ROWS
from app.services.metric_lineage import MetricLineageService
from app.services.replay_engine import ReplayEngineService
from app.store import store

CHECKPOINT = "cp_demo_001"


@pytest.fixture(autouse=True)
def _reset_store():
    """每个用例从示例数据重建内存仓库，避免路径/算子改动互相污染。"""
    store._tables = {name: [copy.deepcopy(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store._lineage_ledger = {}
    store._replay_reports = {}
    store._channels = {}
    store._failures = {}
    yield


@pytest.fixture()
def lineage():
    service = MetricLineageService()
    service.sync_health()
    return service


@pytest.fixture()
def replay(lineage):
    return ReplayEngineService()


def _rename_operator(operator_id: int, new_name: str) -> None:
    row = next(r for r in store.rows("calc_operator") if r["id"] == operator_id)
    row["算子名称"] = new_name


def _add_operator(name: str) -> None:
    store.rows("calc_operator").append({
        "id": max(int(r.get("id", 0)) for r in store.rows("calc_operator")) + 1,
        "status": "运行中",
        "pending": False,
        "abnormal": False,
        "算子名称": name,
    })


class TestLineageVersioning:
    def test_refresh_binds_batch_and_path_version(self, lineage):
        result = lineage.refresh_lineage("metric_bind")
        assert result["ok"] is True
        version = result["version"]
        assert version["source_batch"]
        assert version["path_version"].startswith("v")
        assert version["version_id"]

        row = next(
            r for r in store.rows("metric_lineage") if r.get("指标名称") == "metric_bind"
        )
        assert row["来源批次"] == version["source_batch"]
        assert row["路径版本"] == version["path_version"]
        assert row["血缘版本ID"] == version["version_id"]

    def test_repeated_refresh_is_idempotent(self, lineage):
        first = lineage.refresh_lineage("metric_idem")
        second = lineage.refresh_lineage("metric_idem")
        third = lineage.refresh_lineage("metric_idem")
        assert second["idempotent"] is True
        assert third["idempotent"] is True
        assert first["version_id"] == second["version_id"] == third["version_id"]

        versions = store.lineage_versions("metric_idem")
        assert len(versions) == 1
        assert len([v for v in versions if v["current"]]) == 1

    def test_same_path_with_new_batch_keeps_single_current(self, lineage):
        lineage.refresh_lineage("metric_batch")
        second = lineage.refresh_lineage("metric_batch", source_batch="batch_explicit_2")
        assert second["ok"] is True
        assert second["path_version"] != "v1"

        current = [v for v in store.lineage_versions("metric_batch") if v["current"]]
        assert len(current) == 1
        assert current[0]["version_id"] == second["version_id"]

    def test_concurrent_refresh_only_one_current(self, lineage):
        errors: list[str] = []
        results: list[dict] = []

        def worker() -> None:
            try:
                results.append(lineage.refresh_lineage("metric_concurrent"))
            except Exception as exc:  # pragma: no cover - 失败即测试失败
                errors.append(str(exc))

        threads = [threading.Thread(target=worker) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not errors
        assert all(item["ok"] for item in results)
        assert len({item["version_id"] for item in results}) == 1

        versions = store.lineage_versions("metric_concurrent")
        assert len(versions) == 1
        assert len([v for v in versions if v["current"]]) == 1

    def test_checkpoint_driven_refresh_records_snapshot(self, lineage):
        result = lineage.refresh_lineage("metric_cp", checkpoint_id=CHECKPOINT)
        assert result["ok"] is True
        assert result["checkpoint_id"] == CHECKPOINT
        assert result["version"]["checkpoint_snapshot_ts"] == 1795000000.0

    def test_unknown_checkpoint_rejected(self, lineage):
        result = lineage.refresh_lineage("metric_cp_bad", checkpoint_id="cp_missing")
        assert result["ok"] is False
        assert "不存在" in result["error"]

    def test_operator_chain_change_marks_stale_lineage(self, lineage):
        lineage.refresh_lineage("metric_drift")
        _rename_operator(2, "计算算子_链路调整")

        health = lineage.sync_health()
        assert health["count"] >= 1

        current = lineage.current_version("metric_drift")
        assert current["status"] == "断链"
        assert current["confirmed"] is False
        assert "计算算子样例2" in current["operator_missing"]

        detail = lineage.version_detail(current["version_id"])
        assert detail["constant"] is False

    def test_refresh_after_change_creates_new_version(self, lineage):
        lineage.refresh_lineage("metric_change")
        _add_operator("新增算子_链路")
        refreshed = lineage.refresh_lineage("metric_change")

        assert refreshed["ok"] is True
        assert refreshed["version"]["status"] == "变更中"
        assert refreshed["path_changes"]
        assert refreshed["path_version"] != "v1"

        old = next(
            v for v in store.lineage_versions("metric_change") if not v["current"]
        )
        assert old["path_version"] == "v1"

    def test_confirm_requires_unchanged_path(self, lineage):
        lineage.refresh_lineage("metric_confirm")
        version = lineage.current_version("metric_confirm")
        ok = lineage.confirm_version(version["version_id"])
        assert ok["ok"] is True

        _add_operator("确认后新增算子")
        rejected = lineage.confirm_version(version["version_id"])
        assert rejected["ok"] is False
        assert "变化" in rejected["error"] or "缺失" in rejected["error"]

    def test_transaction_failure_keeps_previous_version(self, lineage, monkeypatch):
        first = lineage.refresh_lineage("metric_rollback")
        assert first["ok"] is True

        def boom(*args, **kwargs):
            raise RuntimeError("模拟写入失败")

        monkeypatch.setattr(store, "save_lineage_version", boom)
        failed = lineage.refresh_lineage("metric_rollback", source_batch="batch_fail")
        assert failed["ok"] is False
        assert failed["retained"]["version_id"] == first["version_id"]

        monkeypatch.undo()
        lineage.sync_health()
        versions = store.lineage_versions("metric_rollback")
        assert len(versions) == 1
        assert versions[0]["current"] is True

    def test_versions_survive_other_entry_reads(self, lineage):
        lineage.refresh_lineage("metric_persist")
        before = lineage.list_versions("metric_persist")
        # 模拟重新进入页面：重新实例化服务再读
        again = MetricLineageService().list_versions("metric_persist")
        assert [v["version_id"] for v in again] == [v["version_id"] for v in before]
        assert again[0]["source_batch"]


class TestReplayValidation:
    def _confirmed_metric(self, lineage: MetricLineageService, metric: str) -> dict:
        lineage.refresh_lineage(metric, checkpoint_id=CHECKPOINT)
        version = lineage.current_version(metric)
        assert lineage.confirm_version(version["version_id"])["ok"] is True
        return lineage.current_version(metric)

    def test_replay_requires_confirmed_lineage(self, lineage, replay):
        lineage.refresh_lineage("metric_unconfirmed", checkpoint_id=CHECKPOINT)
        version = lineage.current_version("metric_unconfirmed")
        version["confirmed"] = False

        blocked = replay.create_replay_job(
            "未确认回放", CHECKPOINT, "", metric_name="metric_unconfirmed",
        )
        assert blocked["ok"] is False
        assert "确认" in blocked["error"]

    def test_replay_rejects_missing_operator(self, lineage, replay):
        self._confirmed_metric(lineage, "metric_missing_op")
        _rename_operator(3, "算子被删除")

        blocked = replay.create_replay_job(
            "算子缺失回放", CHECKPOINT, "", metric_name="metric_missing_op",
        )
        assert blocked["ok"] is False
        assert "算子缺失" in blocked["error"]

    def test_replay_pins_version_and_writes_report(self, lineage, replay):
        self._confirmed_metric(lineage, "metric_replay_ok")
        task = replay.create_replay_job(
            "正常回放", CHECKPOINT, "", metric_name="metric_replay_ok",
        )
        assert task["ok"] is True
        assert task["lineage_version_id"]

        result = replay.execute_replay(task["task_id"])
        assert result["ok"] is True
        report = result["report"]
        assert report["result"] == "passed"
        assert report["operator_missing"] == []
        assert report["path_changes"] == []
        assert report["checkpoint_id"] == CHECKPOINT
        assert report["checkpoint_snapshot_ts"] == 1795000000.0

        version = store.get_lineage_version(task["lineage_version_id"])
        assert version["verification_status"] == "passed"
        assert version["verified_by_replay"] == report["report_id"]

    def test_report_captures_operator_missing(self, lineage, replay):
        self._confirmed_metric(lineage, "metric_report_drift")
        task = replay.create_replay_job(
            "漂移回放", CHECKPOINT, "", metric_name="metric_report_drift",
        )
        assert task["ok"] is True
        _rename_operator(1, "回放期间被删除的算子")

        result = replay.execute_replay(task["task_id"])
        report = result["report"]
        assert report["result"] == "failed"
        assert "计算算子样例1" in report["operator_missing"]
        assert result["report"]["path_changes"]

    def test_replay_keeps_pinned_version_when_path_refreshes(self, lineage, replay):
        self._confirmed_metric(lineage, "metric_pinned")
        task = replay.create_replay_job(
            "固定版本回放", CHECKPOINT, "", metric_name="metric_pinned",
        )
        pinned = task["lineage_version_id"]

        # 回放尚未执行，路径刷新出新的当前版本
        _add_operator("回放期间新增算子")
        assert lineage.refresh_lineage("metric_pinned")["ok"] is True
        current = lineage.current_version("metric_pinned")
        assert current["version_id"] != pinned

        result = replay.execute_replay(task["task_id"])
        report = result["report"]
        assert report["lineage_version_id"] == pinned
        assert report["superseded"] is True
        assert report["path_version"] != current["path_version"]

        # 页面最终停靠在新当前版本，且不会被旧回放标成已验证
        assert current["verification_status"] != "passed"
        history = replay.list_replay_history()
        pinned_entry = next(h for h in history if h["task_id"] == task["task_id"])
        assert pinned_entry["路径版本"] == report["path_version"]

    def test_replay_history_exposes_checkpoint_and_report(self, lineage, replay):
        self._confirmed_metric(lineage, "metric_history")
        task = replay.create_replay_job(
            "历史回放", CHECKPOINT, "", metric_name="metric_history",
        )
        replay.execute_replay(task["task_id"])

        history = replay.list_replay_history()
        entry = next(h for h in history if h["task_id"] == task["task_id"])
        assert entry["checkpoint_snapshot_ts"] == 1795000000.0
        assert entry["验证报告ID"]
        assert entry["血缘版本ID"]

        verification = replay.lineage_verification_history("metric_history")
        assert verification["versions"]
        assert verification["reports"]
        assert verification["reports"][0]["report_id"] == entry["验证报告ID"]

    def test_version_detail_contains_reports_and_history(self, lineage, replay):
        version = self._confirmed_metric(lineage, "metric_detail")
        task = replay.create_replay_job(
            "详情回放", CHECKPOINT, "", metric_name="metric_detail",
        )
        replay.execute_replay(task["task_id"])

        detail = lineage.version_detail(version["version_id"])
        assert detail["version"]["version_id"] == version["version_id"]
        assert detail["checkpoint"]["checkpoint_id"] == CHECKPOINT
        assert detail["reports"]
        assert detail["replay_history"]
        assert detail["constant"] is True

    def test_history_version_cannot_be_confirmed_again(self, lineage):
        lineage.refresh_lineage("metric_old_confirm", checkpoint_id=CHECKPOINT)
        first = lineage.current_version("metric_old_confirm")
        assert lineage.confirm_version(first["version_id"])["ok"] is True

        _add_operator("新路径算子")
        lineage.refresh_lineage("metric_old_confirm")
        rejected = lineage.confirm_version(first["version_id"])
        assert rejected["ok"] is False
        assert "取代" in rejected["error"]

        row = next(
            r for r in store.rows("metric_lineage")
            if r.get("指标名称") == "metric_old_confirm"
        )
        assert row["路径版本"] != first["path_version"]
        assert row["血缘版本ID"] == lineage.current_version("metric_old_confirm")["version_id"]

    def test_history_version_cannot_be_marked_broken(self, lineage):
        lineage.refresh_lineage("metric_old_broken")
        first = lineage.current_version("metric_old_broken")
        _add_operator("新路径算子2")
        lineage.refresh_lineage("metric_old_broken")

        rejected = lineage.mark_version_broken(first["version_id"])
        assert rejected["ok"] is False
        current = lineage.current_version("metric_old_broken")
        assert current["status"] == "变更中"
        assert current["version_id"] != first["version_id"]

    def test_refresh_failure_in_transaction_scope(self, lineage, monkeypatch):
        lineage.refresh_lineage("metric_tx")
        before = lineage.current_version("metric_tx")
        original = store.lineage_versions

        def broken_versions(metric=None, *, current_only=False):
            raise RuntimeError("读取台账失败")

        monkeypatch.setattr(store, "lineage_versions", broken_versions)
        failed = lineage.refresh_lineage("metric_tx", source_batch="batch_broken")
        assert failed["ok"] is False

        monkeypatch.setattr(store, "lineage_versions", original)
        after = lineage.current_version("metric_tx")
        assert after["version_id"] == before["version_id"]
        assert after["current"] is True

    def test_domain_snapshots_expose_version_metrics(self, lineage, replay):
        self._confirmed_metric(lineage, "metric_snapshot")
        task = replay.create_replay_job(
            "快照回放", CHECKPOINT, "", metric_name="metric_snapshot",
        )
        replay.execute_replay(task["task_id"])

        lineage_snapshot = lineage.domain_snapshot()
        assert lineage_snapshot["version_count"] >= 1
        assert lineage_snapshot["bound_records"] >= 1

        replay_snapshot = replay.domain_snapshot()
        assert replay_snapshot["version_bound_tasks"] >= 1
        assert replay_snapshot["reports"] >= 1
