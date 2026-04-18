"""verify_fpln_rollover.py

集成验证：
1. 基础 FPL/DEP/ARR 流程 + CSV 列头正确
2. FPL DOF 由 ETD(UTC)+8 推算：有 DOF/ 字段时用 DOF_UTC+HHMM，无时用收报北京日推算
3. FPL ETD 跨日边界：ETD HHMM=1610 UTC → 北京 00:10 次日 → DOF 为收报北京日 +1
4. cleanup_old_flight_plans 只保留 dof >= today-2
5. daily_flight_plans 严格按 dof 筛选
6. CSV 列头包含「进区域时间」「出区域时间」「飞行程序」「使用跑道」
7. CAT062 track 的 sid/star/runway 回写到对应飞行计划
8. DEP 昨日回退：今日收到 DEP 但无当日计划，关联到昨日 atd=None 的计划
9. ARR 昨日回退：今日收到 ARR 但无当日计划，关联到昨日 ata=None 的计划
10. ARR 跨午夜修正：收报北京日+HHMM 转北京超出收报日 → ATA 退一天，dof 保持不变
11. DOF/ 字段存在时：用 DOF_UTC(6位) + ETD_HHMM 组合 UTC datetime → +8 得执飞日
    跨日验证：DOF_UTC=260401，ETD=1630 UTC → 北京 00:30 次日 → 执飞日=4-2
12. ARR 跨午夜 ATA 修正（原场景 11）
13. 无 DOF/ 字段 + HHMM > 1600：ETD/ATD UTC 日期取 base_day-1，执飞日=base_day
    FPL 示例：北京 4-2 12:00 收报（UTC 04:00），ETD=2350 UTC → UTC 日=4-1 → 北京 4-2 07:50 → 执飞日=4-2
    DEP 示例：北京 4-2 12:00 收报，ATD=1800 UTC → UTC 日=4-1 → 北京 4-2 02:00 → 执飞日=4-2（初步 dof）
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from atc_data_hub.config import load_app_config
from atc_data_hub.models import RadarTrack, FlightDestination
from atc_data_hub.parsers.aftn import AftnParser
from atc_data_hub.reference import load_reference_data
from atc_data_hub.state import ProtectorState
from atc_data_hub.storage import FPLN_HEADERS, StorageManager



def _check(condition: bool, msg: str) -> None:
    if not condition:
        raise AssertionError(msg)


def main() -> int:
    config = load_app_config(PROJECT_ROOT / "config" / "default.json")
    reference = load_reference_data(config.paths.sys_config_root)
    parser = AftnParser(reference)
    state = ProtectorState(reference_data=reference, track_region=config.runtime.track_region)
    storage = StorageManager(config)
    storage.ensure_layout()

    # -----------------------------------------------------------------------
    # 场景 1：正常 FPL/DEP/ARR
    # FPL ETD HHMM=0125 UTC → 北京 09:25 同日 → DOF=2026-04-01
    # 收报 UTC=2026-04-01 00:05，base_day=北京 2026-04-01 08:05 → 4-1
    # -----------------------------------------------------------------------
    messages_day1 = [
        (
            "(FPL-AAR371/A4101-IS-B789/H-SDE2E3FGHIJ1J3J5J7J8J9M1RWXYZ/LB1-ZBAA0125-N0491F330 SAREX N892 SIERA-ZGSZ0437-DOF/260401)",
            datetime(2026, 4, 1, 0, 5, 0),   # UTC 收报
        ),
        (
            "(DEP-AAR371/A4101-ZBAA0125-ZGSZ-DOF/260401)",
            datetime(2026, 4, 1, 1, 30, 0),
        ),
        (
            "(ARR-AAR371-ZBAA-ZGSZ0502)",
            datetime(2026, 4, 1, 5, 5, 0),
        ),
    ]

    for text, received_at in messages_day1:
        result = parser.parse(text, received_at=received_at)
        if not result.accepted or result.flight_plan is None:
            raise RuntimeError(f"parse failed: {text} -> {result.errors}")
        state.record_aftn_message(result.message)
        state.upsert_flight_plan(result.flight_plan, result.action)

    # ETD=0125 UTC → 北京=09:25 同日 → DOF=4-1
    aar371 = next((p for p in state.flight_plans.values() if p.callsign == "AAR371"), None)
    _check(aar371 is not None, "AAR371 plan not found")
    _check(aar371.dof == date(2026, 4, 1), f"AAR371 dof should be 2026-04-01, got {aar371.dof}")
    _check(aar371.ata is not None, "AAR371 should have ATA after ARR")

    # -----------------------------------------------------------------------
    # 场景 2：无 DOF 字段 FPL，ETD HHMM=2350 UTC（>1600）
    # 新规则：HHMM > 1600 时，etd_utc 日期取 base_day-1，执飞日 = base_day
    # 收报 UTC=2026-04-02 15:59 → base_day=北京 2026-04-02 23:59 → 4-2
    # etd_utc = 2026-04-01 23:50（前一天 UTC） → +8 = 2026-04-02 07:50 → DOF=4-2
    # -----------------------------------------------------------------------
    fpl_no_dof = "(FPL-CSN6666/A1234-IS-B738/M-SDFGHIJ1/LB1-ZBAA2350-N0450F280 NODOF TEST-ZGSZ0130-0)"
    result_no_dof = parser.parse(fpl_no_dof, received_at=datetime(2026, 4, 2, 15, 59, 0))
    _check(result_no_dof.accepted and result_no_dof.flight_plan is not None, "CSN6666 FPL parse failed")
    csn6666 = result_no_dof.flight_plan
    # 新规则：HHMM=2350 > 1600 → UTC 日=4-1 → 北京 4-2 07:50 → DOF=4-2
    _check(csn6666.dof == date(2026, 4, 2),
           f"CSN6666 dof should be 2026-04-02 (no-DOF + HHMM=2350>1600 → UTC 4-1 → 北京 4-2), got {csn6666.dof}")
    _check(csn6666.etd is not None and csn6666.etd.date() == date(2026, 4, 1),
           f"CSN6666 etd_utc date should be 2026-04-01, got {csn6666.etd}")

    # -----------------------------------------------------------------------
    # 场景 3：ARR 无 DOF 字段，收报 UTC 16:05 → 北京 2026-04-03 00:05 → DOF=4-3
    # -----------------------------------------------------------------------
    arr_no_dof = "(ARR-CCA001-ZBAA-ZGSZ0005)"
    result_arr = parser.parse(arr_no_dof, received_at=datetime(2026, 4, 2, 16, 5, 0))
    _check(result_arr.accepted and result_arr.flight_plan is not None, "CCA001 ARR parse failed")
    cca001_arr = result_arr.flight_plan
    _check(cca001_arr.dof == date(2026, 4, 3), f"CCA001 ARR dof should be 2026-04-03, got {cca001_arr.dof}")

    # -----------------------------------------------------------------------
    # 场景 4：FPL 无 DOF/ 字段，ETD HHMM=1610（>1600）
    # 新规则：HHMM > 1600 → etd_utc 日期取 base_day-1，执飞日=base_day
    # 收报 UTC=2026-04-03 07:00 → base_day=北京 4-3 15:00 → 4-3
    # etd_utc = 2026-04-02 16:10 → +8 = 2026-04-03 00:10 → DOF=4-3
    # -----------------------------------------------------------------------
    fpl_cross = "(FPL-MU5678/A5678-IS-A320/M-SDFGHIJ1/LB1-ZBAA1610-N0450F280 CROSS DAY TEST-ZGSZ0230-0)"
    result_cross = parser.parse(fpl_cross, received_at=datetime(2026, 4, 3, 7, 0, 0))
    _check(result_cross.accepted and result_cross.flight_plan is not None, "MU5678 FPL parse failed")
    mu5678_fpl = result_cross.flight_plan
    _check(mu5678_fpl.dof == date(2026, 4, 3),
           f"MU5678 dof should be 2026-04-03 (no-DOF + HHMM=1610>1600 → UTC 4-2 → 北京 4-3 00:10), got {mu5678_fpl.dof}")
    # ETD 应存储为 UTC datetime（4-2 16:10）
    _check(mu5678_fpl.etd is not None
           and mu5678_fpl.etd.date() == date(2026, 4, 2)
           and mu5678_fpl.etd.hour == 16 and mu5678_fpl.etd.minute == 10,
           f"MU5678 ETD should be 2026-04-02 16:10 UTC, got {mu5678_fpl.etd}")

    # -----------------------------------------------------------------------
    # 场景 5：cleanup_old_flight_plans 只保留 dof >= today-2
    # today=2026-04-06 → cutoff=2026-04-04，dof=4-1 的 AAR371 应被清理
    # -----------------------------------------------------------------------
    state_cleanup = ProtectorState(reference_data=reference)
    for text, received_at in messages_day1:
        result = parser.parse(text, received_at=received_at)
        if result.accepted and result.flight_plan:
            state_cleanup.upsert_flight_plan(result.flight_plan, result.action)
    # 加一个 dof=4-5 的计划（ETD=0800 UTC → 北京=16:00 同日 → DOF=4-5）
    fpl_day5 = "(FPL-CZ3333/A9999-IS-A321/M-SDFGHIJ1/LB1-ZBAA0800-N0450F280 RECENT TEST-ZGSZ0200-0)"
    r = parser.parse(fpl_day5, received_at=datetime(2026, 4, 5, 0, 5, 0))
    if r.accepted and r.flight_plan:
        state_cleanup.upsert_flight_plan(r.flight_plan, r.action)

    state_cleanup.cleanup_old_flight_plans(datetime(2026, 4, 6, 12, 0, 0), days=2)
    remaining_callsigns = {p.callsign for p in state_cleanup.flight_plans.values()}
    _check("AAR371" not in remaining_callsigns,
           f"AAR371(dof=4-1) should be cleaned up when today=4-6, remaining={remaining_callsigns}")
    _check("CZ3333" in remaining_callsigns,
           f"CZ3333(dof=4-5) should be kept when today=4-6")

    # -----------------------------------------------------------------------
    # 场景 6：CSV 列头验证
    # -----------------------------------------------------------------------
    export_day = date(2099, 1, 1)
    export_path = storage.write_fpln_csv(export_day, list(state.flight_plans.values()))
    rows = export_path.read_text(encoding="utf-8").replace("\r", "").splitlines()
    expected_header = ",".join(FPLN_HEADERS)
    _check(rows and rows[0] == expected_header,
           f"unexpected FPLN header:\n  got:      {rows[:1]}\n  expected: {expected_header}")
    _check("进区域时间" in expected_header, "FPLN_HEADERS missing 进区域时间")
    _check("出区域时间" in expected_header, "FPLN_HEADERS missing 出区域时间")
    _check("飞行程序" in expected_header, "FPLN_HEADERS missing 飞行程序")
    _check("使用跑道" in expected_header, "FPLN_HEADERS missing 使用跑道")

    # -----------------------------------------------------------------------
    # 场景 7：daily_flight_plans 严格按 dof 筛选
    # -----------------------------------------------------------------------
    state_daily = ProtectorState(reference_data=reference)
    for text, received_at in messages_day1:
        result = parser.parse(text, received_at=received_at)
        if result.accepted and result.flight_plan:
            state_daily.upsert_flight_plan(result.flight_plan, result.action)
    # 加一个 ETD=0010 UTC（北京=08:10 同日）→ DOF=4-2 的计划
    # 收报 UTC=2026-04-02 00:05 → base_day=4-2 → etd_utc=4-2 00:10 → 北京=4-2 08:10 → DOF=4-2
    fpl_day2 = "(FPL-AMU006/A3525-IS-A321/M-SDFGHIJ1/LB1-VMMC0010-N0450F280 ANOTHER-ZBAA0319-0)"
    r2 = parser.parse(fpl_day2, received_at=datetime(2026, 4, 2, 0, 5, 0))
    if r2.accepted and r2.flight_plan:
        state_daily.upsert_flight_plan(r2.flight_plan, r2.action)

    day1_plans = state_daily.daily_flight_plans(date(2026, 4, 1))
    day2_plans = state_daily.daily_flight_plans(date(2026, 4, 2))
    _check(all(p.dof == date(2026, 4, 1) for p in day1_plans),
           f"day1 plans contain wrong dof: {[p.dof for p in day1_plans]}")
    _check(all(p.dof == date(2026, 4, 2) for p in day2_plans),
           f"day2 plans contain wrong dof: {[p.dof for p in day2_plans]}")
    _check(any(p.callsign == "AAR371" for p in day1_plans), "AAR371 not in day1 plans")
    _check(any(p.callsign == "AMU006" for p in day2_plans), "AMU006 not in day2 plans")

    # -----------------------------------------------------------------------
    # 场景 8：CAT062 track 的 SID/STAR/跑道回写到飞行计划
    # -----------------------------------------------------------------------
    state_radar = ProtectorState(reference_data=reference)
    # ETD=0800 UTC → 北京=16:00 同日 → DOF=4-6
    fpl_radar = "(FPL-CSH801/A7001-IS-A320/M-SDFGHIJ1/LB1-ZBAA0800-N0450F280 RADAR TEST-ZGSZ0200-0)"
    r_radar = parser.parse(fpl_radar, received_at=datetime(2026, 4, 5, 22, 0, 0))
    _check(r_radar.accepted and r_radar.flight_plan is not None, "CSH801 FPL parse failed")
    state_radar.upsert_flight_plan(r_radar.flight_plan, r_radar.action)

    dep_track = RadarTrack(
        track_number=1001,
        acid="CSH801",
        adep="ZBAA",
        adst="ZGSZ",
        sid="ELKUR5D",
        star="",
        runway="36L",
        flight_destination=FlightDestination.OUTBOUND,
        time_of_track=datetime(2026, 4, 6, 0, 5, 0),
    )
    state_radar.ingest_radar_track(dep_track)

    csh801 = next((p for p in state_radar.flight_plans.values() if p.callsign == "CSH801"), None)
    _check(csh801 is not None, "CSH801 plan not found after radar track")
    _check(csh801.procedure == "ELKUR5D",
           f"CSH801 procedure should be ELKUR5D (SID), got '{csh801.procedure}'")
    _check(csh801.runway == "36L",
           f"CSH801 runway should be 36L, got '{csh801.runway}'")

    # ETD=0900 UTC → 北京=17:00 同日 → DOF=4-6
    fpl_arr_radar = "(FPL-CCA202/A7002-IS-B738/M-SDFGHIJ1/LB1-ZGSZ0900-N0450F280 ARR TEST-ZBAA1100-0)"
    r_arr = parser.parse(fpl_arr_radar, received_at=datetime(2026, 4, 5, 22, 0, 0))
    _check(r_arr.accepted and r_arr.flight_plan is not None, "CCA202 FPL parse failed")
    state_radar.upsert_flight_plan(r_arr.flight_plan, r_arr.action)

    arr_track = RadarTrack(
        track_number=1002,
        acid="CCA202",
        adep="ZGSZ",
        adst="ZBAA",
        sid="",
        star="LATLO1A",
        runway="01",
        flight_destination=FlightDestination.INBOUND,
        time_of_track=datetime(2026, 4, 6, 3, 0, 0),
    )
    state_radar.ingest_radar_track(arr_track)

    cca202 = next((p for p in state_radar.flight_plans.values() if p.callsign == "CCA202"), None)
    _check(cca202 is not None, "CCA202 plan not found after radar track")
    _check(cca202.procedure == "LATLO1A",
           f"CCA202 procedure should be LATLO1A (STAR), got '{cca202.procedure}'")
    _check(cca202.runway == "01",
           f"CCA202 runway should be 01, got '{cca202.runway}'")

    # -----------------------------------------------------------------------
    # 场景 9：DEP 昨日回退
    # FPL: ETD=0800 UTC → 北京=16:00 4-7 → DOF=4-7
    #      收报 UTC=2026-04-07 00:10 → base_day=北京 4-7 08:10 → etd_utc=4-7 08:00 → 北京=4-7 16:00 → DOF=4-7
    # DEP: 收报 UTC=2026-04-08 00:05 → 北京=4-8 08:05 → 初步 dof=4-8
    #      → 无 4-8 计划 → 查 4-7(=4-8-1) 计划 atd is None → 回退，关联到 4-7 计划，dof 保持 4-7
    # -----------------------------------------------------------------------
    state_dep_rollback = ProtectorState(reference_data=reference)
    fpl_rb = "(FPL-HXA100/A1001-IS-A320/M-SDFGHIJ1/LB1-ZBAA0800-N0450F280 DEP ROLLBACK-ZGSZ0200-0)"
    r_rb = parser.parse(fpl_rb, received_at=datetime(2026, 4, 7, 0, 10, 0))
    _check(r_rb.accepted and r_rb.flight_plan is not None, "HXA100 FPL parse failed")
    state_dep_rollback.upsert_flight_plan(r_rb.flight_plan, r_rb.action)
    hxa100_before = next((p for p in state_dep_rollback.flight_plans.values() if p.callsign == "HXA100"), None)
    _check(hxa100_before is not None, "HXA100 plan not found")
    _check(hxa100_before.dof == date(2026, 4, 7),
           f"HXA100 FPL dof should be 2026-04-07, got {hxa100_before.dof}")
    _check(hxa100_before.atd is None, "HXA100 ATD should be None before DEP")

    # DEP 收报 UTC=4-8 00:05 → 北京=4-8 08:05 → 初步 dof=4-8，无 4-8 计划 → 回退到 4-7
    dep_rb = "(DEP-HXA100/A1001-ZBAA0805-ZGSZ-0)"
    r_dep_rb = parser.parse(dep_rb, received_at=datetime(2026, 4, 8, 0, 5, 0))
    _check(r_dep_rb.accepted and r_dep_rb.flight_plan is not None, "HXA100 DEP parse failed")
    state_dep_rollback.upsert_flight_plan(r_dep_rb.flight_plan, r_dep_rb.action)

    hxa100 = next((p for p in state_dep_rollback.flight_plans.values() if p.callsign == "HXA100"), None)
    _check(hxa100 is not None, "HXA100 plan not found after DEP")
    _check(hxa100.dof == date(2026, 4, 7),
           f"HXA100 dof should remain 2026-04-07 after DEP rollback, got {hxa100.dof}")
    _check(hxa100.atd is not None,
           "HXA100 ATD should be set after DEP rollback")
    # 只应有一份计划（不能多出一份 4-8 的计划）
    hxa100_plans = [p for p in state_dep_rollback.flight_plans.values() if p.callsign == "HXA100"]
    _check(len(hxa100_plans) == 1,
           f"HXA100 should have exactly 1 plan, got {len(hxa100_plans)}: {[p.dof for p in hxa100_plans]}")

    # -----------------------------------------------------------------------
    # 场景 10：ARR 昨日回退
    # FPL: ETD=0700 UTC → 北京=15:00 4-7 → DOF=4-7
    #      收报 UTC=2026-04-07 00:10
    # ARR: 收报 UTC=4-8 00:05 → 北京=4-8 → 初步 dof=4-8，无计划 → 回退到 4-7
    # -----------------------------------------------------------------------
    state_arr_rollback = ProtectorState(reference_data=reference)
    fpl_arr_rb = "(FPL-GCR200/A2002-IS-B738/M-SDFGHIJ1/LB1-ZGSZ0700-N0450F280 ARR ROLLBACK-ZBAA0900-0)"
    r_arr_rb = parser.parse(fpl_arr_rb, received_at=datetime(2026, 4, 7, 0, 10, 0))
    _check(r_arr_rb.accepted and r_arr_rb.flight_plan is not None, "GCR200 FPL parse failed")
    state_arr_rollback.upsert_flight_plan(r_arr_rb.flight_plan, r_arr_rb.action)
    gcr200_before = next((p for p in state_arr_rollback.flight_plans.values() if p.callsign == "GCR200"), None)
    _check(gcr200_before is not None, "GCR200 plan not found")
    _check(gcr200_before.dof == date(2026, 4, 7),
           f"GCR200 FPL dof should be 2026-04-07, got {gcr200_before.dof}")

    # ARR 收报 UTC=4-8 00:05 → 北京=4-8 → 初步 dof=4-8，无计划 → 回退到 4-7
    arr_rb = "(ARR-GCR200-ZGSZ-ZBAA0805)"
    r_arr_msg = parser.parse(arr_rb, received_at=datetime(2026, 4, 8, 0, 5, 0))
    _check(r_arr_msg.accepted and r_arr_msg.flight_plan is not None, "GCR200 ARR parse failed")
    state_arr_rollback.upsert_flight_plan(r_arr_msg.flight_plan, r_arr_msg.action)

    gcr200 = next((p for p in state_arr_rollback.flight_plans.values() if p.callsign == "GCR200"), None)
    _check(gcr200 is not None, "GCR200 plan not found after ARR")
    _check(gcr200.dof == date(2026, 4, 7),
           f"GCR200 dof should remain 2026-04-07 after ARR rollback, got {gcr200.dof}")
    _check(gcr200.ata is not None,
           "GCR200 ATA should be set after ARR rollback")
    gcr200_plans = [p for p in state_arr_rollback.flight_plans.values() if p.callsign == "GCR200"]
    _check(len(gcr200_plans) == 1,
           f"GCR200 should have exactly 1 plan, got {len(gcr200_plans)}: {[p.dof for p in gcr200_plans]}")

    # -----------------------------------------------------------------------
    # 场景 11：DOF/ 字段存在 → 用 DOF_UTC + ETD_HHMM 推算执飞日（北京时）
    #
    # 子场景 A（无跨日）：
    #   DOF/260401（UTC 4-1），ETD=0800 UTC → 北京=16:00 同日 → 执飞日=4-1
    #   收报 UTC=2026-04-01 07:55（北京=15:55，4-1）
    #
    # 子场景 B（跨日）：
    #   DOF/260401（UTC 4-1），ETD=1630 UTC → 北京=00:30 次日（4-2）→ 执飞日=4-2
    #   收报 UTC=2026-04-01 16:25（北京=2026-04-02 00:25，base_day=4-2）
    #   若只用收报北京日 base_day=4-2 + HHMM=1630 → UTC 4-2 16:30 → 北京 4-3 00:30 → DOF=4-3（错！）
    #   正确路径：读 DOF/260401 → UTC 4-1 16:30 → 北京 4-2 00:30 → 执飞日=4-2 ✓
    # -----------------------------------------------------------------------
    # 子场景 A
    fpl_dof_a = "(FPL-XDF001/A8001-IS-A320/M-SDFGHIJ1/LB1-ZBAA0800-N0450F280 DOF TEST A-ZGSZ0200-DOF/260401)"
    r_dof_a = parser.parse(fpl_dof_a, received_at=datetime(2026, 4, 1, 7, 55, 0))
    _check(r_dof_a.accepted and r_dof_a.flight_plan is not None, "XDF001 FPL parse failed")
    _check(r_dof_a.flight_plan.dof == date(2026, 4, 1),
           f"XDF001 dof should be 2026-04-01 (DOF/260401 + ETD 0800 UTC → 北京 16:00), got {r_dof_a.flight_plan.dof}")
    _check(r_dof_a.flight_plan.etd is not None and r_dof_a.flight_plan.etd.hour == 8,
           f"XDF001 ETD should be 08:xx UTC, got {r_dof_a.flight_plan.etd}")

    # 子场景 B：DOF/260401 + ETD=1630 UTC → 北京 00:30 次日 → 执飞日=4-2
    fpl_dof_b = "(FPL-XDF002/A8002-IS-A320/M-SDFGHIJ1/LB1-ZBAA1630-N0450F280 DOF TEST B-ZGSZ0200-DOF/260401)"
    r_dof_b = parser.parse(fpl_dof_b, received_at=datetime(2026, 4, 1, 16, 25, 0))
    _check(r_dof_b.accepted and r_dof_b.flight_plan is not None, "XDF002 FPL parse failed")
    _check(r_dof_b.flight_plan.dof == date(2026, 4, 2),
           f"XDF002 dof should be 2026-04-02 (DOF/260401 + ETD 1630 UTC → 北京 00:30 次日), got {r_dof_b.flight_plan.dof}")
    _check(r_dof_b.flight_plan.etd is not None
           and r_dof_b.flight_plan.etd.day == 1 and r_dof_b.flight_plan.etd.hour == 16 and r_dof_b.flight_plan.etd.minute == 30,
           f"XDF002 ETD should be UTC 4-1 16:30, got {r_dof_b.flight_plan.etd}")

    # -----------------------------------------------------------------------
    # 场景 12：ARR 跨午夜 ATA 修正（原场景 11）
    # FPL: ETD=0800 UTC → 北京=16:00 4-7 → DOF=4-7（收报 UTC=4-7 00:10）
    # ARR: HHMM=1605 UTC，收报 UTC=4-7 16:10 → 北京=4-8 00:10 → base_day=4-8
    #      ata_candidate = combine(4-8, 1605) → UTC 4-8 16:05 → 北京 4-9 00:05 > base_day(4-8)
    #      → 退一天：ata = UTC 4-7 16:05（北京 4-8 00:05）✓
    #      ARR dof(初步)=4-8，无 4-8 计划 → 回退到 4-7；
    #      apply_update：ARR 不覆盖 dof → plan.dof 保持 4-7
    # 最终：dof=4-7，ata=datetime(4-7,16,5) UTC，只有一份计划
    # -----------------------------------------------------------------------
    state_arr_midnight = ProtectorState(reference_data=reference)
    fpl_midnight = "(FPL-SKY500/A5005-IS-A320/M-SDFGHIJ1/LB1-ZGSZ0800-N0450F280 MIDNIGHT TEST-ZBAA0200-0)"
    r_midnight_fpl = parser.parse(fpl_midnight, received_at=datetime(2026, 4, 7, 0, 10, 0))
    _check(r_midnight_fpl.accepted and r_midnight_fpl.flight_plan is not None, "SKY500 FPL parse failed")
    state_arr_midnight.upsert_flight_plan(r_midnight_fpl.flight_plan, r_midnight_fpl.action)
    sky500_before = next((p for p in state_arr_midnight.flight_plans.values() if p.callsign == "SKY500"), None)
    _check(sky500_before is not None, "SKY500 plan not found")
    _check(sky500_before.dof == date(2026, 4, 7),
           f"SKY500 FPL dof should be 2026-04-07, got {sky500_before.dof}")

    # ARR 落地 HHMM=1605 UTC，收报 UTC=4-7 16:10（北京 4-8 00:10）
    arr_midnight = "(ARR-SKY500-ZGSZ-ZBAA1605)"
    r_midnight_arr = parser.parse(arr_midnight, received_at=datetime(2026, 4, 7, 16, 10, 0))
    _check(r_midnight_arr.accepted and r_midnight_arr.flight_plan is not None, "SKY500 ARR parse failed")
    # 验证解析结果：ata 日期应退一天（UTC 4-7，而非 4-8）
    parsed_ata = r_midnight_arr.flight_plan.ata
    _check(parsed_ata is not None, "SKY500 parsed ATA should not be None")
    _check(parsed_ata.date() == date(2026, 4, 7),
           f"SKY500 parsed ATA UTC date should be 2026-04-07 (midnight fix), got {parsed_ata}")
    _check(parsed_ata.hour == 16 and parsed_ata.minute == 5,
           f"SKY500 parsed ATA UTC time should be 16:05, got {parsed_ata}")

    state_arr_midnight.upsert_flight_plan(r_midnight_arr.flight_plan, r_midnight_arr.action)

    sky500 = next((p for p in state_arr_midnight.flight_plans.values() if p.callsign == "SKY500"), None)
    _check(sky500 is not None, "SKY500 plan not found after ARR")
    _check(sky500.dof == date(2026, 4, 7),
           f"SKY500 dof should remain 2026-04-07 after midnight ARR, got {sky500.dof}")
    _check(sky500.ata is not None, "SKY500 ATA should be set after ARR")
    _check(sky500.ata.date() == date(2026, 4, 7),
           f"SKY500 ATA UTC date should be 2026-04-07, got {sky500.ata}")
    sky500_plans = [p for p in state_arr_midnight.flight_plans.values() if p.callsign == "SKY500"]
    _check(len(sky500_plans) == 1,
           f"SKY500 should have exactly 1 plan, got {len(sky500_plans)}: {[p.dof for p in sky500_plans]}")

    # -----------------------------------------------------------------------
    # 场景 13：无 DOF/ 字段 + HHMM > 1600
    # 规则：ETD/ATD 的 UTC 日期取收报北京日前一天，执飞日（dof）= 收报北京日。
    #
    # 子场景 A — FPL：
    #   北京 2026-04-02 12:00 收到 FPL（即 UTC 2026-04-02 04:00），base_day=4-2
    #   ETD HHMM=2350 UTC（>1600） → etd_utc = 2026-04-01 23:50
    #   etd_utc +8 = 2026-04-02 07:50（北京） → dof=4-2 ✓
    #
    # 子场景 B — DEP：
    #   同收报时刻（UTC 2026-04-02 04:00），base_day=4-2
    #   ATD HHMM=1800 UTC（>1600） → atd_utc = 2026-04-01 18:00
    #   atd_utc 是起飞时刻，dof 初步 = base_day = 4-2
    #
    # 子场景 C — 边界值 HHMM=1600：不触发规则（<=1600），UTC 日=当日
    #   ETD HHMM=1600 UTC → etd_utc = 2026-04-02 16:00 → +8 = 2026-04-03 00:00 → dof=4-3
    #   （1600 恰好是 +8 后北京 00:00，仍归"今日 UTC"路径，执飞日为 base_day+1）
    #
    # 子场景 D — HHMM=1601（刚好超过临界）：触发规则
    #   ETD HHMM=1601 UTC（>1600） → etd_utc = 2026-04-01 16:01 → +8 = 2026-04-02 00:01 → dof=4-2
    # -----------------------------------------------------------------------

    # 子场景 A：FPL 无 DOF/，HHMM=2350
    # 收报 UTC=2026-04-02 04:00（北京=12:00，base_day=4-2）
    fpl_late_etd = "(FPL-LTE100/A9100-IS-B738/M-SDFGHIJ1/LB1-ZBAA2350-N0450F280 LATE ETD-ZGSZ0130-0)"
    r_late = parser.parse(fpl_late_etd, received_at=datetime(2026, 4, 2, 4, 0, 0))
    _check(r_late.accepted and r_late.flight_plan is not None, "LTE100 FPL parse failed")
    lte100 = r_late.flight_plan
    _check(lte100.dof == date(2026, 4, 2),
           f"LTE100 dof should be 2026-04-02 (no-DOF + HHMM=2350 > 1600), got {lte100.dof}")
    _check(lte100.etd is not None and lte100.etd.date() == date(2026, 4, 1),
           f"LTE100 etd_utc date should be 2026-04-01, got {lte100.etd}")
    _check(lte100.etd.hour == 23 and lte100.etd.minute == 50,
           f"LTE100 etd_utc time should be 23:50, got {lte100.etd}")

    # 子场景 B：DEP 无 DOF/，HHMM=1800
    dep_late = "(DEP-LTE100/A9100-ZBAA1800-ZGSZ-0)"
    r_dep_late = parser.parse(dep_late, received_at=datetime(2026, 4, 2, 4, 0, 0))
    _check(r_dep_late.accepted and r_dep_late.flight_plan is not None, "LTE100 DEP parse failed")
    dep_lte100 = r_dep_late.flight_plan
    _check(dep_lte100.dof == date(2026, 4, 2),
           f"LTE100 DEP dof should be 2026-04-02, got {dep_lte100.dof}")
    _check(dep_lte100.atd is not None and dep_lte100.atd.date() == date(2026, 4, 1),
           f"LTE100 DEP atd_utc date should be 2026-04-01 (HHMM=1800 > 1600), got {dep_lte100.atd}")
    _check(dep_lte100.atd.hour == 18 and dep_lte100.atd.minute == 0,
           f"LTE100 DEP atd_utc time should be 18:00, got {dep_lte100.atd}")

    # 子场景 C：边界值 HHMM=1600，不触发 >1600 规则，走"今日 UTC"路径
    # base_day=4-2，etd_utc=4-2 16:00 → +8=4-3 00:00 → dof=4-3
    fpl_boundary = "(FPL-LTE200/A9200-IS-B738/M-SDFGHIJ1/LB1-ZBAA1600-N0450F280 BOUNDARY-ZGSZ0130-0)"
    r_boundary = parser.parse(fpl_boundary, received_at=datetime(2026, 4, 2, 4, 0, 0))
    _check(r_boundary.accepted and r_boundary.flight_plan is not None, "LTE200 FPL parse failed")
    lte200 = r_boundary.flight_plan
    _check(lte200.dof == date(2026, 4, 3),
           f"LTE200 dof should be 2026-04-03 (HHMM=1600 exact, UTC today → 北京 00:00 次日), got {lte200.dof}")
    _check(lte200.etd is not None and lte200.etd.date() == date(2026, 4, 2),
           f"LTE200 etd_utc date should be 2026-04-02 (UTC today), got {lte200.etd}")

    # 子场景 D：HHMM=1601，触发 >1600 规则
    # base_day=4-2，etd_utc=4-1 16:01 → +8=4-2 00:01 → dof=4-2
    fpl_just_over = "(FPL-LTE300/A9300-IS-B738/M-SDFGHIJ1/LB1-ZBAA1601-N0450F280 JUST OVER-ZGSZ0130-0)"
    r_just = parser.parse(fpl_just_over, received_at=datetime(2026, 4, 2, 4, 0, 0))
    _check(r_just.accepted and r_just.flight_plan is not None, "LTE300 FPL parse failed")
    lte300 = r_just.flight_plan
    _check(lte300.dof == date(2026, 4, 2),
           f"LTE300 dof should be 2026-04-02 (HHMM=1601 > 1600, UTC yesterday), got {lte300.dof}")
    _check(lte300.etd is not None and lte300.etd.date() == date(2026, 4, 1),
           f"LTE300 etd_utc date should be 2026-04-01, got {lte300.etd}")

    # -----------------------------------------------------------------------
    # 场景 14：ARR 无 DOF/ 字段 + ATA HHMM > 1600
    # 规则：无 DOF/ 字段时，若 HHMM > 1600（1601~2359），则 ATA UTC 日期为昨日，执飞日为今日。
    #
    # 示例1：北京 2026-04-02 16:02 收到 ARR 报文（即 UTC 2026-04-02 08:02）
    #   base_day = 4-2
    #   ATA HHMM=1601（>1600）→ ata_utc = UTC 2026-04-01 16:01
    #   ata_utc +8 = 北京 2026-04-02 00:01 → dof=4-2 ✓
    #
    # 示例2：北京 2026-04-02 20:00 收到 ARR 报文（即 UTC 2026-04-02 12:00）
    #   base_day = 4-2
    #   ATA HHMM=2350（>1600）→ ata_utc = UTC 2026-04-01 23:50
    #   ata_utc +8 = 北京 2026-04-02 07:50 → dof=4-2 ✓
    #
    # 示例3：边界值 HHMM=1600 → 不触发 >1600 规则，走"今日 UTC"路径
    #   base_day = 4-2，ata_utc = UTC 4-2 16:00 → +8=北京 4-3 00:00 → dof=4-3
    # -----------------------------------------------------------------------
    # 示例1：ATA HHMM=1601，收报 UTC=2026-04-02 08:02（北京 4-2 16:02）
    arr_1601 = "(ARR-TST001-ZBAA-ZGSZ1601)"
    r_arr_1601 = parser.parse(arr_1601, received_at=datetime(2026, 4, 2, 8, 2, 0))
    _check(r_arr_1601.accepted and r_arr_1601.flight_plan is not None, "TST001 ARR parse failed")
    tst001 = r_arr_1601.flight_plan
    _check(tst001.dof == date(2026, 4, 2),
           f"TST001 dof should be 2026-04-02 (no-DOF + ATA HHMM=1601 > 1600), got {tst001.dof}")
    _check(tst001.ata is not None and tst001.ata.date() == date(2026, 4, 1),
           f"TST001 ata_utc date should be 2026-04-01, got {tst001.ata}")
    _check(tst001.ata.hour == 16 and tst001.ata.minute == 1,
           f"TST001 ata_utc time should be 16:01, got {tst001.ata}")

    # 示例2：ATA HHMM=2350，收报 UTC=2026-04-02 12:00（北京 4-2 20:00）
    arr_2350 = "(ARR-TST002-ZBAA-ZGSZ2350)"
    r_arr_2350 = parser.parse(arr_2350, received_at=datetime(2026, 4, 2, 12, 0, 0))
    _check(r_arr_2350.accepted and r_arr_2350.flight_plan is not None, "TST002 ARR parse failed")
    tst002 = r_arr_2350.flight_plan
    _check(tst002.dof == date(2026, 4, 2),
           f"TST002 dof should be 2026-04-02 (no-DOF + ATA HHMM=2350 > 1600), got {tst002.dof}")
    _check(tst002.ata is not None and tst002.ata.date() == date(2026, 4, 1),
           f"TST002 ata_utc date should be 2026-04-01, got {tst002.ata}")
    _check(tst002.ata.hour == 23 and tst002.ata.minute == 50,
           f"TST002 ata_utc time should be 23:50, got {tst002.ata}")

    # 示例3：边界值 HHMM=1600，收报 UTC=2026-04-02 08:02（北京 4-2 16:02）
    arr_1600 = "(ARR-TST003-ZBAA-ZGSZ1600)"
    r_arr_1600 = parser.parse(arr_1600, received_at=datetime(2026, 4, 2, 8, 2, 0))
    _check(r_arr_1600.accepted and r_arr_1600.flight_plan is not None, "TST003 ARR parse failed")
    tst003 = r_arr_1600.flight_plan
    _check(tst003.dof == date(2026, 4, 3),
           f"TST003 dof should be 2026-04-03 (HHMM=1600 exact, UTC today → 北京 00:00 次日), got {tst003.dof}")
    _check(tst003.ata is not None and tst003.ata.date() == date(2026, 4, 2),
           f"TST003 ata_utc date should be 2026-04-02 (UTC today), got {tst003.ata}")
    _check(tst003.ata.hour == 16 and tst003.ata.minute == 0,
           f"TST003 ata_utc time should be 16:00, got {tst003.ata}")

    print(json.dumps({
        "status": "ALL CHECKS PASSED",
        "fpln_header": rows[0],
        "fpln_columns": FPLN_HEADERS,
        "aar371_dof": aar371.dof.isoformat(),
        "csn6666_dof_etd_cross_midnight": csn6666.dof.isoformat(),
        "cca001_arr_dof_utc_cross_midnight": cca001_arr.dof.isoformat(),
        "mu5678_fpl_dof": mu5678_fpl.dof.isoformat(),
        "mu5678_etd_utc": mu5678_fpl.etd.isoformat(timespec="minutes") if mu5678_fpl.etd else None,
        "cleanup_remaining": sorted(remaining_callsigns),
        "day1_callsigns": sorted(p.callsign for p in day1_plans),
        "day2_callsigns": sorted(p.callsign for p in day2_plans),
        "csh801_procedure": csh801.procedure,
        "csh801_runway": csh801.runway,
        "cca202_procedure": cca202.procedure,
        "cca202_runway": cca202.runway,
        "hxa100_dof_after_dep_rollback": hxa100.dof.isoformat(),
        "hxa100_atd_set": hxa100.atd is not None,
        "gcr200_dof_after_arr_rollback": gcr200.dof.isoformat(),
        "gcr200_ata_set": gcr200.ata is not None,
        "xdf001_dof_field_no_cross": r_dof_a.flight_plan.dof.isoformat(),
        "xdf002_dof_field_cross_midnight": r_dof_b.flight_plan.dof.isoformat(),
        "xdf002_etd_utc": r_dof_b.flight_plan.etd.isoformat(timespec="minutes") if r_dof_b.flight_plan.etd else None,
        "sky500_dof_after_midnight_arr": sky500.dof.isoformat(),
        "sky500_ata_utc": sky500.ata.isoformat(timespec="minutes") if sky500.ata else None,
        "lte100_fpl_no_dof_late_etd_dof": lte100.dof.isoformat(),
        "lte100_fpl_etd_utc": lte100.etd.isoformat(timespec="minutes"),
        "lte100_dep_dof": dep_lte100.dof.isoformat(),
        "lte100_dep_atd_utc": dep_lte100.atd.isoformat(timespec="minutes"),
        "lte200_boundary_1600_dof": lte200.dof.isoformat(),
        "lte300_just_over_1601_dof": lte300.dof.isoformat(),
        "tst001_arr_dof_1601": tst001.dof.isoformat(),
        "tst001_arr_ata_utc": tst001.ata.isoformat(timespec="minutes"),
        "tst002_arr_dof_2350": tst002.dof.isoformat(),
        "tst002_arr_ata_utc": tst002.ata.isoformat(timespec="minutes"),
        "tst003_arr_dof_1600": tst003.dof.isoformat(),
        "tst003_arr_ata_utc": tst003.ata.isoformat(timespec="minutes"),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
