from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from ..models import AftnMessage, FlightPlan
from ..reference import ReferenceData
from ..utils import parse_datetime

_UTC_PLUS_8 = timezone(timedelta(hours=8))


def _utc_to_beijing(utc_dt: datetime) -> datetime:
    """将 UTC datetime 转换为北京时间（UTC+8），结果为 naive datetime。"""
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(_UTC_PLUS_8).replace(tzinfo=None)


def _beijing_date_from_utc(utc_dt: datetime) -> date:
    """从 UTC datetime 获取对应的北京日期。"""
    return _utc_to_beijing(utc_dt).date()


class AftnParseError(ValueError):
    pass


@dataclass(slots=True)
class AftnParseResult:
    raw_text: str
    message: AftnMessage
    action: str = ""
    flight_plan: FlightPlan | None = None
    accepted: bool = False
    errors: list[str] = field(default_factory=list)


class AftnParser:
    SUPPORTED_TYPES = {"FPL", "DEP", "ARR", "DLA", "CPL", "EST"}

    def __init__(self, reference_data: ReferenceData | None = None) -> None:
        self.reference_data = reference_data or ReferenceData()

    def parse(self, payload: bytes | str | dict[str, Any], received_at: datetime | None = None) -> AftnParseResult:
        received_at = received_at or datetime.utcnow()
        wrapper = self._coerce_wrapper(payload)
        raw_text = wrapper.get("raw_text", "")
        message_time = parse_datetime(wrapper.get("utc_time")) or received_at
        raw_type = str(wrapper.get("message_type", "")).strip().upper()
        core_text = self._extract_core_message(raw_text)
        detected_type = self._detect_message_type(core_text) or raw_type
        message = AftnMessage(
            utc_time=message_time,
            message_type=detected_type,
            message_text=core_text or raw_text,
        )
        result = AftnParseResult(raw_text=raw_text, message=message, action=detected_type)
        if detected_type not in self.SUPPORTED_TYPES:
            result.errors.append("不支持的 AFTN 报文类型")
            return result

        if detected_type in {"CPL", "EST"}:
            result.accepted = True
            return result

        try:
            if detected_type == "FPL":
                plan = self._parse_fpl(core_text, message_time)
            elif detected_type == "DEP":
                plan = self._parse_dep_like(core_text, message_time, action="DEP")
            elif detected_type == "DLA":
                plan = self._parse_dep_like(core_text, message_time, action="DLA")
            else:
                plan = self._parse_arr(core_text, message_time)
            plan.source_message_type = detected_type
            plan.last_message_time = message_time
            result.flight_plan = plan
            result.accepted = True
        except AftnParseError as exc:
            result.errors.append(str(exc))
        return result

    def _coerce_wrapper(self, payload: bytes | str | dict[str, Any]) -> dict[str, str]:
        if isinstance(payload, dict):
            return {
                "raw_text": str(payload.get("MessageText", payload.get("message_text", ""))),
                "message_type": str(payload.get("MessageType", payload.get("message_type", ""))),
                "utc_time": str(payload.get("UtcTime", payload.get("utc_time", ""))),
            }
        if isinstance(payload, bytes):
            text = payload.decode("utf-8", errors="ignore")
        else:
            text = payload
        stripped = text.strip()
        if stripped.startswith("{") and "MessageText" in stripped:
            try:
                import json

                data = json.loads(stripped)
                return self._coerce_wrapper(data)
            except Exception:
                pass
        return {"raw_text": stripped, "message_type": "", "utc_time": ""}

    def _extract_core_message(self, text: str) -> str:
        if not text:
            return ""
        flat = text.replace("\r", "").replace("\n", "")
        start = flat.find("(")
        if start < 0:
            return flat.strip()
        end = flat.find(")", start)
        if end < 0:
            return flat[start:].strip()
        return flat[start : end + 1].strip()

    def _detect_message_type(self, text: str) -> str:
        if not text:
            return ""
        if not text.startswith("("):
            return ""
        prefix = text[1:4].upper()
        if prefix in self.SUPPORTED_TYPES:
            return prefix
        return ""

    def _parse_fpl(self, core_text: str, message_time: datetime) -> FlightPlan:
        fields = self._split_fields(core_text)
        if len(fields) < 9:
            raise AftnParseError(f"FPL 报文段数不足: {len(fields)}")
        callsign, ssr = self._parse_callsign_and_ssr(fields[1])
        if not callsign:
            raise AftnParseError("FPL 缺少呼号")

        departure = fields[5].strip().upper()
        arrival = fields[7].strip().upper()
        route_field = fields[6].strip().upper()
        route = route_field.split(" ", 1)[1].strip() if " " in route_field else route_field
        eet_minutes = self._hhmm_to_minutes(arrival[4:8])

        # DOF（执飞日）= ETD_UTC + 8h 所对应的北京日期。
        # 推算步骤：
        #   1. 优先读取报文中的 DOF/ 字段（格式 YYMMDD，UTC 日期）作为 ETD 的 UTC 日期基准；
        #      若字段不存在或解析失败，则按 HHMM 判断。
        #   2. 将确定的 UTC 日期 + 报文 ETD HHMM 组合成 UTC datetime。
        #   3. UTC datetime +8h → 北京时间，取其日期即为执飞日（北京时 DOF）。
        #
        # 无 DOF/ 字段时的 UTC 日期判断规则：
        #   - HHMM > 1600（含 1601~2359）：+8 后必然落入北京次日（>= 00:00），
        #     说明该 ETD 的 UTC 日期是收报北京日的前一天（yesterday），执飞日 = 收报北京日。
        #     示例：北京 4-2 20:00 收报，ETD=2350 UTC → UTC 日期=4-1 → 北京 4-2 07:50 → 执飞日=4-2
        #   - HHMM = 1600：+8 恰为 00:00，同样归入"昨日 UTC"路径（>16 * 60 + 0，严格大于）。
        #   - HHMM <= 1600（即 0000~1600）：UTC 今日 + HHMM，+8 后取北京日期（通常仍是 base_day）。
        base_day = _beijing_date_from_utc(message_time)
        etd_hhmm = departure[4:8]

        # 尝试从报文字段中提取 DOF/ (UTC 日)
        dof_utc_day: date | None = None
        for field in fields:
            marker = field.upper().find("DOF/")
            if marker >= 0:
                digits = field[marker + 4 : marker + 10]
                if len(digits) == 6 and digits.isdigit():
                    try:
                        dof_utc_day = datetime.strptime("20" + digits, "%Y%m%d").date()
                    except ValueError:
                        pass
                break

        if dof_utc_day is not None:
            # 有 DOF/ 字段：用 UTC 日 + HHMM 直接组合，无需猜测
            etd_utc = self._combine_day_hhmm(dof_utc_day, etd_hhmm)
            dof = _beijing_date_from_utc(etd_utc)
        else:
            # 无 DOF/ 字段：根据 ETD HHMM 判断 UTC 日期基准
            #   HHMM > 1600：该时刻 +8 必然跨入次日北京时间（≥ 00:00），
            #                 意味着 ETD 的 UTC 日期是 base_day 的前一天，
            #                 执飞日（北京时）= base_day。
            #   HHMM <= 1600：ETD +8 仍在同一北京日，UTC 日期取 base_day。
            etd_hour = int(etd_hhmm[:2])
            etd_minute = int(etd_hhmm[2:4])
            if etd_hour > 16 or (etd_hour == 16 and etd_minute > 0):
                # HHMM > 1600：UTC 昨日 + HHMM → +8 = 北京今日
                etd_utc = self._combine_day_hhmm(base_day - timedelta(days=1), etd_hhmm)
                dof = base_day
            else:
                # HHMM <= 1600：UTC 今日 + HHMM → +8 仍在今日（或恰好跨日时 dof=base_day+1）
                etd_utc = self._combine_day_hhmm(base_day, etd_hhmm)
                dof = _beijing_date_from_utc(etd_utc)

        etd = etd_utc  # 保存为 UTC

        plan = FlightPlan(
            callsign=callsign,
            adep=departure[:4],
            adest=arrival[:4],
            ssr=ssr,
            aircraft_type=fields[3].strip().upper(),
            flight_rules=fields[2].strip().upper(),
            route=route,
            transfer_fix=self.reference_data.resolve_transfer_fix(route_field),
            dof=dof,
            etd=etd,
            eet_minutes=eet_minutes,
            eta=etd + timedelta(minutes=eet_minutes) if etd else None,
        )
        return plan

    def _parse_dep_like(self, core_text: str, message_time: datetime, action: str) -> FlightPlan:
        fields = self._split_fields(core_text)
        if len(fields) < 5:
            raise AftnParseError(f"{action} 报文段数不足: {len(fields)}")
        callsign, ssr = self._parse_callsign_and_ssr(fields[1])
        if not callsign:
            raise AftnParseError(f"{action} 缺少呼号")

        departure = fields[2].strip().upper()
        hhmm = departure[4:8]
        # DEP/DLA 的 dof 初步取收报时刻（UTC）的北京日期
        base_day = _beijing_date_from_utc(message_time)

        # 无 DOF/ 字段时，与 FPL 相同的规则：HHMM > 1600 时，
        # 该时刻 UTC 日期为前一天，执飞日仍为收报北京日（base_day）。
        # 注：DEP/DLA 一般不带 DOF/ 字段，此处统一按 HHMM 判断。
        h, m = int(hhmm[:2]), int(hhmm[2:4])
        if h > 16 or (h == 16 and m > 0):
            time_utc = self._combine_day_hhmm(base_day - timedelta(days=1), hhmm)
        else:
            time_utc = self._combine_day_hhmm(base_day, hhmm)

        dof = base_day  # 昨日延误回退逻辑仍在 state._find_existing_flight_plan 中处理

        plan = FlightPlan(
            callsign=callsign,
            adep=departure[:4],
            adest=fields[3].strip().upper()[:4],
            ssr=ssr,
            dof=dof,
        )
        if action == "DEP":
            plan.atd = time_utc
        else:
            plan.etd = time_utc
        return plan

    def _parse_arr(self, core_text: str, message_time: datetime) -> FlightPlan:
        fields = self._split_fields(core_text)
        if len(fields) not in {4, 5}:
            raise AftnParseError(f"ARR 报文段数异常: {len(fields)}")
        callsign, _ = self._parse_callsign_and_ssr(fields[1])
        if not callsign:
            raise AftnParseError("ARR 缺少呼号")
        arrival = fields[-1].strip().upper()
        # ARR 的 ATA 是 UTC 时刻，arrival[4:8] 是落地 UTC HHMM。
        # 推算规则（与 FPL/DEP 一致）：
        #   1. 优先读取报文中的 DOF/ 字段（UTC 日期）作为 ATA 的 UTC 日期基准。
        #   2. 无 DOF/ 字段时，根据 ATA HHMM 判断：
        #      - HHMM > 1600（含 1601~2359）：ATA UTC 日期为收报北京日的前一天（yesterday），
        #        执飞日 = 收报北京日（base_day）。
        #      - HHMM = 1600：严格等于 1600 时，ATA UTC 日期 = base_day，
        #        执飞日 = base_day + 1（因为 +8 恰为 00:00 次日）。
        #      - HHMM < 1600（0000~1559）：ATA UTC 日期 = base_day，
        #        执飞日 = ATA UTC +8 后的北京日期。
        base_day = _beijing_date_from_utc(message_time)
        ata_hhmm = arrival[4:8]
        
        # 尝试从报文字段中提取 DOF/ (UTC 日)
        dof_utc_day: date | None = None
        for field in fields:
            marker = field.upper().find("DOF/")
            if marker >= 0:
                digits = field[marker + 4 : marker + 10]
                if len(digits) == 6 and digits.isdigit():
                    try:
                        dof_utc_day = datetime.strptime("20" + digits, "%Y%m%d").date()
                    except ValueError:
                        pass
                break
        
        if dof_utc_day is not None:
            # 有 DOF/ 字段：用 UTC 日 + HHMM 直接组合
            ata_utc = self._combine_day_hhmm(dof_utc_day, ata_hhmm)
            dof = _beijing_date_from_utc(ata_utc)
        else:
            # 无 DOF/ 字段：根据 ATA HHMM 判断 UTC 日期基准（与 FPL/DEP 统一）
            #   HHMM >= 1600：UTC 昨日 + HHMM → +8 = 北京今日（或 00:00 次日），
            #                 执飞日 = base_day。
            #   HHMM < 1600：UTC 今日 + HHMM → +8 后的北京日期即为执飞日。
            ata_hour = int(ata_hhmm[:2])
            ata_minute = int(ata_hhmm[2:4])
            if ata_hour > 16 or (ata_hour == 16 and ata_minute >= 0):
                # HHMM >= 1600：UTC 昨日 + HHMM → +8 = 北京今日
                ata_utc = self._combine_day_hhmm(base_day - timedelta(days=1), ata_hhmm)
                dof = base_day
            else:
                # HHMM < 1600：UTC 今日 + HHMM
                ata_utc = self._combine_day_hhmm(base_day, ata_hhmm)
                dof = _beijing_date_from_utc(ata_utc)
        
        plan = FlightPlan(
            callsign=callsign,
            adep=fields[2].strip().upper()[:4],
            adest=arrival[:4],
            dof=dof,
            ata=ata_utc,
        )
        return plan

    def _split_fields(self, core_text: str) -> list[str]:
        if not core_text:
            raise AftnParseError("AFTN 报文为空")
        if not core_text.startswith("("):
            raise AftnParseError("AFTN 报文缺少起始括号")
        body = core_text[1:-1] if core_text.endswith(")") else core_text[1:]
        return [field.strip() for field in body.split("-")]

    def _parse_callsign_and_ssr(self, field: str) -> tuple[str, str]:
        text = field.strip().upper()
        if "/A" not in text:
            return text, ""
        callsign, suffix = text.split("/A", 1)
        suffix = suffix.strip()
        if not suffix:
            return callsign.strip(), ""
        return callsign.strip(), f"A{suffix[:4]}"


    def _extract_dof(self, field: str, default_day: date) -> date:
        text = field.strip().upper()
        marker = text.find("DOF/")
        if marker < 0:
            return default_day
        digits = text[marker + 4 : marker + 10]
        if len(digits) != 6 or not digits.isdigit():
            raise AftnParseError(f"DOF 非法: {digits!r}")
        return datetime.strptime("20" + digits, "%Y%m%d").date()

    def _combine_day_hhmm(self, day: date, hhmm: str) -> datetime:
        if len(hhmm) != 4 or not hhmm.isdigit():
            raise AftnParseError(f"时间字段非法: {hhmm!r}")
        return datetime(day.year, day.month, day.day, int(hhmm[:2]), int(hhmm[2:4]))

    def _hhmm_to_minutes(self, hhmm: str) -> int:
        if len(hhmm) != 4 or not hhmm.isdigit():
            raise AftnParseError(f"EET 字段非法: {hhmm!r}")
        return int(hhmm[:2]) * 60 + int(hhmm[2:4])
