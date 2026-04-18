# ATC Data Hub Python

空管防护程序（ATC Protector）的 Python 重构版本，从原 C# WinForms 系统解耦而来。实现终端区的航班计划接收、雷达航迹追踪、通话数据归档与统计报告生成，保持与旧系统的输出文件格式兼容。

## 核心能力

### 实时数据接收与解析
- **CAT062 雷达报文**：UDP 组播接收，解码位置、高度、呼号、应答机、起降机场等关键字段
- **AFTN 电报**：FPL/DEP/ARR/DLA 报文解析，提取执飞日（DOF）、预计/实际时间、航路信息
- **语音识别数据**：通话开始时间、频率、扇区、时长记录，支持 UTC 与本地时间转换

### 航班计划管理
- **多报文合并更新**：按呼号、起降机场、执飞日唯一键合并 FPL/DEP/ARR/DLA
- **执飞日推算规则**：
  - 优先读取 DOF/ 字段（UTC 日期）作为基准
  - 无 DOF/ 时：HHMM > 1600（1601~2359）→ UTC日期为昨日，执飞日为今日
  - 无 DOF/ 时：HHMM ≤ 1600 → UTC日期为今日，执飞日按 UTC+8 计算
- **状态维护**：飞行计划随雷达航迹、报文更新而演进（FPL → DEP → ARR/DLA）

### 数据归档与统计
- **雷达记录**：按半小时分片保存 `.rcd` 文件，保持与旧系统一致的格式
- **飞行计划 CSV**：每日换日时按 DOF 生成 `FPLNYYYYMMDD.csv`，包含航段全程状态
- **AFTN 报文 JSON**：按 UTC 日期归档原始报文至 `AFTNMsgYYYYMMDD.data`
- **通话数据**：按 UTC 日期保存语音识别记录至 `SRyymmdd.data`
- **波道占用统计**：七波道（HN/HE/ARW/AS/AD/ARE/ASL）10 分钟占用率输出
- **扇区架次报告**：每小时统计、每日汇总、月度累计（CSV + TXT）

### 运行保障
- **状态快照**：定时（默认 60 秒）保存内存状态至 `runtime/Record/Temp/`
- **重启恢复**：启动时自动加载快照，恢复飞行计划与航迹
- **日志轮转**：按日期分割日志文件，保留在 `runtime/logs/`
- **换日处理**：按 UTC 时间自动切换日期，归档前一日报表

## 项目结构

```text
atc_datahub/
├── atc_data_hub/                    # 核心模块包
│   ├── app.py                      # 主应用循环，UDP 接收与分发
│   ├── config.py                   # 配置文件加载与解析
│   ├── main.py                     # 命令行入口
│   ├── models.py                   # 数据模型定义（RadarTrack, FlightPlan, AftnMessage, ...）
│   ├── reference.py                # 静态参考数据（扇区、机场、移交点、容量表）
│   ├── state.py                    # 全局状态管理（航迹、飞行计划、统计）
│   ├── storage.py                  # 文件读写（.rcd, .data, CSV, TXT）
│   ├── parsers/                    # 协议解析器
│   │   ├── aftn.py                 # AFTN 电报解析（FPL/DEP/ARR/DLA）
│   │   └── cat062.py               # CAT062 雷达报文解析
│   └── __init__.py
├── config/                         # 配置文件目录
│   ├── default.json               # 主配置（网络端口、路径、时间规则）
│   ├── FDRG.txt                   # 终端区水平范围坐标点（度分秒）
│   ├── SectorInfo.txt             # 管制扇区名称列表
│   ├── SectorCapacity.txt         # 扇区小时容量矩阵（Tab 分隔）
│   ├── AirportTrails.txt          # 需要记录落点轨迹的机场
│   ├── TerminalAirports.txt       # 终端区内机场四字码
│   ├── TransPtKeyFix.txt          # 移交点与关键航路点映射
│   ├── Fix.txt                    # 导航点坐标（名称 纬度 经度）
│   ├── RadioStations.txt          # 无线电台列表
│   └── HotSpot.txt                # 热点区域定义
├── output/                        # 运行时归档文件（按日期/小时组织）
│   └── ProtectorRecord/
│       ├── Radar/                 # .rcd 雷达记录
│       ├── AFTN/                  # AFTNMsg*.data 电报记录
│       ├── AGSR/                  # SR*.data 语音识别记录
│       ├── Channel/               # 波道占用统计
│       ├── Trail/                 # 航迹归档
│       └── Sortie/                # 扇区架次报告（CSV + TXT）
├── runtime/                       # 运行时状态与日志
│   ├── Record/
│   │   └── Temp/                  # 内存快照文件（temp*.data）
│   └── logs/                      # 应用日志文件
├── tools/                         # 辅助工具（数据验证、回放）
├── pyproject.toml                 # Python 项目依赖与分发配置
├── README.md                      # 本文档
└── start.bat                      # Windows 启动脚本
```

## 使用说明

### 配置文件
`config/default.json` 定义：
- **网络端口**：CAT062、AFTN、语音接收的绑定地址、组播组
- **路径**：记录根目录、运行时目录、日志目录
- **终端区**：FDRG 多边形文件路径、垂直上限、机场列表
- **兼容性**：是否使用旧系统 XML/TXT 配置覆盖
- **运行参数**：航迹超时、雷达写入批量、快照间隔、换日触发时刻

### 运行方式

#### 开发环境
```bash
cd atc_datahub
python -m atc_data_hub.main run --config config/default.json
```

#### 生产部署
```bash
# 安装到系统环境
pip install -e .
# 启动服务
atc-data-hub run --config /path/to/config.json
```

#### Windows 快捷启动
双击 `start.bat` 或运行：
```powershell
.\start.bat
```

### 输出文件说明

| 文件 | 内容 | 保存时机 |
|---|---|---|
| `RDyymmddHH_{0/1}.rcd` | 雷达原始报文（半小时分片） | 缓冲区满或换日时 |
| `FPLNYYYYMMDD.csv` | 当日执飞航班完整状态表 | 换日（UTC 00:00） |
| `AFTNMsgYYYYMMDD.data` | AFTN 电报记录（UTC 日期） | 快照间隔（默认 60 秒） |
| `SRyymmdd.data` | 语音识别记录（UTC 日期） | 快照间隔 |
| `SortieDatayymmdd.txt` | 当日扇区架次报告（详细） | 换日时 |
| `SortieDatayymm.csv` | 当月扇区架次累计（CSV） | 每日换日时追加 |

### 兼容性设计

1. **文件名习惯**：保持 `.rcd`, `.data`, `.csv`, `.txt` 扩展名与旧系统一致
2. **数据格式**：
   - `.rcd` 完全兼容（8 字节 OLE Automation 时间 + CAT062 原始字节）
   - `.data` 改用 UTF-8 JSON 替代 .NET BinaryFormatter，但结构可互转
3. **目录结构**：`ProtectorRecord/` 及其子目录与旧系统相同
4. **配置覆盖**：支持通过 `IPSetting.xml` 等旧配置文件覆盖网络参数

## 技术实现

### 时间处理原则
- **统一 UTC 存储**：所有内部时间戳使用 `datetime.utcnow()`
- **执飞日（DOF）**：按北京时（UTC+8）计算，用于 CSV 文件分日
- **换日触发**：UTC 00:00（北京时 08:00）
- **报文归档**：按 UTC 日期归档，避免跨日时区偏差

### 飞行计划匹配
1. **雷达与计划关联**：按呼号在已接收报文计划中查找匹配
2. **状态递进**：FPL（计划）→ DEP（起飞）→ ARR（到达）/ DLA（取消）
3. **跨日清理**：按 DOF+1、ATA/ETA 超过 12 小时规则清理无用记录

### 故障恢复
- **快照恢复**：重启时自动加载 `tempFPLN.data` 等快照文件
- **事务性写入**：CSV 文件使用原子替换，避免写入中断损坏
- **日志轮转**：每日分割，避免单个日志文件过大

## 开发与测试

### 验证工具
`tools/verify_fpln_rollover.py` 包含 14 个测试场景：
- DOF 字段优先读取
- HHMM > 1600 跨日触发
- 边界值（1600, 1601, 2350）
- FPL/DEP/ARR 一致性规则

### 安装开发依赖
```bash
pip install -e ".[dev]"
pytest tests/
```

### 数据回放
可通过现有 `.rcd`、`.data` 文件回放验证：
```bash
python tools/replay.py --rcd output/ProtectorRecord/Radar/...
```

## 后续规划

### 短期优化
- 告警信息（`WarnMsg`）接收链路
- 更多 CAT062 字段覆盖（速度、航向、特殊状态）
- 性能监控与告警

### 中期扩展
- Web 管理界面（实时状态、统计图表）
- 数据导出与 API 接口
- 多终端区支持

### 长期目标
- 云原生部署（Docker, Kubernetes）
- AI 辅助决策（流量预测、冲突告警）
- 标准化数据交换格式

---

**项目路径**：`E:\code\atc_datahub\`

**当前版本**：Python 3.8+，保持与 C# 旧系统的输入输出兼容性
