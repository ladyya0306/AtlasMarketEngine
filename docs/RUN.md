# 快速运行指南

> 当前项目定位：独立房地产市场仿真系统。
> 历史上的 OASIS 文档与命名仅保留作来源归档，不代表当前仓库仍按官方 OASIS 方式运行。

## 第一步：配置 API Key

优先使用项目根目录 `.env`：

```env
DEEPSEEK_API_KEY=sk-你的真实密钥
```

也可以在终端里临时设置：

```powershell
$env:DEEPSEEK_API_KEY = "sk-你的真实密钥"
```

## 第二步：安装依赖

```powershell
cd d:\GitProj\real_estate_demo_v2_1
pip install -r requirements.txt
```

## 第三步：运行项目

最直接的入口：

```powershell
python simulation_runner.py
```

最小验证也可以直接实例化一个小规模仿真：

```powershell
@'
from simulation_runner import SimulationRunner
runner = SimulationRunner(agent_count=2, months=1, seed=42, db_path="smoke_test_simulation.db")
runner.run()
runner.close()
'@ | python -
```

## 预期结果

- 控制台打印月度仿真过程
- 生成数据库文件
- 生成 `simulation_run.log`

## 注意

- 历史文档里出现的 `quick_start_deepseek.py`、`import oasis`、`post/comment/trace` 社交平台表结构，不属于当前主线运行方式。
- 当前项目的核心是房地产交易仿真，不是 OASIS 社交平台镜像。
