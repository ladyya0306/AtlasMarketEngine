# 网站启动说明

以后本项目的“启动网站”统一指这一种方式：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_web_ui.ps1
```

这条命令默认就是：

```text
真实 LLM 驱动模式
```

也就是：

- 会正常走项目当前配置的模型调用
- 不会自动开启 mock
- 适合真实演示、真实仿真、真实事件流观察

只有显式加上 `-Mock`，才会进入假数据联调模式。

启动成功后，在浏览器打开：

[http://127.0.0.1:8000/](http://127.0.0.1:8000/)

---

## 1. 标准启动方式

在当前仓库根目录打开 PowerShell，执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_web_ui.ps1
```

这会：

- 使用当前项目约定的 Python 解释器优先级启动服务
- 以 `uvicorn api_server:app` 方式运行网站
- 默认监听 `127.0.0.1:8000`
- 默认关闭 `LLM_MOCK_MODE`
- 默认按真实模型模式运行

停止网站：

```text
在当前窗口按 Ctrl+C
```

---

## 2. Mock 联调模式

如果只是演示前端、联调接口，不希望触发真实大模型调用，才使用：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_web_ui.ps1 -Mock
```

这个模式会自动设置：

```text
LLM_MOCK_MODE=true
```

适用场景：

- 浏览器演示
- 前端接线
- UI 调整
- 自动化 smoke test 前的本地确认

---

## 3. 真实模型模式

如果要跑真实模拟，直接使用默认命令，不加 `-Mock`：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_web_ui.ps1
```

要求：

- 当前仓库根目录 `.env` 中已有可用的模型配置
- 当前机器网络可访问对应模型服务
- 这是默认启动方式，不需要额外参数

---

## 4. 自定义端口

如果 `8000` 被占用，可以改端口：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_web_ui.ps1 -Port 8010
```

然后打开：

[http://127.0.0.1:8010/](http://127.0.0.1:8010/)

---

## 5. 标准浏览顺序

网站打开后，建议按这个顺序操作：

1. 查看左侧 `模拟控制台`
2. 如需演示，先确认是否使用 `-Mock`
3. 点击 `启动模拟`
4. 点击 `推进一回合`
5. 观察：
   - `回合摘要`
   - `大屏舞台`
   - `生成池`
   - `交易舞台`
   - `回合归档`
   - `系统流`

---

## 6. 以后统一约定

以后在本项目里提到：

- “启动网站”
- “打开可视化界面”
- “跑前端联调”

默认都指：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_web_ui.ps1
```

默认含义：

```text
真实 LLM 驱动模式
```

如果我要用 mock 模式，会明确写成：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_web_ui.ps1 -Mock
```
