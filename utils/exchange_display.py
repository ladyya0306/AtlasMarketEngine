"""
命令行交易所显示模块 - 实时可视化交易撮合过程
使用 rich 库实现美观的终端输出
"""
from typing import Any, Dict, List, Optional

# 尝试导入 rich，如果没有则使用简单输出
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class ExchangeDisplay:
    """
    命令行交易所显示器

    功能：
    1. 显示交易所头部（月份、宏观环境）
    2. 显示挂牌房产列表
    3. 显示买家队列
    4. 实时显示谈判轮次
    5. 显示成交结果
    6. 月度汇总
    """

    def __init__(self, use_rich: bool = True):
        self.use_rich = use_rich and RICH_AVAILABLE
        if self.use_rich:
            self.console = Console()

    def _print(self, text: str):
        """兼容输出"""
        if self.use_rich:
            self.console.print(text)
        else:
            print(text)

    def show_exchange_header(self, month: int, macro_status: str):
        """显示交易所头部"""
        if self.use_rich:
            self.console.print(Panel.fit(
                f"[bold cyan]🏠 房产交易所 - 第 {month} 月[/bold cyan]\n"
                f"[dim]宏观环境: {macro_status}[/dim]",
                border_style="blue"
            ))
        else:
            print(f"\n{'=' * 50}")
            print(f"🏠 房产交易所 - 第 {month} 月")
            print(f"宏观环境: {macro_status}")
            print(f"{'=' * 50}\n")

    def show_listings(self, listings: List[Dict], properties_map: Dict = None):
        """显示当前挂牌房产"""
        if not listings:
            self._print("📋 当前无挂牌房产")
            return

        if self.use_rich:
            table = Table(title="📋 当前挂牌房产", show_header=True, header_style="bold magenta")
            table.add_column("房产ID", style="cyan", width=8)
            table.add_column("区域", style="green", width=4)
            table.add_column("户型", style="yellow", width=10)
            table.add_column("面积", justify="right", width=8)
            table.add_column("挂牌价", justify="right", style="bold", width=14)
            table.add_column("标签", style="red", width=8)
            table.add_column("卖家ID", style="dim", width=8)

            for listing in listings[:10]:
                prop_id = listing.get('property_id', '?')
                # 尝试从properties_map获取详细信息
                prop_detail = properties_map.get(prop_id, {}) if properties_map else {}

                zone = listing.get('zone') or prop_detail.get('zone', '?')
                prop_type = prop_detail.get('property_type', '普通住宅')[:8]
                area = prop_detail.get('building_area', listing.get('building_area', 0))
                reason = str(listing.get('last_price_update_reason', '') or '')
                tag = "🏦法拍" if "forced sale" in reason.lower() else ""

                table.add_row(
                    str(prop_id),
                    zone,
                    prop_type,
                    f"{area:.0f}㎡",
                    f"¥{listing.get('listed_price', 0):,.0f}",
                    tag,
                    str(listing.get('seller_id', '?'))
                )
            if len(listings) > 10:
                table.add_row("...", f"共{len(listings)}套", "", "", "", "", "")

            self.console.print(table)
        else:
            print(f"📋 当前挂牌房产 ({len(listings)}套)")
            print("-" * 60)
            for listing in listings[:5]:
                reason = str(listing.get('last_price_update_reason', '') or '')
                tag = " 🏦法拍" if "forced sale" in reason.lower() else ""
                print(f"  房产{listing.get('property_id')}: ¥{listing.get('listed_price', 0):,.0f}{tag}")
            if len(listings) > 5:
                print(f"  ... 共 {len(listings)} 套")
            print()

    def show_buyers(self, buyers: List[Any]):
        """显示买家队列"""
        if not buyers:
            self._print("🛒 当前无活跃买家")
            return

        if self.use_rich:
            table = Table(title="🛒 活跃买家队列", show_header=True, header_style="bold green")
            table.add_column("买家ID", style="cyan", width=8)
            table.add_column("姓名", style="yellow", width=10)
            table.add_column("现金", justify="right", width=14)
            table.add_column("购买力", justify="right", style="bold", width=14)
            table.add_column("目标", style="green", width=6)

            for b in buyers[:10]:
                pref = getattr(b, 'preference', None)
                max_price = pref.max_price if pref else b.cash * 3
                target = pref.target_zone if pref else "?"
                name = getattr(b, 'name', f'买家{b.id}')[:8]

                table.add_row(
                    str(b.id),
                    name,
                    f"¥{b.cash:,.0f}",
                    f"¥{max_price:,.0f}",
                    target
                )
            if len(buyers) > 10:
                table.add_row("...", f"共{len(buyers)}人", "", "", "")

            self.console.print(table)
        else:
            print(f"🛒 活跃买家队列 ({len(buyers)}人)")
            print("-" * 40)
            for b in buyers[:5]:
                print(f"  买家{b.id}: 现金 ¥{b.cash:,.0f}")
            if len(buyers) > 5:
                print(f"  ... 共 {len(buyers)} 人")
            print()

    def show_negotiation_start(self, buyer_id: int, seller_id: int, property_id: int, listed_price: float):
        """显示谈判开始"""
        if self.use_rich:
            self.console.print(f"\n[bold yellow]💬 开始谈判[/bold yellow] "
                               f"买家{buyer_id} ↔ 卖家{seller_id} | 房产{property_id} | ¥{listed_price:,.0f}")
        else:
            print(f"\n💬 开始谈判: 买家{buyer_id} vs 卖家{seller_id}, 房产{property_id}, ¥{listed_price:,.0f}")

    def show_negotiation_round(self, round_num: int, party: str, action: str,
                               price: Optional[float], message: str, thought: str = ""):
        """显示谈判轮次"""
        icon = "🧑‍💼" if party == "buyer" else "🏠"
        party_name = "买方" if party == "buyer" else "卖方"

        # 动作颜色
        action_upper = str(action).upper()
        if action_upper in ["ACCEPT"]:
            color = "green"
        elif action_upper in ["REJECT", "WITHDRAW"]:
            color = "red"
        elif action_upper in ["OFFER", "COUNTER"]:
            color = "yellow"
        else:
            color = "white"

        price_str = f"¥{price:,.0f}" if price else "-"

        if self.use_rich:
            self.console.print(f"  {icon} 第{round_num}轮 [{color}]{party_name}[/{color}]: "
                               f"[bold]{action}[/bold] {price_str}")
            if message:
                msg_short = message[:60] + "..." if len(message) > 60 else message
                self.console.print(f"     [dim]💬 \"{msg_short}\"[/dim]")
            if thought:
                thought_short = thought[:40] + "..." if len(thought) > 40 else thought
                self.console.print(f"     [dim italic]🧠 (内心: {thought_short})[/dim italic]")
        else:
            print(f"  {icon} 第{round_num}轮 {party_name}: {action} {price_str}")
            if message:
                print(f"     💬 \"{message[:50]}...\"" if len(message) > 50 else f"     💬 \"{message}\"")

    def show_deal_result(self, success: bool, buyer_id: int, seller_id: int,
                         property_id: int, price: float, reason: str = ""):
        """显示成交结果"""
        if success:
            if self.use_rich:
                self.console.print(Panel(
                    f"[bold green]✅ 达成意向（待交割）[/bold green]\n"
                    f"买家 {buyer_id} ← 房产 {property_id} ← 卖家 {seller_id}\n"
                    f"意向价: [bold yellow]¥{price:,.0f}[/bold yellow]",
                    border_style="green"
                ))
            else:
                print(f"\n✅ 达成意向（待交割） 买家{buyer_id} -> 房产{property_id}, 意向价 ¥{price:,.0f}")
        else:
            if self.use_rich:
                self.console.print(f"[red]❌ 谈判失败: 买家{buyer_id} vs 卖家{seller_id}[/red]"
                                   f"[dim] ({reason})[/dim]")
            else:
                print(f"❌ 谈判失败: 买家{buyer_id} vs 卖家{seller_id} ({reason})")

    def show_monthly_summary(self, month: int, deals: int, total_volume: float,
                             failed: int = 0, duration: float = 0):
        """月度汇总"""
        if self.use_rich:
            avg_price = total_volume / deals if deals > 0 else 0
            self.console.print(Panel(
                f"[bold]📊 第 {month} 月交易汇总[/bold]\n"
                f"成交套数: [green]{deals}[/green] | 失败: [red]{failed}[/red]\n"
                f"成交总额: [yellow]¥{total_volume:,.0f}[/yellow]\n"
                f"平均成交价: ¥{avg_price:,.0f}\n"
                f"耗时: {duration:.1f}秒",
                border_style="cyan"
            ))
        else:
            print(f"\n{'=' * 40}")
            print(f"📊 第 {month} 月交易汇总")
            print(f"成交: {deals}套 | 失败: {failed}次")
            print(f"总额: ¥{total_volume:,.0f}")
            print(f"{'=' * 40}\n")

    def show_supply_demand(self, supply: int, demand: int):
        """显示供需状态"""
        ratio = supply / max(demand, 1)
        if ratio > 1.2:
            status = "🔵 供过于求 (买方市场)"
            color = "blue"
        elif ratio < 0.8:
            status = "🔴 供不应求 (卖方市场)"
            color = "red"
        else:
            status = "⚪ 供需平衡"
            color = "white"

        if self.use_rich:
            self.console.print(f"[{color}]{status}[/{color}] - 在售{supply}套 / 买家{demand}人 (比例:{ratio:.2f})")
        else:
            print(f"{status} - 在售{supply}套 / 买家{demand}人")
