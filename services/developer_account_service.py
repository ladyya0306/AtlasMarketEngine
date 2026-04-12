import logging
import sqlite3

logger = logging.getLogger(__name__)


class DeveloperAccountService:
    """开发商账户服务：记录投放、回款、统计查询"""
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def _latest_snapshot_before(self, month: int = None):
        cursor = self.conn.cursor()
        if month is None:
            cursor.execute("""
                SELECT month, cash_balance, total_revenue, total_invested, total_sold
                FROM developer_account
                ORDER BY month DESC
                LIMIT 1
            """)
        else:
            cursor.execute("""
                SELECT month, cash_balance, total_revenue, total_invested, total_sold
                FROM developer_account
                WHERE month < ?
                ORDER BY month DESC
                LIMIT 1
            """, (month,))
        return cursor.fetchone()
    
    def record_sale(self, final_price: float, month: int):
        """记录一笔卖房回款"""
        cursor = self.conn.cursor()

        # 查询当月记录是否存在
        cursor.execute(
            "SELECT cash_balance, month_revenue, total_revenue, total_invested, total_sold FROM developer_account WHERE month=?",
            (month,)
        )
        row = cursor.fetchone()

        if row:
            # 更新现有记录
            new_balance = row[0] + final_price
            new_month_rev = row[1] + final_price
            new_total_rev = row[2] + final_price
            new_sold = row[4] + 1

            cursor.execute("""
                UPDATE developer_account 
                SET cash_balance = ?,
                    month_revenue = ?,
                    total_revenue = ?,
                    total_sold = ?
                WHERE month = ?
            """, (new_balance, new_month_rev, new_total_rev, new_sold, month))
        else:
            # 插入新记录（继承上月累计）
            prev = self._latest_snapshot_before(month)
            prev_balance = prev[1] if prev else 0
            prev_total_rev = prev[2] if prev else 0
            prev_total_invested = prev[3] if prev else 0
            prev_total_sold = prev[4] if prev else 0
            cursor.execute("""
                INSERT INTO developer_account 
                (month, cash_balance, month_revenue, total_revenue, total_invested, total_sold, unsold_count)
                VALUES (?, ?, ?, ?, ?, ?, 0)
            """, (
                month,
                prev_balance + final_price,
                final_price,
                prev_total_rev + final_price,
                prev_total_invested,
                prev_total_sold + 1
            ))

        self.conn.commit()
    
    def record_investment(self, count: int, month: int):
        """记录一次房产投放"""
        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT cash_balance, month_revenue, total_revenue, total_invested, total_sold, unsold_count FROM developer_account WHERE month=?",
            (month,)
        )
        row = cursor.fetchone()

        if row:
            cursor.execute("""
                UPDATE developer_account 
                SET total_invested = ?,
                    unsold_count = ?
                WHERE month = ?
            """, (row[3] + count, row[5] + count, month))
        else:
            prev = self._latest_snapshot_before(month)
            prev_balance = prev[1] if prev else 0
            prev_total_rev = prev[2] if prev else 0
            prev_total_invested = prev[3] if prev else 0
            prev_total_sold = prev[4] if prev else 0
            cursor.execute("""
                INSERT INTO developer_account 
                (month, total_invested, unsold_count, cash_balance, month_revenue, total_revenue, total_sold)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                month,
                prev_total_invested + count,
                count,
                prev_balance,
                0,
                prev_total_rev,
                prev_total_sold
            ))

        self.conn.commit()
    
    def get_stats(self):
        """获取开发商账户统计"""
        cursor = self.conn.cursor()

        # Use latest snapshot for cumulative figures
        cursor.execute("""
            SELECT
                COALESCE(cash_balance, 0),
                COALESCE(total_revenue, 0),
                COALESCE(total_invested, 0),
                COALESCE(total_sold, 0)
            FROM developer_account
            ORDER BY month DESC
            LIMIT 1
        """)
        row = cursor.fetchone()

        # 待售数量（实时查询）
        cursor.execute("""
            SELECT COUNT(*) FROM properties_market 
            WHERE owner_id = -1 AND status = 'for_sale'
        """)
        unsold = cursor.fetchone()[0]
        
        return {
            "cash_balance": row[0] if row else 0,
            "total_revenue": row[1] if row else 0,
            "total_invested": row[2] if row else 0,
            "total_sold": row[3] if row else 0,
            "unsold_count": unsold,
        }
    
    def show_report(self, month: int):
        """命令行输出开发商账户报告"""
        stats = self.get_stats()
        
        # 按区域统计
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT ps.zone, COUNT(*) as sold_count, 
                   AVG(t.final_price) as avg_price
            FROM transactions t
            JOIN properties_static ps ON t.property_id = ps.property_id
            WHERE t.seller_id = -1
            GROUP BY ps.zone
        """)
        zone_stats = cursor.fetchall()
        
        print(f"\n{'='*55}")
        print(f"  📊 开发商账户报告 - 第 {month} 月")
        print(f"{'='*55}")
        print(f"  账户余额:  ¥ {stats['cash_balance']:>15,.0f}")
        print(f"  累计收入:  ¥ {stats['total_revenue']:>15,.0f}")
        print(f"  {'-'*51}")
        print(f"  投放总数:  {stats['total_invested']:>8} 套")
        sold_rate = (stats['total_sold']/stats['total_invested']*100 
                     if stats['total_invested'] > 0 else 0)
        print(f"  已售数量:  {stats['total_sold']:>8} 套 ({sold_rate:.0f}%)")
        print(f"  待售数量:  {stats['unsold_count']:>8} 套")
        
        if zone_stats:
            print(f"  {'-'*51}")
            for zone, count, avg in zone_stats:
                print(f"  {zone}区已售: {count:>5} 套, 均价 ¥{avg:>12,.0f}")
        
        print(f"{'='*55}\n")
