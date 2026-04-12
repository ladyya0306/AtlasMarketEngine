
import random


class ChineseNameGenerator:
    """
    Generate unique Chinese names deterministically based on a seed.
    Supports generating millions of unique names by combining surnames and given names.
    """

    # Top 100 Surnames (covering majority of population)
    SURNAMES = [
        "李", "王", "张", "刘", "陈", "杨", "赵", "黄", "周", "吴",
        "徐", "孙", "hu", "朱", "高", "林", "何", "郭", "马", "罗",
        "梁", "宋", "郑", "谢", "韩", "唐", "冯", "于", "董", "萧",
        "程", "曹", "袁", "邓", "许", "傅", "沈", "曾", "彭", "吕",
        "苏", "卢", "蒋", "蔡", "贾", "丁", "魏", "薛", "叶", "阎",
        "余", "潘", "杜", "戴", "夏", "钟", "汪", "田", "任", "姜",
        "范", "方", "石", "姚", "谭", "廖", "邹", "熊", "金", "陆",
        "郝", "孔", "白", "崔", "康", "毛", "邱", "秦", "江", "史",
        "顾", "侯", "邵", "孟", "龙", "万", "段", "漕", "钱", "汤",
        "尹", "黎", "易", "常", "武", "乔", "贺", "赖", "龚", "文"
    ]

    # Common characters for given names
    GIVEN_CHARS = [
        "伟", "芳", "娜", "秀", "敏", "静", "丽", "强", "磊", "军",
        "洋", "勇", "艳", "杰", "娟", "涛", "明", "超", "华", "平",
        "鹏", "宇", "浩", "然", "晨", "宁", "辉", "俊", "建", "文",
        "志", "永", "海", "飞", "雪", "萍", "霞", "峰", "波", "斌",
        "刚", "巍", "亮", "博", "思", "远", "婷", "琳", "玉", "兰",
        "红", "梅", "英", "桂", "春", "秋", "云", "雨", "风", "雷",
        "欣", "悦", "欢", "乐", "阳", "光", "星", "辰", "河", "山",
        "东", "南", "西", "北", "亚", "洲", "国", "家", "安", "定",
        "成", "功", "胜", "利", "兴", "旺", "发", "达", "智", "慧"
    ]

    def __init__(self, seed=None):
        self.used_names = set()
        self.rng = random.Random(seed) if seed is not None else random
        self.counter = 0

    def generate(self) -> str:
        """
        Generate a unique Chinese name.
        Strategy:
        1. Try random combination (Surname + 1 or 2 chars).
        2. If collision, append simplified counter logic (though with millions of combos, collision is rare).
        """
        for _ in range(50):
            surname = self.rng.choice(self.SURNAMES)
            # 60% chance of 2-character given name, 40% 1-character
            if self.rng.random() > 0.4:
                given = self.rng.choice(self.GIVEN_CHARS) + self.rng.choice(self.GIVEN_CHARS)
            else:
                given = self.rng.choice(self.GIVEN_CHARS)

            name = surname + given
            if name not in self.used_names:
                self.used_names.add(name)
                return name

        # Fallback: Deterministic generation using counter to guarantee uniqueness if random fails too often
        self.counter += 1
        # Use counter to pick characters to ensure determinism
        s_idx = (self.counter // 10000) % len(self.SURNAMES)
        g1_idx = (self.counter // 100) % len(self.GIVEN_CHARS)
        g2_idx = self.counter % len(self.GIVEN_CHARS)

        name = f"{self.SURNAMES[s_idx]}{self.GIVEN_CHARS[g1_idx]}{self.GIVEN_CHARS[g2_idx]}"
        if name in self.used_names:
            name += str(self.counter)  # Last resort

        self.used_names.add(name)
        return name
