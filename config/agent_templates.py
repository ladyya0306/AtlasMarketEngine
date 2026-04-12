
"""
Agent Persona Templates for Batch Initialization.
Used to assign realistic occupations, backgrounds, and housing needs based on wealth tier.
"""

AGENT_PERSONA_TEMPLATES = [
    # --- Ultra High Net Worth ---
    {
        "tier_match": "ultra_high",
        "templates": [
            {
                "occupation": "上市公司高管",
                "background": "拥有多年的企业管理经验，持有大量公司股票，现金流充裕，关注资产保值增值。",
                "housing_need": "投资",
                "activation_weight": 0.3,  # 较低的主动交易意愿，除非市场变动
                "age_range": (45, 60)
            },
            {
                "occupation": "民营企业家",
                "background": "白手起家创办了实业公司，家族成员众多，需要购置多套房产供子女居住。",
                "housing_need": "改善",
                "activation_weight": 0.5,
                "age_range": (40, 55)
            },
            {
                "occupation": "天使投资人",
                "background": "眼光独到的投资人，对市场风向极度敏感，倾向于抛售高位资产或通过房产优化税务配置。",
                "housing_need": "投资",
                "activation_weight": 0.7,
                "age_range": (35, 50)
            }
        ]
    },

    # --- High Income ---
    {
        "tier_match": "high",
        "templates": [
            {
                "occupation": "互联网大厂P8",
                "background": "名校毕业，技术能力出众，薪资高但工作压力大，刚结婚，急需在公司附近置换一套品质好的大平层。",
                "housing_need": "刚需",
                "activation_weight": 0.8,  # 高频交易群体
                "age_range": (28, 35)
            },
            {
                "occupation": "金融分析师",
                "background": "在CBD工作，收入不菲但波动较大，希望将部分现金转换为核心地段房产以求稳健。",
                "housing_need": "投资",
                "activation_weight": 0.6,
                "age_range": (30, 40)
            },
            {
                "occupation": "三甲医院专家医生",
                "background": "职业稳定社会地位高，收入逐年上升，考虑到子女教育，计划置换一套顶级学区房。",
                "housing_need": "学区",
                "activation_weight": 0.9,  # 学区需求极强
                "age_range": (35, 45)
            }
        ]
    },

    # --- Middle Income ---
    {
        "tier_match": "middle",
        "templates": [
            {
                "occupation": "公务员",
                "background": "工作极其稳定，公积金充足，生活节奏慢，想买一套离单位近、环境好的养老房。",
                "housing_need": "改善",
                "activation_weight": 0.4,
                "age_range": (30, 50)
            },
            {
                "occupation": "中小学教师",
                "background": "重视教育，收入中等但稳定，为了孩子上学，准备卖掉郊区的小房子换一套老破小名校学区房。",
                "housing_need": "学区",
                "activation_weight": 0.85,
                "age_range": (30, 40)
            },
            {
                "occupation": "普通白领",
                "background": "在私企做行政工作，积蓄不多，父母资助了首付，正在看刚需上车盘，价格敏感度高。",
                "housing_need": "刚需",
                "activation_weight": 0.7,
                "age_range": (25, 30)
            }
        ]
    },

    # --- Lower Middle & Low Income ---
    {
        "tier_match": ["lower_middle", "low"],
        "templates": [
            {
                "occupation": "工厂技工",
                "background": "勤劳肯干，收入勉强维持生活，暂无购房计划，但如果老家拆迁可能会有变动。",
                "housing_need": "无",
                "activation_weight": 0.1,
                "age_range": (20, 45)
            },
            {
                "occupation": "外卖配送员",
                "background": "日夜奔波，通过高强度劳动积攒了一些积蓄，梦想是在老家县城买套房。",
                "housing_need": "回乡置业",  # 在本模型中可能体现为卖掉本地（如果有）或不买
                "activation_weight": 0.05,
                "age_range": (20, 40)
            },
            {
                "occupation": "刚毕业大学生",
                "background": "初入职场，还在实习期，目前租房居住，关注租房市场多于买卖市场。",
                "housing_need": "无",
                "activation_weight": 0.1,
                "age_range": (22, 25)
            }
        ]
    }
]


def get_template_for_tier(tier: str, rng):
    """Retrieve a random template for the given tier"""
    # Normalize tier key
    candidates = []
    for group in AGENT_PERSONA_TEMPLATES:
        match = group["tier_match"]
        if (isinstance(match, list) and tier in match) or match == tier:
            candidates = group["templates"]
            break

    if not candidates:
        # Fallback
        return {
            "occupation": "自由职业",
            "background": "普通市民，生活平淡。",
            "housing_need": "无",
            "activation_weight": 0.1,
            "age_range": (25, 50)
        }

    return rng.choice(candidates)
