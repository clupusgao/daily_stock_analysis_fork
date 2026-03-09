# -*- coding: utf-8 -*-
"""Market strategy blueprints for CN/US daily market recap."""

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class StrategyDimension:
    """Single strategy dimension used by market recap prompts."""

    name: str
    objective: str
    checkpoints: List[str]


@dataclass(frozen=True)
class MarketStrategyBlueprint:
    """Region specific market strategy blueprint."""

    region: str
    title: str
    positioning: str
    principles: List[str]
    dimensions: List[StrategyDimension]
    action_framework: List[str]

    def to_prompt_block(self) -> str:
        """Render blueprint as prompt instructions."""
        principles_text = "\n".join([f"- {item}" for item in self.principles])
        action_text = "\n".join([f"- {item}" for item in self.action_framework])

        dims = []
        for dim in self.dimensions:
            checkpoints = "\n".join([f"  - {cp}" for cp in dim.checkpoints])
            dims.append(f"- {dim.name}: {dim.objective}\n{checkpoints}")
        dimensions_text = "\n".join(dims)

        return (
            f"## Strategy Blueprint: {self.title}\n"
            f"{self.positioning}\n\n"
            f"### Strategy Principles\n{principles_text}\n\n"
            f"### Analysis Dimensions\n{dimensions_text}\n\n"
            f"### Action Framework\n{action_text}"
        )

    def to_markdown_block(self) -> str:
        """Render blueprint as markdown section for template fallback report."""
        dims = "\n".join([f"- **{dim.name}**: {dim.objective}" for dim in self.dimensions])
        section_title = "### 六、策略框架" if self.region == "cn" else "### VI. Strategy Framework"
        return f"{section_title}\n{dims}\n"


CN_BLUEPRINT = MarketStrategyBlueprint(
    region="cn",
    title="A股市场三段式复盘策略",
    positioning="聚焦指数趋势、资金博弈与板块轮动，形成次日交易计划。",
    principles=[
        "先看指数方向，再看量能结构，最后看板块持续性。",
        "结论必须映射到仓位、节奏与风险控制动作。",
        "判断使用当日数据与近3日新闻，不臆测未验证信息。",
    ],
    dimensions=[
        StrategyDimension(
            name="趋势结构",
            objective="判断市场处于上升、震荡还是防守阶段。",
            checkpoints=["上证/深证/创业板是否同向", "放量上涨或缩量下跌是否成立", "关键支撑阻力是否被突破"],
        ),
        StrategyDimension(
            name="资金情绪",
            objective="识别短线风险偏好与情绪温度。",
            checkpoints=["涨跌家数与涨跌停结构", "成交额是否扩张", "高位股是否出现分歧"],
        ),
        StrategyDimension(
            name="主线板块",
            objective="提炼可交易主线与规避方向。",
            checkpoints=["领涨板块是否具备事件催化", "板块内部是否有龙头带动", "领跌板块是否扩散"],
        ),
    ],
    action_framework=[
        "进攻：指数共振上行 + 成交额放大 + 主线强化。",
        "均衡：指数分化或缩量震荡，控制仓位并等待确认。",
        "防守：指数转弱 + 领跌扩散，优先风控与减仓。",
    ],
)

US_BLUEPRINT = MarketStrategyBlueprint(
    region="us",
    title="全球宏观与风险资产复盘策略",
    positioning="聚焦指数趋势、全球地缘政治叙事与资金流动，定义下一个交易日的风险敞口（涵盖美股与加密货币）。无论分析什么标的，必须100%使用简体中文输出！",
    principles=[
        "优先从标普500、纳斯达克以及避险/风险资产（如比特币、VIX恐慌指数）的联动中读取市场情绪。",
        "将宏观政策（如美联储利率）和突发新闻（如战争冲突）映射到资产的风险偏好中。",
        "【语言最高指令】：无论检索到的新闻是英文还是其他语言，最终的复盘报告必须严格、绝对地使用【简体中文（Simplified Chinese）】输出，绝不允许出现哪怕一段英文！"
    ],
    dimensions=[
        StrategyDimension(
            name="趋势与结构",
            objective="将当前市场分类为动能向上、区间震荡或极端避险（Risk-off）。",
            checkpoints=[
                "美股三大股指与核心加密货币（BTC/ETH）是否同向共振",
                "成交量是否确认了当前的极端波动",
                "关键整数关口或支撑阻力位的得失情况",
            ],
        ),
        StrategyDimension(
            name="宏观叙事与流动性",
            objective="评估全球资金的避险与追风情绪。",
            checkpoints=[
                "美债收益率与美元指数的压迫感",
                "VIX恐慌指数的异动及其隐含的黑天鹅风险",
                "战争、地缘冲突对市场流动性的真实抽水效应",
            ],
        ),
        StrategyDimension(
            name="核心巨头与主线",
            objective="识别当前市场最吸金的主线或最脆弱的板块。",
            checkpoints=[
                "AI科技巨头（英伟达/微软/特斯拉）的趋势延续性",
                "马斯克等核心领袖言论对特定资产（如DOGE/TSLA）的情绪刺激",
                "避险资金是否在大规模涌入黄金或加密货币",
            ],
        ),
    ],
    action_framework=[
        "Risk-on（风险偏好上升）：指数广泛突破，参与度增加，可适当加仓风险资产。",
        "Neutral（中性）：多空分歧巨大，信号混合，建议控制仓位观望。",
        "Risk-off（极端避险）：波动率飙升，技术面破位，优先保护本金，增加现金或避险资产配置。",
    ],
)


def get_market_strategy_blueprint(region: str) -> MarketStrategyBlueprint:
    """Return strategy blueprint by market region."""
    return US_BLUEPRINT if region == "us" else CN_BLUEPRINT
