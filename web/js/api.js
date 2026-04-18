import {
  agentCountInput,
  baseYearInput,
  configSchemaCount,
  configSchemaEditList,
  configSchemaForm,
  configSchemaGroupFilterInput,
  configSchemaList,
  configSchemaOnlyEditableInput,
  configSchemaPhaseFilterInput,
  configSchemaReadonlyList,
  configSchemaResetFiltersBtn,
  configSchemaSearchInput,
  configSchemaStartupList,
  configSchemaSummary,
  controlsSummary,
  developerBuildYearInput,
  developerCountInput,
  developerPricePerSqmInput,
  developerSchoolUnitsInput,
  developerSizeInput,
  developerTemplateInput,
  developerZoneInput,
  forensicSummary,
  incomeAdjustmentRateInput,
  incomePctChangeInput,
  incomeTierChange1Input,
  incomeTierChange2Input,
  incomeTierChange3Input,
  incomeTargetTierInput,
  monthsInput,
  monthSummary,
  minCashThresholdInput,
  nightPlanPathInput,
  nightPlanToggleBtn,
  nightPlanAddDeveloperBtn,
  nightPlanAddAtMonthBtn,
  nightPlanExportBtn,
  nightPlanAddIncomeBtn,
  nightPlanAddSupplyCutBtn,
  nightPlanImportBtn,
  nightPlanImportFileInput,
  nightPlanAddPopulationBtn,
  nightPlanEditorBody,
  nightPlanEditor,
  nightPlanList,
  nightPlanNewActionInput,
  nightPlanNewMonthInput,
  nightPlanSummary,
  nightPlanResetBtn,
  nightPlanWrap,
  openReportViewBtn,
  openDbObserverBtn,
  propertyTotalCountInput,
  propertyTotalHelper,
  populationCountInput,
  populationIncomeMultiplierInput,
  populationIncomeMultiplierMaxInput,
  populationIncomeMultiplierMinInput,
  populationTemplateInput,
  populationTierInput,
  resumeRunSelect,
  resumeRunSummary,
  resumeRunWrap,
  scenarioPresetConfirm,
  scenarioPresetConfirmApplyBtn,
  scenarioPresetConfirmCancelBtn,
  scenarioPresetConfirmCopy,
  scenarioPresetHint,
  scenarioPresetInput,
  seedInput,
  startupConfirm,
  startupConfirmApplyBtn,
  startupConfirmCancelBtn,
  startupConfirmCopy,
  startupEnablePanelInput,
  startupBidFloorRatioInput,
  startupDemandCoverage,
  startupDemandMultiplierInput,
  startupDownPaymentRatioInput,
  startupMarketPulseEnabledInput,
  startupMarketGoalInput,
  startupMarketPulseSeedRatioInput,
  startupMaxDtiRatioInput,
  startupOverview,
  startupSupplySnapshotInput,
  startForm,
  startSubmitBtn,
  stepBtn,
  startModeInput,
  startModePanel,
  startModePanelCopy,
  startModePanelTitle,
  startupAnnualInterestRateInput,
  startupPrecheckBufferInput,
  startupPrecheckTaxFeeInput,
  startupTierSummary,
  tierHighCountInput,
  tierHighIncomeMaxInput,
  tierHighIncomeMinInput,
  tierHighPropertyMaxInput,
  tierHighPropertyMinInput,
  tierLowCountInput,
  tierLowIncomeMaxInput,
  tierLowIncomeMinInput,
  tierLowPropertyMaxInput,
  tierLowPropertyMinInput,
  tierLowerMiddleCountInput,
  tierLowerMiddleIncomeMaxInput,
  tierLowerMiddleIncomeMinInput,
  tierLowerMiddlePropertyMaxInput,
  tierLowerMiddlePropertyMinInput,
  tierMiddleCountInput,
  tierMiddleIncomeMaxInput,
  tierMiddleIncomeMinInput,
  tierMiddlePropertyMaxInput,
  tierMiddlePropertyMinInput,
  tierUltraHighCountInput,
  tierUltraHighIncomeMaxInput,
  tierUltraHighIncomeMinInput,
  tierUltraHighPropertyMaxInput,
  tierUltraHighPropertyMinInput,
  zoneAPriceMaxInput,
  zoneAPriceMinInput,
  zoneARentInput,
  zoneBPriceMaxInput,
  zoneBPriceMinInput,
  zoneBRentInput,
  downloadReportJsonBtn,
} from "./dom.js";
import { getLang, localizeNarrativeText, t, translateBool } from "./i18n.js";
import { renderStatus, renderSummary } from "./render.js";
import { appState } from "./state.js";

let pendingPresetConfirmResolver = null;
let presetConfirmFeedbackTimer = null;
let pendingStartupConfirmResolver = null;
let latestConfigSchemaData = null;
let currentRuntimeStatus = "idle";
let startupDefaultsApplied = false;
let availableRuns = [];
let latestForensicDbPath = "";
let latestStartupDefaults = null;
let nightPlanEditorExpanded = false;
const collapsedSchemaGroups = new Set();
let currentSchemaFilters = {
  search: "",
  phase: "all",
  group: "all",
  onlyEditable: true,
};
let currentRuntimeControls = {
  down_payment_ratio: 0.3,
  annual_interest_rate: 0.035,
  max_dti_ratio: 0.5,
  market_pulse_enabled: false,
  macro_override_mode: "",
  negotiation_quote_stream_enabled: false,
  negotiation_quote_filter_mode: "all",
  negotiation_quote_mode: "limited_quotes",
  negotiation_quote_turn_limit: 4,
  negotiation_quote_char_limit: 84,
};

function setButtonLoading(button, loadingText, loading) {
  if (!button) {
    return;
  }
  if (!button.dataset.idleText) {
    button.dataset.idleText = button.textContent || "";
  }
  if (loading) {
    button.disabled = true;
    button.dataset.loading = "true";
    button.textContent = loadingText;
    return;
  }
  button.disabled = false;
  button.dataset.loading = "";
  button.textContent = button.dataset.idleText || button.textContent;
}

const NIGHT_RUN_EXAMPLE_PLANS = [
  {
    month: 2,
    action_type: "developer_supply",
    zone: "B",
    count: 4,
    template: "b_entry_level",
  },
  {
    month: 4,
    action_type: "population_add",
    tier: "lower_middle",
    count: 6,
    template: "young_first_home",
  },
  {
    month: 7,
    action_type: "income_shock",
    target_tier: "middle",
    pct_change: 0.08,
  },
];

const SCHEMA_CONTROL_KEY_MAP = {
  "mortgage.down_payment_ratio": "down_payment_ratio",
  "mortgage.annual_interest_rate": "annual_interest_rate",
  "mortgage.max_dti_ratio": "max_dti_ratio",
  "market_pulse.enabled": "market_pulse_enabled",
  "macro_environment.override_mode": "macro_override_mode",
  "negotiation.quote_stream_enabled": "negotiation_quote_stream_enabled",
  "negotiation.quote_filter_mode": "negotiation_quote_filter_mode",
  "negotiation.quote_mode": "negotiation_quote_mode",
  "negotiation.quote_turn_limit": "negotiation_quote_turn_limit",
  "negotiation.quote_char_limit": "negotiation_quote_char_limit",
};

const STARTUP_FORM_KEY_MAP = {
  "simulation.agent_count": agentCountInput,
  "simulation.months": monthsInput,
  "simulation.random_seed": seedInput,
};

const STARTUP_TIER_LABELS = {
  ultra_high: { zh: "超高", en: "Ultra" },
  high: { zh: "高", en: "High" },
  middle: { zh: "中", en: "Middle" },
  lower_middle: { zh: "中低", en: "Lower-mid" },
  low: { zh: "低", en: "Low" },
};

const STARTUP_TIER_FIELDS = [
  {
    tier: "ultra_high",
    countInput: tierUltraHighCountInput,
    incomeMinInput: tierUltraHighIncomeMinInput,
    incomeMaxInput: tierUltraHighIncomeMaxInput,
    propertyMinInput: tierUltraHighPropertyMinInput,
    propertyMaxInput: tierUltraHighPropertyMaxInput,
  },
  {
    tier: "high",
    countInput: tierHighCountInput,
    incomeMinInput: tierHighIncomeMinInput,
    incomeMaxInput: tierHighIncomeMaxInput,
    propertyMinInput: tierHighPropertyMinInput,
    propertyMaxInput: tierHighPropertyMaxInput,
  },
  {
    tier: "middle",
    countInput: tierMiddleCountInput,
    incomeMinInput: tierMiddleIncomeMinInput,
    incomeMaxInput: tierMiddleIncomeMaxInput,
    propertyMinInput: tierMiddlePropertyMinInput,
    propertyMaxInput: tierMiddlePropertyMaxInput,
  },
  {
    tier: "lower_middle",
    countInput: tierLowerMiddleCountInput,
    incomeMinInput: tierLowerMiddleIncomeMinInput,
    incomeMaxInput: tierLowerMiddleIncomeMaxInput,
    propertyMinInput: tierLowerMiddlePropertyMinInput,
    propertyMaxInput: tierLowerMiddlePropertyMaxInput,
  },
  {
    tier: "low",
    countInput: tierLowCountInput,
    incomeMinInput: tierLowIncomeMinInput,
    incomeMaxInput: tierLowIncomeMaxInput,
    propertyMinInput: tierLowPropertyMinInput,
    propertyMaxInput: tierLowPropertyMaxInput,
  },
];

const SCHEMA_PHASE_META = {
  startup_only: {
    zh: "仅启动前",
    en: "Startup Only",
    hintZh: "这类参数只在点击“启动模拟”时读取一次。启动后自动锁定并变灰。",
    hintEn: "These values are only read when the simulation starts. They lock after launch.",
  },
  between_steps: {
    zh: "回合间可调",
    en: "Between Steps",
    hintZh: "这类参数可以在回合与回合之间调整，下一次推进回合时生效。",
    hintEn: "These values can be updated between rounds and apply on the next step.",
  },
  readonly: {
    zh: "只读",
    en: "Readonly",
    hintZh: "这类参数只作为基线参考，默认折叠展示，不作为现场调参入口。",
    hintEn: "These values are read-only references from the baseline config.",
  },
};

const SCHEMA_GROUP_META = {
  simulation: {
    labelZh: "模拟设置",
    labelEn: "Simulation Setup",
    helpZh: {
      title: "模拟设置",
      explain: "控制这轮实验的基础规模和随机种子，相当于先决定实验样本和时长。",
      range: "人数建议 1 到 500；回合数建议 1 到 120；随机种子通常用整数。",
      higher: "人数或回合数调高：运行时间更长，画面更丰富，但演示等待会更久。",
      lower: "人数或回合数调低：演示更快，但市场波动和行为样本会更少。",
    },
    helpEn: {
      title: "Simulation Setup",
      explain: "Defines the experiment size, duration, and random seed before launch.",
      range: "Agents: 1-500 suggested; rounds: 1-120 suggested; seed: integer.",
      higher: "Higher counts or longer runs produce richer dynamics but increase runtime.",
      lower: "Lower counts or shorter runs are faster but show less market variation.",
    },
  },
  financing: {
    labelZh: "融资条件",
    labelEn: "Financing",
    helpZh: {
      title: "融资条件",
      explain: "控制首付、利率和负债约束，决定买家贷款门槛和月供压力。",
      range: "请按控件上下限调整；每次改动建议小步微调。",
      higher: "首付/利率/DTI 门槛调高：购买门槛变化更明显，可能压低成交。",
      lower: "首付/利率/DTI 门槛调低：更容易成交，但也可能放大杠杆风险。",
    },
    helpEn: {
      title: "Financing",
      explain: "Controls down payment, rates, and debt constraints that shape affordability.",
      range: "Use the control bounds and change values gradually.",
      higher: "Higher thresholds or rates usually tighten affordability and may reduce deals.",
      lower: "Lower thresholds can support activity but may increase leverage sensitivity.",
    },
  },
  macro: {
    labelZh: "宏观环境",
    labelEn: "Macro",
  },
  market_dynamics: {
    labelZh: "市场机制",
    labelEn: "Market Dynamics",
  },
  negotiation: {
    labelZh: "谈判观测",
    labelEn: "Negotiation",
  },
  agents: {
    labelZh: "Agent 画像",
    labelEn: "Agent Profile",
  },
  supply: {
    labelZh: "供给基线",
    labelEn: "Supply Baseline",
  },
  performance: {
    labelZh: "性能漏斗",
    labelEn: "Performance",
  },
  pulse: {
    labelZh: "市场脉冲",
    labelEn: "Market Pulse",
  },
  system: {
    labelZh: "系统运行",
    labelEn: "System Runtime",
  },
  life_events: {
    labelZh: "生活事件",
    labelEn: "Life Events",
  },
  allocation: {
    labelZh: "房产分配",
    labelEn: "Property Allocation",
  },
  transaction_costs: {
    labelZh: "交易成本",
    labelEn: "Transaction Costs",
  },
};

const SCHEMA_FIELD_META = {
  "simulation.agent_count": {
    labelZh: "Agent 数量",
    labelEn: "Agent Count",
    helpZh: {
      title: "Agent 数量",
      explain: "这轮模拟里参与市场的总人数。人数越多，市场越热闹，但运行时间也越长。",
      range: "建议范围：1 到 500。",
      higher: "调高：会增加活跃角色、事件密度和计算时间。",
      lower: "调低：运行更快，但样本更少，市场走势更容易失真。",
    },
    helpEn: {
      title: "Agent Count",
      explain: "Total number of agents participating in the run.",
      range: "Suggested range: 1 to 500.",
      higher: "Higher values create more activity and longer runtimes.",
      lower: "Lower values run faster but reduce variety and stability.",
    },
  },
  "simulation.months": {
    labelZh: "模拟回合",
    labelEn: "Simulation Rounds",
    helpZh: {
      title: "模拟回合",
      explain: "这轮实验要推进多少个虚拟回合。回合越多，越适合看趋势，不只是看单回合波动。",
      range: "建议范围：1 到 120。",
      higher: "调高：更适合看政策和场景的滞后影响，但演示时间更长。",
      lower: "调低：更适合快速演示单回合变化，但难以形成趋势判断。",
    },
    helpEn: {
      title: "Simulation Rounds",
      explain: "Number of virtual rounds to simulate.",
      range: "Suggested range: 1 to 120.",
      higher: "Higher values reveal longer trends but take more time.",
      lower: "Lower values are faster but show less trend information.",
    },
  },
  "simulation.random_seed": {
    labelZh: "随机种子",
    labelEn: "Random Seed",
    helpZh: {
      title: "随机种子",
      explain: "用于固定随机过程，方便复现结果。相同参数 + 相同种子，结果更容易重现。",
      range: "整数即可，常用 1 到 9999。",
      higher: "调高或调低本身没有好坏，只是切换到另一组随机路径。",
      lower: "若要比较不同政策，建议先固定同一个种子。",
    },
    helpEn: {
      title: "Random Seed",
      explain: "Locks the random path so runs can be reproduced more easily.",
      range: "Use any integer; 1 to 9999 is common.",
      higher: "Higher or lower values do not imply better results; they only change the path.",
      lower: "Keep the seed fixed when comparing policies.",
    },
  },
  "mortgage.down_payment_ratio": {
    labelZh: "首付比例",
    labelEn: "Down Payment Ratio",
    helpZh: {
      title: "首付比例",
      explain: "买房时需要先拿出的自有资金比例。越高，越难上车。",
      range: "通常在 0.10 到 0.90 之间。",
      higher: "调高：买家更难凑够首付，成交通常会变少。",
      lower: "调低：更多买家能进入市场，但杠杆会提高。",
    },
    helpEn: {
      title: "Down Payment Ratio",
      explain: "Share of the purchase price that buyers must pay upfront.",
      range: "Usually between 0.10 and 0.90.",
      higher: "Higher values tighten entry and can reduce deals.",
      lower: "Lower values support activity but increase leverage.",
    },
  },
  "mortgage.annual_interest_rate": {
    labelZh: "贷款年利率",
    labelEn: "Annual Interest Rate",
    helpZh: {
      title: "贷款年利率",
      explain: "房贷的年化利率，直接影响月供压力和可负担能力。",
      range: "通常在 0.01 到 0.20 之间。",
      higher: "调高：月供更重，买家更容易放弃或压价。",
      lower: "调低：买家承受力更强，成交和报价意愿通常会上升。",
    },
    helpEn: {
      title: "Annual Interest Rate",
      explain: "Mortgage rate affecting monthly payment burden and affordability.",
      range: "Usually between 0.01 and 0.20.",
      higher: "Higher values increase payment pressure and may suppress demand.",
      lower: "Lower values support affordability and bidding confidence.",
    },
  },
  "mortgage.max_dti_ratio": {
    labelZh: "最高负债收入比",
    labelEn: "Max DTI Ratio",
    helpZh: {
      title: "最高负债收入比",
      explain: "允许月供占收入的最大比例。越高，允许借得越多。",
      range: "通常在 0.10 到 1.00 之间。",
      higher: "调高：更多人能通过贷款校验，但杠杆和压力更大。",
      lower: "调低：审批更保守，成交可能下降，但风险更稳。",
    },
    helpEn: {
      title: "Max DTI Ratio",
      explain: "Upper debt-to-income ratio allowed for financing checks.",
      range: "Usually between 0.10 and 1.00.",
      higher: "Higher values allow more leverage and can support demand.",
      lower: "Lower values are stricter and may reduce transactions.",
    },
  },
  "market_pulse.enabled": {
    labelZh: "启用市场脉冲",
    labelEn: "Market Pulse Enabled",
    helpZh: {
      title: "启用市场脉冲",
      explain: "是否启用额外的市场扰动与按揭存量影响，用于增强市场节奏。",
      range: "布尔开关：开 / 关。",
      higher: "开启：市场波动和事件性更强，更适合做演示。",
      lower: "关闭：市场更平稳，便于观察基础规则。",
    },
    helpEn: {
      title: "Market Pulse Enabled",
      explain: "Turns on additional market pulse effects and seeded mortgage pressure.",
      range: "Boolean switch: on / off.",
      higher: "On makes the market feel more eventful and dynamic.",
      lower: "Off keeps the simulation steadier and easier to isolate.",
    },
  },
  "macro_environment.override_mode": {
    labelZh: "宏观覆盖模式",
    labelEn: "Macro Override Mode",
    helpZh: {
      title: "宏观覆盖模式",
      explain: "用预设宏观语境覆盖当前回合的市场氛围，例如偏乐观、偏保守。",
      range: "可选项以列表为准；留空表示使用默认环境。",
      higher: "切到更乐观：报价和成交意愿通常更积极。",
      lower: "切到更保守：观望、压价和退出概率通常会提高。",
    },
    helpEn: {
      title: "Macro Override Mode",
      explain: "Overrides the macro narrative for the next step with a preset tone.",
      range: "Use the available options; empty means default behavior.",
      higher: "More optimistic modes usually lift activity and confidence.",
      lower: "More cautious modes usually increase hesitation and discount pressure.",
    },
  },
  "negotiation.quote_stream_enabled": {
    labelZh: "开启谈判原话流",
    labelEn: "Negotiation Quote Stream",
    helpZh: {
      title: "开启谈判原话流",
      explain: "决定是否把谈判中的原话片段推到前端，便于舞台展示和复盘。",
      range: "布尔开关：开 / 关。",
      higher: "开启：画面更戏剧化，更适合大屏演示。",
      lower: "关闭：界面更简洁，只保留摘要层。",
    },
    helpEn: {
      title: "Negotiation Quote Stream",
      explain: "Controls whether quote snippets are pushed to the frontend.",
      range: "Boolean switch: on / off.",
      higher: "On adds richer narrative detail for demos.",
      lower: "Off keeps the feed compact and summary-driven.",
    },
  },
  "negotiation.quote_filter_mode": {
    labelZh: "原话筛选模式",
    labelEn: "Quote Filter Mode",
    helpZh: {
      title: "原话筛选模式",
      explain: "决定哪些谈判会输出原话，例如全部输出、只看胶着局、只看高价值局。",
      range: "以选项列表为准。",
      higher: "选择更聚焦的模式：信息更干净，更适合领导演示。",
      lower: "选择更宽的模式：细节更多，但画面会更密。",
    },
    helpEn: {
      title: "Quote Filter Mode",
      explain: "Selects which negotiations are allowed to emit quotes.",
      range: "Use the option list provided.",
      higher: "More focused modes reduce noise and highlight key cases.",
      lower: "Broader modes expose more detail but increase density.",
    },
  },
  "negotiation.quote_mode": {
    labelZh: "原话密度模式",
    labelEn: "Quote Mode",
    helpZh: {
      title: "原话密度模式",
      explain: "控制谈判只显示摘要、有限原话，还是逐轮原话回放。",
      range: "off / summary / limited_quotes / full_quotes。",
      higher: "调到更细：更适合研究和展示具体谈判过程。",
      lower: "调到更粗：更适合快速看全局，不会被文本淹没。",
    },
    helpEn: {
      title: "Quote Mode",
      explain: "Controls whether negotiations show summaries, limited quotes, or full turns.",
      range: "off / summary / limited_quotes / full_quotes.",
      higher: "Higher-detail modes are better for deep inspection.",
      lower: "Lower-detail modes are better for fast overviews.",
    },
  },
  "negotiation.quote_turn_limit": {
    labelZh: "原话回合上限",
    labelEn: "Quote Turn Limit",
    helpZh: {
      title: "原话回合上限",
      explain: "限制单笔谈判最多展示多少个回合，避免滚屏过长。",
      range: "建议 1 到 12。",
      higher: "调高：能看到更多博弈细节，但信息量会迅速上升。",
      lower: "调低：更适合大屏演示，重点更集中。",
    },
    helpEn: {
      title: "Quote Turn Limit",
      explain: "Caps how many turns are shown per negotiation.",
      range: "Suggested: 1 to 12.",
      higher: "Higher values expose more detail but increase feed length.",
      lower: "Lower values keep the stage concise and focused.",
    },
  },
  "negotiation.quote_char_limit": {
    labelZh: "原话字数上限",
    labelEn: "Quote Character Limit",
    helpZh: {
      title: "原话字数上限",
      explain: "限制每条原话在前端显示的最大长度，避免卡片太长。",
      range: "建议 40 到 240 字符。",
      higher: "调高：句子更完整，但会占用更多界面空间。",
      lower: "调低：更适合简报式展示，但信息会更压缩。",
    },
    helpEn: {
      title: "Quote Character Limit",
      explain: "Caps the display length of each quote on the frontend.",
      range: "Suggested: 40 to 240 characters.",
      higher: "Higher values preserve more wording but consume more space.",
      lower: "Lower values keep cards shorter but compress meaning.",
    },
  },
  "system.llm.max_calls_per_month": {
    labelZh: "每回合 LLM 调用上限",
    labelEn: "LLM Calls Per Round",
    helpZh: {
      title: "每回合 LLM 调用上限",
      explain: "限制每个回合最多调用多少次大模型，主要用于控成本和防止事件过密。",
      range: "以基线配置为准，通常应保持在正整数范围。",
      higher: "调高：行为细节和叙事可能更丰富，但成本和等待时间会上升。",
      lower: "调低：成本更稳，但复杂谈判和细节事件可能减少。",
    },
    helpEn: {
      title: "LLM Calls Per Round",
      explain: "Caps per-round LLM usage to control cost and event density.",
      range: "Defined by the baseline config and should remain a positive integer.",
      higher: "Higher values allow richer behavior detail but increase cost and runtime.",
      lower: "Lower values reduce cost but may suppress complex interactions.",
    },
  },
  "transaction_costs.buyer.brokerage_ratio": {
    labelZh: "买方中介费率",
    labelEn: "Buyer Brokerage Ratio",
    helpZh: {
      title: "买方中介费率",
      explain: "买家成交时额外承担的中介费用比例，会直接抬高买房总成本。",
      range: "只读展示，以基线配置为准。",
      higher: "调高：买家实际到手门槛更高，压价和放弃可能增加。",
      lower: "调低：总交易成本更低，更容易促成成交。",
    },
    helpEn: {
      title: "Buyer Brokerage Ratio",
      explain: "Extra brokerage cost paid by buyers on top of the deal price.",
      range: "Read-only reference from the baseline config.",
      higher: "Higher values raise effective buyer cost and may reduce willingness to close.",
      lower: "Lower values reduce friction and can support transactions.",
    },
  },
  "transaction_costs.seller.tax_ratio": {
    labelZh: "卖方税费比例",
    labelEn: "Seller Tax Ratio",
    helpZh: {
      title: "卖方税费比例",
      explain: "卖家成交后需要承担的税费比例，会影响卖家是否愿意接受报价。",
      range: "只读展示，以基线配置为准。",
      higher: "调高：卖家净得减少，更容易坚持高价或退出谈判。",
      lower: "调低：卖家更容易接受接近心理价位的报价。",
    },
    helpEn: {
      title: "Seller Tax Ratio",
      explain: "Tax burden applied to sellers after a deal closes.",
      range: "Read-only reference from the baseline config.",
      higher: "Higher values shrink seller proceeds and may harden ask prices.",
      lower: "Lower values improve net proceeds and can ease negotiations.",
    },
  },
  "life_events.monthly_event_trigger_prob": {
    labelZh: "生活事件回合触发概率",
    labelEn: "Life Event Trigger Probability Per Round",
    helpZh: {
      title: "生活事件回合触发概率",
      explain: "控制家庭变故、工作变化等生活事件在每个回合被触发的概率。",
      range: "只读展示，以基线配置为准。",
      higher: "调高：市场中的被动买卖和突发行为会更多，波动更强。",
      lower: "调低：市场更像纯经济驱动，生活冲击影响更少。",
    },
    helpEn: {
      title: "Life Event Trigger Probability Per Round",
      explain: "Probability that life events such as job or family changes trigger each round.",
      range: "Read-only reference from the baseline config.",
      higher: "Higher values create more forced moves and stronger volatility.",
      lower: "Lower values make the market more purely economics-driven.",
    },
  },
  "property_allocation.strategy": {
    labelZh: "初始房产分配策略",
    labelEn: "Property Allocation Strategy",
    helpZh: {
      title: "初始房产分配策略",
      explain: "决定仿真开始时房产如何分布到不同人群，相当于市场初始盘面的底色。",
      range: "只读展示，以基线配置或选项策略为准。",
      higher: "切到更集中或更偏置的策略：某类人群更容易持有房产，市场结构会变形。",
      lower: "切到更平均的策略：更容易观察常规市场行为。",
    },
    helpEn: {
      title: "Property Allocation Strategy",
      explain: "Defines how properties are distributed across agents at initialization.",
      range: "Read-only reference from the baseline config or strategy options.",
      higher: "More concentrated strategies can skew ownership and reshape the market base state.",
      lower: "More balanced strategies make baseline behavior easier to compare.",
    },
  },
  "smart_agent.count": {
    labelZh: "聪明 Agent 数量",
    labelEn: "Smart Agent Count",
    helpZh: {
      title: "聪明 Agent 数量",
      explain: "设定更积极、更复杂决策角色的数量，会影响市场博弈强度和展示效果。",
      range: "只读展示，以基线配置为准。",
      higher: "调高：市场更活跃，谈判和出价更有戏剧性，但也更耗算力。",
      lower: "调低：市场更接近普通居民行为，节奏更平缓。",
    },
    helpEn: {
      title: "Smart Agent Count",
      explain: "Number of more active or strategically complex agents in the run.",
      range: "Read-only reference from the baseline config.",
      higher: "Higher values create more active bargaining and stronger stage presence.",
      lower: "Lower values keep behavior closer to ordinary households.",
    },
  },
  "simulation.agent.savings_rate": { labelZh: "Agent 储蓄率", labelEn: "Agent Savings Rate" },
  "simulation.agent.income_adjustment_rate": { labelZh: "收入调整倍率", labelEn: "Income Adjustment Rate" },
  "simulation.enable_intervention_panel": { labelZh: "CLI 干预面板", labelEn: "CLI Intervention Panel" },
  "simulation.base_year": { labelZh: "模拟基准年份", labelEn: "Simulation Base Year" },
  "simulation.min_transactions_gate": { labelZh: "最小成交门槛", labelEn: "Min Transactions Gate" },
  "simulation.low_tx_auto_relax_enabled": { labelZh: "低成交自动放宽", labelEn: "Low Tx Auto Relax" },
  "market.panic_sell_threshold": { labelZh: "恐慌性抛售阈值", labelEn: "Panic Sell Threshold" },
  "decision_factors.activation.macro_volatility": { labelZh: "宏观波动度", labelEn: "Macro Volatility" },
  "decision_factors.activation.risk_free_rate": { labelZh: "无风险利率", labelEn: "Risk Free Rate" },
  "decision_factors.activation.rental.zone_a_rent_per_sqm": { labelZh: "A 区租金 / 平米", labelEn: "Zone A Rent Per Sqm" },
  "decision_factors.activation.rental.zone_b_rent_per_sqm": { labelZh: "B 区租金 / 平米", labelEn: "Zone B Rent Per Sqm" },
  "decision_factors.buyer_timeout_months": { labelZh: "买家超时回合数", labelEn: "Buyer Timeout Rounds" },
  "decision_factors.listing_stale_months": { labelZh: "挂牌陈旧回合数", labelEn: "Listing Stale Rounds" },
  "decision_factors.auto_price_cut_rate": { labelZh: "自动降价比例", labelEn: "Auto Price Cut Rate" },
  "smart_agent.enabled": { labelZh: "启用聪明 Agent", labelEn: "Smart Agent Enabled" },
  "smart_agent.max_sells_per_month": { labelZh: "聪明 Agent 每回合卖出上限", labelEn: "Smart Agent Max Sells Per Round" },
  "smart_agent.bid_aggressiveness": { labelZh: "出价激进度", labelEn: "Bid Aggressiveness" },
  "smart_agent.order_ttl_days": { labelZh: "订单存活天数", labelEn: "Order TTL Days" },
  "smart_agent.deposit_ratio": { labelZh: "定金比例", labelEn: "Deposit Ratio" },
  "smart_agent.candidate_top_k": { labelZh: "候选集 Top K", labelEn: "Candidate Top K" },
  "smart_agent.leverage_cap": { labelZh: "杠杆上限", labelEn: "Leverage Cap" },
  "market.zones.A.base_price_per_sqm": { labelZh: "A 区基准单价 / 平米", labelEn: "Zone A Base Price Per Sqm" },
  "market.zones.B.base_price_per_sqm": { labelZh: "B 区基准单价 / 平米", labelEn: "Zone B Base Price Per Sqm" },
  "life_events.llm_reasoning_enabled": { labelZh: "生活事件 LLM 推理", labelEn: "Life Event LLM Reasoning" },
  "transaction_costs.buyer.tax_ratio": { labelZh: "买方税费比例", labelEn: "Buyer Tax Ratio" },
  "transaction_costs.seller.brokerage_ratio": { labelZh: "卖方中介费率", labelEn: "Seller Brokerage Ratio" },
  "system.llm.enable_caching": { labelZh: "启用 LLM 缓存", labelEn: "LLM Caching Enabled" },
  "system.llm.max_concurrency_smart": { labelZh: "Smart LLM 并发上限", labelEn: "Smart LLM Concurrency" },
  "system.llm.max_concurrency_fast": { labelZh: "Fast LLM 并发上限", labelEn: "Fast LLM Concurrency" },
  "system.output.log_level": { labelZh: "输出日志级别", labelEn: "Output Log Level" },
  "market_pulse.seed_existing_mortgage_ratio": { labelZh: "存量按揭注入比例", labelEn: "Seed Existing Mortgage Ratio" },
  "market_pulse.seed_rate_base": { labelZh: "按揭注入基准利率", labelEn: "Seed Mortgage Base Rate" },
  "market_pulse.seed_loan_age_min_months": { labelZh: "贷款账龄下限（周期）", labelEn: "Seed Loan Age Min Cycles" },
  "market_pulse.seed_loan_age_max_months": { labelZh: "贷款账龄上限（周期）", labelEn: "Seed Loan Age Max Cycles" },
};

function localizePresetLabel(id, fallback) {
  const labels = {
    starter_demand_push: {
      zh: "首置需求推动",
      en: "Starter Demand Push",
    },
    upgrade_cycle: {
      zh: "改善循环升温",
      en: "Upgrade Cycle",
    },
    investor_cooldown: {
      zh: "投资降温",
      en: "Investor Cooldown",
    },
  };
  const target = labels[id];
  if (!target) {
    return fallback || id;
  }
  return getLang() === "en" ? target.en : target.zh;
}

function getSchemaGroupMeta(groupId) {
  return SCHEMA_GROUP_META[groupId] || null;
}

function getSchemaFieldMeta(item) {
  return SCHEMA_FIELD_META[item.key] || null;
}

function localizeSchemaGroupLabel(group) {
  const meta = getSchemaGroupMeta(group.id);
  if (meta) {
    return getLang() === "en" ? meta.labelEn : meta.labelZh;
  }
  return group.label;
}

function localizeSchemaFieldLabel(item) {
  const meta = getSchemaFieldMeta(item);
  if (meta) {
    return getLang() === "en" ? meta.labelEn : meta.labelZh;
  }
  return item.label;
}

function localizeSchemaPhaseLabel(phase) {
  const meta = SCHEMA_PHASE_META[phase];
  if (!meta) {
    return phase;
  }
  return getLang() === "en" ? meta.en : meta.zh;
}

function describeRange(item) {
  if (item.type === "boolean") {
    return getLang() === "en" ? "On / Off" : "开 / 关";
  }
  if (item.min != null && item.max != null) {
    const step = item.step != null ? ` · ${getLang() === "en" ? "step" : "步长"} ${item.step}` : "";
    return `${item.min} ~ ${item.max}${step}`;
  }
  if (item.min != null) {
    return `${getLang() === "en" ? "Min" : "最小"} ${item.min}`;
  }
  if (item.max != null) {
    return `${getLang() === "en" ? "Max" : "最大"} ${item.max}`;
  }
  if (Array.isArray(item.options) && item.options.length > 0) {
    return item.options
      .map((option) => {
        if (option === "") {
          return getLang() === "en" ? "default" : "默认";
        }
        return String(option);
      })
      .join(" / ");
  }
  return getLang() === "en" ? "See baseline config" : "以基线配置为准";
}

function buildFallbackHelp(item) {
  const phaseMeta = SCHEMA_PHASE_META[item.editable_phase] || null;
  const localizedDescription = item.description
    ? (getLang() === "en" ? item.description : localizeNarrativeText(item.description))
    : (getLang() === "en" ? "See baseline config for details." : "该项用于控制或说明当前基线配置。");
  return {
    title: localizeSchemaFieldLabel(item),
    explain: localizedDescription,
    range: `${getLang() === "en" ? "Allowed range" : "可调范围"}：${describeRange(item)}`,
    higher: phaseMeta ? (getLang() === "en" ? `Phase: ${phaseMeta.en}.` : `生效阶段：${phaseMeta.zh}。`) : "",
    lower: getLang() === "en" ? "Use small adjustments and observe round-end changes." : "建议小步调整，并结合回合末摘要观察变化。",
  };
}

function getSchemaHelp(item) {
  const fieldMeta = getSchemaFieldMeta(item);
  if (fieldMeta?.helpZh || fieldMeta?.helpEn) {
    const base = getLang() === "en" ? fieldMeta.helpEn : fieldMeta.helpZh;
    return {
      ...base,
      range: `${base.range}${base.range ? " " : ""}${getLang() === "en" ? `(Actual control: ${describeRange(item)})` : `（当前控件：${describeRange(item)}）`}`,
    };
  }
  return buildFallbackHelp(item);
}

function getSchemaGroupHelp(group) {
  const meta = getSchemaGroupMeta(group.id);
  if (meta?.helpZh || meta?.helpEn) {
    return getLang() === "en" ? meta.helpEn : meta.helpZh;
  }
  return {
    title: localizeSchemaGroupLabel(group),
    explain: getLang() === "en" ? "Configuration group in the schema catalog." : "Schema 配置目录中的一个分组。",
    range: getLang() === "en" ? "Use the fields below for exact ranges." : "具体范围请看下方各字段。",
    higher: getLang() === "en" ? "Increase only when you want stronger effects." : "需要更强效果时再调高。",
    lower: getLang() === "en" ? "Lower values usually make the system steadier." : "调低通常会让系统更平稳。",
  };
}

function buildHelpTooltip(help) {
  if (!help) {
    return "";
  }
  return `
    <span class="schema-help" tabindex="0" aria-label="${help.title}">
      <span class="schema-help-trigger">${getLang() === "en" ? "Info" : "说明"}</span>
      <span class="schema-help-card">
        <strong>${help.title}</strong>
        <span>${help.explain}</span>
        <span><em>${getLang() === "en" ? "Range" : "范围"}</em>：${help.range}</span>
        <span><em>${getLang() === "en" ? "Raise" : "调高"}</em>：${help.higher}</span>
        <span><em>${getLang() === "en" ? "Lower" : "调低"}</em>：${help.lower}</span>
      </span>
    </span>
  `;
}

function readNumberInput(input, fallback = 0) {
  const value = Number(input?.value);
  return Number.isFinite(value) ? value : fallback;
}

function getStartupTierConfigs(totalAgentCount) {
  const explicitCount = STARTUP_TIER_FIELDS.slice(0, -1).reduce(
    (sum, item) => sum + Math.max(0, readNumberInput(item.countInput, 0)),
    0
  );
  const lowCount = Math.max(0, totalAgentCount - explicitCount);
  if (tierLowCountInput) {
    tierLowCountInput.value = String(lowCount);
  }
  return STARTUP_TIER_FIELDS.map((item) => ({
    tier: item.tier,
    count: item.tier === "low" ? lowCount : Math.max(0, readNumberInput(item.countInput, 0)),
    income_min: Math.max(0, readNumberInput(item.incomeMinInput, 0)),
    income_max: Math.max(0, readNumberInput(item.incomeMaxInput, 0)),
    property_min: Math.max(0, readNumberInput(item.propertyMinInput, 0)),
    property_max: Math.max(0, readNumberInput(item.propertyMaxInput, 0)),
  }));
}

function getStartupPropertyNeed(tiers) {
  return tiers.reduce(
    (acc, item) => {
      acc.min += item.count * item.property_min;
      acc.max += item.count * item.property_max;
      return acc;
    },
    { min: 0, max: 0 }
  );
}

function tierShortLabel(tier) {
  const meta = STARTUP_TIER_LABELS[tier] || { zh: tier, en: tier };
  return getLang() === "en" ? meta.en : meta.zh;
}

function getReleaseStartupDefaults() {
  return latestStartupDefaults?.release_startup || null;
}

function getReleaseSnapshotMap() {
  const defaults = getReleaseStartupDefaults();
  const snapshots = Array.isArray(defaults?.supply_snapshots) ? defaults.supply_snapshots : [];
  return new Map(snapshots.map((item) => [String(item.snapshot_id || ""), item]));
}

function getDefaultDemandMultiplierForGoal(goal) {
  const defaults = getReleaseStartupDefaults();
  const mapping = defaults?.default_demand_multiplier_by_goal || {};
  const normalizedGoal = String(goal || "balanced").trim().toLowerCase();
  return Number(mapping[normalizedGoal] ?? mapping.balanced ?? 1);
}

function getSelectedReleaseSnapshot() {
  const snapshotId = String(startupSupplySnapshotInput?.value || "").trim();
  return getReleaseSnapshotMap().get(snapshotId) || null;
}

function estimateReleaseDemandPlan(snapshot, demandMultiplier) {
  const supplyCount = Math.max(1, Number(snapshot?.total_selected_supply || 1));
  const bucketCount = Math.max(0, Number(snapshot?.demand_bucket_count || 0));
  const requestedMultiplier = Math.max(0.1, Math.min(2.0, Number(demandMultiplier || 1)));
  const requestedAgentCount = Math.max(1, Math.floor((supplyCount * requestedMultiplier) + 0.5));
  const effectiveAgentCount = Math.max(requestedAgentCount, bucketCount);
  const effectiveDemandMultiplier = effectiveAgentCount / supplyCount;
  return {
    supplyCount,
    bucketCount,
    requestedMultiplier,
    requestedAgentCount,
    effectiveAgentCount,
    effectiveDemandMultiplier,
    wasClamped: effectiveAgentCount !== requestedAgentCount,
    minimumDemandMultiplier: Number(snapshot?.minimum_demand_multiplier || 0),
  };
}

function applyStartupDefaults(defaults) {
  if (!defaults) {
    return;
  }
  latestStartupDefaults = defaults;
  if (agentCountInput) {
    agentCountInput.value = String(defaults.agent_count ?? agentCountInput.value);
  }
  if (monthsInput) {
    monthsInput.value = String(defaults.months ?? monthsInput.value);
  }
  if (seedInput) {
    seedInput.value = String(defaults.seed ?? seedInput.value);
  }
  if (baseYearInput) {
    baseYearInput.value = String(defaults.base_year ?? baseYearInput.value);
  }
  if (incomeAdjustmentRateInput) {
    incomeAdjustmentRateInput.value = String(defaults.income_adjustment_rate ?? incomeAdjustmentRateInput.value);
  }
  if (startupDownPaymentRatioInput && defaults.down_payment_ratio != null) {
    startupDownPaymentRatioInput.value = String(defaults.down_payment_ratio);
  }
  if (startupMaxDtiRatioInput && defaults.max_dti_ratio != null) {
    startupMaxDtiRatioInput.value = String(defaults.max_dti_ratio);
  }
  if (startupAnnualInterestRateInput && defaults.annual_interest_rate != null) {
    startupAnnualInterestRateInput.value = String(defaults.annual_interest_rate);
  }
  if (startupEnablePanelInput) {
    startupEnablePanelInput.checked = Boolean(defaults.enable_intervention_panel);
  }
  if (startupMarketPulseEnabledInput) {
    startupMarketPulseEnabledInput.checked = Boolean(defaults.market_pulse_enabled);
  }
  if (startupMarketPulseSeedRatioInput && defaults.market_pulse_seed_ratio != null) {
    startupMarketPulseSeedRatioInput.value = String(defaults.market_pulse_seed_ratio);
  }
  if (startupBidFloorRatioInput && defaults.effective_bid_floor_ratio != null) {
    startupBidFloorRatioInput.value = String(defaults.effective_bid_floor_ratio);
  }
  if (startupPrecheckBufferInput && defaults.precheck_liquidity_buffer_months != null) {
    startupPrecheckBufferInput.value = String(defaults.precheck_liquidity_buffer_months);
  }
  if (startupPrecheckTaxFeeInput) {
    startupPrecheckTaxFeeInput.checked = Boolean(defaults.precheck_include_tax_and_fee);
  }
  if (minCashThresholdInput && defaults.min_cash_observer_threshold != null) {
    minCashThresholdInput.value = String(Math.round(Number(defaults.min_cash_observer_threshold) / 10000));
  }
  if (propertyTotalCountInput && defaults.property_count != null) {
    propertyTotalCountInput.value = String(defaults.property_count);
  }
  for (const zone of defaults.zones || []) {
    const zoneKey = String(zone.zone || "").toUpperCase();
    if (zoneKey === "A") {
      zoneAPriceMinInput.value = String(zone.price_min ?? zoneAPriceMinInput.value);
      zoneAPriceMaxInput.value = String(zone.price_max ?? zoneAPriceMaxInput.value);
      zoneARentInput.value = String(zone.rent_per_sqm ?? zoneARentInput.value);
    }
    if (zoneKey === "B") {
      zoneBPriceMinInput.value = String(zone.price_min ?? zoneBPriceMinInput.value);
      zoneBPriceMaxInput.value = String(zone.price_max ?? zoneBPriceMaxInput.value);
      zoneBRentInput.value = String(zone.rent_per_sqm ?? zoneBRentInput.value);
    }
  }
  const tierMap = new Map((defaults.agent_tiers || []).map((item) => [item.tier, item]));
  for (const item of STARTUP_TIER_FIELDS) {
    const tier = tierMap.get(item.tier);
    if (!tier) {
      continue;
    }
    item.countInput.value = String(tier.count ?? item.countInput.value);
    item.incomeMinInput.value = String(tier.income_min ?? item.incomeMinInput.value);
    item.incomeMaxInput.value = String(tier.income_max ?? item.incomeMaxInput.value);
    item.propertyMinInput.value = String(tier.property_min ?? item.propertyMinInput.value);
    item.propertyMaxInput.value = String(tier.property_max ?? item.propertyMaxInput.value);
  }
  const releaseDefaults = defaults.release_startup || null;
  if (releaseDefaults && startupSupplySnapshotInput) {
    const snapshots = Array.isArray(releaseDefaults.supply_snapshots) ? releaseDefaults.supply_snapshots : [];
    startupSupplySnapshotInput.innerHTML = snapshots
      .map((item) => {
        const snapshotId = String(item.snapshot_id || "");
        const selected = snapshotId === String(releaseDefaults.recommended_snapshot_id || "") ? " selected" : "";
        return `<option value="${snapshotId}"${selected}>${snapshotId} / ${item.display_name || ""} / ${item.total_selected_supply || 0}${getLang() === "en" ? " units" : " 套"}</option>`;
      })
      .join("");
  }
  if (startupMarketGoalInput) {
    startupMarketGoalInput.value = String(releaseDefaults?.default_market_goal || "balanced");
  }
  if (startupDemandMultiplierInput) {
    startupDemandMultiplierInput.value = String(
      getDefaultDemandMultiplierForGoal(startupMarketGoalInput?.value || releaseDefaults?.default_market_goal || "balanced")
    );
  }
}

function renderInlineError(target, title, copy, className = "preset-hint") {
  if (!target) {
    return;
  }
  target.classList.remove("hidden");
  target.className = `${className} error-state`;
  target.innerHTML = `
    <div class="preset-hint-title">${title}</div>
    <div class="preset-hint-copy">${copy}</div>
  `;
}

function renderStartupTierSummary() {
  const snapshot = getSelectedReleaseSnapshot();
  const demandPlan = estimateReleaseDemandPlan(
    snapshot,
    Number(startupDemandMultiplierInput?.value || getDefaultDemandMultiplierForGoal(startupMarketGoalInput?.value))
  );
  let plannedInterventionCount = 0;
  try {
    plannedInterventionCount = collectNightRunPlans().length;
  } catch (error) {
    plannedInterventionCount = 0;
  }
  if (propertyTotalCountInput) {
    propertyTotalCountInput.value = String(demandPlan.supplyCount);
  }
  if (agentCountInput) {
    agentCountInput.value = String(demandPlan.effectiveAgentCount);
  }
  if (startupTierSummary) {
    startupTierSummary.innerHTML = `
      <div class="preset-hint-title">${getLang() === "en" ? "Release startup" : "发布启动摘要"}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Snapshot" : "供应盘"}：${snapshot?.snapshot_id || "-"}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Requested multiplier" : "请求倍率"}：${demandPlan.requestedMultiplier.toFixed(2)}x</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Effective multiplier" : "有效倍率"}：${demandPlan.effectiveDemandMultiplier.toFixed(2)}x</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Agents / supply" : "人数 / 房源"}：${demandPlan.effectiveAgentCount} / ${demandPlan.supplyCount}</div>
    `;
  }
  if (propertyTotalHelper) {
    propertyTotalHelper.innerHTML = `
      <div class="preset-hint-title">${getLang() === "en" ? "Supply sample" : "样本说明"}</div>
      <div class="preset-hint-copy">${snapshot?.startup_characteristics || "-"}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Speed" : "速度"}：${snapshot?.speed_tradeoff || "-"}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Stability" : "稳定性"}：${snapshot?.accuracy_tradeoff || "-"}</div>
    `;
  }
  if (startupDemandCoverage) {
    startupDemandCoverage.innerHTML = `
      <div class="preset-hint-title">${getLang() === "en" ? "Coverage guard" : "双向覆盖保障"}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Buyer buckets" : "买家桶"}：${demandPlan.bucketCount}/${demandPlan.bucketCount}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Supply buckets" : "供应桶"}：${snapshot?.supply_bucket_count || 0}/${snapshot?.supply_bucket_count || 0}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Coverage floor" : "覆盖下限"}：${demandPlan.minimumDemandMultiplier.toFixed(2)}x</div>
      <div class="preset-hint-copy">${demandPlan.wasClamped ? (getLang() === "en" ? "Requested multiplier will auto-lift to preserve all buckets." : "请求倍率过低时会自动抬升，保证画像桶和供应桶不丢。") : (getLang() === "en" ? "Current multiplier already satisfies full coverage." : "当前倍率已满足全覆盖。")}</div>
    `;
  }
  if (startupOverview) {
    startupOverview.innerHTML = `
      <div class="preset-hint-title">${t("startup.overview_title")}</div>
      <div class="preset-hint-grid">
        <div class="preset-hint-item">
          <span class="preset-hint-label">${getLang() === "en" ? "Snapshot" : "供应盘"}</span>
          <strong>${snapshot?.snapshot_id || "-"}</strong>
          <span>${snapshot?.recommended_use || "-"}</span>
        </div>
        <div class="preset-hint-item">
          <span class="preset-hint-label">${getLang() === "en" ? "Demand" : "需求"}</span>
          <strong>${demandPlan.effectiveAgentCount} ${getLang() === "en" ? "agents" : "人"}</strong>
          <span>${getLang() === "en" ? "requested" : "请求"} ${demandPlan.requestedMultiplier.toFixed(2)}x · ${getLang() === "en" ? "effective" : "有效"} ${demandPlan.effectiveDemandMultiplier.toFixed(2)}x</span>
        </div>
        <div class="preset-hint-item">
          <span class="preset-hint-label">${getLang() === "en" ? "Supply" : "房源"}</span>
          <strong>${demandPlan.supplyCount} ${getLang() === "en" ? "units" : "套"}</strong>
          <span>${snapshot?.family_label || "-"}</span>
        </div>
        <div class="preset-hint-item">
          <span class="preset-hint-label">${getLang() === "en" ? "Run" : "运行"}</span>
          <strong>${Math.max(1, readNumberInput(monthsInput, 1))} ${getLang() === "en" ? "rounds" : "回合"}</strong>
          <span>${getLang() === "en" ? "Seed" : "种子"} ${readNumberInput(seedInput, 42)} · ${getLang() === "en" ? "Goal" : "目标"} ${startupMarketGoalInput?.value || "balanced"}</span>
        </div>
        <div class="preset-hint-item">
          <span class="preset-hint-label">${getLang() === "en" ? "Financing" : "融资"}</span>
          <strong>DP ${readNumberInput(startupDownPaymentRatioInput, 0.3).toFixed(2)} · DTI ${readNumberInput(startupMaxDtiRatioInput, 0.5).toFixed(2)}</strong>
          <span>${getLang() === "en" ? "Rate" : "利率"} ${readNumberInput(startupAnnualInterestRateInput, 0.035).toFixed(3)}</span>
        </div>
        <div class="preset-hint-item preset-hint-item-wide">
          <span class="preset-hint-label">${getLang() === "en" ? "Preplanned shocks" : "预排冲击"}</span>
          <strong>${plannedInterventionCount} ${getLang() === "en" ? "items" : "项"}</strong>
          <span>${getLang() === "en" ? "Income / supply add / supply cut all reuse the same round planner." : "收入冲击、增供、减供都复用同一套回合计划器。"}</span>
        </div>
      </div>
    `;
  }
  return { demandPlan, snapshot };
}

function getNightRunActionLabel(actionType) {
  const normalized = String(actionType || "").trim().toLowerCase();
  if (normalized === "developer_supply") {
    return getLang() === "en" ? "Developer Supply" : "开发商投房";
  }
  if (normalized === "supply_cut") {
    return getLang() === "en" ? "Supply Cut" : "减供下架";
  }
  if (normalized === "population_add") {
    return getLang() === "en" ? "Population Add" : "人口注入";
  }
  if (normalized === "income_shock") {
    return getLang() === "en" ? "Income Shock" : "收入冲击";
  }
  return normalized || (getLang() === "en" ? "Intervention" : "干预");
}

function renderNightRunPlanSummary(plans) {
  if (!nightPlanSummary) {
    return;
  }
  const normalizedPlans = Array.isArray(plans) ? [...plans] : [];
  if (!normalizedPlans.length) {
    nightPlanSummary.innerHTML = `
      <div class="preset-hint-title">${getLang() === "en" ? "Night run summary" : "夜跑总览"}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "No round interventions scheduled yet." : "当前还没有预设任何回合干预。"}</div>
    `;
    return;
  }
  const grouped = new Map();
  normalizedPlans.forEach((item) => {
    const month = Math.max(1, Number(item.month || 1));
    if (!grouped.has(month)) {
      grouped.set(month, []);
    }
    grouped.get(month).push(item);
  });
  const lines = Array.from(grouped.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([month, items]) => {
      const labels = items.map((item) => getNightRunActionLabel(item.action_type)).join(" / ");
      return `<div class="preset-hint-copy">${getLang() === "en" ? `Round ${month}` : `第 ${month} 回合`}：${labels}</div>`;
    })
    .join("");
  nightPlanSummary.innerHTML = `
    <div class="preset-hint-title">${getLang() === "en" ? "Night run summary" : "夜跑总览"}</div>
    ${lines}
  `;
}

function getDefaultNightRunPlan(actionType, month = 1) {
  const normalizedAction = String(actionType || "population_add").trim().toLowerCase();
  const normalizedMonth = Math.max(1, Number(month || 1));
  if (normalizedAction === "developer_supply") {
    return { month: normalizedMonth, action_type: "developer_supply", zone: "B", count: 3 };
  }
  if (normalizedAction === "supply_cut") {
    return { month: normalizedMonth, action_type: "supply_cut", zone: "A", count: 2 };
  }
  if (normalizedAction === "income_shock") {
    return { month: normalizedMonth, action_type: "income_shock", target_tier: "middle", pct_change: 0.05 };
  }
  return { month: normalizedMonth, action_type: "population_add", tier: "lower_middle", count: 3 };
}

function setNightPlanEditorExpanded(expanded) {
  nightPlanEditorExpanded = Boolean(expanded);
  nightPlanEditorBody?.classList.toggle("hidden", !nightPlanEditorExpanded);
  if (nightPlanToggleBtn) {
    nightPlanToggleBtn.textContent = nightPlanEditorExpanded ? t("startup.collapse_night_plan") : t("startup.expand_night_plan");
    nightPlanToggleBtn.classList.toggle("active", nightPlanEditorExpanded);
  }
}

function buildNightRunMonthGroup(month) {
  const group = document.createElement("section");
  group.className = "night-plan-month-group";
  group.dataset.month = String(month);
  group.innerHTML = `
    <div class="night-plan-month-head">
      <div class="night-plan-month-meta">
        <strong>${getLang() === "en" ? `Round ${month}` : `第 ${month} 回合`}</strong>
        <span>${getLang() === "en" ? "Scheduled interventions" : "预设干预"}</span>
      </div>
      <div class="night-plan-month-actions">
        <button type="button" class="ghost compact-btn night-plan-month-copy-prev">${getLang() === "en" ? "Copy prev round" : "复制上一回合"}</button>
        <button type="button" class="ghost compact-btn night-plan-month-add" data-action="population_add">${getLang() === "en" ? "+ Population" : "+ 人口"}</button>
        <button type="button" class="ghost compact-btn night-plan-month-add" data-action="developer_supply">${getLang() === "en" ? "+ Supply" : "+ 投房"}</button>
        <button type="button" class="ghost compact-btn night-plan-month-add" data-action="supply_cut">${getLang() === "en" ? "+ Cut" : "+ 减供"}</button>
        <button type="button" class="ghost compact-btn night-plan-month-add" data-action="income_shock">${getLang() === "en" ? "+ Income" : "+ 收入"}</button>
      </div>
    </div>
    <div class="night-plan-month-list"></div>
  `;
  group.querySelector(".night-plan-month-copy-prev")?.addEventListener("click", () => {
    const plans = collectNightRunPlans();
    const previousMonth = month - 1;
    const previousPlans = plans.filter((item) => Number(item.month || 0) === previousMonth);
    if (!previousPlans.length) {
      return;
    }
    const nextPlans = plans.concat(
      previousPlans.map((item) => ({
        ...item,
        month,
      }))
    );
    renderNightRunPlans(nextPlans);
  });
  group.querySelectorAll(".night-plan-month-add").forEach((button) => {
    button.addEventListener("click", () => {
      const actionType = String(button.dataset.action || "").trim().toLowerCase();
      const plans = collectNightRunPlans();
      if (actionType === "population_add") {
        plans.push({ month, action_type: "population_add", tier: "lower_middle", count: 3 });
      } else if (actionType === "developer_supply") {
        plans.push({ month, action_type: "developer_supply", zone: "B", count: 3 });
      } else if (actionType === "supply_cut") {
        plans.push({ month, action_type: "supply_cut", zone: "A", count: 2 });
      } else if (actionType === "income_shock") {
        plans.push({ month, action_type: "income_shock", target_tier: "middle", pct_change: 0.05 });
      }
      renderNightRunPlans(plans);
    });
  });
  return group;
}

function renderNightRunPlans(plans) {
  if (!nightPlanList) {
    return;
  }
  const normalizedPlans = Array.isArray(plans) ? [...plans] : [];
  normalizedPlans.sort((a, b) => Number(a.month || 0) - Number(b.month || 0));
  nightPlanList.innerHTML = "";
  renderNightRunPlanSummary(normalizedPlans);
  if (!normalizedPlans.length) {
    nightPlanList.innerHTML = `
      <div class="night-plan-empty">
        <strong>${getLang() === "en" ? "No scheduled interventions yet" : "当前还没有夜跑干预"}</strong>
        <span>${getLang() === "en" ? "Use the round and action controls above, or load the example plan." : "可以先用上方的回合和动作控件新增，或者点“载入示例计划”。"}</span>
      </div>
    `;
    return;
  }
  const groups = new Map();
  for (const plan of normalizedPlans) {
    const month = Math.max(1, Number(plan.month || 1));
    let group = groups.get(month);
    if (!group) {
      group = buildNightRunMonthGroup(month);
      groups.set(month, group);
      nightPlanList.appendChild(group);
    }
    group.querySelector(".night-plan-month-list")?.appendChild(buildNightRunPlanRow(plan));
  }
}

function buildNightRunPlanRow(plan = {}) {
  const row = document.createElement("article");
  row.className = "night-plan-row";
  const actionType = String(plan.action_type || "developer_supply").trim().toLowerCase();
  row.innerHTML = `
    <div class="night-plan-row-head">
      <strong class="night-plan-title">${getLang() === "en" ? "Round intervention" : "回合干预"}</strong>
      <button type="button" class="ghost compact-btn night-plan-remove">${getLang() === "en" ? "Remove" : "删除"}</button>
    </div>
    <div class="startup-tier-grid night-plan-row-grid">
      <label class="night-plan-field" data-field="month"><span>${getLang() === "en" ? "Round" : "回合"}</span><input class="night-plan-month" type="number" min="1" step="1" value="${Number(plan.month || 1)}"></label>
      <label class="night-plan-field" data-field="action"><span>${getLang() === "en" ? "Action" : "动作"}</span><select class="night-plan-action"><option value="developer_supply"${actionType === "developer_supply" ? " selected" : ""}>${getNightRunActionLabel("developer_supply")}</option><option value="supply_cut"${actionType === "supply_cut" ? " selected" : ""}>${getNightRunActionLabel("supply_cut")}</option><option value="population_add"${actionType === "population_add" ? " selected" : ""}>${getNightRunActionLabel("population_add")}</option><option value="income_shock"${actionType === "income_shock" ? " selected" : ""}>${getNightRunActionLabel("income_shock")}</option></select></label>
      <label class="night-plan-field" data-field="count"><span>${getLang() === "en" ? "Count" : "数量"}</span><input class="night-plan-count" type="number" min="1" step="1" value="${Number(plan.count || 1)}"></label>
      <label class="night-plan-field" data-field="target"><span>${getLang() === "en" ? "Zone / Tier" : "区域 / 层级"}</span><input class="night-plan-target" type="text" value="${String(plan.zone || plan.tier || plan.target_tier || "")}"></label>
      <label class="night-plan-field" data-field="template"><span>${getLang() === "en" ? "Template" : "模板"}</span><input class="night-plan-template" type="text" value="${String(plan.template || "")}"></label>
      <label class="night-plan-field" data-field="pct"><span>${getLang() === "en" ? "Pct change" : "变动比例"}</span><input class="night-plan-pct" type="number" step="0.01" value="${plan.pct_change != null ? Number(plan.pct_change) : ""}"></label>
    </div>
    <div class="night-plan-row-copy">${getLang() === "en" ? "Developer supply uses zone/count/template. Supply cut uses zone/count. Population add uses tier/count/template. Income shock uses target tier + pct change." : "开发商投房使用区域/数量/模板；减供下架使用区域/数量；人口注入使用层级/数量/模板；收入冲击使用目标层级和变动比例。"}</div>
  `;
  const syncRow = () => syncNightRunPlanRow(row);
  row.querySelector(".night-plan-action")?.addEventListener("change", syncRow);
  row.querySelector(".night-plan-month")?.addEventListener("input", () => {
    syncRow();
    rerenderNightRunPlanTimeline();
  });
  row.querySelector(".night-plan-target")?.addEventListener("input", syncRow);
  row.querySelector(".night-plan-remove")?.addEventListener("click", () => {
    row.remove();
    rerenderNightRunPlanTimeline();
  });
  syncRow();
  return row;
}

function rerenderNightRunPlanTimeline() {
  const plans = collectNightRunPlans();
  renderNightRunPlans(plans);
}

function syncNightRunPlanRow(row) {
  const actionType = String(row.querySelector(".night-plan-action")?.value || "").trim().toLowerCase();
  const month = Math.max(1, Number.parseInt(row.querySelector(".night-plan-month")?.value || "1", 10));
  const targetInput = row.querySelector(".night-plan-target");
  const title = row.querySelector(".night-plan-title");
  const countField = row.querySelector('[data-field="count"]');
  const templateField = row.querySelector('[data-field="template"]');
  const pctField = row.querySelector('[data-field="pct"]');
  if (title) {
    title.textContent = `${getLang() === "en" ? "Round" : "第"} ${month}${getLang() === "en" ? "" : " 回合"} · ${getNightRunActionLabel(actionType)}`;
  }
  if (targetInput) {
    targetInput.placeholder =
      actionType === "developer_supply"
        ? (getLang() === "en" ? "A / B" : "A / B 区")
        : actionType === "supply_cut"
          ? (getLang() === "en" ? "A / B" : "A / B 区")
        : actionType === "population_add"
          ? (getLang() === "en" ? "lower_middle" : "收入层级")
          : (getLang() === "en" ? "all / middle / high" : "目标层级");
  }
  countField?.classList.toggle("hidden", actionType === "income_shock");
  templateField?.classList.toggle("hidden", actionType === "income_shock" || actionType === "supply_cut");
  pctField?.classList.toggle("hidden", actionType !== "income_shock");
}

function resetNightRunPlanEditor() {
  if (!nightPlanList) {
    return;
  }
  renderNightRunPlans([]);
}

function loadNightRunExamplePlans() {
  renderNightRunPlans(NIGHT_RUN_EXAMPLE_PLANS);
}

function exportNightRunPlans() {
  const payload = {
    version: 1,
    exported_at: new Date().toISOString(),
    preplanned_interventions: collectNightRunPlans(),
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "night_run_plan.json";
  link.click();
  URL.revokeObjectURL(url);
}

async function importNightRunPlansFromFile(file) {
  const text = await file.text();
  const data = JSON.parse(text);
  const plans = Array.isArray(data)
    ? data
    : Array.isArray(data?.preplanned_interventions)
      ? data.preplanned_interventions
      : null;
  if (!plans) {
    throw new Error(getLang() === "en" ? "Invalid night run JSON format." : "夜跑 JSON 格式不正确。");
  }
  renderNightRunPlans(plans);
}

function collectNightRunPlans() {
  if (!nightPlanList) {
    return [];
  }
  return Array.from(nightPlanList.querySelectorAll(".night-plan-row"))
    .map((row) => {
      const actionType = String(row.querySelector(".night-plan-action")?.value || "").trim().toLowerCase();
      const month = Math.max(1, Number.parseInt(row.querySelector(".night-plan-month")?.value || "1", 10));
      const count = Math.max(1, Number.parseInt(row.querySelector(".night-plan-count")?.value || "1", 10));
      const target = String(row.querySelector(".night-plan-target")?.value || "").trim();
      const template = String(row.querySelector(".night-plan-template")?.value || "").trim();
      const pctRaw = String(row.querySelector(".night-plan-pct")?.value || "").trim();
      const item = { month, action_type: actionType };
      if (actionType === "developer_supply") {
        if (!/^[AB]$/i.test(target || "")) {
          throw new Error(getLang() === "en" ? `Night run round ${month}: supply zone must be A or B.` : `夜跑第 ${month} 回合：开发商投房的区域必须是 A 或 B。`);
        }
        item.zone = target || "B";
        item.count = count;
        if (template) item.template = template;
      } else if (actionType === "supply_cut") {
        if (!/^[AB]$/i.test(target || "")) {
          throw new Error(getLang() === "en" ? `Night run round ${month}: supply cut zone must be A or B.` : `夜跑第 ${month} 回合：减供下架的区域必须是 A 或 B。`);
        }
        item.zone = target || "A";
        item.count = count;
      } else if (actionType === "population_add") {
        if (!target) {
          throw new Error(getLang() === "en" ? `Night run round ${month}: population add requires a tier.` : `夜跑第 ${month} 回合：人口注入必须填写收入层级。`);
        }
        item.tier = target || "lower_middle";
        item.count = count;
        if (template) item.template = template;
      } else if (actionType === "income_shock") {
        const pctValue = pctRaw === "" ? NaN : Number(pctRaw);
        if (!target) {
          throw new Error(getLang() === "en" ? `Night run round ${month}: income shock requires a target tier.` : `夜跑第 ${month} 回合：收入冲击必须填写目标层级。`);
        }
        if (!Number.isFinite(pctValue) || pctValue <= -1 || pctValue >= 10) {
          throw new Error(getLang() === "en" ? `Night run round ${month}: income shock pct must be between -1 and 10.` : `夜跑第 ${month} 回合：收入冲击比例必须在 -1 到 10 之间。`);
        }
        item.target_tier = target || "all";
        item.pct_change = pctValue;
      }
      return item;
    })
    .filter((item) => item.action_type);
}

function buildStartupConfirmMessage(payload) {
  const overrides = payload?.startup_overrides || {};
  const isNightRun = Boolean(payload?.night_run);
  const nightPlanPath = String(payload?.night_plan_path || "").trim();
  const nightPlans = Array.isArray(payload?.preplanned_interventions) ? payload.preplanned_interventions : [];
  if (overrides.use_release_supply_controls) {
    const snapshot = getReleaseSnapshotMap().get(String(overrides.fixed_supply_snapshot_id || "").trim()) || null;
    const demandPlan = estimateReleaseDemandPlan(snapshot, Number(overrides.demand_multiplier || 1));
    const planLines = nightPlans.slice(0, 6).map(
      (item) => `- ${getLang() === "en" ? "Round" : "第"} ${item.month}${getLang() === "en" ? "" : " 回合"} · ${getNightRunActionLabel(item.action_type)}`
    );
    return [
      isNightRun
        ? (getLang() === "en" ? "Start an unattended night run with the following release setup?" : "确认按以下发布口径启动夜跑模拟？")
        : (getLang() === "en" ? "Start this release-mode simulation?" : "确认按以下发布口径启动模拟？"),
      `${getLang() === "en" ? "Snapshot" : "固定供应盘"}：${snapshot?.snapshot_id || "-"}`,
      `${getLang() === "en" ? "Supply family" : "结构家族"}：${snapshot?.family_label || "-"}`,
      `${getLang() === "en" ? "Market goal" : "市场目标"}：${overrides.market_goal || "balanced"}`,
      `${getLang() === "en" ? "Supply units" : "供应套数"}：${snapshot?.total_selected_supply || 0}`,
      `${getLang() === "en" ? "Requested multiplier" : "请求倍率"}：${Number(overrides.demand_multiplier || 1).toFixed(2)}x`,
      `${getLang() === "en" ? "Effective multiplier" : "有效倍率"}：${demandPlan.effectiveDemandMultiplier.toFixed(2)}x`,
      `${getLang() === "en" ? "Effective agents" : "有效 agent"}：${demandPlan.effectiveAgentCount}`,
      `${getLang() === "en" ? "Coverage floor" : "覆盖下限"}：${Number(snapshot?.minimum_demand_multiplier || 0).toFixed(2)}x`,
      `${getLang() === "en" ? "Auto lift" : "自动抬升"}：${demandPlan.wasClamped ? translateBool(true) : translateBool(false)}`,
      `${getLang() === "en" ? "Rounds" : "模拟回合"}：${payload.months} · ${getLang() === "en" ? "Seed" : "随机种子"}：${payload.seed}`,
      `${getLang() === "en" ? "Income multiplier" : "收入倍率"}：${Number(overrides.income_adjustment_rate || 1).toFixed(2)}`,
      `${getLang() === "en" ? "Financing" : "融资参数"}：DP ${Number(overrides.down_payment_ratio || 0).toFixed(2)} · DTI ${Number(overrides.max_dti_ratio || 0).toFixed(2)} · Rate ${Number(overrides.annual_interest_rate || 0).toFixed(3)}`,
      `${getLang() === "en" ? "Order gate" : "下单门槛"}：${getLang() === "en" ? "bid floor" : "出价下限"} ${Number(overrides.effective_bid_floor_ratio || 0).toFixed(2)} · ${getLang() === "en" ? "buffer" : "缓冲"} ${overrides.precheck_liquidity_buffer_months}${getLang() === "en" ? " cycles" : "个周期"} · ${getLang() === "en" ? "fees" : "税费预检"} ${translateBool(overrides.precheck_include_tax_and_fee)}`,
      `${getLang() === "en" ? "Market Pulse" : "Market Pulse"}：${translateBool(Boolean(overrides.market_pulse_enabled))} · ${getLang() === "en" ? "seed ratio" : "存量按揭覆盖率"} ${Number(overrides.market_pulse_seed_ratio || 0).toFixed(2)}`,
      `${getLang() === "en" ? "Intervention panel" : "回合末人工干预面板"}：${translateBool(Boolean(overrides.enable_intervention_panel))}`,
      `${getLang() === "en" ? "Preplanned shocks" : "预排冲击"}：${nightPlans.length} ${getLang() === "en" ? "items" : "项"}`,
      ...planLines,
      ...(isNightRun ? [`${getLang() === "en" ? "Night plan" : "夜跑计划"}：${nightPlanPath || "config/night_run_example.yaml"}`] : []),
    ].join("\n");
  }
  const tiers = Array.isArray(overrides.agent_tiers) ? overrides.agent_tiers : [];
  const tierLines = tiers
    .filter((item) => item.count > 0)
    .map((item) => {
      const incomeMin = Math.round(Number(item.income_min || 0) / 1000);
      const incomeMax = Math.round(Number(item.income_max || 0) / 1000);
      return `${tierShortLabel(item.tier)}：${item.count}${getLang() === "en" ? " agents" : "人"}，收入 ${incomeMin}-${incomeMax}k，拥房 ${item.property_min}-${item.property_max}${getLang() === "en" ? "" : " 套"}`;
    });
  const zoneMap = new Map((overrides.zones || []).map((item) => [item.zone, item]));
  const zoneA = zoneMap.get("A");
  const zoneB = zoneMap.get("B");
  const onOff = (value) => (value ? (getLang() === "en" ? "on" : "开启") : (getLang() === "en" ? "off" : "关闭"));
  const zhExplain = {
    cashGate: "说明：无房且现金低于这条线的人，不进入本轮交易。",
    mortgage: "说明：首付越高、DTI 越严、利率越高，买房门槛越高。",
    orderGate: "说明：出价下限越高越不容易成交；现金缓冲越大越保守；计入税费会进一步收紧购买资格。",
    pulse: "说明：Market Pulse 用来模拟个贷压力环境；覆盖率越高，压力测试样本越多。",
    zones: "说明：A 区更贵，B 区更便宜；租金会影响收益率和持有判断。",
  };
  return [
    isNightRun
      ? (getLang() === "en" ? "Start an unattended night run with the following setup?" : "确认按以下配置启动夜跑模拟？")
      : (getLang() === "en" ? "Start this simulation with the following setup?" : "确认按以下配置启动模拟？"),
    ...(isNightRun ? [`${getLang() === "en" ? "Night plan" : "夜跑计划"}：${nightPlanPath || "config/night_run_example.yaml"}`] : []),
    ...(isNightRun && nightPlans.length
      ? [
          `${getLang() === "en" ? "Inline interventions" : "网页预设干预"}：${nightPlans.length} ${getLang() === "en" ? "items" : "条"}`,
          ...nightPlans.slice(0, 6).map((item) => `- ${getLang() === "en" ? "Round" : "第"} ${item.month}${getLang() === "en" ? "" : " 回合"} · ${getNightRunActionLabel(item.action_type)}`),
        ]
      : []),
    `${getLang() === "en" ? "Agents" : "Agent 总数"}：${payload.agent_count}`,
    ...tierLines.map((line) => `- ${line}`),
    `${getLang() === "en" ? "Properties" : "房产总数"}：${overrides.property_count}`,
    `${getLang() === "en" ? "Rounds" : "模拟回合"}：${payload.months} · ${getLang() === "en" ? "Seed" : "随机种子"}：${payload.seed}`,
    `${getLang() === "en" ? "Cash gate" : "无房现金门槛"}：${Math.round(Number(overrides.min_cash_observer_threshold || 0) / 10000)} ${getLang() === "en" ? "x10k CNY" : "万元"}`,
    ...(getLang() === "en" ? [] : [zhExplain.cashGate]),
    `${getLang() === "en" ? "Base year" : "基准年份"}：${overrides.base_year} · ${getLang() === "en" ? "Income adjustment" : "收入调整"}：${Number(overrides.income_adjustment_rate || 1).toFixed(2)}`,
    `${getLang() === "en" ? "Mortgage" : "融资参数"}：DP ${Number(overrides.down_payment_ratio || 0).toFixed(2)} · DTI ${Number(overrides.max_dti_ratio || 0).toFixed(2)} · Rate ${Number(overrides.annual_interest_rate || 0).toFixed(3)}`,
    ...(getLang() === "en" ? [] : [zhExplain.mortgage]),
    `${getLang() === "en" ? "Order gate" : "下单门槛"}：${getLang() === "en" ? "bid floor" : "出价下限"} ${Number(overrides.effective_bid_floor_ratio || 0).toFixed(2)} · ${getLang() === "en" ? "buffer" : "缓冲"} ${overrides.precheck_liquidity_buffer_months}${getLang() === "en" ? " cycles" : "个周期"} · ${getLang() === "en" ? "fees" : "税费预检"} ${onOff(overrides.precheck_include_tax_and_fee)}`,
    ...(getLang() === "en" ? [] : [zhExplain.orderGate]),
    `${getLang() === "en" ? "Market Pulse" : "Market Pulse"}：${onOff(overrides.market_pulse_enabled)} · ${getLang() === "en" ? "seed ratio" : "存量按揭覆盖率"} ${Number(overrides.market_pulse_seed_ratio || 0).toFixed(2)}`,
    ...(getLang() === "en" ? [] : [zhExplain.pulse]),
      `${getLang() === "en" ? "Intervention panel" : "回合末人工干预面板"}：${onOff(overrides.enable_intervention_panel)}`,
    `${getLang() === "en" ? "Zone A" : "A 区"}：${zoneA ? `${zoneA.price_min}-${zoneA.price_max} /㎡ · ${zoneA.rent_per_sqm}${getLang() === "en" ? " rent" : " 租金"}` : "-"}`,
    `${getLang() === "en" ? "Zone B" : "B 区"}：${zoneB ? `${zoneB.price_min}-${zoneB.price_max} /㎡ · ${zoneB.rent_per_sqm}${getLang() === "en" ? " rent" : " 租金"}` : "-"}`,
    ...(getLang() === "en" ? [] : [zhExplain.zones]),
  ].join("\n");
}

function getStartupPayloadFromSchema() {
  const mode = startModeInput?.value || "new";
  if (mode === "resume") {
    const dbPath = String(resumeRunSelect?.value || "").trim();
    if (!dbPath) {
      throw new Error(getLang() === "en" ? "Select a resumable run first." : "请先选择一个可恢复运行。");
    }
    return {
      resume: true,
      db_path: dbPath,
      config_path: "config/baseline.yaml",
    };
  }

  const snapshot = getSelectedReleaseSnapshot();
  if (!snapshot) {
    throw new Error(getLang() === "en" ? "Select a fixed supply snapshot first." : "请先选择一个固定供应盘。");
  }
  const marketGoal = String(startupMarketGoalInput?.value || "balanced").trim().toLowerCase();
  const startupSummary = renderStartupTierSummary();
  const demandPlan = startupSummary.demandPlan;
  const payload = {
    agent_count: demandPlan.effectiveAgentCount,
    months: Math.max(1, readNumberInput(monthsInput, 1)),
    seed: readNumberInput(seedInput, 42),
    config_path: "config/baseline.yaml",
    startup_overrides: {
      use_release_supply_controls: true,
      fixed_supply_snapshot_id: snapshot.snapshot_id,
      market_goal: marketGoal,
      demand_multiplier: Number(startupDemandMultiplierInput?.value || demandPlan.requestedMultiplier),
      base_year: readNumberInput(baseYearInput, 2025),
      income_adjustment_rate: Number(readNumberInput(incomeAdjustmentRateInput, 1)),
      down_payment_ratio: Number(readNumberInput(startupDownPaymentRatioInput, 0.3)),
      max_dti_ratio: Number(readNumberInput(startupMaxDtiRatioInput, 0.5)),
      annual_interest_rate: Number(readNumberInput(startupAnnualInterestRateInput, 0.035)),
      enable_intervention_panel: Boolean(startupEnablePanelInput?.checked),
      market_pulse_enabled: Boolean(startupMarketPulseEnabledInput?.checked),
      market_pulse_seed_ratio: Number(readNumberInput(startupMarketPulseSeedRatioInput, 0.55)),
      effective_bid_floor_ratio: Number(readNumberInput(startupBidFloorRatioInput, 0.98)),
      precheck_liquidity_buffer_months: Math.max(0, readNumberInput(startupPrecheckBufferInput, 3)),
      precheck_include_tax_and_fee: Boolean(startupPrecheckTaxFeeInput?.checked),
    },
  };
  const preplannedInterventions = collectNightRunPlans();
  if (preplannedInterventions.length) {
    payload.preplanned_interventions = preplannedInterventions;
  }
  if (mode === "night_run") {
    payload.night_run = true;
    payload.night_plan_path = String(nightPlanPathInput?.value || "config/night_run_example.yaml").trim() || "config/night_run_example.yaml";
    payload.startup_overrides.enable_intervention_panel = false;
  }
  return payload;
}

function formatSchemaValue(value, fieldType) {
  if (value == null || value === "") {
    return "-";
  }
  if (fieldType === "boolean") {
    return value ? "true" : "false";
  }
  if (fieldType === "number") {
    return Number(value).toFixed(Number.isInteger(Number(value)) ? 0 : 3).replace(/\.?0+$/, "");
  }
  return String(value);
}

function renderControlsSummary() {
  if (!controlsSummary) {
    return;
  }
  controlsSummary.innerHTML = `
    <div class="preset-hint-title">${getLang() === "en" ? "Current runtime controls" : "当前运行参数"}</div>
    <div class="preset-hint-grid">
      <div class="preset-hint-item">
        <span class="preset-hint-label">${getLang() === "en" ? "Financing" : "融资"}</span>
        <strong>DP ${formatSchemaValue(currentRuntimeControls.down_payment_ratio, "number")}</strong>
        <span>Rate ${formatSchemaValue(currentRuntimeControls.annual_interest_rate, "number")} · DTI ${formatSchemaValue(currentRuntimeControls.max_dti_ratio, "number")}</span>
      </div>
      <div class="preset-hint-item">
        <span class="preset-hint-label">${getLang() === "en" ? "Macro" : "宏观"}</span>
        <strong>${currentRuntimeControls.macro_override_mode || "default"}</strong>
        <span>Pulse ${translateBool(currentRuntimeControls.market_pulse_enabled)}</span>
      </div>
      <div class="preset-hint-item preset-hint-item-wide">
        <span class="preset-hint-label">${getLang() === "en" ? "Quotes" : "谈判流"}</span>
        <strong>${currentRuntimeControls.negotiation_quote_stream_enabled ? (getLang() === "en" ? "stream on" : "原话流开启") : (getLang() === "en" ? "stream off" : "原话流关闭")}</strong>
        <span>Filter ${currentRuntimeControls.negotiation_quote_filter_mode || "all"} · Mode ${currentRuntimeControls.negotiation_quote_mode || "limited_quotes"} · Turns ${currentRuntimeControls.negotiation_quote_turn_limit || 4} · Chars ${currentRuntimeControls.negotiation_quote_char_limit || 84}</span>
      </div>
    </div>
  `;
}

function matchesSchemaFilters(item) {
  const search = currentSchemaFilters.search.trim().toLowerCase();
  const phase = currentSchemaFilters.phase;
  const group = currentSchemaFilters.group;
  const onlyEditable = currentSchemaFilters.onlyEditable;
  if (onlyEditable && item.editable_phase === "readonly") {
    return false;
  }
  if (phase !== "all" && item.editable_phase !== phase) {
    return false;
  }
  if (group !== "all" && item.group !== group) {
    return false;
  }
  if (!search) {
    return true;
  }
  const haystack = [
    item.label,
    item.key,
    item.description,
    item.group_label,
    item.editable_phase,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(search);
}

function syncSchemaGroupFilter(groups) {
  if (!configSchemaGroupFilterInput) {
    return;
  }
  const nextOptions = [
    `<option value="all">${getLang() === "en" ? "all groups" : "全部分组"}</option>`,
    ...groups.map((group) => `<option value="${group.id}">${localizeSchemaGroupLabel(group)}</option>`),
  ].join("");
  configSchemaGroupFilterInput.innerHTML = nextOptions;
  const validValues = new Set(["all", ...groups.map((group) => group.id)]);
  if (!validValues.has(currentSchemaFilters.group)) {
    currentSchemaFilters.group = "all";
  }
  configSchemaGroupFilterInput.value = currentSchemaFilters.group;
}

function countSchemaPhases(parameters) {
  return parameters.reduce(
    (acc, item) => {
      const phase = item.editable_phase || "unknown";
      acc[phase] = (acc[phase] || 0) + 1;
      return acc;
    },
    { startup_only: 0, between_steps: 0, readonly: 0 }
  );
}

function renderConfigSchema(data) {
  if (!configSchemaList || !configSchemaSummary || !configSchemaCount) {
    return;
  }
  latestConfigSchemaData = data;
  const parameters = Array.isArray(data?.parameters) ? data.parameters : [];
  const groups = Array.isArray(data?.groups) ? data.groups : [];
  syncSchemaGroupFilter(groups);
  const filteredParameters = parameters.filter(matchesSchemaFilters);
  const catalogParameters = filteredParameters.filter((item) => item.editable_phase === "readonly");
  const phaseCounts = countSchemaPhases(filteredParameters);
  configSchemaCount.textContent = `${filteredParameters.length}/${parameters.length}`;
  configSchemaSummary.innerHTML = [
    `<div class="preset-hint-title">${getLang() === "en" ? "Schema-driven controls" : "参数目录联动控件"}</div>`,
    `<div class="preset-hint-copy">${getLang() === "en" ? "Config source" : "配置来源"}：${data?.config_path || "config/baseline.yaml"}</div>`,
    `<div class="preset-hint-copy">${getLang() === "en" ? "Editing rule" : "编辑规则"}：${getLang() === "en" ? "Startup-only fields lock after launch; between-step fields can be changed between rounds." : "“仅启动前”字段会在启动后锁定；“回合间可调”字段可在回合之间调整。"} </div>`,
    `<div class="preset-hint-copy">${getLang() === "en" ? "Filtered view" : "当前筛选结果"}：${filteredParameters.length} ${getLang() === "en" ? "items visible" : "项可见"}</div>`,
    `<div class="preset-hint-copy">${getLang() === "en" ? "Reference catalog" : "下方目录"}：${getLang() === "en" ? "now only shows baseline references to avoid duplicate editable entries." : "现在只显示基线参考，避免与上方可编辑项重复。"} </div>`,
    `<div class="preset-hint-grid">
      <div class="preset-hint-item"><span class="preset-hint-label">${localizeSchemaPhaseLabel("startup_only")}</span><strong>${phaseCounts.startup_only || 0}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${localizeSchemaPhaseLabel("between_steps")}</span><strong>${phaseCounts.between_steps || 0}</strong></div>
      <div class="preset-hint-item preset-hint-item-wide"><span class="preset-hint-label">${localizeSchemaPhaseLabel("readonly")}</span><strong>${phaseCounts.readonly || 0}</strong></div>
    </div>`,
  ].join("");
  if (catalogParameters.length === 0) {
    configSchemaList.innerHTML = `<div class="preset-hint-copy">${getLang() === "en" ? "No baseline reference items under current filters." : "当前筛选下没有基线参考项。"}</div>`;
    renderConfigSchemaEditor(filteredParameters, groups);
    return;
  }

  configSchemaList.innerHTML = groups
    .map((group) => {
      const items = catalogParameters.filter((item) => item.group === group.id);
      if (items.length === 0) {
        return "";
      }
      const isCollapsed = collapsedSchemaGroups.has(group.id);
      return `
        <section class="config-schema-group" data-schema-group="${group.id}" data-collapsed="${isCollapsed ? "true" : "false"}">
          <button type="button" class="config-schema-group-head" data-schema-toggle="${group.id}">
            <span class="config-schema-group-title">${localizeSchemaGroupLabel(group)}${buildHelpTooltip(getSchemaGroupHelp(group))}</span>
            <span class="config-schema-group-count">${items.length}</span>
          </button>
          <div class="config-schema-group-list">
            ${items
              .map(
                (item) => `
                  <article class="config-schema-item">
                    <div class="config-schema-item-head">
                      <strong>${localizeSchemaFieldLabel(item)}${buildHelpTooltip(getSchemaHelp(item))}</strong>
                      <span class="config-schema-phase phase-${item.editable_phase}">${localizeSchemaPhaseLabel(item.editable_phase)}</span>
                    </div>
                    <div class="config-schema-meta">${item.key}</div>
                    <div class="config-schema-copy">${getSchemaHelp(item).explain}</div>
                    <div class="config-schema-values">
                      <span>${getLang() === "en" ? "Current" : "当前"} ${formatSchemaValue(item.current_value, item.type)}</span>
                      <span>${getLang() === "en" ? "Default" : "默认"} ${formatSchemaValue(item.default, item.type)}</span>
                    </div>
                  </article>
                `
              )
              .join("")}
          </div>
        </section>
      `;
    })
    .join("");
  for (const toggle of configSchemaList.querySelectorAll("[data-schema-toggle]")) {
    toggle.addEventListener("click", () => {
      const groupId = toggle.dataset.schemaToggle;
      if (!groupId) {
        return;
      }
      if (collapsedSchemaGroups.has(groupId)) {
        collapsedSchemaGroups.delete(groupId);
      }
      else {
        collapsedSchemaGroups.add(groupId);
      }
      renderConfigSchema(latestConfigSchemaData);
    });
  }
  renderConfigSchemaEditor(filteredParameters, groups);
}

function buildSchemaInput(item) {
  const key = item.key;
  const startupInput = STARTUP_FORM_KEY_MAP[key];
  const value = startupInput ? startupInput.value : item.current_value;
  const label = localizeSchemaFieldLabel(item);
  const phaseLabel = localizeSchemaPhaseLabel(item.editable_phase);
  const help = buildHelpTooltip(getSchemaHelp(item));
  if (item.type === "boolean") {
    return `
      <label class="checkbox-row schema-field schema-field-checkbox">
        <span class="schema-field-head"><span class="schema-field-label">${label}${help}</span><span class="schema-field-phase-tag">${phaseLabel}</span></span>
        <input data-schema-key="${key}" data-schema-type="${item.type}" type="checkbox" ${value ? "checked" : ""}>
        <span class="config-schema-meta">${item.key}</span>
      </label>
    `;
  }
  if (item.type === "enum") {
    return `
      <label class="schema-field">
        <span class="schema-field-head"><span class="schema-field-label">${label}${help}</span><span class="schema-field-phase-tag">${phaseLabel}</span></span>
        <select data-schema-key="${key}" data-schema-type="${item.type}">
          ${(item.options || [])
            .map((option) => {
              const selected = String(option) === String(value ?? "") ? "selected" : "";
              const optionLabel = option === "" ? (getLang() === "en" ? "default" : "默认") : option;
              return `<option value="${option}" ${selected}>${optionLabel}</option>`;
            })
            .join("")}
        </select>
        <span class="config-schema-meta">${item.key}</span>
      </label>
    `;
  }
  const minAttr = item.min != null ? `min="${item.min}"` : "";
  const maxAttr = item.max != null ? `max="${item.max}"` : "";
  const stepAttr = item.step != null ? `step="${item.step}"` : "";
  const inputType = item.type === "integer" ? "number" : "number";
  const displayValue = value ?? item.default ?? "";
  return `
    <label class="schema-field">
      <span class="schema-field-head"><span class="schema-field-label">${label}${help}</span><span class="schema-field-phase-tag">${phaseLabel}</span></span>
      <input
        data-schema-key="${key}"
        data-schema-type="${item.type}"
        type="${inputType}"
        value="${displayValue}"
        ${minAttr}
        ${maxAttr}
        ${stepAttr}
      >
      <span class="config-schema-meta">${item.key}</span>
    </label>
  `;
}

function renderConfigSchemaEditor(parameters, groups) {
  if (!configSchemaEditList || !configSchemaStartupList || !configSchemaReadonlyList) {
    return;
  }
  const editableItems = parameters.filter((item) => item.editable_phase === "between_steps" && SCHEMA_CONTROL_KEY_MAP[item.key]);
  const readonlyItems = parameters.filter((item) => item.editable_phase === "readonly");

  configSchemaStartupList.innerHTML = `
    <div class="preset-hint-title">${getLang() === "en" ? "Startup controls moved" : "启动前参数已上移"}</div>
    <div class="preset-hint-copy">${getLang() === "en" ? "Use the startup wizard above for agent tiers, supply total, zone pricing, rent, threshold, rounds, and seed." : "Agent 分档、房产总量、区域单价、租金、现金门槛、回合数和种子，现在统一在上方启动向导中配置。"} </div>
    <div class="preset-hint-copy">${getLang() === "en" ? SCHEMA_PHASE_META.startup_only.hintEn : SCHEMA_PHASE_META.startup_only.hintZh}</div>
  `;

  configSchemaEditList.innerHTML = groups
    .map((group) => {
      const items = editableItems.filter((item) => item.group === group.id);
      if (items.length === 0) {
        return "";
      }
      return `
        <section class="config-schema-edit-group" data-schema-phase-group="between_steps">
          <div class="config-schema-group-title">
            ${localizeSchemaGroupLabel(group)}
            ${buildHelpTooltip(getSchemaGroupHelp(group))}
          </div>
          <div class="config-schema-group-note">${getLang() === "en" ? SCHEMA_PHASE_META.between_steps.hintEn : SCHEMA_PHASE_META.between_steps.hintZh}</div>
          <div class="config-schema-edit-grid">
            ${items.map((item) => `
              <div class="config-schema-edit-card">
                ${buildSchemaInput(item)}
              </div>
            `).join("")}
          </div>
        </section>
      `;
    })
    .join("");

  configSchemaReadonlyList.innerHTML = groups
    .map((group) => {
      const items = readonlyItems.filter((item) => item.group === group.id);
      if (items.length === 0) {
        return "";
      }
      return `
        <section class="config-schema-edit-group" data-schema-phase-group="readonly">
          <div class="config-schema-group-title">
            ${getLang() === "en" ? "Readonly" : "只读"} · ${localizeSchemaGroupLabel(group)}
            ${buildHelpTooltip(getSchemaGroupHelp(group))}
          </div>
          <div class="config-schema-group-note">${getLang() === "en" ? SCHEMA_PHASE_META.readonly.hintEn : SCHEMA_PHASE_META.readonly.hintZh}</div>
          <div class="config-schema-readonly-grid">
            ${items.map((item) => `
              <article class="config-schema-item compact">
                <div class="config-schema-item-head">
                  <strong>${localizeSchemaFieldLabel(item)}${buildHelpTooltip(getSchemaHelp(item))}</strong>
                  <span class="config-schema-phase phase-readonly">${localizeSchemaPhaseLabel("readonly")}</span>
                </div>
                <div class="config-schema-meta">${item.key}</div>
                <div class="config-schema-values">
                  <span>${getLang() === "en" ? "Current" : "当前"} ${formatSchemaValue(item.current_value, item.type)}</span>
                  <span>${getLang() === "en" ? "Default" : "默认"} ${formatSchemaValue(item.default, item.type)}</span>
                </div>
              </article>
            `).join("")}
          </div>
        </section>
      `;
    })
    .join("");

  syncSchemaEditabilityForStatus({ status: currentRuntimeStatus });
}

export function syncSchemaEditabilityForStatus(status) {
  currentRuntimeStatus = status?.status || currentRuntimeStatus || "idle";
  const startupLocked = currentRuntimeStatus !== "idle";
  const startupInputs = document.querySelectorAll("[data-startup-control]");
  for (const input of startupInputs) {
    input.disabled = startupLocked;
  }
  for (const input of Object.values(STARTUP_FORM_KEY_MAP)) {
    if (input) {
      input.disabled = startupLocked;
    }
  }
  for (const note of configSchemaStartupList?.querySelectorAll(".preset-hint-copy") || []) {
    note.dataset.locked = startupLocked ? "true" : "false";
  }
}

export async function fetchStatus() {
  const resp = await fetch("/status");
  const status = await resp.json();
  renderStatus(status);
  syncSchemaEditabilityForStatus(status);
}

export async function fetchRuns() {
  const resp = await fetch("/runs");
  const data = await resp.json();
  if (!resp.ok) {
    return;
  }
  renderResumeRuns(data?.runs || []);
}

function renderForensicSummary(result) {
  if (!forensicSummary) {
    return;
  }
  const report = result?.report || {};
  const reasons = Array.isArray(report.precheck_reasons) ? report.precheck_reasons.slice(0, 3) : [];
  latestForensicDbPath = String(result?.db_path || "");
  forensicSummary.dataset.hasReport = result ? "true" : "";
  if (!result) {
    forensicSummary.classList.remove("hidden");
    forensicSummary.innerHTML = `
      <div class="preset-hint-title">${getLang() === "en" ? "Forensic Summary" : "体检摘要"}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "No forensic report yet. Select a run and click forensic analysis." : "当前还没有体检结果。先选择一个运行，再点击“运行体检”。"}</div>
    `;
    return;
  }
  forensicSummary.classList.remove("hidden");
  forensicSummary.innerHTML = `
    <div class="preset-hint-title">${getLang() === "en" ? "Forensic Summary" : "体检摘要"}</div>
    <div class="preset-hint-copy">${getLang() === "en" ? "Run" : "运行"} ${result.run_id || "-"} · ${getLang() === "en" ? "Tx" : "成交"} ${report.transactions_total ?? 0} · ${getLang() === "en" ? "Buyers" : "活跃买家"} ${report.active_buyers ?? 0}</div>
    <div class="preset-hint-grid">
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Listings" : "挂牌"}</span><strong>${report.for_sale_listings ?? 0}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Pending" : "待处理订单"}</span><strong>${report.pending_orders ?? 0}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Rejects" : "预检拒绝"}</span><strong>${report.precheck_reject_total ?? 0}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Invalid Bids" : "无效出价"}</span><strong>${report.invalid_bid_total ?? 0}</strong></div>
      <div class="preset-hint-item preset-hint-item-wide"><span class="preset-hint-label">${getLang() === "en" ? "Top Reasons" : "主要阻断原因"}</span><span>${reasons.length > 0 ? reasons.map((item) => `${item.reason || "-"}(${item.count})`).join(" · ") : (getLang() === "en" ? "No dominant reject reason." : "暂无明显拒绝原因。")}</span></div>
      <div class="preset-hint-item preset-hint-item-wide">
        <span class="preset-hint-label">${getLang() === "en" ? "Artifacts" : "报告产物"}</span>
        <span class="inline-actions">
          <button type="button" class="ghost compact-btn" data-forensic-action="view">${t("buttons.open_report")}</button>
          <button type="button" class="ghost compact-btn" data-forensic-action="json">${t("buttons.download_json")}</button>
        </span>
      </div>
    </div>
  `;
  forensicSummary.querySelector('[data-forensic-action="view"]')?.addEventListener("click", openForensicReportView);
  forensicSummary.querySelector('[data-forensic-action="json"]')?.addEventListener("click", downloadForensicJson);
}

async function runForensicAnalysis() {
  const dbPath = String(resumeRunSelect?.value || "").trim();
  if (!dbPath) {
    renderInlineError(
      forensicSummary,
      getLang() === "en" ? "Forensic Summary" : "体检摘要",
      getLang() === "en" ? "Select a run first, then start forensic analysis." : "请先选择一个运行，再执行体检。"
    );
    return;
  }
  setButtonLoading(startSubmitBtn, getLang() === "en" ? "Running Analysis..." : "正在运行体检...", true);
  if (forensicSummary) {
    forensicSummary.classList.remove("hidden");
    forensicSummary.innerHTML = `
      <div class="preset-hint-title">${getLang() === "en" ? "Forensic Summary" : "体检摘要"}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Inspecting the selected run and collecting diagnostic evidence." : "正在检查所选运行，并收集诊断证据，请稍候。"}</div>
    `;
  }
  try {
    const resp = await fetch("/forensics/zero-tx", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ db_path: dbPath }),
    });
    const data = await resp.json();
    if (!resp.ok) {
      renderInlineError(
        forensicSummary,
        getLang() === "en" ? "Forensic Analysis Failed" : "体检失败",
        data.detail || (getLang() === "en" ? "The selected run could not be analyzed." : "当前无法完成该运行的体检，请检查运行目录和数据库状态。")
      );
      return;
    }
    renderForensicSummary(data);
  } finally {
    setButtonLoading(startSubmitBtn, "", false);
  }
}

function openForensicReportView() {
  if (!latestForensicDbPath) {
    return;
  }
  const target = `/forensics/zero-tx/view?db_path=${encodeURIComponent(latestForensicDbPath)}`;
  window.open(target, "_blank", "noopener,noreferrer");
}

async function downloadForensicJson() {
  if (!latestForensicDbPath) {
    return;
  }
  const target = `/forensics/zero-tx/download?format=json&db_path=${encodeURIComponent(latestForensicDbPath)}`;
  const resp = await fetch(target);
  if (!resp.ok) {
    alert(getLang() === "en" ? "Download forensic JSON failed" : "下载体检 JSON 失败");
    return;
  }
  const blob = await resp.blob();
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "zero_tx_diagnostics.json";
  link.click();
  window.URL.revokeObjectURL(url);
}

export async function fetchControls() {
  const resp = await fetch("/controls");
  const data = await resp.json();
  if (!resp.ok) {
    return;
  }
  currentRuntimeControls = {
    down_payment_ratio: Number(data.down_payment_ratio ?? 0.3),
    annual_interest_rate: Number(data.annual_interest_rate ?? 0.035),
    max_dti_ratio: Number(data.max_dti_ratio ?? 0.5),
    market_pulse_enabled: Boolean(data.market_pulse_enabled),
    macro_override_mode: data.macro_override_mode || "",
    negotiation_quote_stream_enabled: Boolean(data.negotiation_quote_stream_enabled),
    negotiation_quote_filter_mode: data.negotiation_quote_filter_mode || "all",
    negotiation_quote_mode: data.negotiation_quote_mode || "limited_quotes",
    negotiation_quote_turn_limit: Number(data.negotiation_quote_turn_limit ?? 4),
    negotiation_quote_char_limit: Number(data.negotiation_quote_char_limit ?? 84),
  };
  renderControlsSummary();
  renderScenarioPresetHint();
}

export async function fetchPresets() {
  const resp = await fetch("/presets");
  const data = await resp.json();
  if (!resp.ok) {
    return;
  }
  scenarioPresetInput.innerHTML = "";
  const presets = Array.isArray(data.presets) ? data.presets : [];
  for (const preset of presets) {
    const option = document.createElement("option");
    option.value = preset.id;
    option.textContent = localizePresetLabel(preset.id, preset.label || preset.id);
    option.dataset.description = preset.description || "";
    option.dataset.populationTemplate = preset.population_template || "";
    option.dataset.populationCount = String(preset.population_count ?? "");
    option.dataset.developerTemplate = preset.developer_template || "";
    option.dataset.developerCount = String(preset.developer_count ?? "");
    option.dataset.incomeStrategy = preset.income_strategy || "";
    option.dataset.downPaymentRatio = String(preset.controls_preview?.down_payment_ratio ?? "");
    option.dataset.annualInterestRate = String(preset.controls_preview?.annual_interest_rate ?? "");
    option.dataset.marketPulseEnabled = String(Boolean(preset.controls_preview?.market_pulse_enabled));
    option.dataset.macroOverrideMode = preset.controls_preview?.macro_override_mode || "";
    option.dataset.quoteStreamEnabled = String(Boolean(preset.negotiation_quote_stream_enabled));
    option.dataset.quoteFilterMode = preset.negotiation_quote_filter_mode || "all";
    if (preset.description) {
      option.title = preset.description;
    }
    scenarioPresetInput.appendChild(option);
  }
  renderScenarioPresetHint();
}

export async function fetchConfigSchema() {
  const resp = await fetch("/config/schema");
  const data = await resp.json();
  if (!resp.ok) {
    return;
  }
  if (!startupDefaultsApplied && currentRuntimeStatus === "idle") {
    applyStartupDefaults(data?.startup_defaults || null);
    startupDefaultsApplied = true;
  }
  renderConfigSchema(data);
  renderStartupTierSummary();
}

function updateStartModeUi() {
  const mode = startModeInput?.value || "new";
  if (startForm) {
    startForm.dataset.startMode = mode;
  }
  if (resumeRunWrap) {
    resumeRunWrap.classList.toggle("hidden", mode !== "resume" && mode !== "forensic");
  }
  if (resumeRunSummary) {
    resumeRunSummary.classList.toggle("hidden", mode !== "resume" && mode !== "forensic");
  }
  if (nightPlanWrap) {
    nightPlanWrap.classList.toggle("hidden", mode !== "night_run");
  }
  if (nightPlanEditor) {
    nightPlanEditor.classList.toggle("hidden", mode !== "night_run");
  }
  if (mode !== "night_run") {
    setNightPlanEditorExpanded(false);
  }
  if (startSubmitBtn) {
    startSubmitBtn.textContent =
      mode === "forensic"
        ? t("buttons.run_forensic")
        : mode === "night_run"
          ? t("buttons.start_night_run")
          : t("buttons.start");
  }
  if (startModePanelTitle && startModePanelCopy) {
    const panelMap = {
      new: {
        title: t("startup.mode_panel_new_title"),
        copy: t("startup.mode_panel_new_copy"),
      },
      resume: {
        title: t("startup.mode_panel_resume_title"),
        copy: t("startup.mode_panel_resume_copy"),
      },
      night_run: {
        title: t("startup.mode_panel_night_title"),
        copy: t("startup.mode_panel_night_copy"),
      },
      forensic: {
        title: t("startup.mode_panel_forensic_title"),
        copy: t("startup.mode_panel_forensic_copy"),
      },
    };
    const panel = panelMap[mode] || panelMap.new;
    startModePanelTitle.textContent = panel.title;
    startModePanelCopy.textContent = panel.copy;
  }
  if (startModePanel) {
    startModePanel.classList.toggle("hidden", mode === "new");
    startModePanel.classList.toggle("mode-panel-accent", mode === "night_run" || mode === "forensic");
  }
  document.querySelectorAll("#start-form .startup-step, #startup-overview").forEach((node) => {
    node.classList.toggle("hidden", mode !== "new" && mode !== "night_run");
  });
  if (forensicSummary && mode !== "forensic" && !forensicSummary.dataset.hasReport) {
    forensicSummary.classList.add("hidden");
  }
  if (forensicSummary && mode === "forensic" && !forensicSummary.dataset.hasReport) {
    renderForensicSummary(null);
  }
}

function getSelectedRun() {
  const selectedDbPath = String(resumeRunSelect?.value || "").trim();
  return availableRuns.find((run) => String(run.db_path || "") === selectedDbPath) || null;
}

function renderSelectedRunSummary() {
  if (!resumeRunSummary) {
    return;
  }
  const run = getSelectedRun();
  if (!run) {
    resumeRunSummary.innerHTML = `
      <div class="preset-hint-title">${getLang() === "en" ? "Selected Run" : "已选运行"}</div>
      <div class="preset-hint-copy">${getLang() === "en" ? "Choose a run to resume or inspect." : "请选择一个运行，用于恢复模拟或执行体检。"}</div>
    `;
    return;
  }
  const created = run.created_at ? String(run.created_at).slice(0, 16).replace("T", " ") : (getLang() === "en" ? "time n/a" : "时间未知");
  const statusMap = getLang() === "en"
    ? {
        initialized: "Initialized",
        paused: "Resumable",
        completed: "Completed",
        unknown: "Unknown",
      }
    : {
        initialized: "仅初始化",
        paused: "可续跑",
        completed: "已完成",
        unknown: "未知",
      };
  const statusLabel = statusMap[String(run.status || "unknown")] || String(run.status || "-");
  const progressLabel = `${run.completed_months ?? run.current_month ?? 0} / ${run.months ?? "-"}`;
  resumeRunSummary.innerHTML = `
    <div class="preset-hint-title">${getLang() === "en" ? "Selected Run" : "已选运行"}</div>
    <div class="preset-hint-copy">${run.run_id || "-"} · ${created}</div>
    <div class="preset-hint-grid">
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Agents" : "Agent 数"}</span><strong>${run.agent_count ?? "-"}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Rounds" : "回合"}</span><strong>${run.months ?? "-"}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Seed" : "种子"}</span><strong>${run.seed ?? "-"}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Status" : "当前状态"}</span><strong>${statusLabel}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Progress" : "运行进度"}</span><strong>${progressLabel}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Transactions" : "累计成交"}</span><strong>${run.transactions_total ?? 0}</strong></div>
      <div class="preset-hint-item"><span class="preset-hint-label">${getLang() === "en" ? "Resume" : "可继续"}</span><strong>${run.can_resume ? (getLang() === "en" ? "Yes" : "是") : (getLang() === "en" ? "No" : "否")}</strong></div>
      <div class="preset-hint-item preset-hint-item-wide"><span class="preset-hint-label">${getLang() === "en" ? "Run Directory" : "运行目录"}</span><span>${run.run_dir || "-"}</span></div>
    </div>
  `;
}

function renderResumeRuns(runs) {
  availableRuns = Array.isArray(runs) ? runs : [];
  if (!resumeRunSelect) {
    return;
  }
  if (availableRuns.length === 0) {
    resumeRunSelect.innerHTML = `<option value="">${getLang() === "en" ? "No resumable run found" : "未找到可恢复运行"}</option>`;
    resumeRunSelect.disabled = true;
    renderSelectedRunSummary();
    updateStartModeUi();
    return;
  }
  resumeRunSelect.innerHTML = availableRuns.map((run) => {
    const created = run.created_at ? String(run.created_at).slice(0, 16).replace("T", " ") : (getLang() === "en" ? "time n/a" : "时间未知");
    const statusText = getLang() === "en"
      ? ({ initialized: "initialized", paused: "resumable", completed: "completed" }[String(run.status || "")] || "unknown")
      : ({ initialized: "仅初始化", paused: "可续跑", completed: "已完成" }[String(run.status || "")] || "未知");
    const statusHint = `${statusText} · ${getLang() === "en" ? "round" : "回合"} ${run.completed_months ?? run.current_month ?? 0}/${run.months ?? "-"} · ${getLang() === "en" ? "tx" : "成交"} ${run.transactions_total ?? 0}`;
    return `<option value="${run.db_path}">${run.run_id} · ${created} · ${statusHint}</option>`;
  }).join("");
  resumeRunSelect.disabled = false;
  renderSelectedRunSummary();
  updateStartModeUi();
}

export function bindConfigSchemaFilters() {
  const rerender = () => {
    if (latestConfigSchemaData) {
      renderConfigSchema(latestConfigSchemaData);
    }
  };
  const resetSchemaFilters = () => {
    currentSchemaFilters = {
      search: "",
      phase: "all",
      group: "all",
      onlyEditable: true,
    };
    if (configSchemaSearchInput) {
      configSchemaSearchInput.value = "";
    }
    if (configSchemaPhaseFilterInput) {
      configSchemaPhaseFilterInput.value = "all";
    }
    if (configSchemaGroupFilterInput) {
      configSchemaGroupFilterInput.value = "all";
    }
    if (configSchemaOnlyEditableInput) {
      configSchemaOnlyEditableInput.checked = true;
    }
    rerender();
  };
  if (configSchemaSearchInput) {
    configSchemaSearchInput.addEventListener("input", () => {
      currentSchemaFilters.search = configSchemaSearchInput.value || "";
      rerender();
    });
  }
  if (configSchemaPhaseFilterInput) {
    configSchemaPhaseFilterInput.addEventListener("change", () => {
      currentSchemaFilters.phase = configSchemaPhaseFilterInput.value || "all";
      rerender();
    });
  }
  if (configSchemaGroupFilterInput) {
    configSchemaGroupFilterInput.addEventListener("change", () => {
      currentSchemaFilters.group = configSchemaGroupFilterInput.value || "all";
      rerender();
    });
  }
  if (configSchemaOnlyEditableInput) {
    configSchemaOnlyEditableInput.addEventListener("change", () => {
      currentSchemaFilters.onlyEditable = Boolean(configSchemaOnlyEditableInput.checked);
      rerender();
    });
  }
  if (configSchemaResetFiltersBtn) {
    configSchemaResetFiltersBtn.addEventListener("click", resetSchemaFilters);
  }
}

export function bindStartModeSwitch() {
  startModeInput?.addEventListener("change", updateStartModeUi);
  resumeRunSelect?.addEventListener("change", renderSelectedRunSummary);
  updateStartModeUi();
}

function readSchemaControlValue(input) {
  const fieldType = input.dataset.schemaType;
  if (fieldType === "boolean") {
    return input.checked;
  }
  if (fieldType === "integer") {
    return input.value === "" ? null : Number.parseInt(input.value, 10);
  }
  if (fieldType === "number") {
    return input.value === "" ? null : Number(input.value);
  }
  return input.value === "" ? null : input.value;
}

export function renderScenarioPresetHint() {
  if (!scenarioPresetHint || !scenarioPresetInput) {
    return;
  }
  const selectedOption = scenarioPresetInput.selectedOptions?.[0];
  if (!selectedOption) {
    scenarioPresetHint.textContent = t("preset.select_hint");
    return;
  }
  const description = selectedOption.dataset.description || selectedOption.textContent || selectedOption.value;
  const populationTemplate = selectedOption.dataset.populationTemplate || "custom";
  const populationCount = selectedOption.dataset.populationCount || "-";
  const developerTemplate = selectedOption.dataset.developerTemplate || "custom";
  const developerCount = selectedOption.dataset.developerCount || "-";
  const incomeStrategy = selectedOption.dataset.incomeStrategy || "tier_adjustments";
  const downPaymentRatio = selectedOption.dataset.downPaymentRatio || "-";
  const annualInterestRate = selectedOption.dataset.annualInterestRate || "-";
  const marketPulseEnabled = selectedOption.dataset.marketPulseEnabled === "true";
  const macroOverrideMode = selectedOption.dataset.macroOverrideMode || "default";
  const quoteStreamEnabled = selectedOption.dataset.quoteStreamEnabled === "true";
  const quoteFilterMode = selectedOption.dataset.quoteFilterMode || "all";
  const currentDownPaymentRatio = Number(currentRuntimeControls.down_payment_ratio || 0);
  const currentAnnualInterestRate = Number(currentRuntimeControls.annual_interest_rate || 0);
  const currentMarketPulseEnabled = Boolean(currentRuntimeControls.market_pulse_enabled);
  const currentMacroOverrideMode = currentRuntimeControls.macro_override_mode || "default";
  const currentQuoteStreamEnabled = Boolean(currentRuntimeControls.negotiation_quote_stream_enabled);
  const currentQuoteFilterMode = currentRuntimeControls.negotiation_quote_filter_mode || "all";
  const controlDiffs = [];

  if (Number.isFinite(Number(downPaymentRatio)) && Number(downPaymentRatio) !== currentDownPaymentRatio) {
    controlDiffs.push(`DP ${currentDownPaymentRatio.toFixed(2)} -> ${Number(downPaymentRatio).toFixed(2)}`);
  }
  if (Number.isFinite(Number(annualInterestRate)) && Number(annualInterestRate) !== currentAnnualInterestRate) {
    controlDiffs.push(`Rate ${currentAnnualInterestRate.toFixed(3)} -> ${Number(annualInterestRate).toFixed(3)}`);
  }
  if (marketPulseEnabled !== currentMarketPulseEnabled) {
    controlDiffs.push(`Pulse ${currentMarketPulseEnabled ? "on" : "off"} -> ${marketPulseEnabled ? "on" : "off"}`);
  }
  if (macroOverrideMode !== currentMacroOverrideMode) {
    controlDiffs.push(`Macro ${currentMacroOverrideMode} -> ${macroOverrideMode}`);
  }
  if (quoteStreamEnabled !== currentQuoteStreamEnabled) {
    controlDiffs.push(`Quote ${currentQuoteStreamEnabled ? "on" : "off"} -> ${quoteStreamEnabled ? "on" : "off"}`);
  }
  if (quoteFilterMode !== currentQuoteFilterMode) {
    controlDiffs.push(`Filter ${currentQuoteFilterMode} -> ${quoteFilterMode}`);
  }
  const diffSummary = controlDiffs.length > 0 ? controlDiffs.join(" · ") : (getLang() === "en" ? "No control changes relative to current form." : "相对当前表单没有参数变化。");
  scenarioPresetHint.innerHTML = `
    <div class="preset-hint-title">${selectedOption.textContent || selectedOption.value}</div>
    <div class="preset-hint-copy">${description}</div>
    <div class="preset-hint-grid">
      <div class="preset-hint-item">
        <span class="preset-hint-label">${getLang() === "en" ? "Population" : "人口"}</span>
        <strong>${populationTemplate}</strong>
        <span>${populationCount} ${getLang() === "en" ? "agents" : "人"}</span>
      </div>
      <div class="preset-hint-item">
        <span class="preset-hint-label">${getLang() === "en" ? "Supply" : "供给"}</span>
        <strong>${developerTemplate}</strong>
        <span>${developerCount} ${getLang() === "en" ? "units" : "套"}</span>
      </div>
      <div class="preset-hint-item">
        <span class="preset-hint-label">${getLang() === "en" ? "Income" : "收入"}</span>
        <strong>${incomeStrategy}</strong>
        <span>${getLang() === "en" ? "bundled shock" : "捆绑冲击"}</span>
      </div>
      <div class="preset-hint-item">
        <span class="preset-hint-label">${getLang() === "en" ? "Quotes" : "谈判流"}</span>
        <strong>${quoteFilterMode}</strong>
        <span>${quoteStreamEnabled ? (getLang() === "en" ? "stream on" : "原话流开启") : (getLang() === "en" ? "stream off" : "原话流关闭")}</span>
      </div>
      <div class="preset-hint-item preset-hint-item-wide">
        <span class="preset-hint-label">${getLang() === "en" ? "Controls" : "参数"}</span>
        <strong>DP ${downPaymentRatio} · Rate ${annualInterestRate}</strong>
        <span>Macro ${macroOverrideMode} · Pulse ${translateBool(marketPulseEnabled)}</span>
      </div>
      <div class="preset-hint-item preset-hint-item-wide preset-diff-item">
        <span class="preset-hint-label">${getLang() === "en" ? "Delta vs Current" : "相对当前变化"}</span>
        <span>${diffSummary}</span>
      </div>
    </div>
  `;
}

function buildScenarioPresetConfirmMessage() {
  const selectedOption = scenarioPresetInput?.selectedOptions?.[0];
  if (!selectedOption) {
    return getLang() === "en" ? "Apply selected preset?" : "确认应用当前场景？";
  }

  const label = selectedOption.textContent || selectedOption.value;
  const populationTemplate = selectedOption.dataset.populationTemplate || "custom";
  const populationCount = selectedOption.dataset.populationCount || "-";
  const developerTemplate = selectedOption.dataset.developerTemplate || "custom";
  const developerCount = selectedOption.dataset.developerCount || "-";
  const quoteFilterMode = selectedOption.dataset.quoteFilterMode || "all";
  const quoteStreamEnabled = selectedOption.dataset.quoteStreamEnabled === "true";
  const downPaymentRatio = selectedOption.dataset.downPaymentRatio || "-";
  const annualInterestRate = selectedOption.dataset.annualInterestRate || "-";
  const macroOverrideMode = selectedOption.dataset.macroOverrideMode || "default";
  const marketPulseEnabled = selectedOption.dataset.marketPulseEnabled === "true";

  return [
    `${getLang() === "en" ? "Apply preset" : "应用场景"}: ${label}?`,
    `${getLang() === "en" ? "Population" : "人口"}: ${populationTemplate} · ${populationCount} ${getLang() === "en" ? "agents" : "人"}`,
    `${getLang() === "en" ? "Supply" : "供给"}: ${developerTemplate} · ${developerCount} ${getLang() === "en" ? "units" : "套"}`,
    `Controls: DP ${downPaymentRatio} · Rate ${annualInterestRate} · Macro ${macroOverrideMode} · Pulse ${marketPulseEnabled ? "on" : "off"}`,
    `Quotes: ${quoteStreamEnabled ? "stream on" : "stream off"} · ${quoteFilterMode}`,
  ].join("\n");
}

function closeScenarioPresetConfirm() {
  if (!scenarioPresetConfirm) {
    return;
  }
  scenarioPresetConfirm.classList.remove("success");
  scenarioPresetConfirm.classList.add("hidden");
}

function openScenarioPresetConfirm(message) {
  if (!scenarioPresetConfirm || !scenarioPresetConfirmCopy || !scenarioPresetConfirmApplyBtn || !scenarioPresetConfirmCancelBtn) {
    return Promise.resolve(true);
  }
  closeScenarioPresetConfirm();
  scenarioPresetConfirmCopy.innerHTML = String(message)
    .split("\n")
    .map((line) => `<div>${line}</div>`)
    .join("");
  scenarioPresetConfirm.classList.remove("hidden");

  return new Promise((resolve) => {
    pendingPresetConfirmResolver = resolve;
  });
}

function closeStartupConfirm() {
  if (!startupConfirm || !startupConfirmCopy) {
    return;
  }
  startupConfirm.classList.add("hidden");
  startupConfirm.classList.remove("success");
  startupConfirmCopy.innerHTML = "";
}

function openStartupConfirm(message) {
  if (!startupConfirm || !startupConfirmCopy || !startupConfirmApplyBtn || !startupConfirmCancelBtn) {
    return Promise.resolve(true);
  }
  closeStartupConfirm();
  startupConfirmCopy.innerHTML = String(message)
    .split("\n")
    .map((line) => `<div>${line}</div>`)
    .join("");
  startupConfirm.classList.remove("hidden");
  return new Promise((resolve) => {
    pendingStartupConfirmResolver = resolve;
  });
}

export function resolveScenarioPresetConfirm(confirmed) {
  if (typeof pendingPresetConfirmResolver === "function") {
    pendingPresetConfirmResolver(Boolean(confirmed));
    pendingPresetConfirmResolver = null;
  }
  closeScenarioPresetConfirm();
}

function resolveStartupConfirm(confirmed) {
  if (typeof pendingStartupConfirmResolver === "function") {
    pendingStartupConfirmResolver(Boolean(confirmed));
    pendingStartupConfirmResolver = null;
  }
  closeStartupConfirm();
}

function showScenarioPresetAppliedFeedback(payload) {
  if (!scenarioPresetConfirm || !scenarioPresetConfirmCopy) {
    return;
  }
  if (presetConfirmFeedbackTimer) {
    window.clearTimeout(presetConfirmFeedbackTimer);
    presetConfirmFeedbackTimer = null;
  }
  const preset = payload?.preset || scenarioPresetInput?.value || "preset";
  const controls = payload?.controls || {};
  scenarioPresetConfirmCopy.innerHTML = [
    `<div><strong>${getLang() === "en" ? "Preset applied" : "场景已应用"}:</strong> ${preset}</div>`,
    `<div>DP ${controls.down_payment_ratio ?? "-"} · Rate ${controls.annual_interest_rate ?? "-"} · Macro ${controls.macro_override_mode || "default"}</div>`,
    `<div>Quotes ${controls.negotiation_quote_stream_enabled ? "on" : "off"} · ${controls.negotiation_quote_filter_mode || "all"}</div>`,
  ].join("");
  scenarioPresetConfirm.classList.remove("hidden");
  scenarioPresetConfirm.classList.add("success");
  presetConfirmFeedbackTimer = window.setTimeout(() => {
    closeScenarioPresetConfirm();
    presetConfirmFeedbackTimer = null;
  }, 2200);
}

export function bindScenarioPresetConfirmActions() {
  if (scenarioPresetConfirmApplyBtn) {
    scenarioPresetConfirmApplyBtn.addEventListener("click", () => resolveScenarioPresetConfirm(true));
  }
  if (scenarioPresetConfirmCancelBtn) {
    scenarioPresetConfirmCancelBtn.addEventListener("click", () => resolveScenarioPresetConfirm(false));
  }
  if (startupConfirmApplyBtn) {
    startupConfirmApplyBtn.addEventListener("click", () => resolveStartupConfirm(true));
  }
  if (startupConfirmCancelBtn) {
    startupConfirmCancelBtn.addEventListener("click", () => resolveStartupConfirm(false));
  }
}

export async function startSimulation(event) {
  event.preventDefault();
  const mode = startModeInput?.value || "new";
  if (mode === "forensic") {
    await runForensicAnalysis();
    return;
  }
  let payload;
  try {
    payload = getStartupPayloadFromSchema();
  }
  catch (error) {
    renderInlineError(
      mode === "resume" ? resumeRunSummary : startupOverview,
      getLang() === "en" ? "Unable to Start" : "无法启动",
      error?.message || (getLang() === "en" ? "Startup parameters are invalid." : "启动参数不合法，请先修正后再启动。")
    );
    return;
  }
  const confirmed = await openStartupConfirm(buildStartupConfirmMessage(payload));
  if (!confirmed) {
    return;
  }

  const endpoint = mode === "night_run" ? "/night-run/start" : "/start";
  setButtonLoading(
    startSubmitBtn,
    mode === "night_run"
      ? (getLang() === "en" ? "Starting Night Run..." : "正在启动夜跑...")
      : mode === "resume"
        ? (getLang() === "en" ? "Resuming Simulation..." : "正在恢复模拟...")
        : (getLang() === "en" ? "Starting Simulation..." : "正在启动模拟..."),
    true
  );
  renderStatus({
    ...(appState.lastStatus || {}),
    status: "running",
    initialized: true,
    run_mode: mode === "night_run" ? "night_run" : "manual",
  });
  try {
    const resp = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (!resp.ok) {
      renderInlineError(
        mode === "resume" ? resumeRunSummary : startupOverview,
        getLang() === "en" ? "Start Failed" : "启动失败",
        data.detail || (getLang() === "en" ? "The simulation could not be started." : "当前无法启动模拟，请检查参数或所选运行。")
      );
      return;
    }
    renderStatus(data);
    syncSchemaEditabilityForStatus(data);
    await fetchControls();
    await fetchConfigSchema();
    await fetchRuns();
  } finally {
    setButtonLoading(startSubmitBtn, "", false);
  }
}

export async function stepSimulation() {
  setButtonLoading(stepBtn, getLang() === "en" ? "Advancing Round..." : "正在推进回合...", true);
  renderStatus({
    ...(appState.lastStatus || {}),
    status: "running",
    initialized: true,
  });
  try {
    const resp = await fetch("/step", { method: "POST" });
    const data = await resp.json();
    if (!resp.ok) {
      renderInlineError(
        monthSummary,
        getLang() === "en" ? "Round Advance Failed" : "推进失败",
        data.detail || (getLang() === "en" ? "The simulation could not advance to the next round." : "当前无法推进到下一个回合，请稍后重试或检查系统流。"),
        "summary-box"
      );
      return;
    }
    renderStatus(data.status);
    syncSchemaEditabilityForStatus(data.status);
    renderSummary(data);
  } finally {
    setButtonLoading(stepBtn, "", false);
  }
}

export async function applySchemaControls(event) {
  event.preventDefault();
  if (!configSchemaForm) {
    return;
  }
  for (const input of configSchemaForm.querySelectorAll("[data-schema-key]")) {
    const startupInput = STARTUP_FORM_KEY_MAP[input.dataset.schemaKey];
    if (!startupInput) {
      continue;
    }
    if (input.type === "checkbox") {
      startupInput.checked = input.checked;
    }
    else {
      startupInput.value = input.value;
    }
  }
  const payload = {};
  for (const input of configSchemaForm.querySelectorAll("[data-schema-key]")) {
    const schemaKey = input.dataset.schemaKey;
    const payloadKey = SCHEMA_CONTROL_KEY_MAP[schemaKey];
    if (!payloadKey) {
      continue;
    }
    payload[payloadKey] = readSchemaControlValue(input);
  }
  const resp = await fetch("/controls", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!resp.ok) {
    alert(data.detail || (getLang() === "en" ? "Apply schema controls failed" : "应用 schema 参数失败"));
    return;
  }
  renderStatus(data.status);
  syncSchemaEditabilityForStatus(data.status);
  await fetchControls();
  await fetchConfigSchema();
  renderScenarioPresetHint();
}

export async function addPopulation(event) {
  event.preventDefault();
  const minValue = populationIncomeMultiplierMinInput.value ? Number(populationIncomeMultiplierMinInput.value) : null;
  const maxValue = populationIncomeMultiplierMaxInput.value ? Number(populationIncomeMultiplierMaxInput.value) : null;
  const payload = {
    count: Number(populationCountInput.value),
    template: populationTemplateInput.value || null,
    tier: populationTierInput.value,
    income_multiplier: minValue == null && maxValue == null ? Number(populationIncomeMultiplierInput.value) : null,
    income_multiplier_min: minValue,
    income_multiplier_max: maxValue,
  };
  const resp = await fetch("/interventions/population/add", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!resp.ok) {
    alert(data.detail || (getLang() === "en" ? "Population intervention failed" : "人口干预失败"));
    return;
  }
  renderStatus(data.status);
  syncSchemaEditabilityForStatus(data.status);
}

export async function applyIncomeShock(event) {
  event.preventDefault();
  const tierAdjustments = [incomeTierChange1Input.value, incomeTierChange2Input.value, incomeTierChange3Input.value]
    .map((value) => value.trim())
    .filter(Boolean)
    .map((value) => {
      const [tier, pctChange] = value.split(":");
      return { tier: tier.trim(), pct_change: Number(pctChange) };
    });
  const payload = {
    pct_change: tierAdjustments.length === 0 ? Number(incomePctChangeInput.value) : null,
    target_tier: incomeTargetTierInput.value,
    tier_adjustments: tierAdjustments.length > 0 ? tierAdjustments : null,
  };
  const resp = await fetch("/interventions/income", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!resp.ok) {
    alert(data.detail || (getLang() === "en" ? "Income shock failed" : "收入冲击失败"));
    return;
  }
  renderStatus(data.status);
  syncSchemaEditabilityForStatus(data.status);
}

export async function injectDeveloperSupply(event) {
  event.preventDefault();
  const payload = {
    template: developerTemplateInput.value || null,
    zone: developerZoneInput.value,
    count: Number(developerCountInput.value),
    price_per_sqm: developerPricePerSqmInput.value ? Number(developerPricePerSqmInput.value) : null,
    size: developerSizeInput.value ? Number(developerSizeInput.value) : null,
    school_units: developerSchoolUnitsInput.value ? Number(developerSchoolUnitsInput.value) : null,
    build_year: developerBuildYearInput.value ? Number(developerBuildYearInput.value) : null,
  };
  const resp = await fetch("/interventions/developer-supply", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!resp.ok) {
    alert(data.detail || (getLang() === "en" ? "Developer supply injection failed" : "开发商供给注入失败"));
    return;
  }
  renderStatus(data.status);
  syncSchemaEditabilityForStatus(data.status);
}

export async function applyScenarioPreset(event) {
  event.preventDefault();
  const confirmed = await openScenarioPresetConfirm(buildScenarioPresetConfirmMessage());
  if (!confirmed) {
    return;
  }
  const payload = {
    preset: scenarioPresetInput.value,
  };
  const resp = await fetch("/presets/apply", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!resp.ok) {
    alert(data.detail || (getLang() === "en" ? "Apply preset failed" : "应用场景失败"));
    return;
  }
  renderStatus(data.status);
  syncSchemaEditabilityForStatus(data.status);
  await fetchControls();
  await fetchConfigSchema();
  renderScenarioPresetHint();
  showScenarioPresetAppliedFeedback(data);
}

export function openFinalReportView() {
  window.open("/report/final/view", "_blank", "noopener,noreferrer");
}

export function openParameterAssumptionReportView() {
  window.open("/report/parameter-assumption/view", "_blank", "noopener,noreferrer");
}

export function openDbObserverView() {
  const selectedDbPath = String(resumeRunSelect?.value || "").trim();
  const target = selectedDbPath
    ? `/db-observer/view?db_path=${encodeURIComponent(selectedDbPath)}`
    : "/db-observer/view";
  window.open(target, "_blank", "noopener,noreferrer");
}

export async function downloadFinalReportJson() {
  const resp = await fetch("/report/final");
  const data = await resp.json();
  if (!resp.ok) {
    alert(data.detail || (getLang() === "en" ? "Export report failed" : "导出报告失败"));
    return;
  }
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `simulation-report-${data.run?.run_id || "run"}.json`;
  link.click();
  window.URL.revokeObjectURL(url);
}

export async function downloadParameterAssumptionReportJson() {
  const resp = await fetch("/report/parameter-assumption");
  const data = await resp.json();
  if (!resp.ok) {
    alert(data.detail || (getLang() === "en" ? "Export parameter report failed" : "导出参数说明表失败"));
    return;
  }
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `parameter-assumption-${data.experiment_info?.run_id || "run"}.json`;
  link.click();
  window.URL.revokeObjectURL(url);
}

export function refreshLocalizedApiUi() {
  renderControlsSummary();
  if (latestStartupDefaults && currentRuntimeStatus === "idle") {
    applyStartupDefaults(latestStartupDefaults);
  }
  if (latestConfigSchemaData) {
    renderConfigSchema(latestConfigSchemaData);
  }
  if (nightPlanList?.children.length) {
    renderNightRunPlans(collectNightRunPlans());
  }
  renderStartupTierSummary();
  syncSchemaEditabilityForStatus({ status: currentRuntimeStatus });
  renderScenarioPresetHint();
  updateStartModeUi();
}

export function bindNightRunPlanEditor() {
  if (nightPlanList && !nightPlanList.children.length) {
    resetNightRunPlanEditor();
  }
  setNightPlanEditorExpanded(false);
  nightPlanToggleBtn?.addEventListener("click", () => {
    setNightPlanEditorExpanded(!nightPlanEditorExpanded);
  });
  nightPlanAddAtMonthBtn?.addEventListener("click", () => {
    const month = Math.max(1, Number.parseInt(nightPlanNewMonthInput?.value || "1", 10));
    const actionType = String(nightPlanNewActionInput?.value || "population_add").trim().toLowerCase();
    renderNightRunPlans([...collectNightRunPlans(), getDefaultNightRunPlan(actionType, month)]);
    setNightPlanEditorExpanded(true);
  });
  nightPlanAddPopulationBtn?.addEventListener("click", () => {
    renderNightRunPlans([...collectNightRunPlans(), getDefaultNightRunPlan("population_add", 1)]);
    setNightPlanEditorExpanded(true);
  });
  nightPlanAddDeveloperBtn?.addEventListener("click", () => {
    renderNightRunPlans([...collectNightRunPlans(), getDefaultNightRunPlan("developer_supply", 1)]);
    setNightPlanEditorExpanded(true);
  });
  nightPlanAddSupplyCutBtn?.addEventListener("click", () => {
    renderNightRunPlans([...collectNightRunPlans(), getDefaultNightRunPlan("supply_cut", 1)]);
    setNightPlanEditorExpanded(true);
  });
  nightPlanAddIncomeBtn?.addEventListener("click", () => {
    renderNightRunPlans([...collectNightRunPlans(), getDefaultNightRunPlan("income_shock", 1)]);
    setNightPlanEditorExpanded(true);
  });
  nightPlanResetBtn?.addEventListener("click", () => {
    loadNightRunExamplePlans();
    setNightPlanEditorExpanded(true);
  });
  nightPlanExportBtn?.addEventListener("click", () => exportNightRunPlans());
  nightPlanImportBtn?.addEventListener("click", () => nightPlanImportFileInput?.click());
  nightPlanImportFileInput?.addEventListener("change", async () => {
    const file = nightPlanImportFileInput.files?.[0];
    if (!file) {
      return;
    }
    try {
      await importNightRunPlansFromFile(file);
      setNightPlanEditorExpanded(true);
    } catch (error) {
      alert(error?.message || (getLang() === "en" ? "Import failed" : "导入失败"));
    } finally {
      nightPlanImportFileInput.value = "";
    }
  });
}

export function bindStartupWizard() {
  const watchedInputs = [
    startupSupplySnapshotInput,
    startupMarketGoalInput,
    startupDemandMultiplierInput,
    agentCountInput,
    propertyTotalCountInput,
    monthsInput,
    seedInput,
    minCashThresholdInput,
    startupDownPaymentRatioInput,
    startupMaxDtiRatioInput,
    startupAnnualInterestRateInput,
    startupBidFloorRatioInput,
    startupPrecheckBufferInput,
    startupPrecheckTaxFeeInput,
    ...STARTUP_TIER_FIELDS.flatMap((item) => [
      item.countInput,
      item.incomeMinInput,
      item.incomeMaxInput,
      item.propertyMinInput,
      item.propertyMaxInput,
    ]),
  ].filter(Boolean);
  const rerender = () => renderStartupTierSummary();
  startupMarketGoalInput?.addEventListener("change", () => {
    if (startupDemandMultiplierInput) {
      startupDemandMultiplierInput.value = String(getDefaultDemandMultiplierForGoal(startupMarketGoalInput.value));
    }
    rerender();
  });
  for (const input of watchedInputs) {
    input.addEventListener("input", rerender);
    input.addEventListener("change", rerender);
  }
  renderStartupTierSummary();
}
