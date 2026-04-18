const path = require("path");
const PptxGenJS = require("pptxgenjs");
const {
  warnIfSlideHasOverlaps,
  warnIfSlideElementsOutOfBounds,
} = require("./pptxgenjs_helpers/layout");

const pptx = new PptxGenJS();
pptx.layout = "LAYOUT_WIDE";
pptx.author = "OpenAI Codex";
pptx.company = "visual_real_estate";
pptx.subject = "中国住房市场推演发布说明";
pptx.title = "中国住房市场推演发布讲解";
pptx.lang = "zh-CN";
pptx.theme = {
  headFontFace: "Microsoft YaHei",
  bodyFontFace: "Microsoft YaHei",
  lang: "zh-CN",
};

const OUT = path.join(__dirname, "中国住房市场推演发布讲解_20260418.pptx");

const COLORS = {
  navy: "15304C",
  blue: "2F6BFF",
  teal: "1C8C84",
  red: "C45A3C",
  gold: "C08A2E",
  green: "2E7D4F",
  ink: "1E293B",
  muted: "5B6575",
  line: "D7DCE5",
  soft: "F5F7FB",
  white: "FFFFFF",
};

function addChrome(slide, title, subtitle = "") {
  slide.addShape(pptx.ShapeType.rect, {
    x: 0,
    y: 0,
    w: 13.333,
    h: 0.55,
    fill: { color: COLORS.navy },
    line: { color: COLORS.navy },
  });
  slide.addText(title, {
    x: 0.45,
    y: 0.14,
    w: 8.5,
    h: 0.24,
    fontFace: "Microsoft YaHei",
    fontSize: 24,
    bold: true,
    color: COLORS.white,
    margin: 0,
  });
  if (subtitle) {
    slide.addText(subtitle, {
      x: 0.45,
      y: 0.64,
      w: 12.1,
      h: 0.22,
      fontFace: "Microsoft YaHei",
      fontSize: 10,
      color: COLORS.muted,
      margin: 0,
    });
  }
  slide.addText("资料来源：发布说明、总控文档、主结果数据库与 smoke_report。", {
    x: 0.45,
    y: 7.08,
    w: 8.3,
    h: 0.18,
    fontFace: "Microsoft YaHei",
    fontSize: 8.5,
    color: COLORS.muted,
    margin: 0,
  });
}

function addBulletList(slide, items, opts = {}) {
  const x = opts.x ?? 0.7;
  const y = opts.y ?? 1.2;
  const w = opts.w ?? 5.8;
  const h = opts.h ?? 4.8;
  const fontSize = opts.fontSize ?? 18;
  slide.addText(
    items.map((t) => ({ text: t, options: { bullet: { indent: 14 } } })),
    {
      x,
      y,
      w,
      h,
      fontFace: "Microsoft YaHei",
      fontSize,
      color: COLORS.ink,
      breakLine: true,
      paraSpaceAfterPt: 8,
      valign: "top",
      margin: 0.02,
    }
  );
}

function addCallout(slide, text, opts = {}) {
  slide.addShape(pptx.ShapeType.roundRect, {
    x: opts.x,
    y: opts.y,
    w: opts.w,
    h: opts.h,
    rectRadius: 0.08,
    fill: { color: opts.fill || COLORS.soft },
    line: { color: opts.line || COLORS.line, pt: 1 },
  });
  slide.addText(text, {
    x: opts.x + 0.18,
    y: opts.y + 0.14,
    w: opts.w - 0.36,
    h: opts.h - 0.28,
    fontFace: "Microsoft YaHei",
    fontSize: opts.fontSize || 14,
    color: opts.color || COLORS.ink,
    margin: 0,
    valign: "mid",
    bold: opts.bold || false,
  });
}

function finalize(slide) {
  warnIfSlideHasOverlaps(slide, pptx);
  warnIfSlideElementsOutOfBounds(slide, pptx);
}

// Slide 1
{
  const slide = pptx.addSlide();
  addChrome(slide, "中国住房市场推演发布讲解", "版本：2026-04-18 · 对外口径统一使用“回合（虚拟市场周期）”");
  slide.addText("当前可以发布的，不是一套空想框架，而是一套已经跑通的可复现市场推演系统。", {
    x: 0.7,
    y: 1.05,
    w: 11.9,
    h: 0.55,
    fontFace: "Microsoft YaHei",
    fontSize: 24,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  addBulletList(slide, [
    "固定供给底座下，市场能自然跑起来，不靠人工强推才能成交。",
    "系统已经区分“假热”和“真竞争”，不会再把曝光误当抢房。",
    "用户可以从固定供应盘出发，设置需求倍率和自动冲击，并在回合末决定是否补供。",
    "发布前核心对照测试 G0 / G1 / G2 / G3 / G4 已全部完成。",
  ], { x: 0.85, y: 1.9, w: 6.2, h: 3.6, fontSize: 18 });
  addCallout(slide, "主证据入口\n1. 发布说明\n2. 通俗版收口摘要\n3. 发布证据包索引\n4. 主结果数据库", {
    x: 7.35, y: 1.85, w: 5.1, h: 1.9, fill: "EEF4FF", line: "A9C3FF", fontSize: 18, bold: true
  });
  addCallout(slide, "发布口径\n回合 = 虚拟市场周期\n不直接等于现实自然月", {
    x: 7.35, y: 4.05, w: 5.1, h: 1.35, fill: "F6FBF8", line: "B7D8C4", fontSize: 18
  });
  finalize(slide);
}

// Slide 2
{
  const slide = pptx.addSlide();
  addChrome(slide, "用户怎么用", "当前对外入口已经统一：先固定供给，再设需求倍率，再决定是否加入自动冲击。");
  slide.addText("推荐操作顺序", {
    x: 0.7, y: 1.0, w: 2.8, h: 0.3, fontFace: "Microsoft YaHei", fontSize: 24, bold: true, color: COLORS.ink, margin: 0
  });
  const steps = [
    ["1", "选固定供应盘", "梭子型 / 金字塔型，且都提供最小、中、大样本。"],
    ["2", "设需求倍率", "允许 0.10x - 2.00x，系统自动保证买家画像不消失、供应桶仍有买家覆盖。"],
    ["3", "决定自动冲击", "收入冲击 / 增供 / 减供，按回合预排。"],
    ["4", "推进回合", "看回合摘要、热点、缺口和 checkpoint。"],
    ["5", "回合末干预", "当市场变薄时，玩家可选定向增供、自动补供、全量补充、减供、强制挂牌。"],
  ];
  let y = 1.55;
  steps.forEach(([n, t, d]) => {
    slide.addShape(pptx.ShapeType.ellipse, { x: 0.85, y, w: 0.42, h: 0.42, fill: { color: COLORS.blue }, line: { color: COLORS.blue } });
    slide.addText(n, { x: 0.99, y: y + 0.08, w: 0.12, h: 0.12, fontFace: "Microsoft YaHei", fontSize: 14, bold: true, color: COLORS.white, margin: 0 });
    slide.addText(t, { x: 1.45, y: y - 0.02, w: 2.4, h: 0.2, fontFace: "Microsoft YaHei", fontSize: 18, bold: true, color: COLORS.ink, margin: 0 });
    slide.addText(d, { x: 1.45, y: y + 0.23, w: 5.1, h: 0.38, fontFace: "Microsoft YaHei", fontSize: 12.5, color: COLORS.muted, margin: 0 });
    y += 1.0;
  });
  addCallout(slide, "结果文件怎么找\n- CLI 和日志都会打印当前数据库位置\n- smoke_report.md 先看是否跑完\n- monthly_checkpoints 复查每个回合", {
    x: 7.2, y: 1.6, w: 5.25, h: 2.1, fill: "FFF8ED", line: "E2C27A", fontSize: 17
  });
  addCallout(slide, "两类固定供应盘\n梭子型：更适合看热点集中和中段变薄\n金字塔型：更适合看入门层更厚时的后半段承接", {
    x: 7.2, y: 4.05, w: 5.25, h: 1.8, fill: "F6FBF8", line: "B7D8C4", fontSize: 17
  });
  finalize(slide);
}

// Slide 3
{
  const slide = pptx.addSlide();
  addChrome(slide, "发布前核心对照测试", "技术对照测试已经够用，当前阶段不再继续扩线。");
  slide.addTable([
    [{ text: "组别" }, { text: "问题" }, { text: "关键结果" }],
    [{ text: "G0 基准" }, { text: "无预设干预下能否自然跑完" }, { text: "6 回合完整跑通；自然基准与轻量干预样本都成立。" }],
    [{ text: "G1 样本量桥接" }, { text: "缩到中样本会不会跑坏" }, { text: "spindle_medium 跑通，主机制仍成立。" }],
    [{ text: "G2 供给结构敏感性" }, { text: "换成金字塔型后是否仍成立" }, { text: "pyramid_medium 跑通，后半段比梭子型更稳。" }],
    [{ text: "G3 需求压力方向" }, { text: "偏买方 / 偏卖方是否按预期偏转" }, { text: "buyer_market 更从容；seller_market 更拥挤、更贴近挂牌价。" }],
    [{ text: "G4 轻量韧性" }, { text: "加轻量冲击后是否仍可解释" }, { text: "收入冲击、增供、减供都成功落盘，系统未崩。" }],
  ], {
    x: 0.75, y: 1.2, w: 11.85, h: 4.9,
    border: { type: "solid", pt: 1, color: COLORS.line },
    fill: COLORS.white,
    color: COLORS.ink,
    fontFace: "Microsoft YaHei",
    fontSize: 15,
    rowH: 0.62,
    valign: "mid",
    margin: 0.06,
    autoFit: false,
    colW: [1.8, 3.2, 6.85],
    bold: true,
  });
  addCallout(slide, "发布判断\n现在剩下的重点，不再是继续扩测试，而是把操作手册、PPT、证据包和演示口径收好。", {
    x: 0.95, y: 6.25, w: 11.2, h: 0.68, fill: "EEF4FF", line: "A9C3FF", fontSize: 17
  });
  finalize(slide);
}

// Slide 4
{
  const slide = pptx.addSlide();
  addChrome(slide, "为什么 seller_market 更偏卖方，但平均总成交价没有更高", "这不是机制失效，而是局部竞价抬价与成交结构下沉同时发生。");
  addBulletList(slide, [
    "第一层：多人竞价和拍卖式分流确实更强。Outbid 落败总数 89，自然基准只有 41。",
    "第二层：多人争抢并最终成交的房源，成交价 / 挂牌价均值 1.0209，自然基准只有 0.9957。",
    "第三层：seller_market 多卖掉了更多低总价主流盘，入门盘成交占比从 14.7% 升到 32.3%。",
    "所以卖方强势是真实存在的，但平均总成交价被成交结构明显稀释。"
  ], { x: 0.8, y: 1.35, w: 7.0, h: 3.7, fontSize: 18 });
  slide.addTable([
    [{ text: "指标" }, { text: "seller_market" }, { text: "自然基准" }],
    [{ text: "总成交套数" }, { text: "93" }, { text: "75" }],
    [{ text: "Outbid 落败总数" }, { text: "89" }, { text: "41" }],
    [{ text: "多人争抢成交房源" }, { text: "18" }, { text: "13" }],
    [{ text: "多人争抢房源 成交价/挂牌价" }, { text: "1.0209" }, { text: "0.9957" }],
    [{ text: "整条样本 成交价/挂牌价" }, { text: "0.9960" }, { text: "0.9820" }],
  ], {
    x: 8.1, y: 1.45, w: 4.55, h: 3.2,
    border: { type: "solid", pt: 1, color: COLORS.line },
    fill: COLORS.white,
    color: COLORS.ink,
    fontFace: "Microsoft YaHei",
    fontSize: 13,
    rowH: 0.48,
    margin: 0.05,
    colW: [2.1, 1.15, 1.3],
    bold: true,
  });
  addCallout(slide, "发布解释建议\n卖方市场不等于所有房子一起涨。\n更真实的表现是：热点房更抢、更接近挂牌价，但整体又卖掉了更多低总价主流盘。", {
    x: 7.95, y: 5.0, w: 4.7, h: 1.35, fill: "FFF8ED", line: "E2C27A", fontSize: 15
  });
  finalize(slide);
}

// Slide 5
{
  const slide = pptx.addSlide();
  addChrome(slide, "seller_market 竞价样例房源", "这些房子不是猜出来的，全部来自 seller_market 主数据库。");
  slide.addTable([
    [{ text: "回合" }, { text: "房源ID" }, { text: "区位/类型" }, { text: "参与人数" }, { text: "挂牌价" }, { text: "成交价" }, { text: "成交价/挂牌价" }],
    [{ text: "2" }, { text: "22" }, { text: "A区 学区 改善大户型" }, { text: "6" }, { text: "4,093,096" }, { text: "4,324,265" }, { text: "1.0565" }],
    [{ text: "2" }, { text: "8" }, { text: "A区 非学区 普通住宅" }, { text: "6" }, { text: "4,093,096" }, { text: "4,321,500" }, { text: "1.0558" }],
    [{ text: "2" }, { text: "46" }, { text: "A区 非学区 刚需小户型" }, { text: "5" }, { text: "2,172,593" }, { text: "2,838,493" }, { text: "1.3065" }],
    [{ text: "2" }, { text: "48" }, { text: "A区 非学区 改善大户型" }, { text: "4" }, { text: "5,080,033" }, { text: "5,716,476" }, { text: "1.1253" }],
    [{ text: "3" }, { text: "56" }, { text: "B区 学区 豪宅" }, { text: "4" }, { text: "1,580,306" }, { text: "1,700,000" }, { text: "1.0757" }],
  ], {
    x: 0.55, y: 1.2, w: 12.25, h: 4.0,
    border: { type: "solid", pt: 1, color: COLORS.line },
    fill: COLORS.white,
    color: COLORS.ink,
    fontFace: "Microsoft YaHei",
    fontSize: 12.5,
    rowH: 0.52,
    margin: 0.04,
    colW: [0.7, 0.9, 3.1, 1.0, 1.7, 1.7, 1.35],
    bold: true,
  });
  addCallout(slide, "数据库位置\nresults/release_longtest/release_longtest_20260418_170045_spindle_large_m6_d1p3_s606/runtime_run/simulation.db", {
    x: 0.75, y: 5.55, w: 12.0, h: 0.5, fill: "EEF4FF", line: "A9C3FF", fontSize: 12.5
  });
  addCallout(slide, "主要取证表\nproperty_buyer_matches / transactions / properties_static / profiled_market_property_buckets", {
    x: 0.75, y: 6.18, w: 12.0, h: 0.48, fill: "F6FBF8", line: "B7D8C4", fontSize: 12.5
  });
  finalize(slide);
}

// Slide 6
{
  const slide = pptx.addSlide();
  addChrome(slide, "当前发布交付物", "技术验证已经基本收完，当前进入交付材料阶段。");
  slide.addTable([
    [{ text: "材料" }, { text: "用途" }, { text: "位置" }],
    [{ text: "通俗版收口摘要" }, { text: "给外行先看结论" }, { text: "docs/中国住房市场推演发布收口摘要_20260418_通俗版.md" }],
    [{ text: "发布说明" }, { text: "讲清系统是什么、边界是什么" }, { text: "docs/发布说明_20260418.md" }],
    [{ text: "证据包索引" }, { text: "告诉别人该看哪条证据" }, { text: "docs/发布证据包索引_20260418.md" }],
    [{ text: "操作手册" }, { text: "告诉用户怎么跑、怎么选、怎么看结果" }, { text: "docs/发布操作手册_20260418.md" }],
    [{ text: "卖方市场证据附录" }, { text: "解释 seller_market 均价问题" }, { text: "docs/卖方市场局部竞价证据_20260418.md" }],
  ], {
    x: 0.6, y: 1.25, w: 12.1, h: 3.9,
    border: { type: "solid", pt: 1, color: COLORS.line },
    fill: COLORS.white,
    color: COLORS.ink,
    fontFace: "Microsoft YaHei",
    fontSize: 13,
    rowH: 0.55,
    margin: 0.05,
    colW: [1.8, 3.6, 6.7],
    bold: true,
  });
  addCallout(slide, "下一步建议\n1. 演示前按操作手册走一次\n2. 用 PPT 做统一口径介绍\n3. 遇到 seller_market 质疑时，直接翻附录表", {
    x: 1.0, y: 5.45, w: 11.3, h: 1.0, fill: "FFF8ED", line: "E2C27A", fontSize: 17
  });
  finalize(slide);
}

// Slide 7
{
  const slide = pptx.addSlide();
  addChrome(slide, "一句话收口", "当前版本已经具备发布条件。");
  slide.addText("这套系统现在已经不是“只能做实验的人才会用”的研究原型，而是：", {
    x: 0.85, y: 1.25, w: 11.7, h: 0.45, fontFace: "Microsoft YaHei", fontSize: 24, bold: true, color: COLORS.ink, margin: 0
  });
  addBulletList(slide, [
    "能自然跑起来的固定供给市场推演系统",
    "能区分假热和真竞争的交易机制",
    "能把长期需求和本回合有效需求拆开处理的激活链",
    "能让玩家在回合末看到缺口并决定是否补供的可干预版本",
    "有完整对照测试、操作手册、发布说明和证据包的可发布版本",
  ], { x: 1.0, y: 2.0, w: 10.6, h: 3.8, fontSize: 22 });
  addCallout(slide, "推荐对外顺序\n通俗结论 → 发布说明 → PPT → 证据索引 → 主结果数据库", {
    x: 1.25, y: 6.0, w: 10.4, h: 0.7, fill: "EEF4FF", line: "A9C3FF", fontSize: 18, bold: true
  });
  finalize(slide);
}

async function main() {
  await pptx.writeFile({ fileName: OUT });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
