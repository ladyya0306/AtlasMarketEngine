import {
  activationCount,
  activationList,
  archiveCount,
  archiveList,
  bulletinBody,
  bulletinMonth,
  bulletinPanel,
  currentMonthEl,
  controlsBadge,
  eventTemplate,
  exchangeOverlay,
  failureCount,
  failureList,
  generatedAgentCount,
  generatedAgentList,
  generatedPropertyCount,
  generatedPropertyList,
  listedPropertyCount,
  listedPropertyList,
  matchAttemptCount,
  matchAttemptList,
  negotiationCount,
  negotiationList,
  monthBanner,
  monthSummary,
  remainingMonthsEl,
  reviewOutcomes,
  reviewTopAgents,
  reviewKeyProperties,
  reviewFailureReasons,
  reviewPanel,
  reviewPresetTimeline,
  reviewSnapshot,
  reviewStatusChip,
  runDirEl,
  screenStageProgressList,
  scenarioPresetImpact,
  scenarioPresetHistory,
  scenarioPresetHistoryCount,
  stepBtn,
  statusBadge,
  successCount,
  successList,
  systemCount,
  systemList,
  totalMonthsEl,
} from "./dom.js";
import { getLang, localizeNarrativeText, monthLabel, monthShort, t, translateBool, translatePhase, translateStatus } from "./i18n.js";
import { renderChart } from "./chart.js";
import { appState, resetLaneStats } from "./state.js";

const EVENT_LABELS = {
  AGENT_GENERATED: { zh: "生成 Agent", en: "Agent Generated" },
  PROPERTY_GENERATED: { zh: "生成房产", en: "Property Generated" },
  AGENT_ACTIVATED: { zh: "激活 Agent", en: "Agent Activated" },
  PROPERTY_LISTED: { zh: "房源挂牌", en: "Property Listed" },
  MATCH_ATTEMPT: { zh: "撮合尝试", en: "Match Attempt" },
  NEGOTIATION_STARTED: { zh: "谈判开启", en: "Negotiation Started" },
  NEGOTIATION_PROGRESS: { zh: "谈判推进", en: "Negotiation Progress" },
  NEGOTIATION_QUOTE: { zh: "谈判原话", en: "Negotiation Quote" },
  NEGOTIATION_TURN: { zh: "谈判回合", en: "Negotiation Turn" },
  NEGOTIATION_TURN_BATCH_END: { zh: "回合批次结束", en: "Turn Batch End" },
  NEGOTIATION_CLOSED: { zh: "谈判收束", en: "Negotiation Closed" },
  DEAL_SUCCESS: { zh: "成交成功", en: "Deal Success" },
  SETTLEMENT_PENDING: { zh: "待交割", en: "Settlement Pending" },
  DEAL_FAIL: { zh: "成交失败", en: "Deal Failed" },
  MONTH_END: { zh: "月度结束", en: "Month End" },
  MARKET_BULLETIN_READY: { zh: "市场公报", en: "Market Bulletin" },
  CONTROLS_UPDATED: { zh: "参数更新", en: "Controls Updated" },
  POPULATION_ADDED: { zh: "人口注入", en: "Population Added" },
  INCOME_SHOCK_APPLIED: { zh: "收入冲击", en: "Income Shock Applied" },
  DEVELOPER_SUPPLY_INJECTED: { zh: "供给投放", en: "Developer Supply Injected" },
  SCENARIO_PRESET_APPLIED: { zh: "场景应用", en: "Scenario Preset Applied" },
  RUN_FAILED: { zh: "运行失败", en: "Run Failed" },
  RUN_FINISHED: { zh: "运行完成", en: "Run Finished" },
  RUN_PROGRESS: { zh: "运行进度", en: "Run Progress" },
  RUN_STARTED: { zh: "启动成功", en: "Run Started" },
  STATUS_SNAPSHOT: { zh: "状态快照", en: "Status Snapshot" },
};

function eventTypeLabel(eventType) {
  const label = EVENT_LABELS[eventType];
  if (!label) {
    return eventType;
  }
  return getLang() === "en" ? label.en : label.zh;
}

function renderCollectionEmpty(target, title, copy) {
  if (!target) {
    return;
  }
  target.innerHTML = `
    <div class="collection-empty">
      <strong>${title}</strong>
      <span>${copy}</span>
    </div>
  `;
}

function clearCollectionEmpty(target) {
  if (!target) {
    return;
  }
  const emptyNode = target.querySelector(".collection-empty");
  emptyNode?.remove();
}

function applyLaneEmptyStates() {
  renderCollectionEmpty(generatedAgentList, getLang() === "en" ? "No generated agents yet" : "尚未生成 Agent", getLang() === "en" ? "Generated agents will appear here once initialization completes." : "初始化完成后，生成的 Agent 会出现在这里。");
  renderCollectionEmpty(generatedPropertyList, getLang() === "en" ? "No generated properties yet" : "尚未生成房产", getLang() === "en" ? "Generated properties will appear here once initialization completes." : "初始化完成后，生成的房产会出现在这里。");
  renderCollectionEmpty(activationList, getLang() === "en" ? "No active participants yet" : "尚无活跃参与者", getLang() === "en" ? "Activated agents will gather here as the month progresses." : "随着月份推进，被激活的 Agent 会汇聚到这里。");
  renderCollectionEmpty(listedPropertyList, getLang() === "en" ? "No listings this month" : "本月暂无挂牌", getLang() === "en" ? "New listings will be staged here when sellers enter the market." : "卖家进入市场后，本月挂牌会显示在这里。");
  renderCollectionEmpty(matchAttemptList, getLang() === "en" ? "No match attempts yet" : "暂无撮合尝试", getLang() === "en" ? "Buyer-property matching attempts will appear here." : "买家和房源的撮合尝试会显示在这里。");
  renderCollectionEmpty(negotiationList, getLang() === "en" ? "No negotiations yet" : "暂无谈判流", getLang() === "en" ? "Negotiation summaries and quotes will appear here." : "谈判摘要和原话会显示在这里。");
  renderCollectionEmpty(successList, getLang() === "en" ? "No successful closings yet" : "暂无成交结果", getLang() === "en" ? "Successful deals and settlements will appear here." : "成交成功和结算推进会显示在这里。");
  renderCollectionEmpty(failureList, getLang() === "en" ? "No failed closings yet" : "暂无失败结果", getLang() === "en" ? "Failed deals and broken negotiations will appear here." : "失败成交和破裂谈判会显示在这里。");
  renderCollectionEmpty(systemList, getLang() === "en" ? "Waiting for system events" : "等待系统事件", getLang() === "en" ? "Status snapshots, controls, presets, and monthly results will appear here." : "状态快照、参数更新、场景应用和月度结果会显示在这里。");
}

export function initializeDashboardEmptyStates() {
  if (!appState.lastMonthPayload) {
    monthSummary.className = "summary-empty";
    monthSummary.textContent = t("summary.waiting");
  }
  renderBulletin({ month: "-", bulletin_excerpt: "" });
  applyLaneEmptyStates();
}

export function renderStatus(status) {
  appState.lastStatus = status || null;
  const normalizedStatus = status.status || "idle";
  const runMode = status.run_mode || "manual";
  statusBadge.textContent = translateStatus(status.status || "idle");
  currentMonthEl.textContent = status.current_month ?? 0;
  totalMonthsEl.textContent = status.total_months ?? 0;
  remainingMonthsEl.textContent = status.remaining_months ?? 0;
  runDirEl.textContent = status.run_dir || "-";
  if (controlsBadge) {
    controlsBadge.textContent = translateStatus(normalizedStatus);
  }
  if (stepBtn) {
    const isNightRun = runMode === "night_run";
    const shouldHighlightStep = !isNightRun && (normalizedStatus === "initialized" || normalizedStatus === "paused");
    stepBtn.classList.toggle("next-step-highlight", shouldHighlightStep);
    stepBtn.disabled = isNightRun || normalizedStatus === "running";
    stepBtn.title = isNightRun
      ? (getLang() === "en" ? "Night run is active. Monthly stepping is automatic." : "夜跑进行中，系统会自动逐月推进。")
      : shouldHighlightStep
        ? (getLang() === "en" ? "Initialization is done. Click to run the next month." : "初始化已完成。点击这里推进下一个月。")
        : "";
  }
  if (runMode === "night_run" && !appState.lastMonthPayload && (normalizedStatus === "initialized" || normalizedStatus === "running")) {
    monthSummary.className = "summary-empty";
    monthSummary.textContent = getLang() === "en"
      ? "Night run is active. The system will advance month by month automatically."
      : "夜跑已启动，系统会按预设方案自动逐月推进。";
  }
  else if (normalizedStatus === "initialized" && !appState.lastMonthPayload) {
    monthSummary.className = "summary-empty";
    monthSummary.textContent = getLang() === "en"
      ? "Initialization finished. Click Step One Month to start the first month."
      : "初始化已完成。请点击“推进一个月”开始第一个月。";
  }
  renderPresetHistory(Array.isArray(status.intervention_history) ? status.intervention_history : []);
}

export function renderRunProgress(progressEvent) {
  if (!screenStageProgressList || !progressEvent?.payload) {
    return;
  }
  const payload = progressEvent.payload || {};
  const month = Number(progressEvent.month || payload.status?.current_month || 0);
  const message = String(payload.message || "").trim();
  if (!message) {
    return;
  }
  const detail = payload.detail || {};
  const entry = {
    id: `${progressEvent.event_id || Date.now()}-${message}`,
    month,
    phase: String(detail.phase || payload.stage || "system"),
    message,
    ts: progressEvent.ts || "",
  };
  const previous = appState.screenProgressEntries.filter((item) => item.message !== entry.message || item.month !== entry.month);
  appState.screenProgressEntries = [...previous, entry].slice(-18);
  screenStageProgressList.innerHTML = appState.screenProgressEntries.map((item) => `
    <article class="screen-progress-item">
      <div class="screen-progress-meta">${monthShort(item.month || 0)} · ${translatePhase(item.phase)}</div>
      <div class="screen-progress-copy">${item.message}</div>
    </article>
  `).join("");
}

export function resetRunProgressFeed() {
  appState.screenProgressEntries = [];
  if (!screenStageProgressList) {
    return;
  }
  screenStageProgressList.innerHTML = `<div class="screen-progress-empty">${t("stage.progress_waiting")}</div>`;
}

export function renderSummary(payload) {
  if (!payload || !payload.month_result) {
    monthSummary.className = "summary-empty";
    monthSummary.textContent = t("summary.waiting");
    return;
  }

  appState.lastMonthPayload = payload;
  const summary = payload.month_result;
  const controls = summary.controls_snapshot || {};
  const review = summary.month_review || {};
  const interventions = Array.isArray(review.interventions) ? review.interventions : [];
  const latestIntervention = interventions.length > 0 ? interventions[interventions.length - 1] : null;
  monthSummary.className = "summary-box";
  monthSummary.innerHTML = [
    `<div><strong>${t("runtime.month")}:</strong> ${summary.month}</div>`,
    `<div><strong>${t("chart.transactions")}:</strong> ${summary.transactions}</div>`,
    `<div><strong>${t("chart.avg_price")}:</strong> ${summary.avg_transaction_price ? `¥${Number(summary.avg_transaction_price).toLocaleString()}` : t("common.none")}</div>`,
    `<div><strong>${t("chart.failed_negotiations")}:</strong> ${summary.failed_negotiations}</div>`,
    `<div><strong>${getLang() === "en" ? "Participating Buyers" : "参与买家数"}:</strong> ${summary.buyer_count}</div>`,
    `<div><strong>${getLang() === "en" ? "Active Listings" : "活跃挂牌"}:</strong> ${summary.active_listing_count}</div>`,
    `<div><strong>${getLang() === "en" ? "Events" : "事件数"}:</strong> ${summary.event_count ?? 0}</div>`,
    `<div><strong>${t("review.top_agents")}:</strong> ${(review.top_agents || []).length}</div>`,
    `<div><strong>${t("review.key_properties")}:</strong> ${(review.key_properties || []).length}</div>`,
    `<div><strong>${t("review.failure_reasons")}:</strong> ${(review.failure_reasons || []).length}</div>`,
    `<div><strong>${getLang() === "en" ? "Interventions" : "干预数"}:</strong> ${interventions.length}</div>`,
    `<div><strong>${getLang() === "en" ? "Down Payment" : "首付比例"}:</strong> ${controls.down_payment_ratio ?? t("common.none")}</div>`,
    `<div><strong>${getLang() === "en" ? "Interest Rate" : "利率"}:</strong> ${controls.annual_interest_rate ?? t("common.none")}</div>`,
    `<div><strong>${getLang() === "en" ? "Max DTI" : "最高负债收入比"}:</strong> ${controls.max_dti_ratio ?? t("common.none")}</div>`,
    `<div><strong>${t("pulse.title")}:</strong> ${translateBool(controls.market_pulse_enabled)}</div>`,
    `<div><strong>${getLang() === "en" ? "Macro Override" : "宏观覆盖"}:</strong> ${controls.macro_override_mode || t("common.default")}</div>`,
    `<div><strong>${getLang() === "en" ? "Quote Stream" : "原话流"}:</strong> ${translateBool(controls.negotiation_quote_stream_enabled)}</div>`,
    `<div><strong>${getLang() === "en" ? "Quote Filter" : "原话筛选"}:</strong> ${controls.negotiation_quote_filter_mode || t("common.all")}</div>`,
    `<div><strong>${getLang() === "en" ? "Latest Intervention" : "最近干预"}:</strong> ${latestIntervention?.summary || t("common.none")}</div>`,
    `<div><strong>${t("bulletin.title")}:</strong> ${summary.bulletin_excerpt || t("common.none")}</div>`,
  ].join("");

  renderBulletin({
    month: summary.month,
    bulletin_excerpt: summary.bulletin_excerpt,
  });
}

export function renderBulletin(payload) {
  if (!bulletinPanel || !bulletinMonth || !bulletinBody) {
    return;
  }
  const month = payload?.month ?? "-";
  const bulletinText = String(payload?.bulletin_excerpt || payload?.bulletin || "").trim();
  bulletinMonth.textContent = typeof month === "number" ? monthShort(month) : `M ${month}`;
  bulletinBody.textContent = bulletinText || t("bulletin.waiting");
  bulletinPanel.dataset.bulletinState = bulletinText ? "ready" : "empty";
}

export function showMonthBanner(text) {
  if (!monthBanner) {
    return;
  }
  monthBanner.textContent = text;
  monthBanner.classList.remove("hidden", "visible");
  void monthBanner.offsetWidth;
  monthBanner.classList.add("visible");
  window.setTimeout(() => {
    monthBanner.classList.remove("visible");
    monthBanner.classList.add("hidden");
  }, 1520);
}

export function showRunFinishedBanner(status) {
  const month = status?.current_month ?? "-";
  showMonthBanner(`${t("review.title")} · ${monthLabel(month)}`);
}

export function setNegotiationDensityMode(mode) {
  if (!negotiationList) {
    return;
  }
  const normalized = ["summary", "hybrid", "quotes"].includes(String(mode)) ? String(mode) : "hybrid";
  negotiationList.dataset.densityMode = normalized;
}

export function setNegotiationQuoteFocusLimit(limit) {
  const parsed = Number.parseInt(String(limit), 10);
  appState.negotiationQuoteFocusLimit = Number.isFinite(parsed) && parsed > 0 ? parsed : 2;
  refreshNegotiationQuoteFocus();
}

function pushReviewOutcome(event, summary) {
  if (event.event_type !== "DEAL_SUCCESS" && event.event_type !== "DEAL_FAIL") {
    return;
  }
  const payload = event.payload || {};
  appState.reviewOutcomes = [
    {
      eventType: event.event_type,
      title: summary.title,
      subtitle: summary.subtitle,
      price: Number(payload.agreed_price || 0),
      status: payload.status || payload.deal_stage || "-",
    },
    ...appState.reviewOutcomes,
  ].slice(0, 6);
}

function renderPresetHistory(records) {
  if (!scenarioPresetHistory || !scenarioPresetHistoryCount) {
    return;
  }

  const presetRecords = records
    .filter((item) => item?.event_type === "SCENARIO_PRESET_APPLIED")
    .slice(-5)
    .reverse()
    .map((item) => ({
      month: item.month ?? 0,
      preset: item.payload?.preset || "unknown",
      summary: item.summary || (getLang() === "en" ? "Scenario preset applied" : "场景已应用"),
      ts: item.ts || "",
    }));

  appState.presetHistory = presetRecords;
  scenarioPresetHistoryCount.textContent = String(presetRecords.length);

  if (presetRecords.length === 0) {
    scenarioPresetHistory.innerHTML = `<div class="preset-history-empty">${getLang() === "en" ? "No preset applied yet." : "尚未应用任何场景。"}</div>`;
    return;
  }

  scenarioPresetHistory.innerHTML = presetRecords.map((item) => `
    <article class="preset-history-item" data-preset-month="${item.month || 0}">
      <div class="preset-history-title">${item.preset}</div>
      <div class="preset-history-meta">${monthLabel(item.month || 0)}</div>
      <div class="preset-history-copy">${item.summary}</div>
    </article>
  `).join("");

  scenarioPresetHistory.querySelectorAll(".preset-history-item").forEach((node) => {
    node.addEventListener("click", () => {
      const targetMonth = Number(node.getAttribute("data-preset-month") || 0);
      highlightArchiveMonth(targetMonth);
    });
  });
}

function highlightArchiveMonth(month) {
  const archiveCards = Array.from(archiveList.querySelectorAll(".archive-card"));
  archiveCards.forEach((card) => card.classList.remove("archive-card-highlight"));
  const presetItems = Array.from(scenarioPresetHistory.querySelectorAll(".preset-history-item"));
  presetItems.forEach((item) => item.classList.remove("preset-history-item-highlight"));
  const reviewPresetItems = Array.from(reviewPresetTimeline?.querySelectorAll(".review-rank-item") || []);
  reviewPresetItems.forEach((item) => item.classList.remove("review-rank-item-highlight"));
  if (!month) {
    return;
  }
  const targetCard = archiveCards.find((card) => Number(card.dataset.archiveMonth || 0) === Number(month));
  if (targetCard) {
    targetCard.classList.add("archive-card-highlight");
    targetCard.scrollIntoView({ block: "nearest", behavior: "smooth" });
    renderPresetImpactFromArchiveCard(targetCard);
  }
  const targetPreset = presetItems.find((item) => Number(item.dataset.presetMonth || 0) === Number(month));
  if (targetPreset) {
    targetPreset.classList.add("preset-history-item-highlight");
    targetPreset.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }
  const targetReviewPreset = reviewPresetItems.find((item) => Number(item.dataset.reviewPresetMonth || 0) === Number(month));
  if (targetReviewPreset) {
    targetReviewPreset.classList.add("review-rank-item-highlight");
    targetReviewPreset.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }
}

function renderPresetImpactFromArchiveCard(card) {
  if (!scenarioPresetImpact || !card) {
    return;
  }
  const month = card.dataset.archiveMonth || "-";
  const monthNumber = Number(month || 0);
  const metrics = Array.from(card.querySelectorAll(".archive-meta")).map((node) => node.textContent?.trim()).filter(Boolean);
  const highlights = Array.from(card.querySelectorAll(".archive-highlight")).slice(0, 3).map((node) => node.textContent?.trim()).filter(Boolean);
  const prevCard = Array.from(archiveList.querySelectorAll(".archive-card")).find((item) => Number(item.dataset.archiveMonth || 0) === monthNumber - 1);
  const currentChartPoint = appState.chartHistory.find((item) => Number(item.month || 0) === monthNumber);
  const prevChartPoint = appState.chartHistory.find((item) => Number(item.month || 0) === monthNumber - 1);
  const deltaLine = currentChartPoint && prevChartPoint
    ? (getLang() === "en"
      ? `Vs M${monthNumber - 1}: Tx ${formatSignedDelta(currentChartPoint.transactions - prevChartPoint.transactions)} · Activations ${formatSignedDelta(currentChartPoint.activations - prevChartPoint.activations)} · Success Rate ${formatSignedDelta(Number((currentChartPoint.successRate - prevChartPoint.successRate).toFixed(1)))}`
      : `较 M${monthNumber - 1}：成交 ${formatSignedDelta(currentChartPoint.transactions - prevChartPoint.transactions)} · 激活 ${formatSignedDelta(currentChartPoint.activations - prevChartPoint.activations)} · 成功率 ${formatSignedDelta(Number((currentChartPoint.successRate - prevChartPoint.successRate).toFixed(1)))}`)
    : (prevCard ? (getLang() === "en" ? `Vs M${monthNumber - 1}: archive comparison available.` : `较 M${monthNumber - 1}：已有归档对比。`) : (getLang() === "en" ? "No previous month to compare." : "没有上月可供对比。"));
  scenarioPresetImpact.innerHTML = [
    `<div class="preset-impact-title">${getLang() === "en" ? `Month ${month} Impact` : `${monthLabel(month)} 影响摘要`}</div>`,
    `<div class="preset-impact-line preset-impact-delta">${deltaLine}</div>`,
    ...metrics.slice(0, 3).map((line) => `<div class="preset-impact-line">${line}</div>`),
    ...highlights.map((line) => `<div class="preset-impact-line">${line}</div>`),
  ].join("");
}

function formatSignedDelta(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric)) {
    return "0";
  }
  return `${numeric >= 0 ? "+" : ""}${numeric}`;
}

function pushNegotiationArchiveSnippet(event) {
  if (event.event_type !== "NEGOTIATION_CLOSED") {
    return;
  }
  const payload = event.payload || {};
  appState.negotiationArchiveSnippets = [
    getLang() === "en"
      ? `P${payload.property_id} ${payload.success ? "closed" : "collapsed"} after ${payload.round_count ?? 0} rounds`
      : `房产 ${payload.property_id} 在 ${payload.round_count ?? 0} 轮后${payload.success ? "收束" : "破裂"}`,
    ...appState.negotiationArchiveSnippets,
  ].slice(0, 3);
}

function bumpNamedStat(store, key, updater) {
  const current = store.get(key) || updater(null);
  store.set(key, updater(current));
}

function trackReviewStats(event) {
  const payload = event.payload || {};

  if (event.event_type === "AGENT_ACTIVATED" && payload.agent_id != null) {
    const key = String(payload.agent_id);
    bumpNamedStat(appState.reviewAgentStats, key, (current) => ({
      agentId: payload.agent_id,
      name: payload.name || `Agent ${payload.agent_id}`,
      agentType: appState.agentTypesById.get(key) || payload.agent_type || "normal",
      activations: (current?.activations || 0) + 1,
      deals: current?.deals || 0,
      failures: current?.failures || 0,
    }));
  }

  if ((event.event_type === "DEAL_SUCCESS" || event.event_type === "DEAL_FAIL") && payload.buyer_id != null) {
    const key = String(payload.buyer_id);
    bumpNamedStat(appState.reviewAgentStats, key, (current) => ({
      agentId: payload.buyer_id,
      name: current?.name || `Agent ${payload.buyer_id}`,
      agentType: current?.agentType || appState.agentTypesById.get(key) || "normal",
      activations: current?.activations || 0,
      deals: (current?.deals || 0) + (event.event_type === "DEAL_SUCCESS" ? 1 : 0),
      failures: (current?.failures || 0) + (event.event_type === "DEAL_FAIL" ? 1 : 0),
    }));
  }

  if (payload.property_id != null && ["PROPERTY_LISTED", "MATCH_ATTEMPT", "DEAL_SUCCESS", "DEAL_FAIL"].includes(event.event_type)) {
    const key = String(payload.property_id);
    bumpNamedStat(appState.reviewPropertyStats, key, (current) => ({
      propertyId: payload.property_id,
      zone: current?.zone || payload.zone || "?",
      propertyType: current?.propertyType || payload.property_type || "Property",
      listings: (current?.listings || 0) + (event.event_type === "PROPERTY_LISTED" ? 1 : 0),
      attempts: (current?.attempts || 0) + (event.event_type === "MATCH_ATTEMPT" ? 1 : 0),
      deals: (current?.deals || 0) + (event.event_type === "DEAL_SUCCESS" ? 1 : 0),
      failures: (current?.failures || 0) + (event.event_type === "DEAL_FAIL" ? 1 : 0),
    }));
  }

  if (event.event_type === "DEAL_FAIL") {
    const reason = String(payload.reason || payload.status || "unknown").trim() || "unknown";
    bumpNamedStat(appState.reviewFailureStats, reason, (current) => ({
      reason,
      count: (current?.count || 0) + 1,
    }));
  }
}

function renderReviewRanking(target, items, renderItem, emptyText) {
  if (!target) {
    return;
  }
  if (!items.length) {
    target.innerHTML = `<div class="review-empty">${emptyText}</div>`;
    return;
  }
  target.innerHTML = items.map(renderItem).join("");
}

function getLane(eventType) {
  if (eventType === "AGENT_GENERATED") {
    return { list: generatedAgentList, counter: generatedAgentCount };
  }
  if (eventType === "PROPERTY_GENERATED") {
    return { list: generatedPropertyList, counter: generatedPropertyCount };
  }
  if (eventType === "PROPERTY_LISTED") {
    return { list: listedPropertyList, counter: listedPropertyCount };
  }
  if (eventType === "MATCH_ATTEMPT") {
    return { list: matchAttemptList, counter: matchAttemptCount };
  }
  if (eventType === "NEGOTIATION_STARTED" || eventType === "NEGOTIATION_PROGRESS" || eventType === "NEGOTIATION_QUOTE" || eventType === "NEGOTIATION_TURN" || eventType === "NEGOTIATION_TURN_BATCH_END" || eventType === "NEGOTIATION_CLOSED") {
    return { list: negotiationList, counter: negotiationCount };
  }
  if (eventType === "AGENT_ACTIVATED") {
    return { list: activationList, counter: activationCount };
  }
  if (eventType === "DEAL_SUCCESS") {
    return { list: successList, counter: successCount };
  }
  if (eventType === "DEAL_FAIL") {
    return { list: failureList, counter: failureCount };
  }
  return { list: systemList, counter: systemCount };
}

function summarizeEvent(event) {
  const payload = event.payload || {};
  const inEn = getLang() === "en";
  const stageStatusLabel = (value) => {
    const normalized = String(value || "").trim().toLowerCase();
    const zhMap = {
      observer: "观察者",
      active_participant: "活跃参与者",
      active: "活跃参与者",
      listed: "已挂牌",
      inventory: "库存房产",
      negotiating: "谈判中",
      settled: "已成交",
      sold: "已成交",
      failed: "失败",
      cooldown: "冷却中",
    };
    const enMap = {
      observer: "Observer",
      active_participant: "Active",
      active: "Active",
      listed: "Listed",
      inventory: "Inventory",
      negotiating: "Negotiating",
      settled: "Settled",
      sold: "Sold",
      failed: "Failed",
      cooldown: "Cooldown",
    };
    return inEn ? (enMap[normalized] || String(value || "-")) : (zhMap[normalized] || String(value || "-"));
  };
  const stageTone = (value) => {
    const normalized = String(value || "").trim().toLowerCase();
    if (["settled", "sold"].includes(normalized)) return "status-success";
    if (["failed"].includes(normalized)) return "status-failure";
    if (["cooldown"].includes(normalized)) return "status-cooldown";
    if (["negotiating"].includes(normalized)) return "status-negotiating";
    if (["listed", "inventory"].includes(normalized)) return "status-listed";
    if (["active_participant", "active"].includes(normalized)) return "status-active";
    if (["observer"].includes(normalized)) return "status-observer";
    return "";
  };
  const getAgentType = (agentId, fallback = "normal") => {
    if (agentId == null) {
      return fallback;
    }
    return appState.agentTypesById.get(String(agentId)) || fallback;
  };
  const localizeRaw = (value) => localizeNarrativeText(value || JSON.stringify(payload, null, 2));

  if (event.event_type === "AGENT_ACTIVATED") {
    const agentType = getAgentType(payload.agent_id, payload.agent_type || "normal");
    return {
      title: `${payload.name || `Agent ${payload.agent_id}`}`,
      subtitle: `${payload.occupation || (inEn ? "Unknown occupation" : "未知职业")} · ${payload.role || "UNKNOWN"}`,
      tags: [
        { text: `Agent #${payload.agent_id}` },
        { text: agentType },
        { text: payload.role || "UNKNOWN", tone: "gold" },
        ...(payload.stage_status ? [{ text: stageStatusLabel(payload.stage_status), tone: stageTone(payload.stage_status) }] : []),
      ],
      avatars: [{ text: `${payload.name || "A"}`.slice(0, 2), kind: "agent", agentType }],
      raw: localizeRaw(payload.reason),
    };
  }

  if (event.event_type === "AGENT_GENERATED") {
    const agentType = payload.agent_type || "normal";
    return {
      title: `${payload.name || `Agent ${payload.agent_id}`}`,
      subtitle: `${payload.occupation || (inEn ? "Unknown occupation" : "未知职业")} · ${agentType}`,
      tags: [
        { text: `Agent #${payload.agent_id}` },
        { text: agentType },
        ...(payload.stage_status ? [{ text: stageStatusLabel(payload.stage_status), tone: stageTone(payload.stage_status) }] : []),
      ],
      avatars: [{ text: `${payload.name || "A"}`.slice(0, 2), kind: "agent", agentType }],
      raw: localizeRaw(JSON.stringify(payload, null, 2)),
    };
  }

  if (event.event_type === "PROPERTY_GENERATED") {
    const propertyName = payload.display_name || `${inEn ? "Property" : "房"}#${payload.property_id}`;
    return {
      title: propertyName,
      subtitle: `${payload.zone || "?"} ${inEn ? "Zone" : "区"} · ${payload.property_type || (inEn ? "Property" : "房产")}`,
      tags: [
        { text: payload.is_school_district ? (inEn ? "School" : "学区") : (inEn ? "Normal" : "普通"), tone: payload.is_school_district ? "gold" : "" },
        { text: `${inEn ? "Pool" : "库存"} ${payload.display_total_properties || 0}` },
        ...(payload.stage_status ? [{ text: stageStatusLabel(payload.stage_status), tone: stageTone(payload.stage_status) }] : []),
      ],
      avatars: [{ text: `${payload.zone || "P"}${payload.property_id}`.slice(0, 2), kind: "property" }],
      raw: localizeRaw(JSON.stringify(payload, null, 2)),
    };
  }

  if (event.event_type === "PROPERTY_LISTED") {
    return {
      title: `${inEn ? "Listed" : "挂牌"} · ${inEn ? "Property" : "房"}#${payload.property_id}`,
      subtitle: `${payload.zone || "?"} ${inEn ? "Zone" : "区"} · ${payload.property_type || (inEn ? "Property" : "房产")}`,
      tags: [
        { text: `¥${Number(payload.listed_price || 0).toLocaleString()}` },
        { text: payload.is_school_district ? (inEn ? "School" : "学区") : (inEn ? "Normal" : "普通"), tone: payload.is_school_district ? "gold" : "" },
        ...(payload.stage_status ? [{ text: stageStatusLabel(payload.stage_status), tone: stageTone(payload.stage_status) }] : []),
      ],
      avatars: [{ text: `${payload.zone || "L"}${payload.property_id}`.slice(0, 2), kind: "property" }],
      raw: localizeRaw(JSON.stringify(payload, null, 2)),
    };
  }

  if (event.event_type === "MATCH_ATTEMPT") {
    const buyerType = getAgentType(payload.buyer_id, "normal");
    return {
      title: `买家 ${payload.buyer_id} 撮合房产 #${payload.property_id}`,
      subtitle: payload.proceeded_to_negotiation ? (inEn ? "Proceeded to negotiation" : "已进入谈判") : (inEn ? "Screened in matching stage" : "停留在匹配阶段"),
      tags: [
        { text: `Bid ¥${Number(payload.buyer_bid || 0).toLocaleString()}` },
        { text: payload.proceeded_to_negotiation ? (inEn ? "Negotiation" : "谈判") : (inEn ? "Attempt" : "尝试"), tone: payload.proceeded_to_negotiation ? "gold" : "" },
        ...(payload.stage_status ? [{ text: stageStatusLabel(payload.stage_status), tone: stageTone(payload.stage_status) }] : []),
      ],
      avatars: [
        { text: `B${payload.buyer_id}`, kind: "agent", agentType: buyerType },
        { text: `P${payload.property_id}`, kind: "property" },
      ],
      raw: localizeRaw(JSON.stringify(payload, null, 2)),
    };
  }

  if (event.event_type === "NEGOTIATION_STARTED") {
    return {
      title: `房产 #${payload.property_id} 谈判开启`,
      subtitle: `${inEn ? "Buyer" : "买家"} ${payload.buyer_id} · ${inEn ? "Seller" : "卖家"} ${payload.seller_id}`,
      tags: [
        { text: `${inEn ? "Rounds" : "轮次"} ${payload.round_count ?? 0}` },
        { text: inEn ? "Started" : "开始", tone: "gold" },
      ],
      avatars: [
        { text: `B${payload.buyer_id}`, kind: "agent", agentType: getAgentType(payload.buyer_id, "normal") },
        { text: `P${payload.property_id}`, kind: "property" },
      ],
      raw: localizeRaw(JSON.stringify(payload, null, 2)),
    };
  }

  if (event.event_type === "NEGOTIATION_PROGRESS") {
    return {
      title: `谈判进行中 · 房产 #${payload.property_id}`,
      subtitle: payload.summary || (inEn ? "Back-and-forth bargaining continues" : "买卖双方持续来回试探"),
      tags: [
        { text: `${inEn ? "Rounds" : "轮次"} ${payload.round_count ?? 0}` },
        { text: inEn ? "Progress" : "推进" },
      ],
      avatars: [
        { text: `B${payload.buyer_id}`, kind: "agent", agentType: getAgentType(payload.buyer_id, "normal") },
        { text: `P${payload.property_id}`, kind: "property" },
      ],
      raw: localizeRaw(payload.summary),
    };
  }

  if (event.event_type === "NEGOTIATION_QUOTE" || event.event_type === "NEGOTIATION_TURN") {
    const speaker = String(payload.speaker || "agent").toLowerCase();
    const speakerLabel = speaker === "buyer" ? (inEn ? "Buyer" : "买家") : (speaker === "seller" ? (inEn ? "Seller" : "卖家") : "Agent");
    return {
      title: `${speakerLabel} ${event.event_type === "NEGOTIATION_TURN" ? (inEn ? "turn" : "回合") : (inEn ? "quote" : "原话")} · ${inEn ? "Property" : "房产"} #${payload.property_id}`,
      subtitle: payload.turn_text || payload.quote || (inEn ? "Negotiation quote" : "谈判原话"),
      tags: [
        { text: `${inEn ? "Turn" : "回合"} ${payload.turn_index ?? 0}` },
        { text: speakerLabel, tone: speaker === "seller" ? "gold" : "" },
      ],
      avatars: [
        { text: `B${payload.buyer_id}`, kind: "agent", agentType: getAgentType(payload.buyer_id, "normal") },
        { text: `P${payload.property_id}`, kind: "property" },
      ],
      raw: localizeRaw(payload.turn_text || payload.quote),
    };
  }

  if (event.event_type === "NEGOTIATION_TURN_BATCH_END") {
    return {
      title: `${inEn ? "Turn batch complete" : "回合批次结束"} · ${inEn ? "Property" : "房产"} #${payload.property_id}`,
      subtitle: `${payload.emitted_turns || 0} ${inEn ? "turns emitted in" : "条回合记录，模式"} ${payload.quote_mode || "full_quotes"}`,
      tags: [
        { text: `${inEn ? "Negotiation" : "谈判"} ${payload.negotiation_id ?? "-"}` },
        { text: inEn ? "Batch End" : "批次结束", tone: "gold" },
      ],
      avatars: [
        { text: `B${payload.buyer_id}`, kind: "agent", agentType: getAgentType(payload.buyer_id, "normal") },
        { text: `P${payload.property_id}`, kind: "property" },
      ],
      raw: localizeRaw(JSON.stringify(payload, null, 2)),
    };
  }

  if (event.event_type === "NEGOTIATION_CLOSED") {
    return {
      title: `${inEn ? "Negotiation closed" : "谈判收束"} · ${inEn ? "Property" : "房产"} #${payload.property_id}`,
      subtitle: payload.success ? (inEn ? "Moved toward settlement" : "进入结算阶段") : (payload.reason || (inEn ? "Negotiation ended without deal" : "谈判结束但未达成交易")),
      tags: [
        { text: `${inEn ? "Rounds" : "轮次"} ${payload.round_count ?? 0}` },
        { text: payload.success ? (inEn ? "Closed" : "收束") : (inEn ? "Collapsed" : "破裂"), tone: payload.success ? "gold" : "warn" },
      ],
      avatars: [
        { text: `B${payload.buyer_id}`, kind: "agent", agentType: getAgentType(payload.buyer_id, "normal") },
        { text: `P${payload.property_id}`, kind: "property" },
      ],
      raw: localizeRaw(JSON.stringify(payload, null, 2)),
    };
  }

  if (event.event_type === "DEAL_SUCCESS") {
    const buyerType = getAgentType(payload.buyer_id, "normal");
    return {
      title: `${inEn ? "Property" : "房"}#${payload.property_id} ${inEn ? "closed" : "成交"}`,
      subtitle: `${inEn ? "Buyer" : "买家"} ${payload.buyer_id} ← ${inEn ? "Seller" : "卖家"} ${payload.seller_id}`,
      tags: [
        { text: `${inEn ? "Order" : "订单"} #${payload.order_id}` },
        { text: payload.deal_stage || payload.status || "success", tone: "gold" },
        { text: `¥${Number(payload.agreed_price || 0).toLocaleString()}` },
        ...(payload.stage_status ? [{ text: stageStatusLabel(payload.stage_status), tone: stageTone(payload.stage_status) }] : []),
      ],
      avatars: [
        { text: `B${payload.buyer_id}`, kind: "agent", agentType: buyerType },
        { text: `P${payload.property_id}`, kind: "property" },
      ],
      raw: localizeRaw(JSON.stringify(payload, null, 2)),
    };
  }

  if (event.event_type === "DEAL_FAIL") {
    const buyerType = getAgentType(payload.buyer_id, "normal");
    return {
      title: `${inEn ? "Property" : "房"}#${payload.property_id} ${inEn ? "failed" : "失败"}`,
      subtitle: `${inEn ? "Buyer" : "买家"} ${payload.buyer_id} · ${inEn ? "Status" : "状态"} ${payload.status || "unknown"}`,
      tags: [
        { text: `${inEn ? "Order" : "订单"} #${payload.order_id}` },
        { text: payload.status || "failed", tone: "warn" },
        ...(payload.stage_status ? [{ text: stageStatusLabel(payload.stage_status), tone: stageTone(payload.stage_status) }] : []),
      ],
      avatars: [
        { text: `B${payload.buyer_id}`, kind: "agent", agentType: buyerType },
        { text: `P${payload.property_id}`, kind: "property" },
      ],
      raw: localizeRaw(payload.reason),
    };
  }

  if (event.event_type === "MONTH_END") {
    const month = payload.month_result || {};
    return {
      title: `${monthLabel(month.month)} ${inEn ? "completed" : "结束"}`,
      subtitle: `${t("chart.transactions")} ${month.transactions} · ${t("chart.failed_negotiations")} ${month.failed_negotiations}`,
      tags: [
        { text: `${inEn ? "Listings" : "挂牌"} ${month.active_listing_count ?? 0}` },
        { text: `${inEn ? "Buyers" : "买家"} ${month.buyer_count ?? 0}` },
        { text: `${inEn ? "Events" : "事件"} ${month.event_count ?? 0}`, tone: "gold" },
      ],
      avatars: [],
      raw: localizeRaw(month.bulletin_excerpt),
    };
  }

  if (event.event_type === "MARKET_BULLETIN_READY") {
    return {
      title: `${inEn ? "Market bulletin ready for" : "市场公报已就绪"} ${monthLabel(payload.month ?? event.month)}`,
      subtitle: inEn ? "Macro narrative prepared before transaction flow" : "交易流开始前，宏观叙事已准备完成",
      tags: [
        { text: monthLabel(payload.month ?? event.month) },
        { text: inEn ? "Bulletin" : "公报", tone: "gold" },
      ],
      avatars: [],
      raw: localizeRaw(payload.bulletin_excerpt || payload.bulletin),
    };
  }

  if (event.event_type === "CONTROLS_UPDATED") {
    const controls = payload.controls || {};
    return {
      title: inEn ? "Runtime controls updated" : "运行参数已更新",
      subtitle: inEn ? "Policy knobs changed for upcoming months" : "后续月份将使用新的政策旋钮",
      tags: [
        { text: `DP ${controls.down_payment_ratio ?? "-"}` },
        { text: `Rate ${controls.annual_interest_rate ?? "-"}` },
        { text: `DTI ${controls.max_dti_ratio ?? "-"}` },
        { text: controls.market_pulse_enabled ? (inEn ? "Pulse on" : "脉冲开启") : (inEn ? "Pulse off" : "脉冲关闭") },
        { text: controls.macro_override_mode || (inEn ? "default" : "默认"), tone: "gold" },
        { text: controls.negotiation_quote_stream_enabled ? (inEn ? "Quote on" : "原话开启") : (inEn ? "Quote off" : "原话关闭") },
        { text: controls.negotiation_quote_filter_mode || "all" },
      ],
      avatars: [],
      raw: localizeRaw(JSON.stringify(controls, null, 2)),
    };
  }

  if (event.event_type === "POPULATION_ADDED") {
    const result = payload.result || {};
    const range = result.income_multiplier_range;
    const incomeLabel = range
      ? `${inEn ? "Income" : "收入"} ${Number(range.min ?? 0).toFixed(2)}-${Number(range.max ?? 0).toFixed(2)}x`
      : `${inEn ? "Income" : "收入"} x${Number(result.income_multiplier ?? 1).toFixed(2)}`;
    return {
      title: `${inEn ? "Population injected" : "人口注入"} · ${result.added_count ?? 0} ${inEn ? "agents" : "人"}`,
      subtitle: `${inEn ? "Tier" : "层级"} ${result.tier || "middle"} · ${result.template || (inEn ? "custom" : "自定义")} · ${inEn ? "newcomers staged for later months" : "新增人口将在后续月份生效"}`,
      tags: [
        { text: `${inEn ? "Count" : "数量"} ${result.added_count ?? 0}` },
        { text: result.tier || "middle" },
        { text: result.template || (inEn ? "custom" : "自定义") },
        { text: incomeLabel, tone: "gold" },
      ],
      avatars: [],
      raw: localizeRaw(JSON.stringify(result, null, 2)),
    };
  }

  if (event.event_type === "INCOME_SHOCK_APPLIED") {
    const result = payload.result || {};
    const pct = Number(result.pct_change || 0);
    const pctLabel = `${pct >= 0 ? "+" : ""}${(pct * 100).toFixed(1)}%`;
    return {
      title: `${inEn ? "Income shock applied" : "收入冲击已应用"} · ${pctLabel}`,
      subtitle: `${inEn ? "Target tier" : "目标层级"} ${result.target_tier || "all"} · ${result.updated_count ?? 0} ${inEn ? "agents updated" : "位 agent 已更新"}`,
      tags: [
        { text: pctLabel, tone: pct >= 0 ? "gold" : "warn" },
        { text: result.target_tier || "all" },
        { text: `${inEn ? "Updated" : "已更新"} ${result.updated_count ?? 0}` },
      ],
      avatars: [],
      raw: localizeRaw(JSON.stringify(result, null, 2)),
    };
  }

  if (event.event_type === "DEVELOPER_SUPPLY_INJECTED") {
    const result = payload.result || {};
    return {
      title: `${inEn ? "Developer supply injected" : "开发商供给已注入"} · ${result.count ?? 0} ${inEn ? "units" : "套"}`,
      subtitle: `${inEn ? "Zone" : "区域"} ${result.zone || "?"} · ${result.template || (inEn ? "custom" : "自定义")} · ${inEn ? "staged directly into market inventory" : "已直接进入市场库存"}`,
      tags: [
        { text: `${inEn ? "Zone" : "区域"} ${result.zone || "?"}` },
        { text: `${inEn ? "Count" : "数量"} ${result.count ?? 0}` },
        { text: result.template || (inEn ? "custom" : "自定义") },
        { text: result.school_units != null ? `${inEn ? "School" : "学区"} ${result.school_units}` : (inEn ? "School default" : "学区默认"), tone: "gold" },
      ],
      avatars: [],
      raw: localizeRaw(JSON.stringify(result, null, 2)),
    };
  }

  if (event.event_type === "SCENARIO_PRESET_APPLIED") {
    return {
      title: `${inEn ? "Scenario preset applied" : "实验场景已应用"} · ${payload.preset || "unknown"}`,
      subtitle: inEn ? "Bundled policy, population, income, and supply actions staged together" : "政策、人口、收入和供给动作已打包生效",
      tags: [
        { text: payload.preset || "unknown", tone: "gold" },
      ],
      avatars: [],
      raw: localizeRaw(JSON.stringify(payload, null, 2)),
    };
  }

  return {
    title: event.event_type,
    subtitle: `${monthLabel(event.month)} · ${translatePhase(event.phase)}`,
    tags: [],
    avatars: [],
    raw: localizeRaw(JSON.stringify(payload, null, 2)),
  };
}

function launchGhost(sourceNode, event) {
  if (!exchangeOverlay || !sourceNode) {
    return;
  }

  const overlayRect = exchangeOverlay.getBoundingClientRect();
  const sourceRect = sourceNode.getBoundingClientRect();
  const targetList = event.event_type === "DEAL_SUCCESS" ? successList : failureList;
  const targetRect = targetList.getBoundingClientRect();

  const ghost = document.createElement("div");
  ghost.className = `event-ghost ${event.event_type === "DEAL_SUCCESS" ? "success" : "failure"}`;
  ghost.textContent = event.event_type === "DEAL_SUCCESS"
    ? `B${event.payload.buyer_id} -> P${event.payload.property_id}`
    : `B${event.payload.buyer_id} x P${event.payload.property_id}`;

  const startX = sourceRect.left - overlayRect.left;
  const startY = sourceRect.top - overlayRect.top;
  const endX = targetRect.left - overlayRect.left + 12;
  const endY = targetRect.top - overlayRect.top + 12;

  ghost.style.left = `${startX}px`;
  ghost.style.top = `${startY}px`;
  exchangeOverlay.appendChild(ghost);

  requestAnimationFrame(() => {
    ghost.style.transition = "transform 520ms cubic-bezier(0.2, 0.9, 0.2, 1), opacity 520ms ease";
    ghost.style.transform = `translate(${endX - startX}px, ${endY - startY}px) scale(0.92)`;
    ghost.style.opacity = "0.18";
  });

  setTimeout(() => {
    ghost.remove();
  }, 560);
}

function pulseNode(node) {
  if (!node) {
    return;
  }
  node.classList.remove("linked");
  void node.offsetWidth;
  node.classList.add("linked");
}

function resolveNode(node, outcomeClass) {
  if (!node) {
    return;
  }
  node.classList.remove("resolved", "resolved-success", "resolved-failure");
  void node.offsetWidth;
  node.classList.add("resolved");
  if (outcomeClass) {
    node.classList.add(outcomeClass);
  }
}

function launchMatchBeam(sourceNode, targetNode, label, tone) {
  if (!exchangeOverlay || !sourceNode || !targetNode) {
    return;
  }

  const overlayRect = exchangeOverlay.getBoundingClientRect();
  const sourceRect = sourceNode.getBoundingClientRect();
  const targetRect = targetNode.getBoundingClientRect();
  const startX = sourceRect.left - overlayRect.left + sourceRect.width * 0.5;
  const startY = sourceRect.top - overlayRect.top + sourceRect.height * 0.5;
  const endX = targetRect.left - overlayRect.left + 22;
  const endY = targetRect.top - overlayRect.top + targetRect.height * 0.5;
  const deltaX = endX - startX;
  const deltaY = endY - startY;
  const length = Math.hypot(deltaX, deltaY);
  const angle = Math.atan2(deltaY, deltaX);

  if (length < 8) {
    return;
  }

  const beam = document.createElement("div");
  beam.className = `match-beam ${tone || ""}`.trim();
  beam.innerHTML = `
    <span class="match-beam-core"></span>
    <span class="match-beam-label">${label}</span>
  `;
  beam.style.left = `${startX}px`;
  beam.style.top = `${startY}px`;
  beam.style.width = `${length}px`;
  beam.style.transform = `rotate(${angle}rad) scaleX(0.08)`;
  exchangeOverlay.appendChild(beam);

  requestAnimationFrame(() => {
    beam.style.transform = `rotate(${angle}rad) scaleX(1)`;
    beam.style.opacity = "1";
  });

  setTimeout(() => {
    beam.classList.add("done");
  }, 260);

  setTimeout(() => {
    beam.remove();
  }, 900);
}

function insertIntoLane(list, node, event) {
  if (list === negotiationList && event.payload?.negotiation_id != null) {
    const thread = getOrCreateNegotiationThread(String(event.payload.negotiation_id), event);
    const body = thread.querySelector(".negotiation-thread-body");
    body.appendChild(node);
    updateNegotiationThreadMeta(thread, event);
    return;
  }

  if (list === generatedPropertyList && event.event_type === "PROPERTY_GENERATED") {
    list.prepend(node);
    return;
  }

  if (list !== generatedAgentList || event.event_type !== "AGENT_GENERATED") {
    list.prepend(node);
    return;
  }

  const agentType = String(event.payload?.agent_type || "normal");
  if (agentType === "smart") {
    node.classList.add("smart-presence");
    list.prepend(node);
    return;
  }

  const firstNonSmart = Array.from(list.children).find((child) => !child.classList.contains("smart-presence"));
  if (firstNonSmart) {
    list.insertBefore(node, firstNonSmart);
    return;
  }
  list.appendChild(node);
}

function buildNegotiationThreadTitle(event) {
  const payload = event.payload || {};
  return `Negotiation #${payload.negotiation_id ?? "-"} · Property #${payload.property_id ?? "-"}`;
}

function buildNegotiationThreadMeta(event) {
  const payload = event.payload || {};
  if (event.event_type === "NEGOTIATION_CLOSED") {
    return payload.success ? "Closed toward settlement" : "Closed without deal";
  }
  if (event.event_type === "NEGOTIATION_TURN_BATCH_END") {
    return `${payload.emitted_turns || 0} turns ready`;
  }
  return `Rounds ${payload.round_count ?? 0}`;
}

function extractPriceSignal(text) {
  if (!text) {
    return null;
  }
  const normalized = String(text).replaceAll(",", "");
  const matches = normalized.match(/\d+(?:\.\d+)?/g);
  if (!matches?.length) {
    return null;
  }
  const numericValues = matches
    .map((item) => Number(item))
    .filter((value) => Number.isFinite(value) && value >= 1000);
  if (!numericValues.length) {
    return null;
  }
  return numericValues[numericValues.length - 1];
}

function formatCurrency(value) {
  return `¥${Number(value || 0).toLocaleString()}`;
}

function buildNegotiationGapHint(thread, event) {
  const payload = event.payload || {};
  let buyerOffer = null;
  let sellerAsk = null;
  if (thread) {
    const nodes = Array.from(thread.querySelectorAll(".event-item"));
    for (const node of nodes) {
      const nodeType = node.dataset.eventTypeHint || "";
      if (nodeType !== "NEGOTIATION_QUOTE" && nodeType !== "NEGOTIATION_TURN") {
        continue;
      }
      let nodePayload = {};
      try {
        nodePayload = JSON.parse(node.dataset.eventPayloadHint || "{}");
      }
      catch {
        nodePayload = {};
      }
      const signal = extractPriceSignal(nodePayload.turn_text || nodePayload.quote || "");
      if (!signal) {
        continue;
      }
      const speaker = String(nodePayload.speaker || "").toLowerCase();
      if (speaker === "buyer") {
        buyerOffer = signal;
      }
      else if (speaker === "seller") {
        sellerAsk = signal;
      }
    }
  }

  const finalPrice = Number(payload.final_price || 0);
  if (buyerOffer && sellerAsk) {
    const rawGap = sellerAsk - buyerOffer;
    if (rawGap <= 0) {
      return {
        label: "spread crossed",
        tone: "tight",
      };
    }
    return {
      label: `gap ${formatCurrency(rawGap)}`,
      tone: rawGap <= 50000 ? "tight" : "wide",
    };
  }
  if (finalPrice > 0) {
    return {
      label: `settled ${formatCurrency(finalPrice)}`,
      tone: "tight",
    };
  }
  const roundCount = Number(payload.round_count ?? payload.emitted_turns ?? 0);
  if (roundCount >= 5) {
    return {
      label: "stance locked",
      tone: "wide",
    };
  }
  return {
    label: "gap n/a",
    tone: "neutral",
  };
}

function buildNegotiationPriceTrail(thread, event) {
  const payload = event.payload || {};
  if (!thread) {
    return "";
  }
  const points = [];
  const nodes = Array.from(thread.querySelectorAll(".event-item"));
  for (const node of nodes) {
    const nodeType = node.dataset.eventTypeHint || "";
    if (nodeType !== "NEGOTIATION_QUOTE" && nodeType !== "NEGOTIATION_TURN") {
      continue;
    }
    let nodePayload = {};
    try {
      nodePayload = JSON.parse(node.dataset.eventPayloadHint || "{}");
    }
    catch {
      nodePayload = {};
    }
    const price = extractPriceSignal(nodePayload.turn_text || nodePayload.quote || "");
    if (!price) {
      continue;
    }
    const speaker = String(nodePayload.speaker || "agent").toLowerCase();
    points.push({
      speaker,
      price,
      turnIndex: Number(nodePayload.turn_index || 0),
    });
  }

  if (payload.final_price) {
    points.push({
      speaker: "settled",
      price: Number(payload.final_price),
      turnIndex: Number(payload.round_count || points.length || 0),
    });
  }

  const tail = points.slice(-6);
  if (!tail.length) {
    return "";
  }
  const maxPrice = Math.max(...tail.map((point) => point.price));
  const minPrice = Math.min(...tail.map((point) => point.price));
  const spread = Math.max(maxPrice - minPrice, 1);
  const items = tail.map((point) => {
    const height = 26 + (((point.price - minPrice) / spread) * 34);
    const tone = point.speaker === "seller" ? "seller" : (point.speaker === "buyer" ? "buyer" : "settled");
    const label = point.speaker === "settled" ? "Settle" : `${point.speaker === "seller" ? "S" : "B"}${point.turnIndex || ""}`;
    return `<span class="negotiation-thread-trail-point" data-trail-tone="${tone}" style="height:${height.toFixed(1)}px"><span class="negotiation-thread-trail-tip">${label}</span></span>`;
  }).join("");
  return `<span class="negotiation-thread-trail"><span class="negotiation-thread-trail-label">price trail</span><span class="negotiation-thread-trail-plot">${items}</span></span>`;
}

function collectNegotiationPricePoints(thread, event) {
  const payload = event.payload || {};
  if (!thread) {
    return [];
  }
  const points = [];
  const nodes = Array.from(thread.querySelectorAll(".event-item"));
  for (const node of nodes) {
    const nodeType = node.dataset.eventTypeHint || "";
    if (nodeType !== "NEGOTIATION_QUOTE" && nodeType !== "NEGOTIATION_TURN") {
      continue;
    }
    let nodePayload = {};
    try {
      nodePayload = JSON.parse(node.dataset.eventPayloadHint || "{}");
    }
    catch {
      nodePayload = {};
    }
    const price = extractPriceSignal(nodePayload.turn_text || nodePayload.quote || "");
    if (!price) {
      continue;
    }
    points.push({
      speaker: String(nodePayload.speaker || "agent").toLowerCase(),
      price,
      turnIndex: Number(nodePayload.turn_index || points.length + 1),
    });
  }
  if (payload.final_price) {
    points.push({
      speaker: "settled",
      price: Number(payload.final_price),
      turnIndex: Number(payload.round_count || points.length || 0),
    });
  }
  return points;
}

function buildNegotiationReplayChart(thread, event) {
  const points = collectNegotiationPricePoints(thread, event);
  if (points.length < 2) {
    return "";
  }
  const width = 220;
  const height = 92;
  const padding = 10;
  const prices = points.map((point) => point.price);
  const maxPrice = Math.max(...prices);
  const minPrice = Math.min(...prices);
  const spread = Math.max(maxPrice - minPrice, 1);
  const xStep = points.length > 1 ? (width - padding * 2) / (points.length - 1) : 0;
  const mapped = points.map((point, index) => {
    const x = padding + index * xStep;
    const y = height - padding - (((point.price - minPrice) / spread) * (height - padding * 2));
    return {
      ...point,
      x,
      y,
    };
  });
  const buyerLine = mapped.filter((point) => point.speaker === "buyer").map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const sellerLine = mapped.filter((point) => point.speaker === "seller").map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const settlePoint = mapped.findLast ? mapped.findLast((point) => point.speaker === "settled") : [...mapped].reverse().find((point) => point.speaker === "settled");
  const lastActivePoint = [...mapped].reverse().find((point) => point.speaker === "buyer" || point.speaker === "seller");
  const dots = mapped.map((point) => {
    const tone = point.speaker === "seller" ? "seller" : point.speaker === "buyer" ? "buyer" : "settled";
    const title = point.speaker === "settled"
      ? `Settled at ${formatCurrency(point.price)}`
      : `${point.speaker === "seller" ? "Seller" : "Buyer"} turn ${point.turnIndex}: ${formatCurrency(point.price)}`;
    return `<circle cx="${point.x.toFixed(1)}" cy="${point.y.toFixed(1)}" r="${point.speaker === "settled" ? 4.5 : 3.2}" class="negotiation-replay-dot" data-dot-tone="${tone}" data-turn-index="${point.turnIndex}"><title>${title}</title></circle>`;
  }).join("");
  const settleMarkup = settlePoint ? `<circle cx="${settlePoint.x.toFixed(1)}" cy="${settlePoint.y.toFixed(1)}" r="6.5" class="negotiation-replay-settle-ring"></circle>` : "";
  const failedBreakpoint = event.event_type === "NEGOTIATION_CLOSED" && event.payload?.success === false && lastActivePoint
    ? `
      <g class="negotiation-replay-breakpoint">
        <line x1="${lastActivePoint.x.toFixed(1)}" y1="${padding}" x2="${lastActivePoint.x.toFixed(1)}" y2="${height - padding}" class="negotiation-replay-break-line"></line>
        <line x1="${(lastActivePoint.x - 5).toFixed(1)}" y1="${(lastActivePoint.y - 5).toFixed(1)}" x2="${(lastActivePoint.x + 5).toFixed(1)}" y2="${(lastActivePoint.y + 5).toFixed(1)}" class="negotiation-replay-break-mark"></line>
        <line x1="${(lastActivePoint.x - 5).toFixed(1)}" y1="${(lastActivePoint.y + 5).toFixed(1)}" x2="${(lastActivePoint.x + 5).toFixed(1)}" y2="${(lastActivePoint.y - 5).toFixed(1)}" class="negotiation-replay-break-mark"></line>
      </g>
    `
    : "";
  const closeReason = event.event_type === "NEGOTIATION_CLOSED"
    ? String(event.payload?.reason || (event.payload?.success ? "Moved toward settlement" : "Closed without deal")).trim()
    : "";
  return `
    <div class="negotiation-replay-chart">
      <div class="negotiation-replay-chart-head">
        <span class="negotiation-replay-chart-title">Price Replay</span>
        <span class="negotiation-replay-chart-range">${formatCurrency(minPrice)} - ${formatCurrency(maxPrice)}</span>
      </div>
      <div class="negotiation-replay-chart-legend">
        <span class="negotiation-replay-legend-item"><span class="negotiation-replay-legend-dot buyer"></span>Buyer</span>
        <span class="negotiation-replay-legend-item"><span class="negotiation-replay-legend-dot seller"></span>Seller</span>
        <span class="negotiation-replay-legend-item"><span class="negotiation-replay-legend-dot settled"></span>Settle</span>
      </div>
        <svg class="negotiation-replay-chart-svg" viewBox="0 0 ${width} ${height}" aria-hidden="true">
          <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" class="negotiation-replay-axis"></line>
          <line x1="${padding}" y1="${padding}" x2="${padding}" y2="${height - padding}" class="negotiation-replay-axis faint"></line>
          ${buyerLine ? `<polyline points="${buyerLine}" class="negotiation-replay-line buyer-line"></polyline>` : ""}
          ${sellerLine ? `<polyline points="${sellerLine}" class="negotiation-replay-line seller-line"></polyline>` : ""}
          ${failedBreakpoint}
          ${settleMarkup}
          ${dots}
        </svg>
      ${closeReason ? `<div class="negotiation-replay-outcome" data-outcome-tone="${event.payload?.success ? "success" : "failed"}">${closeReason}</div>` : ""}
    </div>
  `;
}

function highlightNegotiationReplayTurn(thread, turnIndex) {
  if (!thread) {
    return;
  }
  const normalized = String(turnIndex || "");
  thread.querySelectorAll(".negotiation-replay-dot").forEach((dot) => {
    dot.classList.toggle("active", normalized !== "" && dot.getAttribute("data-turn-index") === normalized);
  });
  thread.querySelectorAll(".negotiation-turn-item").forEach((node) => {
    node.classList.toggle("turn-highlight", normalized !== "" && String(node.dataset.turnIndex || "") === normalized);
  });
}

function bindNegotiationReplayChart(thread) {
  if (!thread) {
    return;
  }
  const chart = thread.querySelector(".negotiation-replay-chart");
  if (!chart || chart.dataset.bound === "true") {
    return;
  }
  chart.dataset.bound = "true";
  chart.querySelectorAll(".negotiation-replay-dot[data-turn-index]").forEach((dot) => {
    const turnIndex = dot.getAttribute("data-turn-index") || "";
    dot.addEventListener("mouseenter", () => highlightNegotiationReplayTurn(thread, turnIndex));
    dot.addEventListener("mouseleave", () => highlightNegotiationReplayTurn(thread, ""));
    dot.addEventListener("click", () => {
      highlightNegotiationReplayTurn(thread, turnIndex);
      const targetTurn = thread.querySelector(`.negotiation-turn-item[data-turn-index="${turnIndex}"]`);
      targetTurn?.scrollIntoView({ block: "nearest", behavior: "smooth" });
    });
  });
  thread.querySelectorAll(".negotiation-turn-item").forEach((node) => {
    if (node.dataset.chartBound === "true") {
      return;
    }
    node.dataset.chartBound = "true";
    const turnIndex = String(node.dataset.turnIndex || "");
    node.addEventListener("mouseenter", () => highlightNegotiationReplayTurn(thread, turnIndex));
    node.addEventListener("mouseleave", () => highlightNegotiationReplayTurn(thread, ""));
  });
}

function buildNegotiationThreadSummary(event, expanded = false, thread = null) {
  const payload = event.payload || {};
  const roundCount = payload.round_count ?? payload.emitted_turns ?? 0;
  const outcome = event.event_type === "NEGOTIATION_CLOSED"
    ? (payload.success ? "success" : "failed")
    : (event.event_type === "NEGOTIATION_TURN_BATCH_END" ? "turns ready" : "live");
  const finalPrice = payload.final_price || 0;
  const densityLevel = roundCount >= 5 ? "heated" : (roundCount >= 3 ? "steady" : "brief");
  const densityPercent = roundCount > 0 ? Math.min(100, Math.max(14, roundCount * 18)) : 10;
  const gapHint = buildNegotiationGapHint(thread, event);
  return [
    `<span class="negotiation-thread-chip">${roundCount ? `${roundCount} rounds` : "rounds -"}</span>`,
    `<span class="negotiation-thread-chip">${outcome}</span>`,
    `<span class="negotiation-thread-chip">${finalPrice ? `¥${Number(finalPrice).toLocaleString()}` : "price -"}</span>`,
    `<span class="negotiation-thread-chip ${expanded ? "thread-open" : ""}">${expanded ? "expanded" : "collapsed"}</span>`,
    `<span class="negotiation-thread-gap" data-gap-tone="${gapHint.tone}">${gapHint.label}</span>`,
    `<span class="negotiation-thread-density" data-density-level="${densityLevel}"><span class="negotiation-thread-density-label">turn flow</span><span class="negotiation-thread-density-meter"><span class="negotiation-thread-density-fill" style="width: ${densityPercent}%"></span></span></span>`,
    buildNegotiationPriceTrail(thread, event),
  ].join("");
}

function getNegotiationOutcomeState(event) {
  const payload = event.payload || {};
  if (event.event_type === "NEGOTIATION_CLOSED") {
    if (payload.success === true) {
      return "success";
    }
    if (payload.success === false || payload.outcome === "failed" || payload.outcome === "collapsed") {
      return "failed";
    }
  }
  if (event.event_type === "DEAL_SUCCESS") {
    return "success";
  }
  if (event.event_type === "DEAL_FAIL") {
    return "failed";
  }
  return "live";
}

function getNegotiationStatusTone(outcomeState) {
  if (outcomeState === "success") {
    return "settled";
  }
  if (outcomeState === "failed") {
    return "broken";
  }
  return "live";
}

function updateNegotiationThreadMeta(thread, event) {
  if (!thread) {
    return;
  }
  const title = thread.querySelector(".negotiation-thread-title");
  const meta = thread.querySelector(".negotiation-thread-meta");
  const status = thread.querySelector(".negotiation-thread-status");
  const summary = thread.querySelector(".negotiation-thread-summary");
  const body = thread.querySelector(".negotiation-thread-body");
  const expanded = thread.dataset.replayExpanded === "true";
  const outcomeState = getNegotiationOutcomeState(event);
  const statusTone = getNegotiationStatusTone(outcomeState);
  thread.dataset.negotiationOutcome = outcomeState;
  if (title) {
    title.textContent = buildNegotiationThreadTitle(event);
  }
  if (meta) {
    meta.textContent = buildNegotiationThreadMeta(event);
  }
  if (status) {
    status.textContent = event.event_type.replace("NEGOTIATION_", "").replaceAll("_", " ");
    status.dataset.statusTone = statusTone;
  }
  if (summary) {
    summary.innerHTML = buildNegotiationThreadSummary(event, expanded, thread);
  }
    if (body) {
      let chart = body.querySelector(".negotiation-replay-chart");
      const chartMarkup = expanded ? buildNegotiationReplayChart(thread, event) : "";
    if (chartMarkup) {
      if (!chart) {
        body.insertAdjacentHTML("afterbegin", chartMarkup);
      }
      else {
        chart.outerHTML = chartMarkup;
      }
    }
      else if (chart) {
        chart.remove();
      }
      bindNegotiationReplayChart(thread);
    }
  }

function getOrCreateNegotiationThread(negotiationId, event) {
  const existing = appState.negotiationGroupNodesById.get(negotiationId);
  if (existing) {
    return existing;
  }

  const thread = document.createElement("section");
  thread.className = "negotiation-thread";
  thread.dataset.negotiationId = negotiationId;
  thread.dataset.negotiationOutcome = "live";
  thread.innerHTML = `
    <button type="button" class="negotiation-thread-head">
      <span class="negotiation-thread-bar" aria-hidden="true"></span>
      <div class="negotiation-thread-copy">
        <div class="negotiation-thread-title"></div>
        <div class="negotiation-thread-meta"></div>
      </div>
      <span class="negotiation-thread-status"></span>
    </button>
    <div class="negotiation-thread-summary"></div>
    <div class="negotiation-thread-body"></div>
  `;
  const head = thread.querySelector(".negotiation-thread-head");
  head?.addEventListener("click", () => toggleNegotiationReplay(negotiationId));
  negotiationList.prepend(thread);
  appState.negotiationGroupNodesById.set(negotiationId, thread);
  updateNegotiationThreadMeta(thread, event);
  return thread;
}

function refreshNegotiationQuoteFocus() {
  const focusLimit = Math.max(1, Number(appState.negotiationQuoteFocusLimit || 2));
  const topNegotiationIds = new Set(
    Array.from(appState.negotiationQuoteScores.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, focusLimit)
      .map(([key]) => key)
  );

  for (const [negotiationId, nodes] of appState.negotiationQuoteNodesById.entries()) {
    const focused = topNegotiationIds.has(negotiationId);
    for (const node of nodes) {
      node.dataset.quoteFocus = focused ? "focus" : "suppressed";
    }
  }
}


function refreshNegotiationReplayVisibility() {
  for (const [negotiationId, thread] of appState.negotiationGroupNodesById.entries()) {
    const expanded = appState.negotiationReplayExpanded.has(negotiationId);
    thread.dataset.replayExpanded = expanded ? "true" : "false";
    const latestNode = thread.querySelector(".event-item:last-child");
    if (latestNode?.dataset?.eventTypeHint) {
      updateNegotiationThreadMeta(thread, {
        event_type: latestNode.dataset.eventTypeHint,
        payload: JSON.parse(latestNode.dataset.eventPayloadHint || "{}"),
      });
    }
  }
  for (const [negotiationId, nodes] of appState.negotiationReplayNodesById.entries()) {
    const expanded = appState.negotiationReplayExpanded.has(negotiationId);
    for (const node of nodes) {
      node.dataset.turnVisibility = expanded ? "expanded" : "collapsed";
    }
  }
}

function toggleNegotiationReplay(negotiationId) {
  if (!negotiationId) {
    return;
  }
  if (appState.negotiationReplayExpanded.has(negotiationId)) {
    appState.negotiationReplayExpanded.delete(negotiationId);
  }
  else {
    appState.negotiationReplayExpanded.add(negotiationId);
  }
  refreshNegotiationReplayVisibility();
}

function bindNegotiationReplayToggle(node, negotiationId) {
  if (!node || !negotiationId) {
    return;
  }
  node.classList.add("negotiation-toggle-anchor");
  node.addEventListener("click", () => toggleNegotiationReplay(String(negotiationId)));
}

export function addEvent(event) {
  const node = eventTemplate.content.firstElementChild.cloneNode(true);
  const summary = summarizeEvent(event);
  if (event.event_type === "MARKET_BULLETIN_READY") {
    renderBulletin(event.payload || {});
  }
  node.classList.add("compact");
  if (event.event_type === "AGENT_GENERATED") node.classList.add("generated");
  else if (event.event_type === "PROPERTY_GENERATED") node.classList.add("generated", "property");
  else if (event.event_type === "PROPERTY_LISTED") node.classList.add("listing");
  else if (event.event_type === "MATCH_ATTEMPT") node.classList.add("match");
  else if (event.event_type === "NEGOTIATION_STARTED" || event.event_type === "NEGOTIATION_PROGRESS" || event.event_type === "NEGOTIATION_QUOTE" || event.event_type === "NEGOTIATION_TURN" || event.event_type === "NEGOTIATION_TURN_BATCH_END" || event.event_type === "NEGOTIATION_CLOSED") node.classList.add("negotiation");
  else if (event.event_type === "AGENT_ACTIVATED") node.classList.add("activation");
  else if (event.event_type === "DEAL_SUCCESS") node.classList.add("success");
  else if (event.event_type === "DEAL_FAIL") node.classList.add("failure");
  else node.classList.add("system");
  if (event.event_type === "DEAL_SUCCESS" || event.event_type === "DEAL_FAIL") {
    node.classList.add("arrival");
  }
  if (event.event_type === "NEGOTIATION_STARTED") {
    node.classList.add("negotiation-system");
    node.dataset.negotiationKind = "summary";
  }
  if (event.event_type === "NEGOTIATION_PROGRESS") {
    node.classList.add("negotiation-bubble", "buyer-bubble");
    node.dataset.negotiationKind = "summary";
  }
  if (event.event_type === "NEGOTIATION_QUOTE" || event.event_type === "NEGOTIATION_TURN") {
    const speaker = String(event.payload?.speaker || "agent").toLowerCase();
    node.classList.add("negotiation-bubble", speaker === "seller" ? "seller-bubble" : "buyer-bubble", "quote-bubble");
    node.dataset.negotiationKind = "quote";
    if (event.event_type === "NEGOTIATION_TURN") {
      node.classList.add("negotiation-turn-item");
      node.dataset.turnIndex = String(event.payload?.turn_index ?? "");
    }
  }
  if (event.payload?.negotiation_id != null) {
    node.dataset.negotiationId = String(event.payload.negotiation_id);
  }
  if (event.event_type === "NEGOTIATION_TURN_BATCH_END") {
    node.classList.add("negotiation-system");
    node.dataset.negotiationKind = "summary";
  }
  if (event.event_type === "NEGOTIATION_CLOSED") {
    node.classList.add("negotiation-bubble", "seller-bubble");
    node.dataset.negotiationKind = "summary";
  }
  node.querySelector(".event-type").textContent = eventTypeLabel(event.event_type);
  node.querySelector(".event-meta").textContent = `${monthShort(event.month)} · ${translatePhase(event.phase)}`;
  node.dataset.eventTypeHint = event.event_type;
  node.dataset.eventPayloadHint = JSON.stringify(event.payload || {});
  node.dataset.eventMonthHint = String(event.month ?? 0);
  node.dataset.eventPhaseHint = String(event.phase || "system");

  const pre = node.querySelector(".event-payload");
  const main = document.createElement("div");
  main.className = "event-main";
  main.innerHTML = `
    <div class="event-title"></div>
    <div class="event-subtitle"></div>
    <div class="event-tags"></div>
  `;
  main.querySelector(".event-title").textContent = summary.title;
  main.querySelector(".event-subtitle").textContent = summary.subtitle;

  if (summary.avatars && summary.avatars.length > 0) {
    const avatars = document.createElement("div");
    avatars.className = "event-avatars";
    summary.avatars.forEach((avatar, index) => {
      const badge = document.createElement("span");
      badge.className = "event-avatar";
      if (avatar.kind) {
        badge.classList.add(avatar.kind);
      }
      if (avatar.agentType) {
        badge.classList.add(avatar.agentType === "smart" ? "smart-agent" : "normal-agent");
      }
      const glyph = document.createElement("img");
      glyph.className = "event-avatar-glyph";
      glyph.alt = `${avatar.kind || "item"} icon`;
      if (avatar.kind === "property") {
        glyph.src = "/web/assets/icons/property.svg";
      }
      else if (avatar.agentType === "smart") {
        glyph.src = "/web/assets/icons/agent-smart.svg";
      }
      else {
        glyph.src = "/web/assets/icons/agent-normal.svg";
      }
      const label = document.createElement("span");
      label.className = "event-avatar-label";
      label.textContent = avatar.text;
      badge.appendChild(glyph);
      badge.appendChild(label);
      avatars.appendChild(badge);
      if (index < summary.avatars.length - 1) {
        const connector = document.createElement("span");
        connector.className = "event-connector";
        avatars.appendChild(connector);
      }
    });
    main.insertBefore(avatars, main.querySelector(".event-tags"));
  }

  const tags = main.querySelector(".event-tags");
  for (const tag of summary.tags) {
    const chip = document.createElement("span");
    chip.className = `event-tag${tag.tone ? ` ${tag.tone}` : ""}`;
    chip.textContent = tag.text;
    tags.appendChild(chip);
  }

  pre.textContent = summary.raw;
  node.insertBefore(main, pre);

  if (event.event_type === "NEGOTIATION_TURN") {
    const turnRail = document.createElement("div");
    turnRail.className = "negotiation-turn-rail";
    turnRail.innerHTML = `<span class="negotiation-turn-dot"></span><span class="negotiation-turn-label">Turn ${event.payload?.turn_index ?? "-"}</span>`;
    node.insertBefore(turnRail, main);
  }
  pushReviewOutcome(event, summary);
  pushNegotiationArchiveSnippet(event);
  trackReviewStats(event);

  const lane = getLane(event.event_type);
  clearCollectionEmpty(lane.list);
  insertIntoLane(lane.list, node, event);
  lane.counter.textContent = String(Number(lane.counter.textContent || "0") + 1);

  if (event.event_type === "MATCH_ATTEMPT" && event.payload?.proceeded_to_negotiation) {
    node.classList.add("negotiation-ready");
  }

  if (lane.list === activationList) appState.laneStats.activation += 1;
  if (lane.list === successList) appState.laneStats.success += 1;
  if (lane.list === failureList) appState.laneStats.failure += 1;
  if (lane.list === systemList) appState.laneStats.system += 1;

  if (event.event_type === "AGENT_GENERATED" && event.payload?.agent_id != null) {
    appState.agentTypesById.set(String(event.payload.agent_id), String(event.payload.agent_type || "normal"));
    appState.generatedNodesByAgent.set(String(event.payload.agent_id), node);
  }

  if (event.event_type === "AGENT_ACTIVATED" && event.payload?.agent_id != null) {
    if (event.payload?.agent_type) {
      appState.agentTypesById.set(String(event.payload.agent_id), String(event.payload.agent_type));
    }
    appState.activationNodesByAgent.set(String(event.payload.agent_id), node);
    const generated = appState.generatedNodesByAgent.get(String(event.payload.agent_id));
    if (generated) {
      pulseNode(generated);
    }
  }

  if (event.event_type === "PROPERTY_LISTED" && event.payload?.property_id != null) {
    appState.listedPropertyNodesById.set(String(event.payload.property_id), node);
  }

  if (event.event_type === "MATCH_ATTEMPT") {
    const listedNode = appState.listedPropertyNodesById.get(String(event.payload?.property_id));
    const activationNode = appState.activationNodesByAgent.get(String(event.payload?.buyer_id));
    const attemptKey = `${event.payload?.buyer_id ?? "?"}:${event.payload?.property_id ?? "?"}`;
    pulseNode(listedNode);
    pulseNode(activationNode);
    if (activationNode) {
      activationNode.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
    launchMatchBeam(listedNode, node, `P${event.payload?.property_id ?? "?"}`, "property-flow");
    launchMatchBeam(activationNode, node, `B${event.payload?.buyer_id ?? "?"}`, "buyer-flow");
    if (event.payload?.proceeded_to_negotiation) {
      appState.negotiationAttemptNodesByKey.set(attemptKey, node);
      node.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
  }

  if (event.event_type === "NEGOTIATION_CLOSED") {
    const negotiationKey = `${event.payload?.buyer_id ?? "?"}:${event.payload?.property_id ?? "?"}`;
    appState.negotiationClosedNodesByKey.set(negotiationKey, node);
    node.classList.add("negotiation-anchor");
  }

  if (["NEGOTIATION_STARTED", "NEGOTIATION_PROGRESS", "NEGOTIATION_CLOSED", "NEGOTIATION_TURN_BATCH_END"].includes(event.event_type) && event.payload?.negotiation_id != null) {
    bindNegotiationReplayToggle(node, String(event.payload.negotiation_id));
  }

  if ((event.event_type === "NEGOTIATION_QUOTE" || event.event_type === "NEGOTIATION_TURN") && event.payload?.negotiation_id != null) {
    const negotiationId = String(event.payload.negotiation_id);
    const currentNodes = appState.negotiationQuoteNodesById.get(negotiationId) || [];
    currentNodes.push(node);
    appState.negotiationQuoteNodesById.set(negotiationId, currentNodes);
    appState.negotiationQuoteScores.set(negotiationId, (appState.negotiationQuoteScores.get(negotiationId) || 0) + 1);
    if (event.event_type === "NEGOTIATION_TURN") {
      const replayNodes = appState.negotiationReplayNodesById.get(negotiationId) || [];
      replayNodes.push(node);
      appState.negotiationReplayNodesById.set(negotiationId, replayNodes);
      node.dataset.turnVisibility = appState.negotiationReplayExpanded.has(negotiationId) ? "expanded" : "collapsed";
    }
    refreshNegotiationQuoteFocus();
    refreshNegotiationReplayVisibility();
  }

  if ((event.event_type === "DEAL_SUCCESS" || event.event_type === "DEAL_FAIL") && event.payload?.buyer_id != null) {
    const attemptKey = `${event.payload.buyer_id}:${event.payload.property_id ?? "?"}`;
    const negotiationNode = appState.negotiationClosedNodesByKey.get(attemptKey);
    const attemptNode = appState.negotiationAttemptNodesByKey.get(attemptKey);
    const linked = appState.activationNodesByAgent.get(String(event.payload.buyer_id));
    if (negotiationNode) {
      pulseNode(negotiationNode);
      resolveNode(negotiationNode, event.event_type === "DEAL_SUCCESS" ? "resolved-success" : "resolved-failure");
      negotiationNode.scrollIntoView({ block: "nearest", behavior: "smooth" });
      launchGhost(negotiationNode, event);
      return;
    }
    if (attemptNode) {
      pulseNode(attemptNode);
      resolveNode(attemptNode, event.event_type === "DEAL_SUCCESS" ? "resolved-success" : "resolved-failure");
      attemptNode.scrollIntoView({ block: "nearest", behavior: "smooth" });
      launchGhost(attemptNode, event);
      return;
    }
    if (linked) {
      pulseNode(linked);
      linked.scrollIntoView({ block: "nearest", behavior: "smooth" });
      launchGhost(linked, event);
    }
  }
}

export function hideReviewPanel() {
  if (!reviewPanel) {
    return;
  }
  reviewPanel.classList.add("hidden");
}

export function showReviewPanel() {
  if (!reviewPanel) {
    return;
  }
  reviewPanel.classList.remove("hidden");
}

export function renderFinalReview(status, finalSummary = null) {
  if (!reviewPanel || !reviewSnapshot || !reviewOutcomes || !reviewStatusChip) {
    return;
  }

  const latestMonth = appState.chartHistory.at(-1);
  const archiveCountValue = appState.chartHistory.length;
  const interventions = Array.isArray(finalSummary?.interventions) ? finalSummary.interventions : [];
  const latestIntervention = interventions.length > 0 ? interventions[interventions.length - 1] : null;
  const latestPreset = [...interventions].reverse().find((item) => item.event_type === "SCENARIO_PRESET_APPLIED");
  appState.lastFinalReviewStatus = status || null;
  appState.lastFinalSummary = finalSummary || null;
  reviewStatusChip.textContent = translateStatus(status?.status || "completed");
  reviewSnapshot.innerHTML = [
    `<div><strong>${t("runtime.title")}:</strong> ${translateStatus(status?.status || "completed")}</div>`,
    `<div><strong>${getLang() === "en" ? "Completed Month" : "完成月份"}:</strong> ${status?.current_month ?? latestMonth?.month ?? "-"}</div>`,
    `<div><strong>${getLang() === "en" ? "Months Archived" : "归档月份数"}:</strong> ${archiveCountValue}</div>`,
    `<div><strong>${getLang() === "en" ? "Latest Transactions" : "最近成交量"}:</strong> ${latestMonth?.transactions ?? 0}</div>`,
    `<div><strong>${getLang() === "en" ? "Latest Activations" : "最近激活数"}:</strong> ${latestMonth?.activations ?? 0}</div>`,
    `<div><strong>${getLang() === "en" ? "Latest Success Rate" : "最近成功率"}:</strong> ${latestMonth?.successRate ?? 0}%</div>`,
    `<div><strong>${getLang() === "en" ? "Interventions Logged" : "已记录干预"}:</strong> ${interventions.length}</div>`,
    `<div><strong>${getLang() === "en" ? "Latest Preset" : "最近场景"}:</strong> ${latestPreset?.payload?.preset || t("common.none")}</div>`,
    `<div><strong>${getLang() === "en" ? "Latest Intervention" : "最近干预"}:</strong> ${latestIntervention?.summary || t("common.none")}</div>`,
    `<div><strong>${t("runtime.run_dir")}:</strong> ${status?.run_dir || t("common.none")}</div>`,
  ].join("");

  if (appState.reviewOutcomes.length === 0) {
    reviewOutcomes.innerHTML = `<div class="review-empty">${getLang() === "en" ? "No closing events recorded yet." : "尚无成交收束事件。"}</div>`;
    return;
  }

  reviewOutcomes.innerHTML = appState.reviewOutcomes.map((item) => `
    <article class="review-outcome ${item.eventType === "DEAL_SUCCESS" ? "success" : "failure"}">
      <div class="review-outcome-title">${item.title}</div>
      <div class="review-outcome-subtitle">${item.subtitle}</div>
      <div class="review-outcome-meta">¥${Number(item.price || 0).toLocaleString()} · ${item.status}</div>
    </article>
  `).join("");

  const topAgents = Array.isArray(finalSummary?.top_agents)
    ? finalSummary.top_agents
    : Array.from(appState.reviewAgentStats.values())
      .sort((a, b) => (b.deals * 3 + b.activations - b.failures) - (a.deals * 3 + a.activations - a.failures))
      .slice(0, 5);
  renderReviewRanking(
    reviewTopAgents,
    topAgents,
    (item) => `
      <article class="review-rank-item">
        <div class="review-rank-title">${item.name}</div>
        <div class="review-rank-meta">${item.agent_type || item.agentType || "normal"} · ${getLang() === "en" ? "Activations" : "激活"} ${item.activations ?? 0} · ${getLang() === "en" ? "Deals" : "成交"} ${item.deals ?? 0} · ${getLang() === "en" ? "Failures" : "失败"} ${item.failures ?? 0}</div>
      </article>
    `,
    getLang() === "en" ? "No agent activity recorded." : "尚无 Agent 活动。"
  );

  const keyProperties = Array.isArray(finalSummary?.key_properties)
    ? finalSummary.key_properties
    : Array.from(appState.reviewPropertyStats.values())
      .sort((a, b) => (b.deals * 4 + b.attempts + b.listings) - (a.deals * 4 + a.attempts + a.listings))
      .slice(0, 5);
  renderReviewRanking(
    reviewKeyProperties,
    keyProperties,
    (item) => `
      <article class="review-rank-item">
        <div class="review-rank-title">Property #${item.property_id ?? item.propertyId}</div>
        <div class="review-rank-meta">${item.zone || "?"} 区 · ${item.property_type || item.propertyType || "Property"}</div>
        <div class="review-rank-meta">${getLang() === "en" ? "Listings" : "挂牌"} ${item.listings ?? 0} · ${getLang() === "en" ? "Attempts" : "尝试"} ${item.attempts ?? 0} · ${getLang() === "en" ? "Deals" : "成交"} ${item.deals ?? 0} · ${getLang() === "en" ? "Failures" : "失败"} ${item.failures ?? 0}</div>
      </article>
    `,
    getLang() === "en" ? "No property activity recorded." : "尚无房产活动。"
  );

  const failureReasons = Array.isArray(finalSummary?.failure_reasons)
    ? finalSummary.failure_reasons
    : Array.from(appState.reviewFailureStats.values())
      .sort((a, b) => b.count - a.count)
      .slice(0, 5);
  renderReviewRanking(
    reviewFailureReasons,
    failureReasons,
    (item) => `
      <article class="review-rank-item">
        <div class="review-rank-title">${item.reason}</div>
        <div class="review-rank-meta">${getLang() === "en" ? "Count" : "次数"} ${item.count}</div>
      </article>
    `,
    getLang() === "en" ? "No failure reasons recorded." : "尚无失败原因记录。"
  );

  const presetTimeline = interventions
    .filter((item) => item?.event_type === "SCENARIO_PRESET_APPLIED")
    .slice(-6)
    .reverse();
  renderReviewRanking(
    reviewPresetTimeline,
    presetTimeline,
    (item) => `
      <article class="review-rank-item" data-review-preset-month="${item.month ?? 0}">
        <div class="review-rank-title">${item.payload?.preset || "unknown"}</div>
        <div class="review-rank-meta">${monthLabel(item.month ?? 0)}</div>
        <div class="review-rank-meta">${item.summary || (getLang() === "en" ? "Scenario preset applied" : "场景已应用")}</div>
      </article>
    `,
    getLang() === "en" ? "No scenario preset applied." : "尚未应用任何场景。"
  );
  reviewPresetTimeline?.querySelectorAll(".review-rank-item").forEach((node) => {
    node.addEventListener("click", () => {
      const month = Number(node.getAttribute("data-review-preset-month") || 0);
      highlightArchiveMonth(month);
    });
  });
}

export function archiveCurrentMonth(monthPayload) {
  appState.archivePayloads.push(monthPayload);
  const monthResult = monthPayload.month_result || {};
  const monthReview = monthResult.month_review || {};
  const controls = monthResult.controls_snapshot || {};
  const negotiationStory = appState.negotiationArchiveSnippets.length > 0
    ? appState.negotiationArchiveSnippets.join(" · ")
    : "No notable negotiation arc recorded.";
  const topAgent = Array.isArray(monthReview.top_agents) && monthReview.top_agents.length > 0
    ? monthReview.top_agents[0]
    : null;
  const keyProperty = Array.isArray(monthReview.key_properties) && monthReview.key_properties.length > 0
    ? monthReview.key_properties[0]
    : null;
  const failureHotspot = Array.isArray(monthReview.failure_reasons) && monthReview.failure_reasons.length > 0
    ? monthReview.failure_reasons[0]
    : null;
  const latestPreset = Array.isArray(monthReview.interventions)
    ? [...monthReview.interventions].reverse().find((item) => item.event_type === "SCENARIO_PRESET_APPLIED")
    : null;
  const interventionHotline = Array.isArray(monthReview.interventions) && monthReview.interventions.length > 0
    ? monthReview.interventions.map((item) => item.summary).slice(-2).join(" · ")
    : "No staged intervention recorded.";
  const card = document.createElement("article");
  card.className = "archive-card";
  card.dataset.archiveMonth = String(monthResult.month || 0);
  card.innerHTML = `
    <div class="archive-title">${monthLabel(monthResult.month || "-")}</div>
    <div class="archive-meta">${t("chart.transactions")} ${monthResult.transactions ?? 0} · ${t("chart.failed_negotiations")} ${monthResult.failed_negotiations ?? 0}</div>
    <div class="archive-meta">${getLang() === "en" ? "Activation" : "激活"} ${appState.laneStats.activation} · ${getLang() === "en" ? "Success" : "成功"} ${appState.laneStats.success} · ${getLang() === "en" ? "Failure" : "失败"} ${appState.laneStats.failure} · ${getLang() === "en" ? "System" : "系统"} ${appState.laneStats.system}</div>
    <div class="archive-meta">DP ${controls.down_payment_ratio ?? "-"} · Rate ${controls.annual_interest_rate ?? "-"} · DTI ${controls.max_dti_ratio ?? "-"} · Pulse ${translateBool(controls.market_pulse_enabled)} · Macro ${controls.macro_override_mode || t("common.default")} · Quote ${translateBool(controls.negotiation_quote_stream_enabled)} · Filter ${controls.negotiation_quote_filter_mode || t("common.all")}</div>
    <div class="archive-meta">${getLang() === "en" ? "Preset" : "场景"} ${latestPreset?.payload?.preset || t("common.none")}</div>
    <div class="archive-highlight"><strong>${getLang() === "en" ? "Policy Track" : "政策轨迹"}:</strong> ${interventionHotline}</div>
    <div class="archive-highlight"><strong>${getLang() === "en" ? "Negotiation Arc" : "谈判剧情"}:</strong> ${negotiationStory}</div>
    <div class="archive-highlight"><strong>${getLang() === "en" ? "Top Agent" : "关键 Agent"}:</strong> ${topAgent ? `${topAgent.name} · ${getLang() === "en" ? "Deals" : "成交"} ${topAgent.deals ?? 0}` : t("common.none")}</div>
    <div class="archive-highlight"><strong>${getLang() === "en" ? "Key Property" : "关键房产"}:</strong> ${keyProperty ? `#${keyProperty.property_id ?? keyProperty.propertyId} · ${getLang() === "en" ? "Attempts" : "尝试"} ${keyProperty.attempts ?? 0}` : t("common.none")}</div>
    <div class="archive-highlight"><strong>${getLang() === "en" ? "Failure Hotspot" : "失败热点"}:</strong> ${failureHotspot ? `${failureHotspot.reason} · ${failureHotspot.count}` : t("common.none")}</div>
  `;
  card.addEventListener("click", () => {
    highlightArchiveMonth(monthResult.month || 0);
  });
  archiveList.prepend(card);
  archiveCount.textContent = String(Number(archiveCount.textContent || "0") + 1);

  const activations = Number(appState.laneStats.activation || 0);
  const transactions = Number(monthResult.transactions || 0);
  const avgTransactionPrice = Number(monthResult.avg_transaction_price || 0);
  const failedNegotiations = Number(monthResult.failed_negotiations || 0);
  const successRate = activations > 0 ? Number(((transactions / activations) * 100).toFixed(1)) : 0;
  appState.chartHistory = [
    ...appState.chartHistory,
    {
      month: monthResult.month || 0,
      transactions,
      avgTransactionPrice,
      activations,
      failedNegotiations,
      successRate,
    },
  ].slice(-12);
  renderChart();
}

export function resetLanes() {
  generatedAgentList.innerHTML = "";
  generatedPropertyList.innerHTML = "";
  listedPropertyList.innerHTML = "";
  matchAttemptList.innerHTML = "";
  negotiationList.innerHTML = "";
  activationList.innerHTML = "";
  successList.innerHTML = "";
  failureList.innerHTML = "";
  systemList.innerHTML = "";
  generatedAgentCount.textContent = "0";
  generatedPropertyCount.textContent = "0";
  listedPropertyCount.textContent = "0";
  matchAttemptCount.textContent = "0";
  negotiationCount.textContent = "0";
  activationCount.textContent = "0";
  successCount.textContent = "0";
  failureCount.textContent = "0";
  systemCount.textContent = "0";
  renderBulletin({ month: "-", bulletin_excerpt: "" });
  resetLaneStats();
  if (exchangeOverlay) {
    exchangeOverlay.innerHTML = "";
  }
  applyLaneEmptyStates();
}

export function clearArchive() {
  archiveList.innerHTML = "";
  archiveCount.textContent = "0";
  appState.archivePayloads = [];
  appState.chartHistory = [];
  renderChart();
}

function refreshEventNodeCopy(node) {
  const eventType = node.dataset.eventTypeHint;
  const payload = JSON.parse(node.dataset.eventPayloadHint || "{}");
  const month = Number(node.dataset.eventMonthHint || 0);
  const phase = node.dataset.eventPhaseHint || "system";
  if (!eventType) {
    return;
  }
  const event = { event_type: eventType, payload, month, phase };
  const summary = summarizeEvent(event);
  const typeEl = node.querySelector(".event-type");
  const metaEl = node.querySelector(".event-meta");
  const titleEl = node.querySelector(".event-title");
  const subtitleEl = node.querySelector(".event-subtitle");
  const tagsEl = node.querySelector(".event-tags");
  const pre = node.querySelector(".event-payload");
  if (typeEl) {
    typeEl.textContent = eventTypeLabel(eventType);
  }
  if (metaEl) {
    metaEl.textContent = `${monthShort(month)} · ${translatePhase(phase)}`;
  }
  if (titleEl) {
    titleEl.textContent = summary.title;
  }
  if (subtitleEl) {
    subtitleEl.textContent = summary.subtitle;
  }
  if (tagsEl) {
    tagsEl.innerHTML = "";
    for (const tag of summary.tags) {
      const chip = document.createElement("span");
      chip.className = `event-tag${tag.tone ? ` ${tag.tone}` : ""}`;
      chip.textContent = tag.text;
      tagsEl.appendChild(chip);
    }
  }
  if (pre) {
    pre.textContent = summary.raw;
  }
}

export function refreshLocalizedUi() {
  if (appState.lastStatus) {
    renderStatus(appState.lastStatus);
  }
  if (appState.lastMonthPayload) {
    renderSummary(appState.lastMonthPayload);
  }
  if (appState.lastFinalReviewStatus || appState.lastFinalSummary) {
    renderFinalReview(appState.lastFinalReviewStatus, appState.lastFinalSummary);
  }
  if (appState.screenProgressEntries.length > 0) {
    const entries = [...appState.screenProgressEntries];
    appState.screenProgressEntries = [];
    for (const entry of entries) {
      renderRunProgress({
        event_id: entry.id,
        month: entry.month,
        ts: entry.ts,
        payload: {
          message: entry.message,
          stage: entry.phase,
          detail: { phase: entry.phase },
        },
      });
    }
  } else {
    resetRunProgressFeed();
  }
  for (const node of document.querySelectorAll(".event-item[data-event-type-hint]")) {
    refreshEventNodeCopy(node);
  }
}
