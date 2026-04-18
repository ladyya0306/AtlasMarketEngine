import { clearBtn, configSchemaForm, demoModeToggleBtn, developerForm, downloadParameterReportJsonBtn, downloadReportJsonBtn, incomeForm, langEnBtn, langZhBtn, negotiationDensityModeInput, negotiationQuoteFocusLimitInput, openDbObserverBtn, openParameterReportViewBtn, openReportViewBtn, populationForm, presetForm, resetSidebarWidthBtn, scenarioPresetInput, screenStageFullscreenBtn, screenStageToggleBtn, sidebarResizeHandle, startForm, stepBtn } from "./dom.js";
import { addPopulation, applyIncomeShock, applyScenarioPreset, applySchemaControls, bindConfigSchemaFilters, bindNightRunPlanEditor, bindScenarioPresetConfirmActions, bindStartModeSwitch, bindStartupWizard, downloadFinalReportJson, downloadParameterAssumptionReportJson, fetchConfigSchema, fetchControls, fetchPresets, fetchRuns, fetchStatus, injectDeveloperSupply, openDbObserverView, openFinalReportView, openParameterAssumptionReportView, refreshLocalizedApiUi, renderScenarioPresetHint, startSimulation, stepSimulation, syncSchemaEditabilityForStatus } from "./api.js";
import { renderChart } from "./chart.js";
import { applyStaticI18n, getLang, monthLabel, setLang, t } from "./i18n.js";
import { connectSocket } from "./socket.js";
import {
  addEvent,
  archiveCurrentMonth,
  clearArchive,
  hideReviewPanel,
  initializeDashboardEmptyStates,
  refreshLocalizedUi,
  renderStatus,
  renderSummary,
  renderRunProgress,
  renderFinalReview,
  resetLanes,
  resetRunProgressFeed,
  setNegotiationDensityMode,
  setNegotiationQuoteFocusLimit,
  showReviewPanel,
  showMonthBanner,
  showRunFinishedBanner,
} from "./render.js";
import { hydrateScreenStage, initScreenStage, ingestScreenEvent, refreshScreenStageHud, resetScreenStage, toggleScreenStageFullscreen, toggleScreenStageMotion } from "./screen.js";
import { appState } from "./state.js";

const NEGOTIATION_DENSITY_MODE_KEY = "vre.negotiationDensityMode";
const NEGOTIATION_QUOTE_FOCUS_LIMIT_KEY = "vre.negotiationQuoteFocusLimit";
const SIDEBAR_WIDTH_KEY = "vre.sidebarWidth";
const DEMO_MODE_KEY = "vre.demoMode";
const DEFAULT_SIDEBAR_WIDTH = 320;
const MIN_SIDEBAR_WIDTH = 264;
const MAX_SIDEBAR_WIDTH = 520;

function loadUiPreference(key, fallback) {
  try {
    const value = window.localStorage.getItem(key);
    return value == null || value === "" ? fallback : value;
  }
  catch {
    return fallback;
  }
}

function saveUiPreference(key, value) {
  try {
    window.localStorage.setItem(key, String(value));
  }
  catch {
    // Ignore storage failures and keep the UI responsive.
  }
}

function clampSidebarWidth(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return DEFAULT_SIDEBAR_WIDTH;
  }
  return Math.min(MAX_SIDEBAR_WIDTH, Math.max(MIN_SIDEBAR_WIDTH, Math.round(numeric)));
}

function applySidebarWidth(width) {
  const nextWidth = clampSidebarWidth(width);
  document.documentElement.style.setProperty("--sidebar-width", `${nextWidth}px`);
  saveUiPreference(SIDEBAR_WIDTH_KEY, nextWidth);
}

function initSidebarResize() {
  applySidebarWidth(loadUiPreference(SIDEBAR_WIDTH_KEY, DEFAULT_SIDEBAR_WIDTH));
  if (!sidebarResizeHandle) {
    return;
  }
  let dragging = false;
  const onPointerMove = (event) => {
    if (!dragging) {
      return;
    }
    applySidebarWidth(event.clientX - 24);
  };
  const stopDrag = () => {
    if (!dragging) {
      return;
    }
    dragging = false;
    document.body.classList.remove("is-resizing-sidebar");
  };
  sidebarResizeHandle.addEventListener("pointerdown", (event) => {
    dragging = true;
    document.body.classList.add("is-resizing-sidebar");
    sidebarResizeHandle.setPointerCapture?.(event.pointerId);
    onPointerMove(event);
  });
  sidebarResizeHandle.addEventListener("dblclick", () => {
    applySidebarWidth(DEFAULT_SIDEBAR_WIDTH);
  });
  window.addEventListener("pointermove", onPointerMove);
  window.addEventListener("pointerup", stopDrag);
  window.addEventListener("pointercancel", stopDrag);
}

function isDemoModeEnabled() {
  return loadUiPreference(DEMO_MODE_KEY, "false") === "true";
}

function applyDemoMode(enabled) {
  document.body.classList.toggle("demo-mode", enabled);
  saveUiPreference(DEMO_MODE_KEY, enabled ? "true" : "false");
  if (demoModeToggleBtn) {
    demoModeToggleBtn.classList.toggle("active", enabled);
    demoModeToggleBtn.textContent = enabled ? t("buttons.exit_demo_mode") : t("buttons.demo_mode");
  }
}

function handleSocketEvent(event) {
  ingestScreenEvent(event);

  if (event.event_type !== "MONTH_END") {
    addEvent(event);
  }

  if (event.event_type === "STATUS_SNAPSHOT") {
    renderStatus(event.payload.status);
    syncSchemaEditabilityForStatus(event.payload.status);
    hydrateScreenStage(event.payload.status);
  }

  if (event.event_type === "RUN_PROGRESS") {
    renderStatus(event.payload.status);
    syncSchemaEditabilityForStatus(event.payload.status);
    renderRunProgress(event);
    return;
  }

  if (event.event_type === "RUN_STARTED") {
    renderStatus(event.payload.status);
    syncSchemaEditabilityForStatus(event.payload.status);
    resetLanes();
    appState.laneMonth = null;
    resetScreenStage();
    resetRunProgressFeed();
    appState.announcedMonth = null;
    hideReviewPanel();
    fetchControls();
  }

  if (event.event_type === "RUN_FINISHED") {
    renderStatus(event.payload.status);
    syncSchemaEditabilityForStatus(event.payload.status);
    showRunFinishedBanner(event.payload.status);
    renderFinalReview(event.payload.status, event.payload.final_summary || null);
    showReviewPanel();
  }

  if (event.event_type === "MONTH_END") {
    renderStatus(event.payload.status);
    syncSchemaEditabilityForStatus(event.payload.status);
    renderSummary(event.payload);
    archiveCurrentMonth(event.payload);
    const completedMonth = event.payload.month_result?.month;
    appState.laneMonth = completedMonth ?? appState.laneMonth;
    showMonthBanner(`${monthLabel(completedMonth)} · ${getLang() === "en" ? "Complete" : "已结束"}`);
    addEvent(event);
    appState.announcedMonth = completedMonth;
    return;
  }

  if (
    typeof event.month === "number"
    && event.month > 0
    && event.event_type !== "STATUS_SNAPSHOT"
    && event.event_type !== "RUN_PROGRESS"
  ) {
    if (appState.laneMonth !== null && event.month !== appState.laneMonth) {
      resetLanes();
    }
    appState.laneMonth = event.month;
  }

  if (typeof event.month === "number" && event.month > 0 && event.month !== appState.announcedMonth) {
    showMonthBanner(`${monthLabel(event.month)} · ${getLang() === "en" ? "Live" : "进行中"}`);
    appState.announcedMonth = event.month;
  }
}

function applyLanguage(lang) {
  setLang(lang);
  applyStaticI18n();
  langZhBtn?.classList.toggle("active", getLang() === "zh");
  langEnBtn?.classList.toggle("active", getLang() === "en");
  applyDemoMode(document.body.classList.contains("demo-mode"));
  refreshLocalizedApiUi();
  refreshLocalizedUi();
  refreshScreenStageHud();
}

startForm.addEventListener("submit", startSimulation);
configSchemaForm?.addEventListener("submit", applySchemaControls);
presetForm.addEventListener("submit", applyScenarioPreset);
scenarioPresetInput.addEventListener("change", renderScenarioPresetHint);
populationForm.addEventListener("submit", addPopulation);
incomeForm.addEventListener("submit", applyIncomeShock);
developerForm.addEventListener("submit", injectDeveloperSupply);
negotiationDensityModeInput.addEventListener("change", () => {
  setNegotiationDensityMode(negotiationDensityModeInput.value);
  saveUiPreference(NEGOTIATION_DENSITY_MODE_KEY, negotiationDensityModeInput.value);
});
negotiationQuoteFocusLimitInput.addEventListener("change", () => {
  setNegotiationQuoteFocusLimit(negotiationQuoteFocusLimitInput.value);
  saveUiPreference(NEGOTIATION_QUOTE_FOCUS_LIMIT_KEY, negotiationQuoteFocusLimitInput.value);
});
stepBtn.addEventListener("click", stepSimulation);
clearBtn.addEventListener("click", () => {
  resetLanes();
  resetScreenStage();
  resetRunProgressFeed();
  clearArchive();
  hideReviewPanel();
});
screenStageToggleBtn?.addEventListener("click", toggleScreenStageMotion);
screenStageFullscreenBtn?.addEventListener("click", toggleScreenStageFullscreen);
openReportViewBtn?.addEventListener("click", openFinalReportView);
downloadReportJsonBtn?.addEventListener("click", downloadFinalReportJson);
openParameterReportViewBtn?.addEventListener("click", openParameterAssumptionReportView);
downloadParameterReportJsonBtn?.addEventListener("click", downloadParameterAssumptionReportJson);
openDbObserverBtn?.addEventListener("click", openDbObserverView);
resetSidebarWidthBtn?.addEventListener("click", () => applySidebarWidth(DEFAULT_SIDEBAR_WIDTH));
demoModeToggleBtn?.addEventListener("click", () => applyDemoMode(!document.body.classList.contains("demo-mode")));
langZhBtn?.addEventListener("click", () => applyLanguage("zh"));
langEnBtn?.addEventListener("click", () => applyLanguage("en"));

bindScenarioPresetConfirmActions();
bindNightRunPlanEditor();
bindConfigSchemaFilters();
bindStartModeSwitch();
bindStartupWizard();
applyLanguage(getLang());
applyDemoMode(isDemoModeEnabled());
initSidebarResize();
fetchStatus();
fetchControls();
fetchConfigSchema();
fetchRuns();
fetchPresets();
renderChart();
hideReviewPanel();
initializeDashboardEmptyStates();
initScreenStage();
resetRunProgressFeed();
negotiationDensityModeInput.value = loadUiPreference(NEGOTIATION_DENSITY_MODE_KEY, negotiationDensityModeInput.value);
negotiationQuoteFocusLimitInput.value = loadUiPreference(NEGOTIATION_QUOTE_FOCUS_LIMIT_KEY, negotiationQuoteFocusLimitInput.value);
setNegotiationDensityMode(negotiationDensityModeInput.value);
setNegotiationQuoteFocusLimit(negotiationQuoteFocusLimitInput.value);
connectSocket(handleSocketEvent);
