import { activationLine, avgPriceLine, failedLine, successRateLine, txBars, txLine } from "./dom.js";
import { appState } from "./state.js";

function buildLinePath(values, width, height, maxValue) {
  if (!values.length) {
    return "";
  }
  if (values.length === 1) {
    const y = height - (values[0] / Math.max(maxValue, 1)) * (height - 12) - 6;
    return `M 8 ${y} L ${width - 8} ${y}`;
  }

  return values
    .map((value, index) => {
      const x = 8 + (index * (width - 16)) / (values.length - 1);
      const y = height - (value / Math.max(maxValue, 1)) * (height - 12) - 6;
      return `${index === 0 ? "M" : "L"} ${x} ${y}`;
    })
    .join(" ");
}

export function renderChart() {
  if (!txLine || !txBars || !avgPriceLine || !activationLine || !failedLine || !successRateLine) {
    return;
  }

  if (!appState.chartHistory.length) {
    txBars.innerHTML = "";
    txLine.setAttribute("d", "");
    avgPriceLine.setAttribute("d", "");
    activationLine.setAttribute("d", "");
    failedLine.setAttribute("d", "");
    successRateLine.setAttribute("d", "");
    return;
  }

  const width = 320;
  const height = 140;
  const txValues = appState.chartHistory.map((item) => item.transactions);
  const avgPriceValues = appState.chartHistory.map((item) => item.avgTransactionPrice);
  const activationValues = appState.chartHistory.map((item) => item.activations);
  const failedValues = appState.chartHistory.map((item) => item.failedNegotiations);
  const successRateValues = appState.chartHistory.map((item) => item.successRate);
  const maxCountValue = Math.max(1, ...txValues, ...activationValues, ...failedValues, ...successRateValues);
  const maxAvgPrice = Math.max(1, ...avgPriceValues);
  const barWidth = appState.chartHistory.length > 0 ? Math.max(10, (width - 16) / appState.chartHistory.length - 6) : 10;

  txBars.innerHTML = txValues.map((value, index) => {
    const x = 8 + index * ((width - 16) / Math.max(appState.chartHistory.length, 1)) + 3;
    const barHeight = (value / maxCountValue) * (height - 18);
    const y = height - barHeight - 6;
    return `<rect class="chart-bar tx-bar" x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${barHeight.toFixed(1)}" rx="4" ry="4"></rect>`;
  }).join("");

  txLine.setAttribute("d", buildLinePath(txValues, width, height, maxCountValue));
  avgPriceLine.setAttribute("d", buildLinePath(avgPriceValues, width, height, maxAvgPrice));
  activationLine.setAttribute("d", buildLinePath(activationValues, width, height, maxCountValue));
  failedLine.setAttribute("d", buildLinePath(failedValues, width, height, maxCountValue));
  successRateLine.setAttribute("d", buildLinePath(successRateValues, width, height, maxCountValue));
}
