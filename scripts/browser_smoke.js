const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

function parseArgs(argv) {
  const args = {
    url: process.env.BROWSER_SMOKE_URL || "http://127.0.0.1:8012/",
    outDir: process.env.BROWSER_SMOKE_OUTDIR || path.resolve("output", "browser-smoke"),
    headless: process.env.BROWSER_SMOKE_HEADLESS !== "false",
    mode: process.env.BROWSER_SMOKE_MODE || "manual",
  };
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    if (arg === "--url" && next) {
      args.url = next;
      index += 1;
    } else if (arg === "--out-dir" && next) {
      args.outDir = path.resolve(next);
      index += 1;
    } else if (arg === "--headed") {
      args.headless = false;
    } else if (arg === "--mode" && next) {
      args.mode = next;
      index += 1;
    }
  }
  return args;
}

function findBrowserExecutable() {
  const browserPathCandidates = [
    "C:/Program Files/Microsoft/Edge/Application/msedge.exe",
    "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe",
    "C:/Program Files/Google/Chrome/Application/chrome.exe",
  ];
  return browserPathCandidates.find((item) => fs.existsSync(item));
}

function classifyErrors(errors) {
  return errors.filter((item) => {
    if (item.type === "response") {
      if (item.status === 404) {
        return false;
      }
      if (item.status === 409 && String(item.url || "").endsWith("/controls")) {
        return false;
      }
    }
    const text = String(item.text || "");
    if (text === "Failed to load resource: the server responded with a status of 404 (Not Found)") {
      return false;
    }
    if (text === "Failed to load resource: the server responded with a status of 409 (Conflict)") {
      return false;
    }
    return true;
  });
}

async function run() {
  const args = parseArgs(process.argv);
  const executablePath = findBrowserExecutable();
  if (!executablePath) {
    throw new Error("No local Chromium-based browser found");
  }

  fs.mkdirSync(args.outDir, { recursive: true });
  const browser = await chromium.launch({
    headless: args.headless,
    executablePath,
    args: ["--use-gl=angle", "--use-angle=swiftshader"],
  });

  const page = await browser.newPage({ viewport: { width: 1600, height: 1100 } });
  const errors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") {
      errors.push({ type: "console", text: msg.text() });
    }
  });
  page.on("response", (resp) => {
    if (resp.status() >= 400) {
      errors.push({ type: "response", status: resp.status(), url: resp.url() });
    }
  });
  page.on("pageerror", (err) => errors.push({ type: "pageerror", text: String(err) }));

  await page.goto(args.url, { waitUntil: "networkidle" });
  await page.waitForSelector("#screen-canvas");
  const isNightRun = args.mode === "night_run";
  await page.selectOption("#start-mode", isNightRun ? "night_run" : "new");
  await page.fill("#agent-count", "5");
  await page.fill("#tier-ultra-high-count", "1");
  await page.fill("#tier-high-count", "1");
  await page.fill("#tier-middle-count", "1");
  await page.fill("#tier-lower-middle-count", "1");
  await page.fill("#months", "1");
  await page.fill("#property-total-count", "6");
  await page.fill("#seed", "42");
  await page.click('#start-form button[type="submit"]');
  await page.waitForSelector("#startup-confirm:not(.hidden)");
  await page.click("#startup-confirm-apply");
  if (isNightRun) {
    await page.waitForFunction(
      async () => {
        const resp = await fetch("/status");
        const payload = await resp.json();
        return payload.status === "completed";
      },
      {},
      { timeout: 30000 },
    );
    await page.reload({ waitUntil: "networkidle" });
  } else {
    await page.waitForFunction(
      async () => {
        try {
          const resp = await fetch("/status");
          if (!resp.ok) {
            return false;
          }
          const payload = await resp.json();
          return payload.status === "initialized";
        } catch {
          return false;
        }
      },
      {},
      { timeout: 30000 },
    );
  }
  await page.screenshot({ path: path.join(args.outDir, "after-start.png"), fullPage: true });

  if (!isNightRun) {
    await page.click("#step-btn");
    await page.waitForFunction(
      async () => {
        try {
          const resp = await fetch("/status");
          if (!resp.ok) {
            return false;
          }
          const payload = await resp.json();
          return payload.status === "completed";
        } catch {
          return false;
        }
      },
      {},
      { timeout: 30000 },
    );
  }
  await page.waitForTimeout(1000);
  await page.screenshot({ path: path.join(args.outDir, "after-step.png"), fullPage: true });

  const state = await page.evaluate(() => ({
    status: document.getElementById("status-badge")?.textContent || "",
    currentMonth: document.getElementById("current-month")?.textContent || "",
    archiveCount: document.getElementById("archive-count")?.textContent || "",
    reviewVisible: !document.getElementById("review-panel")?.classList.contains("hidden"),
    screenMode: document.getElementById("screen-stage-mode")?.textContent || "",
    canvasText: typeof window.render_game_to_text === "function" ? window.render_game_to_text() : null,
  }));

  const unexpectedErrors = classifyErrors(errors);
  const report = {
    executablePath,
    outDir: args.outDir,
    url: args.url,
    mode: args.mode,
    state,
    errors,
    unexpectedErrors,
    ok:
      unexpectedErrors.length === 0 &&
      (state.status.includes("completed") || state.status.includes("已完成")) &&
      (
        isNightRun
          ? (state.currentMonth === "1" || state.currentMonth === 1)
          : state.reviewVisible
      ),
  };

  fs.writeFileSync(path.join(args.outDir, "state.json"), JSON.stringify(report, null, 2));
  await browser.close();
  console.log(JSON.stringify(report, null, 2));

  if (!report.ok) {
    process.exitCode = 1;
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
