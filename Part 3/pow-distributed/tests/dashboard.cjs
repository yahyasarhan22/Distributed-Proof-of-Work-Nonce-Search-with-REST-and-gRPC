// Optional browser verification. Runtime has no Node dependency.
// PLAYWRIGHT_MODULE may point to an existing playwright package directory.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const http = require('node:http');
let modulePath = process.env.PLAYWRIGHT_MODULE || 'playwright';
const { chromium } = require(modulePath);
const base = process.env.POW_URL || 'http://127.0.0.1:8080';
const captures = path.resolve(__dirname, '../docs/screenshots');
fs.mkdirSync(captures, { recursive: true });

(async () => {
  // A transparent local test proxy can sever an established SSE TCP connection.
  // Browser offline emulation alone does not reliably terminate existing streams.
  const streams = new Set();
  const proxy = http.createServer((request, response) => {
    const upstream = http.request(new URL(request.url, base), { method: request.method, headers: request.headers }, (incoming) => {
      response.writeHead(incoming.statusCode, incoming.headers);
      if (request.url.endsWith('/events')) streams.add(response);
      incoming.pipe(response);
      incoming.on('error', () => response.destroy());
    });
    upstream.on('error', () => response.destroy());
    response.on('close', () => { streams.delete(response); upstream.destroy(); });
    request.pipe(upstream);
  });
  await new Promise((resolve) => proxy.listen(0, '127.0.0.1', resolve));
  const browserURL = `http://127.0.0.1:${proxy.address().port}`;
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}) });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1100 }, reducedMotion: 'reduce' });
  const page = await context.newPage();
  const errors = []; page.on('pageerror', (e) => errors.push(e.message));
  let sseResponses = 0;
  page.on('response', (response) => { if (response.url().endsWith('/events') && response.status() === 200) sseResponses++; });
  try {
    await page.goto(browserURL);
    await page.locator('#api-status.online').waitFor();
    await page.screenshot({ path: path.join(captures, 'desktop-ready.png'), fullPage: true });
    await page.locator('#difficulty').fill('3');
    await page.getByRole('button', { name: 'Start search' }).click();
    await page.locator('#job-status.found').waitFor({ timeout: 20000 });
    const nonce = await page.locator('#nonce').innerText();
    const hash = await page.locator('#hash').innerText();
    assert.equal(createHash('sha256').update(`hello${nonce}`).digest('hex'), hash);
    assert.ok(hash.startsWith('000')); assert.equal(hash.length, 64);
    await page.locator('#history tr').first().waitFor();
    await page.screenshot({ path: path.join(captures, 'desktop-found.png'), fullPage: true });

    // Inject an HTTP error at the network boundary: the real form must recover.
    await page.route('**/jobs', async (route) => {
      if (route.request().method() === 'POST') await route.fulfill({ status: 400, contentType: 'application/json', body: '{"error":"Invalid demo request"}' });
      else await route.continue();
    });
    await page.getByRole('button', { name: 'Start search' }).click();
    await page.locator('#error').filter({ hasText: 'Invalid demo request' }).waitFor();
    assert.equal(await page.locator('#submit').isEnabled(), true);
    await page.unroute('**/jobs');

    // Long-running real work lets us observe SSE, then HTTP cancellation.
    await page.locator('#data').fill('<script>dashboard safety</script>');
    await page.locator('#difficulty').fill('8');
    await page.getByRole('button', { name: 'Start search' }).click();
    await page.locator('#job-status.running').waitFor();
    await page.waitForFunction(() => document.getElementById('stream-status').textContent === 'Live updates connected');
    await page.waitForFunction(() => BigInt(document.getElementById('checked').textContent.replace(/\D/g, '') || '0') > 0n);
    assert.equal(await page.locator('#job-data').innerText(), '<script>dashboard safety</script>');
    assert.ok(sseResponses >= 1, 'native EventSource must connect');
    await page.waitForFunction(() => [...document.querySelectorAll('.worker-task')].some((n) => n.textContent.includes('Searching')));
    await page.screenshot({ path: path.join(captures, 'desktop-running.png'), fullPage: true });

    // Force a live SSE interruption and verify a useful, recoverable state.
    for (const stream of streams) stream.destroy();
    await page.waitForFunction(() => document.getElementById('stream-status').textContent.includes('interrupted'));
    await page.waitForFunction(() => document.getElementById('stream-status').textContent === 'Live updates connected', null, { timeout: 12000 });
    await page.getByRole('button', { name: 'Cancel search', exact: true }).click();
    await page.locator('#job-status.cancelled').waitFor();
    assert.equal(await page.locator('#cancel').isVisible(), false);
    assert.equal(await page.locator('#solution').isVisible(), false);
    await page.waitForFunction(() => document.getElementById('stream-status').textContent.includes('Final result'));

    await page.setViewportSize({ width: 390, height: 844 });
    await page.screenshot({ path: path.join(captures, 'mobile-cancelled.png'), fullPage: true });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'no page-level horizontal overflow');
    assert.equal(await page.locator('#hash').evaluate((n) => getComputedStyle(n).overflowWrap), 'anywhere');
    assert.equal(await page.locator('h1').evaluate((n) => getComputedStyle(n).animationName), 'none', 'reduced motion');

    // Completed jobs reopen without keeping an EventSource retry loop alive.
    const foundRow = page.locator('#history tr').filter({ has: page.locator('.badge.found') }).first();
    await foundRow.getByRole('button').click();
    await page.locator('#job-status.found').waitFor();
    assert.equal(await page.locator('#hash').innerText(), hash);
    assert.deepEqual(errors, [], 'browser errors');
    console.log('PASS: real submit/proof, SSE progress, cancellation, history, workers, HTTP error recovery, SSE reconnect, mobile overflow, reduced motion, safe input rendering.');
    console.log('Screenshots: docs/screenshots/{desktop-ready,desktop-found,desktop-running,mobile-cancelled}.png');
  } finally {
    // Leave no high-difficulty browser-created work behind, even on failure.
    const response = await context.request.get(`${base}/jobs`).catch(() => null);
    if (response?.ok()) for (const job of await response.json()) if (job.status === 'RUNNING' && job.data === '<script>dashboard safety</script>') await context.request.delete(`${base}/jobs/${job.job_id}`);
    await browser.close();
    proxy.closeAllConnections(); proxy.close();
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
