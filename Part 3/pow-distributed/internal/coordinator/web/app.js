const $ = (id) => document.getElementById(id);
const terminal = (job) => job.status === 'FOUND' || job.status === 'CANCELLED';
const labels = { RUNNING: 'Running', FOUND: 'Found', CANCELLED: 'Cancelled' };
let selected = null;
let source = null;
let selectionVersion = 0;
let refreshTimer;
let pollingStopped = false;

// Preserve uint64 values beyond JavaScript's safe integer range. Matching JSON
// strings first prevents this conversion from touching user input inside strings.
function parseJSON(text) {
  return JSON.parse(text.replace(/("(?:\\.|[^"\\])*")|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g,
    (match, string, number) => !string && /^\d{16,}$/.test(number) ? JSON.stringify(number) : match));
}
function count(value) { try { return BigInt(value ?? 0).toLocaleString(); } catch { return String(value ?? 0); } }
function elapsed(value) { return `${Number(value || 0).toFixed(2)} s`; }
function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
function badge(status) { return element('span', `badge ${String(status).toLowerCase()}`, labels[status] || status); }
function showError(message = '') { $('error').textContent = message; $('error').hidden = !message; }
async function api(path, options = {}) {
  const response = await fetch(path, { ...options, signal: AbortSignal.timeout(8000) });
  let body;
  try { body = parseJSON(await response.text()); } catch { throw new Error(`Coordinator returned an unreadable response (${response.status}).`); }
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status}).`);
  return body;
}
function renderJob(job) {
  if (!selected || job.job_id !== selected.job_id) return;
  // REST polling and SSE may race: never regress a terminal state or count.
  if (terminal(selected) && !terminal(job)) return;
  if (!terminal(job) && Number(job.elapsed_seconds) < Number(selected.elapsed_seconds)) return;
  selected = job;
  $('job-empty').hidden = true; $('job-detail').hidden = false;
  $('job-status').className = `badge ${job.status.toLowerCase()}`;
  $('job-status').textContent = labels[job.status] || job.status;
  $('job-id').textContent = job.job_id;
  $('checked').textContent = count(job.nonces_checked);
  $('elapsed').textContent = elapsed(job.elapsed_seconds);
  $('job-difficulty').textContent = job.difficulty;
  $('job-data').textContent = job.data;
  const messages = { RUNNING: 'Searching for a matching hash. Jobs wait here if no workers are available.', FOUND: 'Solution verified by the coordinator. Other searches for this job have been cancelled.', CANCELLED: 'Search cancelled. No further results will be accepted for this job.' };
  $('job-message').textContent = messages[job.status] || '';
  $('cancel').hidden = terminal(job);
  $('solution').hidden = job.status !== 'FOUND';
  if (job.status === 'FOUND') { $('nonce').textContent = String(job.nonce); $('hash').textContent = job.hash; }
  if (terminal(job)) {
    source?.close(); source = null;
    $('stream-status').textContent = 'Final result · live connection closed';
  }
}
async function selectJob(id) {
  const version = ++selectionVersion;
  source?.close(); source = null;
  selected = null;
  $('cancel').disabled = true;
  $('stream-status').textContent = 'Loading selected job…';
  showError();
  try {
    const job = await api(`/jobs/${encodeURIComponent(id)}`);
    if (version !== selectionVersion) return;
    selected = job; renderJob(job); $('cancel').disabled = false;
    if (terminal(job)) return;
    const events = new EventSource(`/jobs/${encodeURIComponent(id)}/events`);
    source = events;
    const update = (event) => {
      if (version !== selectionVersion) return;
      try {
        renderJob(parseJSON(event.data));
        if (!terminal(selected)) $('stream-status').textContent = 'Live updates connected';
      } catch { $('stream-status').textContent = 'Unreadable update. Status will refresh automatically.'; }
    };
    for (const type of ['progress', 'found', 'cancelled']) events.addEventListener(type, update);
    events.onopen = () => { if (version === selectionVersion && selected && !terminal(selected)) $('stream-status').textContent = 'Live updates connected'; };
    events.onerror = () => {
      if (version !== selectionVersion || !selected || terminal(selected)) return;
      $('stream-status').textContent = 'Live connection interrupted. Reconnecting; status also refreshes automatically.';
      // Native EventSource reconnects automatically. Polling supplies a fallback.
    };
  } catch (error) {
    if (version !== selectionVersion) return;
    $('job-detail').hidden = true; $('job-empty').hidden = false;
    $('job-status').className = 'badge dead'; $('job-status').textContent = 'Could not load job';
    showError(`${error.message} Select a job from history to try again.`);
  }
}
function renderWorkers(workers) {
  const alive = workers.filter((w) => w.alive).length;
  $('worker-count').textContent = `${alive} online / ${workers.length} registered`;
  $('workers-empty').hidden = workers.length !== 0;
  const nodes = workers.map((worker) => {
    const card = element('article', 'worker-card');
    const header = element('header'); header.append(element('h3', '', worker.worker_id), badge(worker.alive ? 'alive' : 'dead'));
    header.lastChild.textContent = worker.alive ? 'Online' : 'Offline';
    card.append(header, element('p', 'mono worker-address', worker.address));
    const assignment = worker.current_assignment;
    const task = element('p', 'worker-task');
    if (assignment) {
      task.append(element('span', '', 'Searching '), element('span', 'mono', assignment.job_id.slice(0, 10)));
      task.append(document.createElement('br'), element('span', 'mono', `[${count(assignment.start)}, ${count(assignment.end)})`));
      task.title = `Job ${assignment.job_id} / assignment ${assignment.assignment_id}`;
    } else task.textContent = worker.alive ? 'Available for work' : 'Waiting for re-registration';
    const beat = new Date(worker.last_heartbeat);
    card.append(task, element('p', 'worker-beat', `Last heartbeat ${beat.toLocaleTimeString()}`));
    return card;
  });
  $('workers').replaceChildren(...nodes);
}
function renderHistory(jobs) {
  $('job-count').textContent = `${jobs.length} ${jobs.length === 1 ? 'job' : 'jobs'}`;
  $('history-empty').hidden = jobs.length !== 0; $('history-wrap').hidden = jobs.length === 0;
  $('history').replaceChildren(...[...jobs].reverse().map((job) => {
    const row = element('tr', selected?.job_id === job.job_id ? 'selected' : '');
    const first = element('td');
    const id = element('p', 'mono history-id', job.job_id.slice(0, 12)); id.title = job.job_id;
    first.append(id, element('p', 'history-data', job.data));
    const state = element('td'); state.append(badge(job.status));
    const action = element('td'); const open = element('button', 'open-job', 'Open');
    open.type = 'button'; open.setAttribute('aria-label', `Open job ${job.job_id}`);
    open.addEventListener('click', () => { selectJob(job.job_id); $('live-title').scrollIntoView({ block: 'nearest' }); }); action.append(open);
    row.append(first, state, element('td', '', String(job.difficulty)), element('td', 'mono', count(job.nonces_checked)), element('td', '', elapsed(job.elapsed_seconds)), action);
    return row;
  }));
}
async function refresh() {
  const version = selectionVersion;
  const results = await Promise.allSettled([api('/workers'), api('/jobs')]);
  if (results[0].status === 'fulfilled') renderWorkers(results[0].value);
  if (results[1].status === 'fulfilled') {
    const jobs = results[1].value;
    renderHistory(jobs);
    if (version === selectionVersion && selected) {
      const current = jobs.find((j) => j.job_id === selected.job_id);
      if (current) renderJob(current);
      else {
        source?.close(); source = null; selected = null;
        $('job-detail').hidden = true; $('job-empty').hidden = false;
        $('job-status').className = 'badge idle'; $('job-status').textContent = 'Job no longer available';
        showError('The selected job is no longer available. The coordinator may have restarted; submit a new job.');
      }
    }
  }
  const online = results.every((result) => result.status === 'fulfilled');
  $('api-status').className = `connection ${online ? 'online' : 'offline'}`;
  $('api-status').textContent = online ? 'Coordinator connected' : 'Coordinator unavailable. Retrying…';
}
$('job-form').addEventListener('submit', async (event) => {
  event.preventDefault(); if (!$('job-form').reportValidity()) return;
  const data = $('data').value; const difficulty = Number($('difficulty').value);
  if (!data || !Number.isInteger(difficulty) || difficulty < 1 || difficulty > 8) { showError('Enter data and a whole-number difficulty from 1 to 8.'); return; }
  $('submit').disabled = true; $('submit').textContent = 'Submitting…'; showError();
  try {
    const result = await api('/jobs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ data, difficulty }) });
    await selectJob(result.job_id); await refresh();
  } catch (error) { showError(`Could not start the search. ${error.message}`); }
  finally { $('submit').disabled = false; $('submit').textContent = 'Start search →'; }
});
$('cancel').addEventListener('click', async () => {
  if (!selected || terminal(selected)) return;
  const id = selected.job_id; const version = selectionVersion;
  $('cancel').disabled = true; showError();
  try {
    const job = await api(`/jobs/${encodeURIComponent(id)}`, { method: 'DELETE' });
    if (version === selectionVersion) renderJob(job);
    await refresh();
  } catch (error) {
    if (version === selectionVersion) showError(`Could not cancel the search. ${error.message}`);
    await refresh(); // A simultaneous solution can legitimately cause HTTP 409.
  } finally { if (version === selectionVersion) $('cancel').disabled = false; }
});
async function poll() { try { await refresh(); } finally { if (!pollingStopped) refreshTimer = setTimeout(poll, 2000); } }
poll();
window.addEventListener('pagehide', () => { pollingStopped = true; source?.close(); clearTimeout(refreshTimer); });
window.addEventListener('pageshow', (event) => { if (event.persisted) { pollingStopped = false; if (selected) selectJob(selected.job_id); poll(); } });
