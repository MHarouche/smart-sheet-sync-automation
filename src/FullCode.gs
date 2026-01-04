/**
 * Automated Record Transfer & Cleanup System
 * 
 * A two-job automation that transfers records between Google Sheets based on
 * status changes and performs intelligent cleanup with timeout protection.
 * 
 * Features:
 * - Dual-job architecture (sync + cleanup)
 * - Multi-pass cleanup with state persistence
 * - Edit conflict prevention (60-second window)
 * - Automated email notifications
 * - Soft lock concurrency control
 * - Business rule validation
 * 
 * Author: Mariana Harouche
 * License: MIT

/* ====================================================================== */
/* ============================== CONFIG (CFG) =========================== */
/* ====================================================================== */

const CFG = {
  TZ: 'America/xxx',

  // SOURCE spreadsheet + tab
  SOURCE_SPREADSHEET_ID: 'YOUR_SOURCE_SHEET_ID',
  SOURCE_SHEET_NAME: 'YOUR_SOURCE_TAB_NAME',

  // TARGET spreadsheet + tabs (two destinations)
  TARGET_SPREADSHEET_ID: 'YOUR_TARGET_SHEET_ID',
  TARGET_TAB_A_NAME: 'YOUR_TARGET_TAB_A', // e.g., "xxx"
  TARGET_TAB_B_NAME: 'YOUR_TARGET_TAB_B', // e.g., "xxx"

  // Headers (SOURCE)
  HDR_CITY: 'xxx',
  HDR_STATUS: 'Status',
  HDR_TYPE: 'Type',
  HDR_REVIEW: 'xxxxx',

  // Destination headers (TARGET Tab B) for optional defaults
  DEST_HDR_xx_AMOUNT: 'xxxx',
  DEST_HDR_xxxx_xxxx: 'xxxxx',
  DEST_HDR_xxxx_BALANCE: 'Refund / Balance',

  // Defaults to set on TARGET Tab B when amount is blank
  DEFAULT_PROPERTY_SETTLED: 'Yes',
  DEFAULT_REFUND_BALANCE: 'No Balance/Refund',

  // Log recipients + subject prefixes (set to your real recipients)
  LOG_RECIPIENTS: ['email1@example.com', 'email2@example.com'],
  LOG_SUBJECT_SYNC_PREFIX: 'LOG - Dropped Sync (SYNC)',
  LOG_SUBJECT_CLEANUP_PREFIX: 'LOG - Dropped Sync (CLEANUP)',

  // Lock behavior (soft lock = best effort; proceeds even if lock not acquired)
  SOFT_LOCK_WAIT_MS: 15000,
  SOFT_LOCK_STEP_MS: 3000,

  // Cleanup tuning
  CLEANUP_CHUNK_SIZE: 75,
  CLEANUP_MAX_RUNTIME_MS: 4 * 60 * 1000, // per pass runtime cap
  CLEANUP_MAX_PASSES: 5,                 // total passes/day (via triggers)
  RECENT_EDIT_WINDOW_MS: 60 * 1000,      // do not delete if edited in last 60s
  RECENT_EDITS_TTL_MS: 24 * 60 * 60 * 1000,
  JOB2_NOTE_LIMIT: 250,

  // Properties keys (state/queue)
  PROP_DELETE_QUEUE_KEY: 'PUB_DELETE_QUEUE_KEYS_v1',
  PROP_CLEANUP_STATE_KEY: 'PUB_CLEANUP_STATE_v1',
  PROP_RECENT_EDITS_KEY: 'PUB_RECENT_EDITS_KEYMAP_v1',

  // Type routing (edit if your Type values differ)
  TYPE_VALUE_FOR_TAB_B: 'xxxxxxx'
};

/* ====================================================================== */
/* =============================== JOB 1 (SYNC) ========================== */
/* ====================================================================== */

function syncAppendDroppedRows() {
  const lock = LockService.getScriptLock();
  const lockInfo = trySoftLock_(lock, CFG.SOFT_LOCK_WAIT_MS, CFG.SOFT_LOCK_STEP_MS, 'Job 1');

  const notes = [];
  if (!lockInfo.locked) {
    notes.push({ cityPO: '', note: `Warning: could not acquire script lock within ${Math.round(CFG.SOFT_LOCK_WAIT_MS / 1000)}s. Proceeding best-effort.` });
  }

  try {
    const srcSS = SpreadsheetApp.openById(CFG.SOURCE_SPREADSHEET_ID);
    const srcSheet = srcSS.getSheetByName(CFG.SOURCE_SHEET_NAME);
    if (!srcSheet) throw new Error(`Source tab not found: "${CFG.SOURCE_SHEET_NAME}"`);

    const lastRow = srcSheet.getLastRow();
    const lastCol = srcSheet.getLastColumn();

    if (lastRow < 2) {
      setDeleteQueue_([]);
      clearCleanupState_();
      sendLogEmail_Sync_([], [], [], null, notes.concat([{ cityPO: '', note: 'No data to process (headers only). Queue cleared and cleanup state reset.' }]));
      return;
    }

    const values = srcSheet.getRange(1, 1, lastRow, lastCol).getValues();
    const headers = values[0].map(asText_);

    const idxCity = findIndex_(headers, CFG.HDR_CITY_PO, 0);
    const idxStatus = findIndex_(headers, CFG.HDR_STATUS, 9);
    const idxType = findIndex_(headers, CFG.HDR_TYPE, 12);
    const idxAutoPay = findIndex_(headers, CFG.HDR_AUTOPAY_REVIEW, -1);

    // Strict adjacency pairs: [Payment set in ...] immediately-left of [Mon-YYYY]
    const paymentPairs = buildPaymentPairsStrict_(headers);

    // AutoPay boundary: "after current month" means >= first day of next month
    const firstDayThisMonth = new Date();
    firstDayThisMonth.setDate(1);
    firstDayThisMonth.setHours(0, 0, 0, 0);
    const firstDayNextMonth = new Date(firstDayThisMonth);
    firstDayNextMonth.setMonth(firstDayNextMonth.getMonth() + 1);

    // Targets
    const tgtSS = SpreadsheetApp.openById(CFG.TARGET_SPREADSHEET_ID);
    const tabA = tgtSS.getSheetByName(CFG.TARGET_TAB_A_NAME);
    const tabB = tgtSS.getSheetByName(CFG.TARGET_TAB_B_NAME);
    if (!tabA || !tabB) throw new Error('Target tabs not found. Check CFG.TARGET_TAB_A_NAME / CFG.TARGET_TAB_B_NAME.');

    const existingA = getExistingKeysColA_(tabA);
    const existingB = getExistingKeysColA_(tabB);

    const addedA = [];
    const addedB = [];
    const exceptions = [];
    const deleteQueue = [];

    let droppedEvaluated = 0;

    for (let r = 1; r < values.length; r++) {
      const row = values[r];

      const key = asText_(row[idxCity]);
      const status = normalizeStatus_(row[idxStatus]);
      const type = normalizeType_(row[idxType]);

      if (!key) continue;
      if (status !== 'dropped') continue;

      droppedEvaluated++;

      const reasons = [];

      // Rule A: Payment set in blank AND paired Month-Year has value (strict adjacency)
      if (shouldSkipByPaymentSetInRule_(row, paymentPairs)) {
        reasons.push('Payment set in is blank AND the paired Month-Year column has a value');
      }

      // Rule B: review rule
      const autoPayCheck = checkAutoPayReviewRule_(row, idxAutoPay, firstDayNextMonth);
      if (!autoPayCheck.ok) reasons.push(autoPayCheck.reason);

      if (reasons.length > 0) {
        exceptions.push({ cityPO: key, note: `Not transferred: ${reasons.join(' | ')}` });
        continue;
      }

      // If already exists in targets, do not append, but DO queue for deletion
      const existsA = existingA.has(key);
      const existsB = existingB.has(key);
      if (existsA || existsB) {
        exceptions.push({
          cityPO: key,
          note: `Not appended (already exists in target). Added to deletion queue for cleanup.`
        });
        deleteQueue.push(key);
        continue;
      }

      // Route
      const goesToB = (type === CFG.TYPE_VALUE_FOR_TAB_B);
      if (goesToB) {
        addedB.push(key);
        existingB.add(key);
      } else {
        addedA.push(key);
        existingA.add(key);
      }

      deleteQueue.push(key);
    }

    // Append to Tab A
    if (addedA.length > 0) {
      tabA.getRange(tabA.getLastRow() + 1, 1, addedA.length, 1).setValues(addedA.map(k => [k]));
    }

    // Append to Tab B + apply defaults (when SD amount blank)
    if (addedB.length > 0) {
      const startRow = tabB.getLastRow() + 1;
      tabB.getRange(startRow, 1, addedB.length, 1).setValues(addedB.map(k => [k]));
      applyTabBDefaultsIfSDAmountBlank_(tabB, startRow, addedB.length);
    }

    // Persist queue + reset cleanup state for a fresh cycle
    const uniqueQueue = uniqueKeepOrder_(deleteQueue);
    setDeleteQueue_(uniqueQueue);
    clearCleanupState_();

    exceptions.unshift({
      cityPO: '',
      note: `Dropped evaluated: ${droppedEvaluated}. Added: ${addedA.length + addedB.length}. Deletion queue: ${uniqueQueue.length}.`
    });

    sendLogEmail_Sync_(addedA, addedB, uniqueQueue, null, notes.concat(exceptions));

  } catch (err) {
    sendLogEmail_Sync_([], [], [], err, [{ cityPO: '', note: 'Job 1 failed. Queue/state were not cleared to avoid losing pending deletions.' }]);
  } finally {
    if (lockInfo.locked) lock.releaseLock();
  }
}

/* ====================================================================== */
/* ============================== JOB 2 (CLEANUP) ======================== */
/* ====================================================================== */

function cleanupDroppedRowsFromSource() {
  const lock = LockService.getScriptLock();
  const lockInfo = trySoftLock_(lock, CFG.SOFT_LOCK_WAIT_MS, CFG.SOFT_LOCK_STEP_MS, 'Job 2');

  const runNotes = [];
  if (!lockInfo.locked) {
    runNotes.push({ xxxxx: '', note: `Warning: could not acquire script lock within ${Math.round(CFG.SOFT_LOCK_WAIT_MS / 1000)}s. Proceeding best-effort.` });
  }

  const startTs = Date.now();

  try {
    pruneRecentEdits_();

    const srcSS = SpreadsheetApp.openById(CFG.SOURCE_SPREADSHEET_ID);
    const srcSheet = srcSS.getSheetByName(CFG.SOURCE_SHEET_NAME);
    if (!srcSheet) throw new Error(`Source tab not found: "${CFG.SOURCE_SHEET_NAME}"`);

    const lastCol = srcSheet.getLastColumn();
    const headers = srcSheet.getRange(1, 1, 1, lastCol).getValues()[0].map(asText_);
    const idxCity = findIndex_(headers, CFG.HDR_CITY_PO, -1);
    if (idxCity < 0) throw new Error('Cleanup failed: City-PO header not found.');

    // Load/init state
    let state = getCleanupState_();
    if (!state) {
      const queue = getDeleteQueue_();
      if (queue.length === 0) return; // nothing to do

      state = {
        originalQueue: queue.slice(),
        remainingSet: queue.map(v => asText_(v).toLowerCase()).filter(Boolean),
        deletedSet: [],
        skippedRecentEdits: [],
        passSummaries: [],
        passes: 0,
        startedAt: new Date().toISOString()
      };
      setCleanupState_(state);
    }

    // Start pass
    state.passes = Number(state.passes || 0) + 1;

    const remaining = new Set(state.remainingSet || []);
    const deleted = new Set(state.deletedSet || []);
    const skippedNotes = Array.isArray(state.skippedRecentEdits) ? state.skippedRecentEdits : [];
    const passSummaries = Array.isArray(state.passSummaries) ? state.passSummaries : [];
    const recentEditsMap = getRecentEditsMap_();

    let deletedThisRun = 0;
    let skippedThisRun = 0;
    let scannedChunks = 0;

    // Scan bottom->top in chunks
    let cursorRow = srcSheet.getLastRow();

    while (cursorRow >= 2 && remaining.size > 0) {
      if (Date.now() - startTs > CFG.CLEANUP_MAX_RUNTIME_MS) break;

      const currentLast = srcSheet.getLastRow();
      cursorRow = Math.min(cursorRow, currentLast);

      const startRow = Math.max(2, cursorRow - CFG.CLEANUP_CHUNK_SIZE + 1);
      const numRows = cursorRow - startRow + 1;
      if (numRows <= 0) break;

      const block = srcSheet.getRange(startRow, idxCity + 1, numRows, 1).getValues();
      const keys = block.map(r => asText_(r[0]).toLowerCase());

      scannedChunks++;

      const rowsToDelete = [];
      const nowMs = Date.now();

      for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        if (!key) continue;
        if (!remaining.has(key)) continue;

        const lastEditMs = recentEditsMap[key];
        if (lastEditMs && (nowMs - lastEditMs) < CFG.RECENT_EDIT_WINDOW_MS) {
          skippedThisRun++;
          if (skippedNotes.length < CFG.JOB2_NOTE_LIMIT) {
            const ageSec = Math.max(0, Math.floor((nowMs - lastEditMs) / 1000));
            skippedNotes.push({
              cityPO: key.toUpperCase(),
              note: `Skipped delete: edited ${ageSec}s ago (within 60s window). Will retry next pass.`
            });
          }
          continue;
        }

        rowsToDelete.push(startRow + i);
        remaining.delete(key);
        deleted.add(key);
        deletedThisRun++;
      }

      if (rowsToDelete.length > 0) {
        rowsToDelete.sort((a, b) => b - a);
        const blocks = groupContiguousRowBlocks_(rowsToDelete);

        blocks.forEach(b => {
          const currentLast2 = srcSheet.getLastRow();
          if (b.start <= currentLast2) {
            const maxCount = currentLast2 - b.start + 1;
            srcSheet.deleteRows(b.start, Math.min(b.count, maxCount));
          }
        });
      }

      cursorRow = startRow - 1;

      // Persist frequently
      state.remainingSet = Array.from(remaining);
      state.deletedSet = Array.from(deleted);
      state.skippedRecentEdits = skippedNotes;
      setCleanupState_(state);
    }

    passSummaries.push({
      pass: state.passes,
      scannedChunks,
      deletedThisRun,
      skippedThisRun,
      remainingAfter: remaining.size,
      endedAt: new Date().toISOString()
    });

    state.passSummaries = passSummaries;
    state.remainingSet = Array.from(remaining);
    state.deletedSet = Array.from(deleted);
    state.skippedRecentEdits = skippedNotes;
    setCleanupState_(state);

    const finished = (remaining.size === 0);
    const exhausted = (state.passes >= CFG.CLEANUP_MAX_PASSES);

    // Send ONE final log when finished OR exhausted
    if (finished || exhausted) {
      const original = (state.originalQueue || []).map(asText_).filter(Boolean);
      const deletedAll = Array.from(deleted).map(k => k.toUpperCase()).sort();

      const deletedKeySet = new Set(Array.from(deleted)); // lowercase keys
      const notFound = original
        .filter(v => !deletedKeySet.has(asText_(v).toLowerCase()))
        .sort();

      const summaryNotes = []
        .concat(runNotes)
        .concat([{
          cityPO: '',
          note: `Cleanup final: ${finished ? 'COMPLETED' : 'STOPPED (max passes reached)'} | Passes used: ${state.passes}/${CFG.CLEANUP_MAX_PASSES} | Original queue: ${original.length} | Deleted total: ${deletedAll.length} | Not found: ${notFound.length}`
        }])
        .concat(passSummaries.map(s => ({
          cityPO: '',
          note: `Pass #${s.pass}: chunks=${s.scannedChunks}, deleted=${s.deletedThisRun}, skippedRecentEdits=${s.skippedThisRun}, remainingAfter=${s.remainingAfter}`
        })))
        .concat(skippedNotes.slice(0, CFG.JOB2_NOTE_LIMIT));

      sendLogEmail_Cleanup_(deletedAll, notFound, null, summaryNotes);

      // End cycle
      setDeleteQueue_([]);
      clearCleanupState_();
      return;
    }

    // Partial: no email; next trigger continues.
    Logger.log(`Cleanup pass #${state.passes} finished (partial). Remaining: ${remaining.size}.`);

  } catch (err) {
    const state = getCleanupState_();
    const passes = state ? Number(state.passes || 0) : 0;

    if (passes >= CFG.CLEANUP_MAX_PASSES) {
      sendLogEmail_Cleanup_([], [], err, runNotes.concat([{ cityPO: '', note: 'Cleanup ended with an error at max passes. Queue/state cleared to avoid endless cycle.' }]));
      setDeleteQueue_([]);
      clearCleanupState_();
      return;
    }

    Logger.log(`Cleanup error (pass ${passes}). Will retry next trigger. Error: ${err && err.message ? err.message : err}`);

  } finally {
    if (lockInfo.locked) lock.releaseLock();
  }
}

/* ====================================================================== */
/* =============================== onEdit Tracker ======================== */
/* ====================================================================== */

function onEdit(e) {
  try {
    if (!e || !e.range) return;
    const sheet = e.range.getSheet();
    if (!sheet) return;

    if (sheet.getName() !== CFG.SOURCE_SHEET_NAME) return;
    if (sheet.getParent().getId() !== CFG.SOURCE_SPREADSHEET_ID) return;

    const row = e.range.getRow();
    if (row < 2) return;

    const lastCol = sheet.getLastColumn();
    const headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(asText_);
    const idxCity = findIndex_(headers, CFG.HDR_CITY_PO, -1);
    if (idxCity < 0) return;

    const key = asText_(sheet.getRange(row, idxCity + 1).getValue()).toLowerCase();
    if (!key) return;

    const map = getRecentEditsMap_();
    map[key] = Date.now();
    PropertiesService.getScriptProperties().setProperty(CFG.PROP_RECENT_EDITS_KEY, JSON.stringify(map));

  } catch (err) {
    Logger.log(`onEdit tracker error: ${err && err.message ? err.message : err}`);
  }
}

/* ====================================================================== */
/* ============================= RULES / HELPERS ========================= */
/* ====================================================================== */

function asText_(v) { return v == null ? '' : String(v).trim(); }
function normalizeStatus_(v) { return asText_(v).toLowerCase(); }
function normalizeType_(v) {
  const c = asText_(v).toLowerCase().replace(/\s+/g, '');
  return c.replace(/[^\w]/g, '') === 'reloapp' ? 'relo/app' : asText_(v).toLowerCase();
}
function findIndex_(headers, name, fallback) {
  const n = String(name || '').toLowerCase();
  const i = headers.findIndex(h => String(h || '').toLowerCase() === n);
  return i >= 0 ? i : fallback;
}
function getExistingKeysColA_(sheet) {
  const last = sheet.getLastRow();
  if (last < 2) return new Set();
  const vals = sheet.getRange(2, 1, last - 1, 1).getValues();
  const set = new Set();
  vals.forEach(r => { const k = asText_(r[0]); if (k) set.add(k); });
  return set;
}

/* ---------- Rule A: Payment set in vs Month-Year (STRICT ADJACENCY) ---------- */

function isMonthYearHeader_(h) {
  return /^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[\s\-\/]\d{4}$/i.test(String(h || '').trim());
}

/**
 * Builds strict adjacency pairs:
 * If headers[i] is Month-Year and headers[i-1] contains "payment set in", pair them as (idxSet=i-1, idxAmount=i)
 */
function buildPaymentPairsStrict_(headers) {
  const pairs = [];
  const lower = headers.map(h => String(h || '').toLowerCase());
  for (let i = 0; i < headers.length; i++) {
    if (!isMonthYearHeader_(headers[i])) continue;
    if (i > 0 && lower[i - 1].includes('payment set in')) {
      pairs.push({ idxSet: i - 1, idxAmount: i });
    }
  }
  return pairs;
}

/**
 * Skip if: Payment set in cell is BLANK AND Month-Year cell has a value (AND only)
 */
function shouldSkipByPaymentSetInRule_(row, pairs) {
  if (!pairs || pairs.length === 0) return false;
  return pairs.some(p => {
    const setVal = asText_(row[p.idxSet]);       // BLANK only
    const monthVal = asText_(row[p.idxAmount]);  // has value
    return (!setVal) && !!monthVal;
  });
}

/* ---------- Rule B: AutoPay review rule ---------- */

function parseDate_(v) {
  if (v instanceof Date && !isNaN(v.getTime())) return v;
  const s = asText_(v);
  if (!s) return null;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

/**
 * Skip if:
 * - value is "NED"
 * - OR value is a date >= first day of next month (i.e., after current month)
 */
function checkAutoPayReviewRule_(row, idxAutoPay, firstDayNextMonth) {
  if (idxAutoPay < 0) return { ok: true };

  const raw = asText_(row[idxAutoPay]);
  if (!raw) return { ok: true };

  if (raw.toUpperCase() === 'NED') {
    return { ok: false, reason: 'AutoPay review value is NED' };
  }

  const d = parseDate_(raw);
  if (!d) return { ok: true };

  if (d >= firstDayNextMonth) {
    return { ok: false, reason: `AutoPay review date is after the current month ("${raw}")` };
  }

  return { ok: true };
}

/* ---------- Optional: Tab B defaults when SD amount blank ---------- */

function applyTabBDefaultsIfSDAmountBlank_(tabB, startRow, numRows) {
  const lastCol = tabB.getLastColumn();
  const destHeaders = tabB.getRange(1, 1, 1, lastCol).getValues()[0].map(asText_);

  const idxSd = findIndex_(destHeaders, CFG.DEST_HDR_SD_AMOUNT, -1);
  const idxSettled = findIndex_(destHeaders, CFG.DEST_HDR_PROPERTY_SETTLED, -1);
  const idxRefund = findIndex_(destHeaders, CFG.DEST_HDR_REFUND_BALANCE, -1);

  if (idxSd < 0 || idxSettled < 0 || idxRefund < 0) return;

  const sdVals = tabB.getRange(startRow, idxSd + 1, numRows, 1).getValues();
  const settledVals = tabB.getRange(startRow, idxSettled + 1, numRows, 1).getValues();
  const refundVals = tabB.getRange(startRow, idxRefund + 1, numRows, 1).getValues();

  const newSettled = [];
  const newRefund = [];

  for (let i = 0; i < numRows; i++) {
    const sd = asText_(sdVals[i][0]);
    const currentSettled = asText_(settledVals[i][0]);
    const currentRefund = asText_(refundVals[i][0]);

    if (!sd) {
      newSettled.push([currentSettled ? currentSettled : CFG.DEFAULT_PROPERTY_SETTLED]);
      newRefund.push([currentRefund ? currentRefund : CFG.DEFAULT_REFUND_BALANCE]);
    } else {
      newSettled.push([currentSettled]);
      newRefund.push([currentRefund]);
    }
  }

  tabB.getRange(startRow, idxSettled + 1, numRows, 1).setValues(newSettled);
  tabB.getRange(startRow, idxRefund + 1, numRows, 1).setValues(newRefund);
}

/* ====================================================================== */
/* ============================ QUEUE + STATE ============================ */
/* ====================================================================== */

function uniqueKeepOrder_(arr) {
  const out = [];
  const seen = new Set();
  (arr || []).forEach(v => {
    const s = asText_(v);
    const k = s.toLowerCase();
    if (!s) return;
    if (seen.has(k)) return;
    seen.add(k);
    out.push(s);
  });
  return out;
}
function setDeleteQueue_(keys) {
  const unique = uniqueKeepOrder_(keys || []);
  PropertiesService.getScriptProperties().setProperty(CFG.PROP_DELETE_QUEUE_KEY, JSON.stringify(unique));
}
function getDeleteQueue_() {
  const raw = PropertiesService.getScriptProperties().getProperty(CFG.PROP_DELETE_QUEUE_KEY);
  try {
    const arr = JSON.parse(raw || '[]');
    return Array.isArray(arr) ? arr.map(asText_).filter(Boolean) : [];
  } catch (e) {
    return [];
  }
}
function getCleanupState_() {
  const raw = PropertiesService.getScriptProperties().getProperty(CFG.PROP_CLEANUP_STATE_KEY);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (e) { return null; }
}
function setCleanupState_(state) {
  PropertiesService.getScriptProperties().setProperty(CFG.PROP_CLEANUP_STATE_KEY, JSON.stringify(state));
}
function clearCleanupState_() {
  PropertiesService.getScriptProperties().deleteProperty(CFG.PROP_CLEANUP_STATE_KEY);
}

/* ====================================================================== */
/* ============================ RECENT EDITS MAP ========================== */
/* ====================================================================== */

function getRecentEditsMap_() {
  const raw = PropertiesService.getScriptProperties().getProperty(CFG.PROP_RECENT_EDITS_KEY);
  if (!raw) return {};
  try {
    const obj = JSON.parse(raw);
    return (obj && typeof obj === 'object') ? obj : {};
  } catch (e) {
    return {};
  }
}
function pruneRecentEdits_() {
  const map = getRecentEditsMap_();
  const now = Date.now();
  let changed = false;

  Object.keys(map).forEach(k => {
    const ts = Number(map[k] || 0);
    if (!ts || (now - ts) > CFG.RECENT_EDITS_TTL_MS) {
      delete map[k];
      changed = true;
    }
  });

  if (changed) {
    PropertiesService.getScriptProperties().setProperty(CFG.PROP_RECENT_EDITS_KEY, JSON.stringify(map));
  }
}

/* ====================================================================== */
/* ============================== LOCK + DELETE HELPERS ================== */
/* ====================================================================== */

function trySoftLock_(lock, waitMs, stepMs, label) {
  const start = Date.now();
  while (Date.now() - start < waitMs) {
    if (lock.tryLock(30000)) return { locked: true, label };
    Utilities.sleep(stepMs);
  }
  return { locked: false, label };
}

function groupContiguousRowBlocks_(rowsDescSorted) {
  const blocks = [];
  let start = rowsDescSorted[0];
  let prev = rowsDescSorted[0];
  let count = 1;

  for (let i = 1; i < rowsDescSorted.length; i++) {
    const r = rowsDescSorted[i];
    if (r === prev - 1) {
      count++;
      prev = r;
      start = r;
    } else {
      blocks.push({ start, count });
      start = r;
      prev = r;
      count = 1;
    }
  }
  blocks.push({ start, count });
  return blocks;
}

/* ====================================================================== */
/* ================================= LOG EMAILS ========================== */
/* ====================================================================== */

function sendLogEmail_Sync_(addedA, addedB, deleteQueue, errorObj, notes) {
  addedA = Array.isArray(addedA) ? addedA : [];
  addedB = Array.isArray(addedB) ? addedB : [];
  deleteQueue = Array.isArray(deleteQueue) ? deleteQueue : [];
  notes = Array.isArray(notes) ? notes : [];

  const dateStr = Utilities.formatDate(new Date(), CFG.TZ, 'yyyy-MM-dd');
  const subject = `${CFG.LOG_SUBJECT_SYNC_PREFIX} | ${dateStr} | TabA:${addedA.length} | TabB:${addedB.length} | Queue:${deleteQueue.length}`;

  const htmlBody = buildLogHtml_({
    title: 'Daily Sync Log — Job 1 (Append)',
    dateStr,
    sections: [
      { title: 'Added to Target Tab A', items: addedA },
      { title: 'Added to Target Tab B', items: addedB },
      { title: 'Deletion Queue (to be removed by Job 2)', items: deleteQueue }
    ],
    notes,
    errorObj
  });

  GmailApp.sendEmail(CFG.LOG_RECIPIENTS.join(','), subject, '', { htmlBody });
}

function sendLogEmail_Cleanup_(deletedArr, notFoundArr, errorObj, notes) {
  deletedArr = Array.isArray(deletedArr) ? deletedArr : [];
  notFoundArr = Array.isArray(notFoundArr) ? notFoundArr : [];
  notes = Array.isArray(notes) ? notes : [];

  const dateStr = Utilities.formatDate(new Date(), CFG.TZ, 'yyyy-MM-dd');
  const subject = `${CFG.LOG_SUBJECT_CLEANUP_PREFIX} | ${dateStr} | Deleted:${deletedArr.length} | NotFound:${notFoundArr.length}`;

  const htmlBody = buildLogHtml_({
    title: 'Cleanup Log — Job 2 (Delete from Source)',
    dateStr,
    sections: [
      { title: 'Deleted from Source', items: deletedArr },
      { title: 'Not found in Source at delete time', items: notFoundArr }
    ],
    notes,
    errorObj
  });

  GmailApp.sendEmail(CFG.LOG_RECIPIENTS.join(','), subject, '', { htmlBody });
}

function buildLogHtml_({ title, dateStr, sections, notes, errorObj }) {
  const tableStyle = "width:100%;border-collapse:collapse;margin:6px 0 14px;font-size:13px;";
  const thStyle = "background:#007F8C;color:#fff;text-align:left;padding:8px 10px;border:1px solid #e0e0e0;";
  const tdStyle = "padding:8px 10px;border:1px solid #e0e0e0;color:#111;";
  const wrapStyle = "border:1px solid #e0e0e0;border-radius:8px;padding:12px;";

  const sectionTable = (t, items) => {
    items = Array.isArray(items) ? items : [];
    const total = items.length;
    const header = `<h3 style='margin:16px 0 8px;'>${t} — <span style='font-weight:600;'>${total}</span></h3>`;
    if (total === 0) return `${header}<p>None.</p>`;
    const rows = items.map((v, i) => `<tr><td style='${tdStyle}'>${i + 1}</td><td style='${tdStyle}'>${asText_(v)}</td></tr>`).join('');
    return `${header}<table style='${tableStyle}'><thead><tr><th style='${thStyle}'>#</th><th style='${thStyle}'>Key</th></tr></thead><tbody>${rows}</tbody></table>`;
  };

  const notesTable = (() => {
    notes = Array.isArray(notes) ? notes : [];
    const total = notes.length;
    const header = `<h3 style='margin:16px 0 8px;'>Exceptions / Notes — <span style='font-weight:600;'>${total}</span></h3>`;
    if (total === 0) return `${header}<p>No exceptions.</p>`;
    const rows = notes.map((e, i) => `
      <tr>
        <td style='${tdStyle}'>${i + 1}</td>
        <td style='${tdStyle}'>${asText_(e.cityPO)}</td>
        <td style='${tdStyle}'>${asText_(e.note)}</td>
      </tr>
    `).join('');
    return `${header}<table style='${tableStyle}'><thead><tr><th style='${thStyle}'>#</th><th style='${thStyle}'>Key</th><th style='${thStyle}'>Note</th></tr></thead><tbody>${rows}</tbody></table>`;
  })();

  const errorBlock = errorObj
    ? `<div style='border:1px solid #e57373;background:#ffebee;padding:12px;border-radius:8px;margin-top:8px;'>
         <strong>Error:</strong> ${asText_(errorObj && errorObj.message ? errorObj.message : errorObj)}
       </div>`
    : '';

  const sectionsHtml = (sections || []).map(s => sectionTable(s.title, s.items)).join('');

  return `
    <div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#111;">
      <div style="max-width:700px;margin:0 auto;">
        <h2 style="margin:0 0 8px;">${title}</h2>
        <p>Date: <strong>${dateStr} (${CFG.TZ})</strong></p>
        <div style="${wrapStyle}">
          ${sectionsHtml}
          ${notesTable}
        </div>
        ${errorBlock}
      </div>
    </div>
  `;
}

/* ====================================================================== */
/* =============================== TRIGGERS ============================== */
/* ====================================================================== */

/**
 * Install DAILY trigger for Job 1 (sync). Run once manually.
 * Example: 00:00 local time.
 */
function installDailyTrigger_Sync() {
  deleteTriggersByHandler_('syncAppendDroppedRows');
  ScriptApp.newTrigger('syncAppendDroppedRows')
    .timeBased()
    .everyDays(1)
    .atHour(0)
    .inTimezone(CFG.TZ)
    .create();
}

/**
 * Install DAILY triggers for Job 2 (cleanup) — up to 5 attempts, 30 minutes apart.
 * This avoids the cleanup function creating/disabling triggers at runtime.
 * Example schedule: 5:00, 5:30, 6:00, 6:30, 7:00.
 */
function installDailyTriggers_Cleanup_5Passes() {
  deleteTriggersByHandler_('cleanupDroppedRowsFromSource');

  ScriptApp.newTrigger('cleanupDroppedRowsFromSource').timeBased().everyDays(1).atHour(5).nearMinute(0).inTimezone(CFG.TZ).create();
  ScriptApp.newTrigger('cleanupDroppedRowsFromSource').timeBased().everyDays(1).atHour(5).nearMinute(30).inTimezone(CFG.TZ).create();
  ScriptApp.newTrigger('cleanupDroppedRowsFromSource').timeBased().everyDays(1).atHour(6).nearMinute(0).inTimezone(CFG.TZ).create();
  ScriptApp.newTrigger('cleanupDroppedRowsFromSource').timeBased().everyDays(1).atHour(6).nearMinute(30).inTimezone(CFG.TZ).create();
  ScriptApp.newTrigger('cleanupDroppedRowsFromSource').timeBased().everyDays(1).atHour(7).nearMinute(0).inTimezone(CFG.TZ).create();
}

function deleteTriggersByHandler_(handlerName) {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction && t.getHandlerFunction() === handlerName) {
      ScriptApp.deleteTrigger(t);
    }
  });
}
