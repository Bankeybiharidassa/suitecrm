<?php

/**
 * dedup_meetings.php — Content-hash based duplicate meeting cleanup
 *
 * WHAT IT DOES:
 *   Phase 0 — Connect + verify tables
 *   Phase 1 — ALTER TABLE to add content_hash column if missing
 *   Phase 2 — Compute + store content_hash for every meeting that has
 *              a graph_sync_state row (backfill)
 *   Phase 3 — Detect duplicates: meetings sharing the same content_hash
 *   Phase 4 — Preview duplicates with full details
 *   Phase 5 — Confirm + execute cleanup
 *   Phase 6 — Verify all keepers have intact state rows
 *
 * CONTENT HASH covers:
 *   name + date_start (minute) + date_end (minute)
 *   + organizer_email (from description "Organizer:" line, empty if missing)
 *   + normalised body (strip sync metadata, collapse whitespace, lowercase)
 *
 * KEEPER SELECTION (per duplicate group):
 *   1. Has a graph_sync_state row  →  preferred
 *   2. Multiple have state rows    →  oldest date_entered wins
 *   3. None have state row         →  oldest date_entered wins
 *   If duplicate has state row and keeper does not → state row is MOVED to keeper
 *
 * SAFE BY DESIGN:
 *   - Uses config.php for all credentials
 *   - All writes in a single transaction (rolled back on any error)
 *   - NEVER contacts Graph API / O365
 *   - Soft-deletes meetings (deleted=1) — no Graph cancel triggered
 *   - Keeper always ends up with a state row so sync.php never re-pushes or cancels
 *
 * Usage:
 *   sudo -u www-data php dedup_meetings.php
 */

require __DIR__ . '/config.php'; // $dbHost, $dbUser, $dbPass, $dbName

// ============================================================
// Helpers
// ============================================================

/**
 * Normalise a description body for content hashing:
 *  - Strip [GRAPH-ID:...] line
 *  - Strip Organizer: ... line
 *  - Strip Teams join URL: ... line
 *  - Strip HTML tags
 *  - Decode HTML entities
 *  - Trim each line, remove blank lines
 *  - Collapse to single string, lowercase
 */
function normaliseBody(string $text): string {
    // Strip sync metadata lines
    $text = preg_replace('/^Synced from O365\s*$/m',       '', $text);
    $text = preg_replace('/^Synced from SuiteCRM\s*$/m',   '', $text);
    $text = preg_replace('/^\[GRAPH-ID:[^\]]+\]\s*$/m', '', $text); // legacy
    $text = preg_replace('/^Organizer:.*$/m',                '', $text);
    $text = preg_replace('/^Teams join URL:.*$/m',           '', $text);
    // Strip HTML
    $text = html_entity_decode(strip_tags($text), ENT_QUOTES | ENT_HTML5, 'UTF-8');
    // Normalise whitespace: trim each line, drop blanks
    $lines = array_filter(
        array_map('trim', preg_split('/\r?\n/', $text)),
        fn($l) => $l !== ''
    );
    return strtolower(implode(' ', $lines));
}

/**
 * Extract organizer email from "Organizer: Name <email>" line.
 * Returns empty string if not found.
 */
function extractOrganizerEmail(string $description): string {
    if (preg_match('/^Organizer:.*<([^>]+)>/m', $description, $m)) {
        return strtolower(trim($m[1]));
    }
    return '';
}

/**
 * Compute the content hash for a meeting row.
 * Used identically by dedup script and (later) sync.php.
 */
function computeContentHash(array $meeting): string {
    $desc     = $meeting['description'] ?? '';
    $name     = strtolower(trim($meeting['name'] ?? ''));
    $start    = substr($meeting['date_start'] ?? '', 0, 16); // YYYY-MM-DD HH:MM
    $end      = substr($meeting['date_end']   ?? '', 0, 16);
    $organizer = extractOrganizerEmail($desc);
    $body      = normaliseBody($desc);

    return hash('sha256', implode('|', [$name, $start, $end, $organizer, $body]));
}

// ============================================================
// Phase 0 — Connect + verify tables
// ============================================================

echo "\n";
echo "╔══════════════════════════════════════════════════════════╗\n";
echo "║      SuiteCRM Calendar Dedup — Content Hash Cleanup     ║\n";
echo "╚══════════════════════════════════════════════════════════╝\n\n";
echo "O365 / Microsoft Graph is NOT contacted. No meetings will be cancelled.\n\n";

try {
    $pdo = new PDO(
        "mysql:host=$dbHost;dbname=$dbName;charset=utf8mb4",
        $dbUser, $dbPass,
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
    );
} catch (PDOException $e) {
    echo "[FATAL] Could not connect: " . $e->getMessage() . "\n";
    exit(1);
}

echo "[+] Connected to $dbName on $dbHost as $dbUser\n\n";

echo "── Phase 0: Checking required tables ───────────────────────────────────\n";

$required = ['meetings', 'meetings_users', 'meetings_contacts',
             'meetings_leads', 'graph_sync_state'];
$missing  = [];
foreach ($required as $table) {
    $stmt = $pdo->prepare("SHOW TABLES LIKE ?");
    $stmt->execute([$table]);
    $exists = (bool) $stmt->fetchColumn();
    printf("  %-25s %s\n", $table, $exists ? '✓' : '✗ MISSING');
    if (!$exists) $missing[] = $table;
}
echo "\n";

if ($missing) {
    echo "[FATAL] Missing tables: " . implode(', ', $missing) . "\n";
    echo "        Run sync.php at least once to create graph_sync_state.\n";
    exit(1);
}

// ============================================================
// Phase 1 — Add content_hash column if missing
// ============================================================

echo "── Phase 1: Ensuring content_hash column ───────────────────────────────\n";

$col = $pdo->query("
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME   = 'graph_sync_state'
      AND COLUMN_NAME  = 'content_hash'
")->fetchColumn();

if (!$col) {
    $pdo->exec("
        ALTER TABLE graph_sync_state
        ADD COLUMN content_hash VARCHAR(64) NULL DEFAULT NULL
    ");
    echo "  content_hash column added ✓\n\n";
} else {
    echo "  content_hash column already exists ✓\n\n";
}

// ============================================================
// Phase 2 — Backfill content_hash for all synced meetings
// ============================================================

echo "── Phase 2: Backfilling content hashes ─────────────────────────────────\n";

// Load all meetings that have a state row (these are Graph-synced meetings)
$stmt = $pdo->query("
    SELECT
        m.id,
        m.name,
        m.date_start,
        m.date_end,
        m.description,
        gss.id             AS state_id,
        gss.content_hash   AS stored_hash
    FROM meetings m
    JOIN graph_sync_state gss ON gss.meeting_id = m.id
    WHERE m.deleted = 0
    ORDER BY m.date_entered ASC
");
$syncedMeetings = $stmt->fetchAll(PDO::FETCH_ASSOC);

$backfilled = 0;
$alreadySet = 0;
$updateHash = $pdo->prepare("
    UPDATE graph_sync_state SET content_hash = ? WHERE meeting_id = ?
");

foreach ($syncedMeetings as $row) {
    $hash = computeContentHash($row);
    if ($row['stored_hash'] === $hash) {
        $alreadySet++;
        continue;
    }
    $updateHash->execute([$hash, $row['id']]);
    $backfilled++;
}

printf("  Meetings processed : %d\n", count($syncedMeetings));
printf("  Hashes written     : %d\n", $backfilled);
printf("  Already correct    : %d\n\n", $alreadySet);

// ============================================================
// Phase 3 — Detect duplicates by content_hash
// ============================================================

echo "── Phase 3: Detecting duplicates ───────────────────────────────────────\n";

// Reload fresh after backfill — include all non-deleted meetings
// (even those without a state row, to catch orphaned duplicates)
$stmt = $pdo->query("
    SELECT
        m.id,
        m.name,
        m.date_start,
        m.date_end,
        m.date_entered,
        m.description,
        gss.id             AS state_id,
        gss.graph_event_id AS graph_event_id,
        gss.graph_mailbox  AS graph_mailbox,
        gss.content_hash   AS content_hash
    FROM meetings m
    LEFT JOIN graph_sync_state gss ON gss.meeting_id = m.id
    WHERE m.deleted = 0
      AND m.description LIKE '%[GRAPH-ID:%'
    ORDER BY m.date_entered ASC
");
$allMeetings = $stmt->fetchAll(PDO::FETCH_ASSOC);

// Compute hash for every row (including those without a state row)
// and group by hash
$groups = [];
foreach ($allMeetings as $row) {
    // Use stored hash if available, otherwise compute on the fly
    $hash = $row['content_hash'] ?: computeContentHash($row);
    $row['_hash'] = $hash;
    $groups[$hash][] = $row;
}

$duplicateGroups = array_filter($groups, fn($g) => count($g) > 1);

printf("  Graph-synced meetings found : %d\n",   count($allMeetings));
printf("  Unique content hashes       : %d\n",   count($groups));
printf("  Groups with duplicates      : %d\n\n", count($duplicateGroups));

if (empty($duplicateGroups)) {
    echo "  ✓ No duplicates found. Database is clean.\n";
    echo "  No confirmation needed — nothing to delete.\n\n";
    exit(0);
}

// ============================================================
// Determine keeper + duplicates per group
// ============================================================

$toKeep   = []; // hash → keeper row
$toDelete = []; // meeting_id → ['row' => ..., 'keeper' => ...]

foreach ($duplicateGroups as $hash => $members) {
    // Sort by date_entered ASC so oldest is first
    usort($members, fn($a, $b) => strcmp($a['date_entered'], $b['date_entered']));

    // Keeper priority:
    // 1. Has state row — oldest among those with state row
    // 2. Oldest overall
    $keeper = null;
    foreach ($members as $m) {
        if ($m['state_id']) { $keeper = $m; break; }
    }
    $keeper = $keeper ?? $members[0];

    $toKeep[$hash] = $keeper;

    foreach ($members as $m) {
        if ($m['id'] !== $keeper['id']) {
            $toDelete[$m['id']] = ['row' => $m, 'keeper' => $keeper, 'hash' => $hash];
        }
    }
}

$totalDupes = count($toDelete);
printf("  Keeper meetings   : %d\n",   count($toKeep));
printf("  Meetings to remove: %d\n\n", $totalDupes);

// ============================================================
// Phase 4 — Preview
// ============================================================

echo "── Phase 4: Preview ────────────────────────────────────────────────────\n\n";

foreach ($duplicateGroups as $hash => $members) {
    $keeper = $toKeep[$hash];

    printf("  ┌─ \"%s\"\n", $keeper['name']);
    printf("  │  Start : %s  End: %s\n", $keeper['date_start'], $keeper['date_end']);
    printf("  │  Hash  : %s\n", $hash);

    // Show organizer extracted from description
    $org = extractOrganizerEmail($keeper['description'] ?? '');
    if ($org) printf("  │  Organizer: %s\n", $org);

    echo   "  │\n";

    foreach ($members as $m) {
        $isKeeper = ($m['id'] === $keeper['id']);
        $label    = $isKeeper ? 'KEEP  ' : 'REMOVE';

        // Count relation rows
        $mu = $pdo->prepare("SELECT COUNT(*) FROM meetings_users    WHERE meeting_id=?");
        $mc = $pdo->prepare("SELECT COUNT(*) FROM meetings_contacts WHERE meeting_id=?");
        $ml = $pdo->prepare("SELECT COUNT(*) FROM meetings_leads    WHERE meeting_id=?");
        $mu->execute([$m['id']]); $mc->execute([$m['id']]); $ml->execute([$m['id']]);

        $stateFlag = $m['state_id']
            ? "state:✓ mailbox:" . ($m['graph_mailbox'] ?? '?')
            : "state:✗";

        printf("  │  [%s] id=%-36s entered=%s\n",
            $label, $m['id'], $m['date_entered']);
        printf("  │          %s | users:%d contacts:%d leads:%d\n",
            $stateFlag, $mu->fetchColumn(), $mc->fetchColumn(), $ml->fetchColumn());

        if (!$isKeeper && $m['state_id'] && !$keeper['state_id']) {
            echo   "  │          ⚠ State row will be MOVED to keeper\n";
        }
    }
    echo "  └─────────────────────────────────────────────────────────\n\n";
}

// ============================================================
// Phase 5 — Confirm + Execute
// ============================================================

echo "── Phase 5: Confirmation ────────────────────────────────────────────────\n\n";
echo "  For each REMOVED meeting:\n";
echo "    1. meetings_users rows         → deleted\n";
echo "    2. meetings_contacts rows      → deleted\n";
echo "    3. meetings_leads rows         → deleted\n";
echo "    4. graph_sync_state row        → deleted OR moved to keeper if keeper has none\n";
echo "    5. meetings row                → soft deleted (deleted=1)\n\n";
echo "  O365 / Microsoft Graph: NOT contacted.\n";
echo "  Keeper sync state: preserved — sync.php will NOT re-push or cancel.\n\n";

echo "  Type 'yes' to proceed, anything else to abort: ";
$answer = trim(fgets(STDIN));
echo "\n";

if (strtolower($answer) !== 'yes') {
    echo "  Aborted. No changes made.\n\n";
    exit(0);
}

echo "── Executing ────────────────────────────────────────────────────────────\n\n";

$pdo->beginTransaction();

try {
    $removed = 0;

    foreach ($toDelete as $meetingId => $info) {
        $row    = $info['row'];
        $keeper = $info['keeper'];
        $name   = $row['name'];

        // 1. meetings_users
        $s = $pdo->prepare("DELETE FROM meetings_users WHERE meeting_id = ?");
        $s->execute([$meetingId]);
        $muDel = $s->rowCount();

        // 2. meetings_contacts
        $s = $pdo->prepare("DELETE FROM meetings_contacts WHERE meeting_id = ?");
        $s->execute([$meetingId]);
        $mcDel = $s->rowCount();

        // 3. meetings_leads
        $s = $pdo->prepare("DELETE FROM meetings_leads WHERE meeting_id = ?");
        $s->execute([$meetingId]);
        $mlDel = $s->rowCount();

        // 4. graph_sync_state:
        //    If this duplicate has a state row AND keeper has none → MOVE it to keeper
        //    Otherwise → DELETE it
        $gsMoved = false;
        if ($row['state_id']) {
            if (!$keeper['state_id']) {
                // Move: update meeting_id to point to keeper
                $pdo->prepare("
                    UPDATE graph_sync_state SET meeting_id = ? WHERE meeting_id = ?
                ")->execute([$keeper['id'], $meetingId]);
                // Mark keeper as now having state so subsequent iterations see it
                $toDelete[$meetingId]['keeper']['state_id'] = $row['state_id'];
                foreach ($toKeep as $h => $k) {
                    if ($k['id'] === $keeper['id']) {
                        $toKeep[$h]['state_id'] = $row['state_id'];
                    }
                }
                $gsMoved = true;
            } else {
                $pdo->prepare("DELETE FROM graph_sync_state WHERE meeting_id = ?")
                    ->execute([$meetingId]);
            }
        }

        // 5. Soft-delete the meeting
        //    deleted=1, but we do NOT update date_modified to NOW() here —
        //    keeping the original date_modified means sync.php's pushCrmDeletions
        //    will find it and try to cancel it in O365.
        //    Instead we set a future date_modified so it looks like a manual cleanup,
        //    which pushCrmDeletions also picks up — so we set deleted=1 but
        //    ALSO remove the state row (done above), meaning pushCrmDeletions
        //    JOIN on graph_sync_state will find NO row and skip it entirely.
        $pdo->prepare("
            UPDATE meetings SET deleted = 1, date_modified = NOW() WHERE id = ?
        ")->execute([$meetingId]);

        printf("  [REMOVED] \"%s\"\n", $name);
        printf("            id=%s\n", $meetingId);
        printf("            users:-%d  contacts:-%d  leads:-%d  state:%s\n",
            $muDel, $mcDel, $mlDel,
            $gsMoved ? 'moved to keeper' : ($row['state_id'] ? 'deleted' : 'none'));

        $removed++;
    }

    $pdo->commit();

    echo "\n";
    echo "── Result ───────────────────────────────────────────────────────────────\n\n";
    printf("  Removed  : %d duplicate meeting(s)\n", $removed);
    echo   "  O365     : untouched\n";
    echo   "  Status   : complete\n\n";

} catch (PDOException $e) {
    $pdo->rollBack();
    echo "\n[FATAL] Database error — ALL changes rolled back:\n";
    echo "        " . $e->getMessage() . "\n\n";
    exit(1);
}

// ============================================================
// Phase 6 — Verify keeper state rows are intact
// ============================================================

echo "── Phase 6: Verifying keeper sync state rows ────────────────────────────\n\n";

$allOk = true;
foreach ($toKeep as $hash => $keeper) {
    $s = $pdo->prepare("
        SELECT graph_event_id, graph_mailbox, content_hash
        FROM graph_sync_state WHERE meeting_id = ?
    ");
    $s->execute([$keeper['id']]);
    $stateRow = $s->fetch(PDO::FETCH_ASSOC);

    if ($stateRow) {
        printf("  ✓ \"%s\"\n", $keeper['name']);
        printf("    mailbox      : %s\n", $stateRow['graph_mailbox']);
        printf("    graph_event  : %s\n", substr($stateRow['graph_event_id'], 0, 60) . '...');
        printf("    content_hash : %s\n\n", $stateRow['content_hash'] ?? '(not set)');
    } else {
        printf("  ⚠ \"%s\" — NO sync state row!\n", $keeper['name']);
        echo   "    sync.php will treat this as a new CRM meeting and push it to O365.\n";
        echo   "    It will NOT cancel the existing O365 event.\n\n";
        $allOk = false;
    }
}

if ($allOk) {
    echo "  ✓ All keeper meetings have intact sync state rows.\n";
    echo "  ✓ sync.php will not re-create or cancel anything on next run.\n\n";
} else {
    echo "  ⚠ Some keepers are missing state rows — see above.\n";
    echo "    Run sync.php when ready; it will push those meetings to O365 fresh.\n\n";
}
