<?php

/**
 * SuiteCRM ↔ Microsoft Graph Calendar — Bidirectional Sync
 *
 * Flow per run:
 *   1. createSyncStateTable()        — idempotent DDL
 *   2. getAccessToken()
 *   3. INBOUND  (Graph → CRM)        — per mailbox, calendarView window
 *   4. DELETION DETECTION            — events gone from Graph soft-cancel in CRM
 *   5. DELETIONS CRM→Graph           — deleted CRM meetings cancelled in Graph
 *   6. OUTBOUND NEW (CRM → Graph)    — new CRM meetings pushed to Graph
 *   7. OUTBOUND CHANGES (CRM→Graph)  — CRM-modified synced meetings patched
 *
 * Conflict resolution: most-recently-modified wins.
 * Recurring meetings: handled (Graph expands instances via calendarView).
 * External attendees: created as Contacts in SuiteCRM.
 */

require __DIR__ . '/config.php';

// ============================================================
// DB
// ============================================================
function db(): PDO {
    global $dbHost, $dbUser, $dbPass, $dbName;
    static $pdo;
    if (!$pdo) {
        $pdo = new PDO(
            "mysql:host=$dbHost;dbname=$dbName;charset=utf8mb4",
            $dbUser, $dbPass,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
    }
    return $pdo;
}

// ============================================================
// Sync state table (idempotent)
// ============================================================
function createSyncStateTable(): void {
    db()->exec("
        CREATE TABLE IF NOT EXISTS graph_sync_state (
            id                  VARCHAR(36)   NOT NULL PRIMARY KEY,
            meeting_id          VARCHAR(36)   NOT NULL,
            graph_event_id      VARCHAR(500)  NOT NULL,
            graph_mailbox       VARCHAR(255)  NOT NULL,
            last_sync_hash      VARCHAR(64)   NOT NULL,
            last_modified_crm   DATETIME      NOT NULL,
            last_modified_graph DATETIME      NOT NULL,
            last_synced_at      DATETIME      NOT NULL,
            UNIQUE KEY uq_meeting (meeting_id),
            UNIQUE KEY uq_graph   (graph_event_id(255), graph_mailbox)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    ");
}

// ============================================================
// Mailboxes / users
// ============================================================
function getMailboxes(): array {
    $stmt = db()->query("
        SELECT ea.email_address
        FROM users u
        JOIN email_addr_bean_rel eabr
            ON eabr.bean_id = u.id AND eabr.deleted = 0 AND eabr.primary_address = 1
        JOIN email_addresses ea
            ON ea.id = eabr.email_address_id AND ea.deleted = 0
        WHERE u.deleted = 0
    ");
    return $stmt->fetchAll(PDO::FETCH_COLUMN);
}

function getUserIdByEmail(string $email): ?string {
    static $cache = [];
    $email = strtolower(trim($email));
    if (array_key_exists($email, $cache)) return $cache[$email];

    $stmt = db()->prepare("
        SELECT u.id
        FROM users u
        JOIN email_addr_bean_rel eabr
            ON eabr.bean_id = u.id AND eabr.deleted = 0 AND eabr.primary_address = 1
        JOIN email_addresses ea
            ON ea.id = eabr.email_address_id AND ea.deleted = 0
        WHERE LOWER(ea.email_address) = ?
          AND u.deleted = 0
        LIMIT 1
    ");
    $stmt->execute([$email]);
    $result = $stmt->fetchColumn() ?: null;
    $cache[$email] = $result;
    return $result;
}

function getEmailByUserId(string $userId): ?string {
    static $cache = [];
    if (array_key_exists($userId, $cache)) return $cache[$userId];

    $stmt = db()->prepare("
        SELECT ea.email_address
        FROM email_addr_bean_rel eabr
        JOIN email_addresses ea ON ea.id = eabr.email_address_id AND ea.deleted = 0
        WHERE eabr.bean_id = ? AND eabr.deleted = 0 AND eabr.primary_address = 1
        LIMIT 1
    ");
    $stmt->execute([$userId]);
    $result = $stmt->fetchColumn() ?: null;
    $cache[$userId] = $result;
    return $result;
}

// ============================================================
// Microsoft Graph — auth
// ============================================================
function getAccessToken(): string {
    global $tenantId, $clientId, $clientSecret;

    $ch = curl_init("https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token");
    curl_setopt_array($ch, [
        CURLOPT_POST           => true,
        CURLOPT_POSTFIELDS     => http_build_query([
            'client_id'     => $clientId,
            'client_secret' => $clientSecret,
            'grant_type'    => 'client_credentials',
            'scope'         => 'https://graph.microsoft.com/.default',
        ]),
        CURLOPT_RETURNTRANSFER => true,
    ]);
    $response = json_decode(curl_exec($ch), true);
    curl_close($ch);

    if (!isset($response['access_token'])) {
        echo "[FATAL] Token request failed\n";
        print_r($response);
        exit(1);
    }
    return $response['access_token'];
}

// ============================================================
// Microsoft Graph — read
// ============================================================
function getEvents(string $token, string $email, string $start, string $end): array {
    $url = "https://graph.microsoft.com/v1.0/users/"
         . urlencode($email)
         . "/calendarView"
         . "?startDateTime=" . urlencode($start)
         . "&endDateTime="   . urlencode($end)
         . "&\$select=id,subject,start,end,location,body,organizer,"
         . "onlineMeeting,isOnlineMeeting,attendees,lastModifiedDateTime,"
         . "seriesMasterId,type,recurrence,isCancelled"
         . "&\$top=100";

    $events = [];
    while ($url) {
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_HTTPHEADER     => [
                "Authorization: Bearer $token",
                "Prefer: outlook.timezone=\"UTC\"",
                "Content-Type: application/json",
            ],
            CURLOPT_RETURNTRANSFER => true,
        ]);
        $response = json_decode(curl_exec($ch), true);
        curl_close($ch);

        if (isset($response['error'])) {
            return ['error' => $response['error']];
        }
        $events = array_merge($events, $response['value'] ?? []);
        $url    = $response['@odata.nextLink'] ?? null;
    }
    return $events;
}

// ============================================================
// Microsoft Graph — write
// ============================================================
function graphRequest(string $token, string $method, string $url, ?array $body = null): array {
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_CUSTOMREQUEST  => $method,
        CURLOPT_HTTPHEADER     => [
            "Authorization: Bearer $token",
            "Content-Type: application/json",
        ],
        CURLOPT_RETURNTRANSFER => true,
    ]);
    if ($body !== null) {
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($body));
    }
    $raw      = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    $decoded = $raw ? (json_decode($raw, true) ?? []) : [];
    $decoded['_httpCode'] = $httpCode;
    return $decoded;
}

function createGraphEvent(string $token, string $mailbox, array $payload): ?string {
    $url      = "https://graph.microsoft.com/v1.0/users/" . urlencode($mailbox) . "/events";
    $response = graphRequest($token, 'POST', $url, $payload);

    if (!empty($response['error'])) {
        echo "    [ERR] createGraphEvent: " . ($response['error']['message'] ?? json_encode($response['error'])) . "\n";
        return null;
    }
    return $response['id'] ?? null;
}

function updateGraphEvent(string $token, string $mailbox, string $graphId, array $payload): bool {
    $url      = "https://graph.microsoft.com/v1.0/users/" . urlencode($mailbox)
              . "/events/" . urlencode($graphId);
    $response = graphRequest($token, 'PATCH', $url, $payload);

    if (!empty($response['error'])) {
        echo "    [ERR] updateGraphEvent: " . ($response['error']['message'] ?? json_encode($response['error'])) . "\n";
        return false;
    }
    return true;
}

function cancelGraphEvent(string $token, string $mailbox, string $graphId): bool {
    $url      = "https://graph.microsoft.com/v1.0/users/" . urlencode($mailbox)
              . "/events/" . urlencode($graphId) . "/cancel";
    $response = graphRequest($token, 'POST', $url, ['comment' => 'Cancelled via SuiteCRM sync']);
    $code     = $response['_httpCode'] ?? 0;
    return $code >= 200 && $code < 300;
}

// ============================================================
// Sync state helpers
// ============================================================
function getSyncState(string $meetingId): ?array {
    $stmt = db()->prepare("SELECT * FROM graph_sync_state WHERE meeting_id = ? LIMIT 1");
    $stmt->execute([$meetingId]);
    return $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
}

function getSyncStateByGraphId(string $graphEventId, string $mailbox): ?array {
    $stmt = db()->prepare("
        SELECT * FROM graph_sync_state
        WHERE graph_event_id = ? AND graph_mailbox = ? LIMIT 1
    ");
    $stmt->execute([$graphEventId, $mailbox]);
    return $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
}

function upsertSyncState(
    string $meetingId,
    string $graphEventId,
    string $mailbox,
    string $hash,
    string $modifiedCrm,
    string $modifiedGraph
): void {
    $existing = getSyncState($meetingId);
    if ($existing) {
        db()->prepare("
            UPDATE graph_sync_state SET
                graph_event_id      = ?,
                graph_mailbox       = ?,
                last_sync_hash      = ?,
                last_modified_crm   = ?,
                last_modified_graph = ?,
                last_synced_at      = NOW()
            WHERE meeting_id = ?
        ")->execute([$graphEventId, $mailbox, $hash, $modifiedCrm, $modifiedGraph, $meetingId]);
    } else {
        db()->prepare("
            INSERT INTO graph_sync_state
                (id, meeting_id, graph_event_id, graph_mailbox,
                 last_sync_hash, last_modified_crm, last_modified_graph, last_synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
        ")->execute([
            generateUuid(), $meetingId, $graphEventId, $mailbox,
            $hash, $modifiedCrm, $modifiedGraph,
        ]);
    }
}

function deleteSyncState(string $meetingId): void {
    db()->prepare("DELETE FROM graph_sync_state WHERE meeting_id = ?")->execute([$meetingId]);
}

// ============================================================
// Hashing
// ============================================================
function computeHash(
    string $name,
    string $dateStart,
    string $dateEnd,
    string $location,
    array  $attendeeEmails
): string {
    $emails = array_map('strtolower', $attendeeEmails);
    sort($emails);
    return hash('sha256', implode('|', [$name, $dateStart, $dateEnd, $location, implode(',', $emails)]));
}

// ============================================================
// Attendees
// ============================================================

/** Collect all attendee emails for a CRM meeting from users, contacts, leads */
function getCrmAttendeeEmails(string $meetingId): array {
    $pdo    = db();
    $emails = [];

    // Internal users
    $s = $pdo->prepare("
        SELECT ea.email_address
        FROM meetings_users mu
        JOIN email_addr_bean_rel eabr
            ON eabr.bean_id = mu.user_id AND eabr.deleted = 0 AND eabr.primary_address = 1
        JOIN email_addresses ea ON ea.id = eabr.email_address_id AND ea.deleted = 0
        WHERE mu.meeting_id = ? AND mu.deleted = 0
    ");
    $s->execute([$meetingId]);
    $emails = array_merge($emails, $s->fetchAll(PDO::FETCH_COLUMN));

    // Contacts
    $s = $pdo->prepare("
        SELECT ea.email_address
        FROM meetings_contacts mc
        JOIN email_addr_bean_rel eabr
            ON eabr.bean_id = mc.contact_id AND eabr.deleted = 0 AND eabr.primary_address = 1
        JOIN email_addresses ea ON ea.id = eabr.email_address_id AND ea.deleted = 0
        WHERE mc.meeting_id = ? AND mc.deleted = 0
    ");
    $s->execute([$meetingId]);
    $emails = array_merge($emails, $s->fetchAll(PDO::FETCH_COLUMN));

    // Leads
    $s = $pdo->prepare("
        SELECT ea.email_address
        FROM meetings_leads ml
        JOIN email_addr_bean_rel eabr
            ON eabr.bean_id = ml.lead_id AND eabr.deleted = 0 AND eabr.primary_address = 1
        JOIN email_addresses ea ON ea.id = eabr.email_address_id AND ea.deleted = 0
        WHERE ml.meeting_id = ? AND ml.deleted = 0
    ");
    $s->execute([$meetingId]);
    $emails = array_merge($emails, $s->fetchAll(PDO::FETCH_COLUMN));

    return array_values(array_unique(array_filter(array_map('strtolower', $emails))));
}

/**
 * Ensure an external Graph attendee exists in SuiteCRM as a Contact
 * and is linked to the meeting via meetings_contacts.
 */
function ensureAttendeeContact(string $meetingId, string $email, string $name, string $ownerUserId): void {
    $pdo   = db();
    $email = strtolower(trim($email));

    // Already a known internal user — handled via meetings_users
    if (getUserIdByEmail($email)) return;

    // Find existing contact by email
    $s = $pdo->prepare("
        SELECT eabr.bean_id
        FROM email_addr_bean_rel eabr
        JOIN email_addresses ea ON ea.id = eabr.email_address_id AND ea.deleted = 0
        WHERE LOWER(ea.email_address) = ?
          AND eabr.bean_module = 'Contacts'
          AND eabr.deleted = 0
        LIMIT 1
    ");
    $s->execute([$email]);
    $contactId = $s->fetchColumn() ?: null;

    if (!$contactId) {
        $contactId = generateUuid();
        $parts     = explode(' ', trim($name), 2);
        $firstName = $parts[0] ?? '';
        $lastName  = isset($parts[1]) ? $parts[1] : ($parts[0] ?? $email);

        $pdo->prepare("
            INSERT INTO contacts
                (id, first_name, last_name, assigned_user_id,
                 date_entered, date_modified, created_by, modified_user_id, deleted)
            VALUES (?, ?, ?, ?, NOW(), NOW(), ?, ?, 0)
        ")->execute([$contactId, $firstName, $lastName, $ownerUserId, $ownerUserId, $ownerUserId]);

        // Find or create email_addresses row
        $s = $pdo->prepare("SELECT id FROM email_addresses WHERE LOWER(email_address) = ? LIMIT 1");
        $s->execute([$email]);
        $eaId = $s->fetchColumn();
        if (!$eaId) {
            $eaId = generateUuid();
            $pdo->prepare("
                INSERT INTO email_addresses (id, email_address, date_created, date_modified, deleted)
                VALUES (?, ?, NOW(), NOW(), 0)
            ")->execute([$eaId, $email]);
        }

        $pdo->prepare("
            INSERT INTO email_addr_bean_rel
                (id, email_address_id, bean_module, bean_id, primary_address, deleted)
            VALUES (?, ?, 'Contacts', ?, 1, 0)
        ")->execute([generateUuid(), $eaId, $contactId]);

        echo "      [CONTACT+] $name <$email>\n";
    }

    // Link to meeting (idempotent)
    $s = $pdo->prepare("
        SELECT id FROM meetings_contacts
        WHERE meeting_id = ? AND contact_id = ? AND deleted = 0 LIMIT 1
    ");
    $s->execute([$meetingId, $contactId]);
    if (!$s->fetch()) {
        $pdo->prepare("
            INSERT INTO meetings_contacts (id, meeting_id, contact_id, required, accept_status, deleted)
            VALUES (?, ?, ?, 1, 'none', 0)
        ")->execute([generateUuid(), $meetingId, $contactId]);
    }
}

/** Link all Graph attendees to a CRM meeting */
function linkGraphAttendees(string $meetingId, array $rawAttendees, string $ownerUserId): void {
    foreach ($rawAttendees as $att) {
        $email = strtolower(trim($att['emailAddress']['address'] ?? ''));
        $name  = $att['emailAddress']['name'] ?? $email;
        if (!$email) continue;

        $uid = getUserIdByEmail($email);
        if ($uid) {
            ensureMeetingUser($meetingId, $uid);
        } else {
            ensureAttendeeContact($meetingId, $email, $name, $ownerUserId);
        }
    }
}

// ============================================================
// meetings_users relation
// ============================================================
function ensureMeetingUser(string $meetingId, string $userId): void {
    $s = db()->prepare("
        SELECT id FROM meetings_users
        WHERE meeting_id = ? AND user_id = ? AND deleted = 0 LIMIT 1
    ");
    $s->execute([$meetingId, $userId]);
    if (!$s->fetch()) {
        db()->prepare("
            INSERT INTO meetings_users (id, meeting_id, user_id, required, accept_status, deleted)
            VALUES (?, ?, ?, 1, 'accept', 0)
        ")->execute([generateUuid(), $meetingId, $userId]);
    }
}

// ============================================================
// Helpers
// ============================================================
function generateUuid(): string {
    $data    = random_bytes(16);
    $data[6] = chr(ord($data[6]) & 0x0f | 0x40);
    $data[8] = chr(ord($data[8]) & 0x3f | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
}

function parseDateTimeUtc(string $dt): DateTime {
    return new DateTime(rtrim($dt, 'Z') . 'Z', new DateTimeZone('UTC'));
}

function buildCrmDescription(array $event, string $graphId): string {
    $lines   = ["[GRAPH-ID:$graphId]"];
    $org     = $event['organizer']['emailAddress'] ?? null;
    if ($org) {
        $lines[] = "Organizer: " . ($org['name'] ?? '') . " <" . ($org['address'] ?? '') . ">";
    }
    $joinUrl = $event['onlineMeeting']['joinUrl'] ?? null;
    if (!$joinUrl && !empty($event['isOnlineMeeting'])) {
        if (preg_match('#https://teams\.microsoft\.com/[^\s"<>]+#', $event['body']['content'] ?? '', $m)) {
            $joinUrl = $m[0];
        }
    }
    if ($joinUrl) $lines[] = "Teams join URL: $joinUrl";

    $bodyText = trim(html_entity_decode(strip_tags($event['body']['content'] ?? ''), ENT_QUOTES | ENT_HTML5, 'UTF-8'));
    $bodyText = preg_replace("/(\r?\n){3,}/", "\n\n", $bodyText);
    if ($bodyText !== '') { $lines[] = ''; $lines[] = $bodyText; }

    return implode("\n", $lines);
}

/** Strip sync metadata so only the user-facing description is sent to Graph */
function stripSyncMarker(string $description): string {
    $description = preg_replace('/^\[GRAPH-ID:[^\]]+\]\n?/m', '', $description);
    $description = preg_replace('/^Organizer:.*\n?/m', '', $description);
    $description = preg_replace('/^Teams join URL:.*\n?/m', '', $description);
    return trim($description);
}

/** Build Graph API event payload from a CRM meeting row */
function buildGraphPayload(array $meeting, array $attendeeEmails): array {
    $start = new DateTime($meeting['date_start'], new DateTimeZone('UTC'));
    $end   = new DateTime($meeting['date_end'],   new DateTimeZone('UTC'));

    $attendees = [];
    foreach ($attendeeEmails as $email) {
        $attendees[] = [
            'emailAddress' => ['address' => $email],
            'type'         => 'required',
        ];
    }

    return [
        'subject' => $meeting['name'] ?? '(No Subject)',
        'start'   => ['dateTime' => $start->format('Y-m-d\TH:i:s'), 'timeZone' => 'UTC'],
        'end'     => ['dateTime' => $end->format('Y-m-d\TH:i:s'),   'timeZone' => 'UTC'],
        'location' => ['displayName' => mb_substr($meeting['location'] ?? '', 0, 255)],
        'body'     => [
            'contentType' => 'text',
            'content'     => stripSyncMarker($meeting['description'] ?? ''),
        ],
        'attendees' => $attendees,
    ];
}

// ============================================================
// INBOUND: apply one Graph event to CRM
// ============================================================
function applyGraphEventToCrm(array $event, string $mailboxEmail): array {
    $pdo    = db();
    $userId = getUserIdByEmail($mailboxEmail);
    if (!$userId) return ['skip' => true, 'reason' => "No CRM user for $mailboxEmail"];

    $graphId = $event['id'];
    $state   = getSyncStateByGraphId($graphId, $mailboxEmail);

    // Times
    $startDt = parseDateTimeUtc($event['start']['dateTime']);
    $endDt   = parseDateTimeUtc($event['end']['dateTime']);
    $dateStart = $startDt->format('Y-m-d H:i:s');
    $dateEnd   = $endDt->format('Y-m-d H:i:s');

    $durationSec     = max(0, $endDt->getTimestamp() - $startDt->getTimestamp());
    $durationHours   = (int) floor($durationSec / 3600);
    $durationMinutes = (int) floor(($durationSec % 3600) / 60);

    // Fields
    $name     = $event['subject'] ?? '(No Subject)';
    $location = trim($event['location']['displayName'] ?? '');
    if ($location === '' || stripos($location, 'microsoft teams') !== false) {
        $location = 'Microsoft Teams';
    }
    $location = mb_substr($location, 0, 255);

    // Graph attendees
    $rawAttendees        = $event['attendees'] ?? [];
    $graphAttendeeEmails = array_values(array_filter(array_map(
        fn($a) => strtolower(trim($a['emailAddress']['address'] ?? '')),
        $rawAttendees
    )));

    $graphModified = (new DateTime(
        $event['lastModifiedDateTime'] ?? 'now',
        new DateTimeZone('UTC')
    ))->format('Y-m-d H:i:s');

    $hashGraph = computeHash($name, $dateStart, $dateEnd, $location, $graphAttendeeEmails);

    // Organizer
    $organizerEmail  = strtolower(trim($event['organizer']['emailAddress']['address'] ?? ''));
    $organizerUserId = ($organizerEmail ? getUserIdByEmail($organizerEmail) : null) ?? $userId;

    $isCancelled = !empty($event['isCancelled']);

    // ── No state yet: new event from Graph ──
    if (!$state) {
        if ($isCancelled) return ['skip' => true, 'reason' => 'Already cancelled, not in CRM'];

        // Check legacy description marker
        $s = $pdo->prepare("SELECT id, date_modified FROM meetings WHERE description LIKE ? LIMIT 1");
        $s->execute(["[GRAPH-ID:$graphId]%"]);
        $legacy = $s->fetch(PDO::FETCH_ASSOC);

        if ($legacy) {
            $meetingId   = $legacy['id'];
            $crmModified = $legacy['date_modified'];
        } else {
            $meetingId = generateUuid();
            $pdo->prepare("
                INSERT INTO meetings
                    (id, name, date_start, date_end,
                     duration_hours, duration_minutes,
                     assigned_user_id, status, location, description,
                     date_entered, date_modified, created_by, modified_user_id, deleted)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'Planned', ?, ?, NOW(), NOW(), ?, ?, 0)
            ")->execute([
                $meetingId, $name, $dateStart, $dateEnd,
                $durationHours, $durationMinutes,
                $organizerUserId, $location,
                buildCrmDescription($event, $graphId),
                $organizerUserId, $organizerUserId,
            ]);
            $crmModified = (new DateTime('now', new DateTimeZone('UTC')))->format('Y-m-d H:i:s');
        }

        ensureMeetingUser($meetingId, $userId);
        if ($organizerUserId !== $userId) ensureMeetingUser($meetingId, $organizerUserId);
        linkGraphAttendees($meetingId, $rawAttendees, $userId);

        upsertSyncState($meetingId, $graphId, $mailboxEmail, $hashGraph, $crmModified, $graphModified);
        return ['inserted' => true];
    }

    // ── State exists ──
    $meetingId  = $state['meeting_id'];
    $hashStored = $state['last_sync_hash'];

    $s = $pdo->prepare("SELECT * FROM meetings WHERE id = ? LIMIT 1");
    $s->execute([$meetingId]);
    $crmMeeting = $s->fetch(PDO::FETCH_ASSOC);

    if (!$crmMeeting) {
        deleteSyncState($meetingId);
        return ['skip' => true, 'reason' => 'CRM meeting missing, state cleaned'];
    }

    // Handle cancelled
    if ($isCancelled) {
        $pdo->prepare("UPDATE meetings SET status='Not Held', date_modified=NOW() WHERE id=?")
            ->execute([$meetingId]);
        deleteSyncState($meetingId);
        echo "      [CANCEL←] $name\n";
        return ['cancelled' => true];
    }

    $crmAttendees = getCrmAttendeeEmails($meetingId);
    $crmModified  = $crmMeeting['date_modified'];
    $hashCrm      = computeHash(
        $crmMeeting['name'],
        $crmMeeting['date_start'],
        $crmMeeting['date_end'],
        $crmMeeting['location'] ?? '',
        $crmAttendees
    );

    $graphChanged = $hashGraph !== $hashStored;
    $crmChanged   = $hashCrm   !== $hashStored;

    if (!$graphChanged && !$crmChanged) return ['skip' => true, 'reason' => 'No changes'];

    // Only Graph changed → update CRM
    if ($graphChanged && !$crmChanged) {
        $pdo->prepare("
            UPDATE meetings SET
                name=?, date_start=?, date_end=?,
                duration_hours=?, duration_minutes=?,
                location=?, description=?,
                assigned_user_id=?, date_modified=NOW()
            WHERE id=?
        ")->execute([
            $name, $dateStart, $dateEnd,
            $durationHours, $durationMinutes,
            $location, buildCrmDescription($event, $graphId),
            $organizerUserId, $meetingId,
        ]);
        linkGraphAttendees($meetingId, $rawAttendees, $userId);
        upsertSyncState($meetingId, $graphId, $mailboxEmail, $hashGraph, $graphModified, $graphModified);
        return ['updated' => true, 'direction' => 'graph→crm'];
    }

    // Only CRM changed → outbound pass will handle it
    if ($crmChanged && !$graphChanged) {
        return ['skip' => true, 'reason' => 'CRM changed, handled in outbound'];
    }

    // Both changed → most recently modified wins
    $crmTs   = strtotime($crmModified);
    $graphTs = strtotime($graphModified);

    if ($crmTs >= $graphTs) {
        echo "      [CONFLICT] $name — CRM wins (newer)\n";
        return ['skip' => true, 'reason' => 'Conflict: CRM wins, outbound will patch Graph'];
    }

    echo "      [CONFLICT] $name — Graph wins (newer)\n";
    $pdo->prepare("
        UPDATE meetings SET
            name=?, date_start=?, date_end=?,
            duration_hours=?, duration_minutes=?,
            location=?, description=?,
            assigned_user_id=?, date_modified=NOW()
        WHERE id=?
    ")->execute([
        $name, $dateStart, $dateEnd,
        $durationHours, $durationMinutes,
        $location, buildCrmDescription($event, $graphId),
        $organizerUserId, $meetingId,
    ]);
    linkGraphAttendees($meetingId, $rawAttendees, $userId);
    upsertSyncState($meetingId, $graphId, $mailboxEmail, $hashGraph, $graphModified, $graphModified);
    return ['updated' => true, 'direction' => 'graph→crm (conflict)'];
}

// ============================================================
// OUTBOUND: push new CRM meetings to Graph
// ============================================================
function pushNewCrmMeetings(string $token): array {
    $outStart = gmdate("Y-m-d H:i:s");
    $outEnd   = gmdate("Y-m-d H:i:s", strtotime('+6 months'));

    $stmt = db()->prepare("
        SELECT m.*
        FROM meetings m
        LEFT JOIN graph_sync_state gss ON gss.meeting_id = m.id
        WHERE m.deleted = 0
          AND m.status NOT IN ('Not Held')
          AND m.date_end  >= ?
          AND m.date_start <= ?
          AND gss.id IS NULL
          AND (m.description IS NULL OR m.description NOT LIKE '[GRAPH-ID:%')
        ORDER BY m.date_start ASC
    ");
    $stmt->execute([$outStart, $outEnd]);
    $meetings = $stmt->fetchAll(PDO::FETCH_ASSOC);

    $ins = $skip = $err = 0;

    foreach ($meetings as $meeting) {
        $mailbox = $meeting['assigned_user_id'] ? getEmailByUserId($meeting['assigned_user_id']) : null;
        if (!$mailbox) {
            echo "      [SKIP] No mailbox for meeting: " . $meeting['name'] . "\n";
            $skip++; continue;
        }

        $attendeeEmails = getCrmAttendeeEmails($meeting['id']);
        $payload        = buildGraphPayload($meeting, $attendeeEmails);
        $graphId        = createGraphEvent($token, $mailbox, $payload);

        if (!$graphId) { $err++; continue; }

        // Write Graph ID back into CRM description
        $cleanDesc = stripSyncMarker($meeting['description'] ?? '');
        $newDesc   = trim("[GRAPH-ID:$graphId]\n" . $cleanDesc);
        db()->prepare("UPDATE meetings SET description=?, date_modified=NOW() WHERE id=?")
            ->execute([$newDesc, $meeting['id']]);

        $hash   = computeHash($meeting['name'], $meeting['date_start'], $meeting['date_end'], $meeting['location'] ?? '', $attendeeEmails);
        $nowUtc = (new DateTime('now', new DateTimeZone('UTC')))->format('Y-m-d H:i:s');
        upsertSyncState($meeting['id'], $graphId, $mailbox, $hash, $nowUtc, $nowUtc);

        echo "      [OUT+] " . $meeting['name'] . " → $mailbox\n";
        $ins++;
    }

    return ['inserted' => $ins, 'skipped' => $skip, 'errors' => $err];
}

// ============================================================
// OUTBOUND: patch changed CRM meetings in Graph
// ============================================================
function pushChangedCrmMeetings(string $token): array {
    $stmt = db()->query("
        SELECT m.*, gss.graph_event_id, gss.graph_mailbox,
               gss.last_sync_hash, gss.last_modified_crm, gss.last_modified_graph
        FROM meetings m
        JOIN graph_sync_state gss ON gss.meeting_id = m.id
        WHERE m.deleted = 0
          AND m.status NOT IN ('Not Held')
    ");
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

    $upd = $skip = $err = 0;

    foreach ($rows as $row) {
        $attendeeEmails = getCrmAttendeeEmails($row['id']);
        $hashCrm        = computeHash(
            $row['name'], $row['date_start'], $row['date_end'],
            $row['location'] ?? '', $attendeeEmails
        );

        if ($hashCrm === $row['last_sync_hash']) { $skip++; continue; }

        // Only push if CRM is newer than last known Graph modification
        $crmTs   = strtotime($row['date_modified']);
        $graphTs = strtotime($row['last_modified_graph']);
        if ($crmTs < $graphTs) { $skip++; continue; }

        $payload = buildGraphPayload($row, $attendeeEmails);
        $ok      = updateGraphEvent($token, $row['graph_mailbox'], $row['graph_event_id'], $payload);

        if (!$ok) { $err++; continue; }

        $nowUtc = (new DateTime('now', new DateTimeZone('UTC')))->format('Y-m-d H:i:s');
        upsertSyncState($row['id'], $row['graph_event_id'], $row['graph_mailbox'], $hashCrm, $nowUtc, $nowUtc);

        echo "      [OUT~] " . $row['name'] . "\n";
        $upd++;
    }

    return ['updated' => $upd, 'skipped' => $skip, 'errors' => $err];
}

// ============================================================
// DELETIONS: CRM deleted → cancel in Graph
// ============================================================
function pushCrmDeletions(string $token): array {
    $stmt = db()->query("
        SELECT m.id, m.name, gss.graph_event_id, gss.graph_mailbox
        FROM meetings m
        JOIN graph_sync_state gss ON gss.meeting_id = m.id
        WHERE m.deleted = 1
    ");
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

    $cancelled = $err = 0;

    foreach ($rows as $row) {
        $ok = cancelGraphEvent($token, $row['graph_mailbox'], $row['graph_event_id']);
        if ($ok) {
            deleteSyncState($row['id']);
            echo "      [DEL→] " . $row['name'] . "\n";
            $cancelled++;
        } else {
            $err++;
        }
    }

    return ['cancelled' => $cancelled, 'errors' => $err];
}

// ============================================================
// DELETION DETECTION: events gone from Graph → soft-cancel CRM
// ============================================================
function detectGraphDeletions(array $seenGraphIds): void {
    foreach ($seenGraphIds as $mailbox => $ids) {
        if (empty($ids)) continue;

        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        $params       = array_merge([$mailbox], $ids);

        $stmt = db()->prepare("
            SELECT gss.meeting_id, gss.graph_event_id, m.name
            FROM graph_sync_state gss
            JOIN meetings m ON m.id = gss.meeting_id
            WHERE gss.graph_mailbox = ?
              AND gss.graph_event_id NOT IN ($placeholders)
              AND m.deleted = 0
              AND m.status != 'Not Held'
        ");
        $stmt->execute($params);
        $gone = $stmt->fetchAll(PDO::FETCH_ASSOC);

        foreach ($gone as $row) {
            db()->prepare("UPDATE meetings SET status='Not Held', date_modified=NOW() WHERE id=?")
                ->execute([$row['meeting_id']]);
            deleteSyncState($row['meeting_id']);
            echo "      [DEL←] " . $row['name'] . " (gone from Graph)\n";
        }
    }
}

// ============================================================
// MAIN
// ============================================================

createSyncStateTable();

$inboundStart = gmdate("Y-m-d\TH:i:s\Z", strtotime('-1 day'));
$inboundEnd   = gmdate("Y-m-d\TH:i:s\Z", strtotime('+14 days'));

echo "[+] Sync started at " . date('Y-m-d H:i:s') . "\n";
echo "[+] Inbound window : $inboundStart → $inboundEnd\n";
echo "[+] Outbound window: now → +6 months\n\n";

$token     = getAccessToken();
$mailboxes = getMailboxes();

echo "[+] Mailboxes: " . count($mailboxes) . "\n\n";

$totals = [
    'in_inserted'   => 0, 'in_updated'    => 0,
    'in_cancelled'  => 0, 'in_skipped'    => 0, 'in_errors'  => 0,
    'out_inserted'  => 0, 'out_updated'   => 0,
    'out_cancelled' => 0, 'out_errors'    => 0,
];

$seenGraphIds = []; // mailbox → [graphId, ...]

// ── INBOUND ──────────────────────────────────────────────────
echo "── INBOUND (Graph → CRM) ───────────────────────────────\n";

foreach ($mailboxes as $email) {
    echo "[>] $email\n";
    $seenGraphIds[$email] = [];

    $events = getEvents($token, $email, $inboundStart, $inboundEnd);

    if (isset($events['error'])) {
        echo "    [ERR] " . ($events['error']['message'] ?? json_encode($events['error'])) . "\n";
        $totals['in_errors']++;
        continue;
    }

    $ins = $upd = $can = $skip = 0;

    foreach ($events as $event) {
        $seenGraphIds[$email][] = $event['id'];
        $res = applyGraphEventToCrm($event, $email);

        if (!empty($res['inserted']))  { $ins++;  $totals['in_inserted']++; }
        if (!empty($res['updated']))   { $upd++;  $totals['in_updated']++; }
        if (!empty($res['cancelled'])) { $can++;  $totals['in_cancelled']++; }
        if (!empty($res['skip']))      { $skip++; $totals['in_skipped']++; }
    }

    echo "    events: " . count($events) . "  |  +$ins  ~$upd  ✗$can  skip:$skip\n\n";
}

// ── DELETION DETECTION (Graph → CRM) ─────────────────────────
echo "── DELETION DETECTION (Graph → CRM) ────────────────────\n";
detectGraphDeletions($seenGraphIds);
echo "\n";

// ── DELETIONS (CRM → Graph) ───────────────────────────────────
echo "── DELETIONS (CRM → Graph) ──────────────────────────────\n";
$delRes = pushCrmDeletions($token);
echo "    Cancelled: " . $delRes['cancelled'] . "  Errors: " . $delRes['errors'] . "\n\n";
$totals['out_cancelled'] += $delRes['cancelled'];
$totals['out_errors']    += $delRes['errors'];

// ── OUTBOUND NEW (CRM → Graph) ────────────────────────────────
echo "── OUTBOUND NEW (CRM → Graph) ───────────────────────────\n";
$newRes = pushNewCrmMeetings($token);
echo "    Inserted: " . $newRes['inserted'] . "  Skipped: " . $newRes['skipped'] . "  Errors: " . $newRes['errors'] . "\n\n";
$totals['out_inserted'] += $newRes['inserted'];
$totals['out_errors']   += $newRes['errors'];

// ── OUTBOUND CHANGES (CRM → Graph) ───────────────────────────
echo "── OUTBOUND CHANGES (CRM → Graph) ──────────────────────\n";
$chgRes = pushChangedCrmMeetings($token);
echo "    Updated: " . $chgRes['updated'] . "  Skipped: " . $chgRes['skipped'] . "  Errors: " . $chgRes['errors'] . "\n\n";
$totals['out_updated'] += $chgRes['updated'];
$totals['out_errors']  += $chgRes['errors'];

// ── SUMMARY ───────────────────────────────────────────────────
echo "════════════════════════════════════════════════════════\n";
echo "[+] DONE at " . date('Y-m-d H:i:s') . "\n\n";
echo "INBOUND  (Graph → CRM)\n";
echo "  Inserted  : " . $totals['in_inserted']  . "\n";
echo "  Updated   : " . $totals['in_updated']   . "\n";
echo "  Cancelled : " . $totals['in_cancelled'] . "\n";
echo "  Skipped   : " . $totals['in_skipped']   . "\n";
echo "  Errors    : " . $totals['in_errors']    . "\n\n";
echo "OUTBOUND (CRM → Graph)\n";
echo "  Inserted  : " . $totals['out_inserted']  . "\n";
echo "  Updated   : " . $totals['out_updated']   . "\n";
echo "  Cancelled : " . $totals['out_cancelled'] . "\n";
echo "  Errors    : " . $totals['out_errors']    . "\n";