<?php

/**
 * migrate_markers.php — One-time migration
 *
 * Replaces the [GRAPH-ID:xxx] first line in meeting descriptions
 * with the clean "Synced from O365" label.
 *
 * Safe to run multiple times (idempotent).
 * Does NOT touch graph_sync_state or any other table.
 * Does NOT contact Graph / O365.
 *
 * Usage:
 *   sudo -u www-data php migrate_markers.php
 */

require __DIR__ . '/config.php';

try {
    $pdo = new PDO(
        "mysql:host=$dbHost;dbname=$dbName;charset=utf8mb4",
        $dbUser, $dbPass,
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
    );
} catch (PDOException $e) {
    echo "[FATAL] " . $e->getMessage() . "\n";
    exit(1);
}

echo "[+] Connected to $dbName on $dbHost\n\n";

// Find all meetings that still have the old [GRAPH-ID: marker
$stmt = $pdo->query("
    SELECT id, name, description
    FROM meetings
    WHERE deleted = 0
      AND description LIKE '%[GRAPH-ID:%'
");
$rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

echo "[+] Found " . count($rows) . " meeting(s) to migrate\n\n";

if (empty($rows)) {
    echo "[+] Nothing to do. Exiting.\n";
    exit(0);
}

$update = $pdo->prepare("
    UPDATE meetings SET description = ?, date_modified = NOW() WHERE id = ?
");

$migrated = 0;
$skipped  = 0;

foreach ($rows as $row) {
    $desc = $row['description'];

    // Replace [GRAPH-ID:xxx] line (anywhere in description) with "Synced from O365"
    // Only replace if not already preceded by "Synced from O365"
    $new = preg_replace('/^\[GRAPH-ID:[^\]]+\]\n?/m', '', $desc);
    $new = trim($new);

    // Prepend the clean marker
    $new = "Synced from O365\n" . $new;
    $new = trim($new);

    if ($new === trim($desc)) {
        $skipped++;
        continue;
    }

    $update->execute([$new, $row['id']]);
    printf("  [OK] %-50s\n", mb_substr($row['name'], 0, 50));
    $migrated++;
}

echo "\n[+] Done.\n";
echo "    Migrated : $migrated\n";
echo "    Skipped  : $skipped (already clean)\n";
