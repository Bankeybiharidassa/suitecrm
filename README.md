# SuiteCRM ↔ Microsoft Graph Calendar Sync

## Overview

This project provides a **bidirectional calendar synchronization** between SuiteCRM and Microsoft 365 (via Microsoft Graph API).

It enables:

* Two-way sync (SuiteCRM ↔ Outlook / Microsoft 365)
* Conflict resolution (most recently modified wins)
* Automatic attendee handling (users + external contacts)
* Soft delete handling (cancel instead of hard delete)
* Multi-mailbox support
* Idempotent and stateful synchronization

This is a **server-side sync engine**, not a client plugin.

---

## Features

### Inbound (Graph → CRM)

* Sync calendar events from Microsoft 365 into SuiteCRM
* Automatically create meetings if they don’t exist
* Update existing meetings when changes are detected
* Handle cancellations (Graph → CRM → “Not Held”)
* Import attendees (users + auto-create contacts)

### Outbound (CRM → Graph)

* Create new events in Microsoft 365 from SuiteCRM
* Update modified CRM meetings in Outlook
* Cancel events in Outlook when deleted in CRM

### Conflict Handling

* Uses hash comparison + timestamps
* “Most recently modified wins” logic
* Prevents overwrite loops and duplication

---

## Requirements

* SuiteCRM (tested with 7.x / 8.x backend)
* PHP 7.4+
* MySQL / MariaDB
* Microsoft 365 tenant
* Microsoft Entra ID (Azure AD) application

---

## Microsoft Graph Setup

### Required API Permissions (Application)

* `Calendars.ReadWrite`
* (Optional) `User.Read.All`

After assigning permissions:

1. Grant **Admin Consent**
2. (Recommended) Restrict access using:

   * Application Access Policies (Exchange Online)

---

## Installation

### 1. Configure

Create:

```bash
config.php
```

Fill in:

* Database credentials
* Tenant ID
* Client ID
* Client Secret

---

### 2. Place script

Example:

```bash
/var/www/html/suitecrm/custom/scripts/graph-sync/sync.php
```

---

### 3. Set permissions

```bash
chown -R www-data:www-data /path/to/script
chmod 600 config.php
```

---

### 4. Test manually

```bash
sudo -u www-data php sync.php
```

---

### 5. Setup cron

```bash
*/5 * * * * flock -n /tmp/graph-sync.lock sudo -u www-data php /path/to/sync.php >> /var/log/graph-sync.log 2>&1
```

---

## How It Works

Each run performs:

1. Graph → CRM sync (inbound)
2. Deletion detection (Graph → CRM)
3. CRM deletions → Graph cancellations
4. CRM → Graph new events
5. CRM → Graph updates

State is tracked in:

```sql
graph_sync_state
```

This ensures:

* No duplicates
* Proper change tracking
* Safe bidirectional behavior

---

## Data Model Notes

* CRM meetings are linked via `meetings_users`
* External attendees are created as Contacts
* Graph event ID is stored in the meeting description
* Hash-based comparison ensures efficient sync

---

## Limitations

* No Microsoft Graph delta API (full window polling used)
* No webhook/push notifications (cron-based)
* No full contact directory sync (intentional design choice)
* Recurring events are handled as expanded instances

---

## Security Notes

* Store credentials securely
* Restrict file permissions (`chmod 600`)
* Do not expose config files publicly
* Consider environment variables for production deployments

---

## Disclaimer

This software is provided **"as is"**, without warranty of any kind.

* Use at your own risk
* Always test in a non-production environment first
* The author is not responsible for data loss, sync conflicts, or unexpected behavior

---

## Final Notes

This project was built as a **practical, transparent alternative** to commercial plugins, with:

* Full control
* No vendor lock-in
* Deterministic behavior
* Easy debugging

---

## License

Free to use, modify, and distribute.
