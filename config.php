<?php

/**
 * Configuration file for SuiteCRM ↔ Microsoft Graph Calendar Sync
 *
 * SECURITY NOTES:
 * - Restrict file permissions: chmod 600 config.php
 * - Owner should be web user (e.g. www-data)
 * - NEVER commit this file to version control
 * - Consider moving secrets to environment variables for production
 */

// ============================================================
// Database configuration
// ============================================================

$dbHost = "localhost";
$dbName = "suitecrm";
$dbUser = "suitecrm_user";
$dbPass = "your_secure_password";

// ============================================================
// Microsoft Entra ID (Azure AD) configuration
// ============================================================

$tenantId     = "your-tenant-id";          // e.g. xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
$clientId     = "your-client-id";          // Application (client) ID
$clientSecret = "your-client-secret";      // Client secret (or use certificate instead)

// ============================================================
// Optional settings
// ============================================================

// Debug mode (true = verbose output)
$debug = true;

// Default timezone (keep UTC unless you REALLY know what you're doing)
date_default_timezone_set("UTC");

// ============================================================
// Notes
// ============================================================

/**
 * Required Microsoft Graph API permissions (Application level):
 *
 * - Calendars.ReadWrite
 * - User.Read.All (optional, depending on usage)
 *
 * After assigning permissions:
 * → Grant admin consent in Entra ID
 *
 * Recommended:
 * → Use Application Access Policy to restrict mailbox access
 *
 * Example Graph endpoint used:
 * https://graph.microsoft.com/v1.0/users/{mailbox}/calendarView
 */
