-- Altahhan CRM MySQL schema for SmarterASP
-- 1) Create the MySQL database from SmarterASP control panel.
-- 2) Select that database in phpMyAdmin / WebConnect / MySQL Workbench.
-- 3) Run this file.

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(255) UNIQUE,
    password TEXT,
    display_name VARCHAR(255) DEFAULT '',
    role VARCHAR(100) DEFAULT 'user',
    is_active INTEGER DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    company TEXT,
    contact_person VARCHAR(255) DEFAULT '',
    phone VARCHAR(255) DEFAULT '',
    email VARCHAR(255),
    country VARCHAR(255),
    city VARCHAR(255) DEFAULT '',
    source VARCHAR(255) DEFAULT '',
    assigned_to VARCHAR(255) DEFAULT '',
    tags TEXT,
    type VARCHAR(255),
    status VARCHAR(255),
    notes LONGTEXT,
    stage VARCHAR(255) DEFAULT 'Lead',
    estimated_value DOUBLE DEFAULT 0,
    score INTEGER DEFAULT 0,
    next_followup_at DOUBLE DEFAULT 0,
    last_activity DOUBLE DEFAULT 0,
    created_by VARCHAR(255) DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    message TEXT,
    kind VARCHAR(100) DEFAULT 'general',
    related_type VARCHAR(100) DEFAULT '',
    related_id INTEGER DEFAULT 0,
    created_at DOUBLE,
    is_read INTEGER DEFAULT 0,
    target_username VARCHAR(255) DEFAULT '',
    actor_username VARCHAR(255) DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS chat_channels (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) UNIQUE,
    description TEXT,
    created_by VARCHAR(255) DEFAULT '',
    created_at DOUBLE,
    is_active INTEGER DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(255),
    message LONGTEXT,
    created_at DOUBLE,
    channel_id INTEGER DEFAULT 1,
    reply_to_id INTEGER DEFAULT 0,
    file_path TEXT,
    file_name VARCHAR(255) DEFAULT '',
    file_type VARCHAR(50) DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS templates (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) UNIQUE,
    subject TEXT,
    body LONGTEXT,
    created_at DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS campaigns (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    sender_mode VARCHAR(50) DEFAULT 'outlook',
    template_id INTEGER NULL,
    subject TEXT,
    body LONGTEXT,
    status VARCHAR(50) DEFAULT 'draft',
    created_by VARCHAR(255) DEFAULT '',
    created_at DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS campaign_jobs (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    campaign_id INTEGER,
    lead_id INTEGER,
    email VARCHAR(255),
    status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT,
    created_at DOUBLE,
    sent_at DOUBLE DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tracking_events (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    lead_id INTEGER DEFAULT 0,
    email VARCHAR(255) DEFAULT '',
    campaign_id INTEGER DEFAULT 0,
    event_type VARCHAR(100),
    details TEXT,
    created_at DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS announcements (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(255),
    body LONGTEXT,
    author VARCHAR(255),
    created_at DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS announcement_replies (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    announcement_id INTEGER,
    author VARCHAR(255),
    body LONGTEXT,
    created_at DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lead_notes (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    lead_id INTEGER,
    author VARCHAR(255),
    body LONGTEXT,
    created_at DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS activity_logs (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(255),
    action VARCHAR(255),
    entity_type VARCHAR(100) DEFAULT '',
    entity_id INTEGER DEFAULT 0,
    details TEXT,
    created_at DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(255),
    description LONGTEXT,
    assigned_to VARCHAR(255) DEFAULT '',
    status VARCHAR(50) DEFAULT 'Open',
    priority VARCHAR(50) DEFAULT 'Medium',
    due_at DOUBLE DEFAULT 0,
    created_by VARCHAR(255) DEFAULT '',
    created_at DOUBLE,
    completed_at DOUBLE DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS task_comments (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    task_id INTEGER,
    author VARCHAR(255),
    body LONGTEXT,
    created_at DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lead_attachments (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    lead_id INTEGER,
    filename VARCHAR(255),
    original_name VARCHAR(255),
    created_at DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS current_clients (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    customer_code VARCHAR(255) DEFAULT '',
    country VARCHAR(255) DEFAULT '',
    company_ar TEXT,
    company_en TEXT,
    address TEXT,
    ice VARCHAR(255) DEFAULT '',
    bank_name TEXT,
    bank_address TEXT,
    swift_code VARCHAR(255) DEFAULT '',
    postal_code VARCHAR(255) DEFAULT '',
    iban_account TEXT,
    source_file VARCHAR(255) DEFAULT '',
    created_at DOUBLE,
    created_by VARCHAR(255) DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS client_invoices (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    client_id INTEGER,
    invoice_no VARCHAR(255) DEFAULT '',
    invoice_date VARCHAR(50) DEFAULT '',
    amount DOUBLE DEFAULT 0,
    currency VARCHAR(20) DEFAULT 'USD',
    notes TEXT,
    attachment_filename VARCHAR(255) DEFAULT '',
    attachment_original_name VARCHAR(255) DEFAULT '',
    created_at DOUBLE,
    created_by VARCHAR(255) DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS trade_reference_items (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(500) NOT NULL,
    category VARCHAR(50) DEFAULT 'agreement',
    item_type VARCHAR(100) DEFAULT '',
    region VARCHAR(255) DEFAULT '',
    partner_countries TEXT,
    effective_year VARCHAR(50) DEFAULT '',
    status VARCHAR(100) DEFAULT '',
    summary LONGTEXT,
    benefits LONGTEXT,
    rules_of_origin LONGTEXT,
    source_org VARCHAR(255) DEFAULT '',
    source_url TEXT,
    tags TEXT,
    sort_order INTEGER DEFAULT 0,
    created_at DOUBLE DEFAULT 0,
    updated_at DOUBLE DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
