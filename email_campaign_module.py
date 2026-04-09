from __future__ import annotations

import json
import os
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Optional

from fastapi import Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy import text


class _Deps:
    engine = None
    templates = None
    require_login: Callable[[Request], Any] | None = None
    is_mobile_request: Callable[[Request], bool] | None = None
    send_email: Callable[[str, str, str, str], None] | None = None
    send_with_profile: Callable[[Any, str, str, str], None] | None = None
    generate_agent_content: Callable[[str, str, str, str], dict] | None = None
    build_tracking_base_url: Callable[[Optional[Request]], str] | None = None
    add_notification: Callable[..., None] | None = None
    log_activity: Callable[..., None] | None = None


TERMINAL_CAMPAIGN_STATUSES = {'replied', 'unsubscribed', 'bounced'}
TERMINAL_SEQUENCE_STATUSES = {'completed', 'replied', 'unsubscribed', 'bounced'}
MAIL_EVENT_TYPES = {'reply', 'bounce', 'unsubscribe'}


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _is_pg(engine) -> bool:
    return engine.dialect.name == 'postgresql'


def _ensure_tables(engine) -> None:
    pg = _is_pg(engine)
    id_col = 'SERIAL PRIMARY KEY' if pg else 'INTEGER PRIMARY KEY AUTOINCREMENT'
    text_col = 'TEXT'
    real_col = 'DOUBLE PRECISION' if pg else 'REAL'
    int_col = 'INTEGER'
    with engine.begin() as conn:
        ddls = [
            f"""
            CREATE TABLE IF NOT EXISTS oauth_accounts (
                id {id_col},
                username {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                provider {'VARCHAR(50)' if pg else 'TEXT'} NOT NULL,
                account_email {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                display_name {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                access_token TEXT DEFAULT '',
                refresh_token TEXT DEFAULT '',
                token_expires_at {real_col} DEFAULT 0,
                scopes TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                created_at {real_col} DEFAULT 0,
                updated_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaign_profiles_v2 (
                id {id_col},
                username {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                oauth_account_id INTEGER DEFAULT 0,
                profile_name {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                provider {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'smtp',
                from_name {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                from_email {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                reply_to {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                signature_html TEXT DEFAULT '',
                smtp_host TEXT DEFAULT '',
                smtp_port INTEGER DEFAULT 587,
                smtp_username TEXT DEFAULT '',
                smtp_password TEXT DEFAULT '',
                smtp_use_tls INTEGER DEFAULT 1,
                graph_access_token TEXT DEFAULT '',
                daily_limit INTEGER DEFAULT 150,
                hourly_limit INTEGER DEFAULT 40,
                min_delay_seconds INTEGER DEFAULT 20,
                max_delay_seconds INTEGER DEFAULT 90,
                is_default INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at {real_col} DEFAULT 0,
                updated_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaign_templates_v2 (
                id {id_col},
                owner_scope {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'user',
                owner_username {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                team_name {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                template_name {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                subject_template TEXT DEFAULT '',
                body_html TEXT DEFAULT '',
                body_text TEXT DEFAULT '',
                variables_json TEXT DEFAULT '[]',
                is_active INTEGER DEFAULT 1,
                created_at {real_col} DEFAULT 0,
                updated_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaigns_v2 (
                id {id_col},
                created_by {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                campaign_name {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                profile_id INTEGER DEFAULT 0,
                template_id INTEGER DEFAULT 0,
                audience_source {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'manual',
                status {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'draft',
                scheduled_at {real_col} DEFAULT 0,
                started_at {real_col} DEFAULT 0,
                finished_at {real_col} DEFAULT 0,
                total_recipients INTEGER DEFAULT 0,
                sent_count INTEGER DEFAULT 0,
                failed_count INTEGER DEFAULT 0,
                open_count INTEGER DEFAULT 0,
                click_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at {real_col} DEFAULT 0,
                updated_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaign_recipients_v2 (
                id {id_col},
                campaign_id INTEGER NOT NULL,
                lead_id INTEGER DEFAULT 0,
                email {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                contact_name {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                company {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                merge_data_json TEXT DEFAULT '{{}}',
                status {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'queued',
                provider_message_id {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                error_message TEXT DEFAULT '',
                last_attempt_at {real_col} DEFAULT 0,
                sent_at {real_col} DEFAULT 0,
                opened_at {real_col} DEFAULT 0,
                clicked_at {real_col} DEFAULT 0,
                replied_at {real_col} DEFAULT 0,
                created_at {real_col} DEFAULT 0,
                updated_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaign_send_attempts_v2 (
                id {id_col},
                campaign_id INTEGER NOT NULL,
                recipient_id INTEGER NOT NULL,
                attempt_no INTEGER DEFAULT 1,
                provider {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT '',
                status {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'queued',
                response_code {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT '',
                response_message TEXT DEFAULT '',
                created_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaign_events_v2 (
                id {id_col},
                campaign_id INTEGER NOT NULL,
                recipient_id INTEGER DEFAULT 0,
                event_type {'VARCHAR(50)' if pg else 'TEXT'} NOT NULL,
                event_value TEXT DEFAULT '',
                event_meta_json TEXT DEFAULT '{{}}',
                created_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaign_sequences_v2 (
                id {id_col},
                created_by {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                sequence_name {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                profile_id INTEGER DEFAULT 0,
                status {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'active',
                stop_on_reply INTEGER DEFAULT 1,
                stop_on_click INTEGER DEFAULT 0,
                stop_on_open INTEGER DEFAULT 0,
                notes TEXT DEFAULT '',
                total_enrollments INTEGER DEFAULT 0,
                active_enrollments INTEGER DEFAULT 0,
                completed_enrollments INTEGER DEFAULT 0,
                created_at {real_col} DEFAULT 0,
                updated_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaign_sequence_steps_v2 (
                id {id_col},
                sequence_id INTEGER NOT NULL,
                step_order INTEGER DEFAULT 1,
                step_name {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                delay_hours INTEGER DEFAULT 0,
                subject_template TEXT DEFAULT '',
                body_html TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                created_at {real_col} DEFAULT 0,
                updated_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaign_sequence_enrollments_v2 (
                id {id_col},
                sequence_id INTEGER NOT NULL,
                lead_id INTEGER DEFAULT 0,
                email {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                contact_name {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                company {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                merge_data_json TEXT DEFAULT '{{}}',
                current_step INTEGER DEFAULT 1,
                status {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'active',
                next_run_at {real_col} DEFAULT 0,
                last_event {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT '',
                provider_message_id {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                error_message TEXT DEFAULT '',
                last_attempt_at {real_col} DEFAULT 0,
                last_sent_at {real_col} DEFAULT 0,
                opened_at {real_col} DEFAULT 0,
                clicked_at {real_col} DEFAULT 0,
                replied_at {real_col} DEFAULT 0,
                completed_at {real_col} DEFAULT 0,
                created_at {real_col} DEFAULT 0,
                updated_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS campaign_sequence_events_v2 (
                id {id_col},
                sequence_id INTEGER NOT NULL,
                enrollment_id INTEGER DEFAULT 0,
                step_order INTEGER DEFAULT 0,
                event_type {'VARCHAR(50)' if pg else 'TEXT'} NOT NULL,
                event_value TEXT DEFAULT '',
                event_meta_json TEXT DEFAULT '{{}}',
                created_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS suppression_list_v2 (
                id {id_col},
                email {'VARCHAR(255)' if pg else 'TEXT'} NOT NULL,
                reason {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'manual',
                source {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'manual',
                notes TEXT DEFAULT '',
                created_by {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                is_active INTEGER DEFAULT 1,
                created_at {real_col} DEFAULT 0,
                updated_at {real_col} DEFAULT 0
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS inbox_events_v2 (
                id {id_col},
                username {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                provider {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'manual',
                event_type {'VARCHAR(50)' if pg else 'TEXT'} DEFAULT 'reply',
                email {'VARCHAR(255)' if pg else 'TEXT'} DEFAULT '',
                campaign_id INTEGER DEFAULT 0,
                recipient_id INTEGER DEFAULT 0,
                sequence_id INTEGER DEFAULT 0,
                enrollment_id INTEGER DEFAULT 0,
                details TEXT DEFAULT '',
                payload_json TEXT DEFAULT '{{}}',
                created_at {real_col} DEFAULT 0
            )
            """,
        ]
        for ddl in ddls:
            conn.execute(text(ddl))

        profile_cols = {r[0] for r in conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='campaign_profiles_v2'")).fetchall()} if pg else set()
        if not pg:
            try:
                profile_cols = {r[1] for r in conn.exec_driver_sql('PRAGMA table_info(campaign_profiles_v2)').fetchall()}
            except Exception:
                profile_cols = set()
        alter_specs = [
            ('smtp_host', "ALTER TABLE campaign_profiles_v2 ADD COLUMN smtp_host TEXT DEFAULT ''"),
            ('smtp_port', "ALTER TABLE campaign_profiles_v2 ADD COLUMN smtp_port INTEGER DEFAULT 587"),
            ('smtp_username', "ALTER TABLE campaign_profiles_v2 ADD COLUMN smtp_username TEXT DEFAULT ''"),
            ('smtp_password', "ALTER TABLE campaign_profiles_v2 ADD COLUMN smtp_password TEXT DEFAULT ''"),
            ('smtp_use_tls', "ALTER TABLE campaign_profiles_v2 ADD COLUMN smtp_use_tls INTEGER DEFAULT 1"),
            ('graph_access_token', "ALTER TABLE campaign_profiles_v2 ADD COLUMN graph_access_token TEXT DEFAULT ''"),
        ]
        for col, sql in alter_specs:
            if col not in profile_cols:
                try:
                    conn.execute(text(sql))
                except Exception:
                    pass


def _insert_and_get_id(conn, engine, sql: str, params: dict) -> int:
    if _is_pg(engine):
        row = conn.execute(text(sql.strip() + ' RETURNING id'), params).fetchone()
        return int(row.id)
    res = conn.execute(text(sql), params)
    return int(getattr(res, 'lastrowid', 0) or 0)


def _template_context(request: Request, user: Any, extra: dict | None = None) -> dict:
    base = {
        'request': request,
        'username': getattr(user, 'username', ''),
        'user': user,
        'is_mobile': _Deps.is_mobile_request(request) if _Deps.is_mobile_request else False,
    }
    if extra:
        base.update(extra)
    return base


def _json_load(value: str, default):
    try:
        return json.loads(value or '')
    except Exception:
        return default


def _seed_default_templates(engine):
    now = time.time()
    with engine.begin() as conn:
        row = conn.execute(text('SELECT id FROM campaign_templates_v2 WHERE template_name=:n LIMIT 1'), {'n': 'Intro Offer V2'}).fetchone()
        if not row:
            conn.execute(text("""
                INSERT INTO campaign_templates_v2
                (owner_scope, owner_username, team_name, template_name, subject_template, body_html, body_text, variables_json, created_at, updated_at)
                VALUES
                ('team', '', 'export', :name, :subject, :body_html, :body_text, :vars, :t, :t)
            """), {
                'name': 'Intro Offer V2',
                'subject': 'Altahhan Dates | Supply for {{company}}',
                'body_html': '<p>Hello {{contact_name}},</p><p>We would like to introduce Altahhan Dates for {{company}}.</p><p>Products: dates, date paste, date syrup.</p><p>Best regards,<br>{{sender_name}}</p>',
                'body_text': 'Hello {{contact_name}},\nWe would like to introduce Altahhan Dates for {{company}}.\nProducts: dates, date paste, date syrup.\nBest regards,\n{{sender_name}}',
                'vars': json.dumps(['company', 'contact_name', 'country', 'sender_name']),
                't': now,
            })


def _render_merge(template: str, merge_data: dict, sender_name: str) -> str:
    result = template or ''
    data = dict(merge_data or {})
    data['sender_name'] = sender_name or ''
    for key, value in data.items():
        result = result.replace('{{' + str(key) + '}}', str(value or ''))
    return result


def _rewrite_links_for_tracking(html_body: str, tracked_url_builder) -> str:
    if not html_body:
        return ''

    def repl(match):
        url = match.group(1)
        if url.startswith('mailto:') or url.startswith('javascript:'):
            return match.group(0)
        tracked = tracked_url_builder(url)
        return match.group(0).replace(url, tracked)

    return re.sub(r"href=[\"']([^\"']+)[\"']", repl, html_body, flags=re.I)


def _log_attempt(conn, campaign_id: int, recipient_id: int, provider: str, status: str, code: str, message: str):
    attempt_no = conn.execute(
        text('SELECT COALESCE(MAX(attempt_no),0)+1 FROM campaign_send_attempts_v2 WHERE recipient_id=:rid'),
        {'rid': recipient_id}
    ).scalar() or 1
    conn.execute(text("""
        INSERT INTO campaign_send_attempts_v2 (campaign_id, recipient_id, attempt_no, provider, status, response_code, response_message, created_at)
        VALUES (:cid, :rid, :ano, :provider, :status, :code, :msg, :t)
    """), {
        'cid': campaign_id,
        'rid': recipient_id,
        'ano': int(attempt_no),
        'provider': provider,
        'status': status,
        'code': code,
        'msg': (message or '')[:1000],
        't': time.time(),
    })


def _refresh_campaign_stats(conn, campaign_id: int):
    stats = conn.execute(text("""
        SELECT
          COUNT(*) total,
          SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) sent_count,
          SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) failed_count,
          SUM(CASE WHEN opened_at > 0 THEN 1 ELSE 0 END) open_count,
          SUM(CASE WHEN clicked_at > 0 THEN 1 ELSE 0 END) click_count,
          SUM(CASE WHEN replied_at > 0 THEN 1 ELSE 0 END) reply_count
        FROM campaign_recipients_v2 WHERE campaign_id=:cid
    """), {'cid': campaign_id}).fetchone()
    conn.execute(text("""
        UPDATE campaigns_v2
        SET total_recipients=:total, sent_count=:sent_count, failed_count=:failed_count,
            open_count=:open_count, click_count=:click_count, reply_count=:reply_count,
            updated_at=:t
        WHERE id=:cid
    """), {
        'cid': campaign_id,
        'total': int(getattr(stats, 'total', 0) or 0),
        'sent_count': int(getattr(stats, 'sent_count', 0) or 0),
        'failed_count': int(getattr(stats, 'failed_count', 0) or 0),
        'open_count': int(getattr(stats, 'open_count', 0) or 0),
        'click_count': int(getattr(stats, 'click_count', 0) or 0),
        'reply_count': int(getattr(stats, 'reply_count', 0) or 0),
        't': time.time(),
    })


def _refresh_sequence_stats(conn, sequence_id: int):
    stats = conn.execute(text("""
        SELECT
          COUNT(*) total,
          SUM(CASE WHEN status IN ('active','waiting','failed') THEN 1 ELSE 0 END) active_count,
          SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) completed_count
        FROM campaign_sequence_enrollments_v2
        WHERE sequence_id=:sid
    """), {'sid': sequence_id}).fetchone()
    conn.execute(text("""
        UPDATE campaign_sequences_v2
        SET total_enrollments=:total,
            active_enrollments=:active_count,
            completed_enrollments=:completed_count,
            updated_at=:t
        WHERE id=:sid
    """), {
        'sid': sequence_id,
        'total': int(getattr(stats, 'total', 0) or 0),
        'active_count': int(getattr(stats, 'active_count', 0) or 0),
        'completed_count': int(getattr(stats, 'completed_count', 0) or 0),
        't': time.time(),
    })


def _email_norm(value: str) -> str:
    return (value or '').strip().lower()


def _add_suppression(conn, email: str, reason: str, created_by: str, source: str = 'manual', notes: str = ''):
    email = _email_norm(email)
    if not email:
        return
    existing = conn.execute(text('SELECT id FROM suppression_list_v2 WHERE LOWER(email)=:e AND is_active=1 ORDER BY id DESC LIMIT 1'), {'e': email}).fetchone()
    if existing:
        conn.execute(text("UPDATE suppression_list_v2 SET reason=:r, source=:s, notes=:n, updated_at=:t WHERE id=:id"), {
            'r': reason,
            's': source,
            'n': notes[:1000],
            't': time.time(),
            'id': existing.id,
        })
        return
    conn.execute(text("""
        INSERT INTO suppression_list_v2 (email, reason, source, notes, created_by, is_active, created_at, updated_at)
        VALUES (:e,:r,:s,:n,:u,1,:t,:t)
    """), {
        'e': email,
        'r': reason,
        's': source,
        'n': notes[:1000],
        'u': created_by,
        't': time.time(),
    })


def _is_suppressed(conn, email: str):
    email = _email_norm(email)
    if not email:
        return None
    return conn.execute(text('SELECT * FROM suppression_list_v2 WHERE LOWER(email)=:e AND is_active=1 ORDER BY id DESC LIMIT 1'), {'e': email}).fetchone()


def _log_campaign_event(conn, campaign_id: int, recipient_id: int, event_type: str, value: str, meta: dict | None = None):
    conn.execute(text('INSERT INTO campaign_events_v2 (campaign_id, recipient_id, event_type, event_value, event_meta_json, created_at) VALUES (:cid,:rid,:etype,:val,:meta,:t)'), {
        'cid': campaign_id,
        'rid': recipient_id,
        'etype': event_type,
        'val': value[:1000],
        'meta': json.dumps(meta or {}, ensure_ascii=False),
        't': time.time(),
    })


def _log_sequence_event(conn, sequence_id: int, enrollment_id: int, step_order: int, event_type: str, value: str, meta: dict | None = None):
    conn.execute(text('INSERT INTO campaign_sequence_events_v2 (sequence_id, enrollment_id, step_order, event_type, event_value, event_meta_json, created_at) VALUES (:sid,:eid,:step,:etype,:val,:meta,:t)'), {
        'sid': sequence_id,
        'eid': enrollment_id,
        'step': step_order,
        'etype': event_type,
        'val': value[:1000],
        'meta': json.dumps(meta or {}, ensure_ascii=False),
        't': time.time(),
    })


def _create_unsubscribe_block(base_url: str, email: str, campaign_id: int = 0, recipient_id: int = 0, sequence_id: int = 0, enrollment_id: int = 0) -> str:
    params = {
        'email': email or '',
        'campaign_id': int(campaign_id or 0),
        'recipient_id': int(recipient_id or 0),
        'sequence_id': int(sequence_id or 0),
        'enrollment_id': int(enrollment_id or 0),
    }
    qs = urllib.parse.urlencode(params)
    url = f"{base_url}/unsubscribe?{qs}" if base_url else '#'
    return f"<div style='margin-top:18px;font-size:12px;color:#6b7280'>Don't want more emails? <a href='{url}'>Unsubscribe</a></div>"


def _campaign_open_url(base_url: str, campaign_id: int, recipient) -> str:
    return (
        f"{base_url}/track/v2/open?campaign_id={campaign_id}&recipient_id={recipient.id}"
        f"&email={urllib.parse.quote(recipient.email or '')}&lead_id={int(getattr(recipient, 'lead_id', 0) or 0)}"
    )


def _campaign_click_url_builder(base_url: str, campaign_id: int, recipient):
    def builder(url: str) -> str:
        return (
            f"{base_url}/track/v2/click?campaign_id={campaign_id}&recipient_id={recipient.id}"
            f"&email={urllib.parse.quote(recipient.email or '')}&lead_id={int(getattr(recipient, 'lead_id', 0) or 0)}"
            f"&url={urllib.parse.quote(url, safe='')}"
        )
    return builder


def _sequence_open_url(base_url: str, sequence_id: int, enrollment, step_order: int) -> str:
    return (
        f"{base_url}/track/v2/open?sequence_id={sequence_id}&enrollment_id={enrollment.id}&step_order={step_order}"
        f"&email={urllib.parse.quote(enrollment.email or '')}&lead_id={int(getattr(enrollment, 'lead_id', 0) or 0)}"
    )


def _sequence_click_url_builder(base_url: str, sequence_id: int, enrollment, step_order: int):
    def builder(url: str) -> str:
        return (
            f"{base_url}/track/v2/click?sequence_id={sequence_id}&enrollment_id={enrollment.id}&step_order={step_order}"
            f"&email={urllib.parse.quote(enrollment.email or '')}&lead_id={int(getattr(enrollment, 'lead_id', 0) or 0)}"
            f"&url={urllib.parse.quote(url, safe='')}"
        )
    return builder




def _create_reply_task(conn, *, username: str, email: str, details: str, campaign_id: int = 0, sequence_id: int = 0):
    note = (details or '').strip()
    title_bits = ['Reply received']
    if email:
        title_bits.append(email)
    title = ' — '.join(title_bits)
    if len(title) > 180:
        title = title[:177] + '...'
    conn.execute(text("""
        INSERT INTO tasks (title, description, assigned_to, status, priority, due_at, created_by, created_at, completed_at)
        VALUES (:title, :description, :assigned_to, 'Open', 'High', :due_at, :created_by, :created_at, 0)
    """), {
        'title': title,
        'description': ((f'Campaign #{campaign_id} Sequence #{sequence_id}\n\n{note}') if note else (f'Campaign #{campaign_id} Sequence #{sequence_id}'))[:2000],
        'assigned_to': username,
        'due_at': time.time() + 3600,
        'created_by': username,
        'created_at': time.time(),
    })


def _apply_mail_event(conn, *, username: str, event_type: str, email: str = '', campaign_id: int = 0, recipient_id: int = 0, sequence_id: int = 0, enrollment_id: int = 0, provider: str = 'manual', details: str = '', payload: dict | None = None):
    event_type = (event_type or '').strip().lower()
    email = _email_norm(email)
    if event_type not in MAIL_EVENT_TYPES or not email:
        return False
    now = time.time()
    conn.execute(text("""
        INSERT INTO inbox_events_v2 (username, provider, event_type, email, campaign_id, recipient_id, sequence_id, enrollment_id, details, payload_json, created_at)
        VALUES (:u,:p,:etype,:e,:cid,:rid,:sid,:eid,:d,:payload,:t)
    """), {
        'u': username,
        'p': provider,
        'etype': event_type,
        'e': email,
        'cid': int(campaign_id or 0),
        'rid': int(recipient_id or 0),
        'sid': int(sequence_id or 0),
        'eid': int(enrollment_id or 0),
        'd': (details or '')[:1000],
        'payload': json.dumps(payload or {}, ensure_ascii=False),
        't': now,
    })

    if event_type in {'bounce', 'unsubscribe'}:
        _add_suppression(conn, email, event_type, username, source='manual_sync', notes=details)

    recipient_rows = []
    if recipient_id:
        row = conn.execute(text('SELECT * FROM campaign_recipients_v2 WHERE id=:id'), {'id': recipient_id}).fetchone()
        if row:
            recipient_rows = [row]
    else:
        recipient_rows = conn.execute(text("SELECT * FROM campaign_recipients_v2 WHERE LOWER(email)=:e AND status NOT IN ('replied','unsubscribed','bounced') ORDER BY id DESC LIMIT 50"), {'e': email}).fetchall()

    for row in recipient_rows:
        updates = {'id': row.id, 't': now}
        if event_type == 'reply':
            conn.execute(text("UPDATE campaign_recipients_v2 SET status='replied', replied_at=:t, updated_at=:t WHERE id=:id"), updates)
            _log_campaign_event(conn, row.campaign_id, row.id, 'recipient_replied', email, {'provider': provider, 'details': details})
        elif event_type == 'bounce':
            conn.execute(text("UPDATE campaign_recipients_v2 SET status='bounced', error_message=:err, updated_at=:t WHERE id=:id"), {'id': row.id, 't': now, 'err': (details or 'Bounce received')[:1000]})
            _log_campaign_event(conn, row.campaign_id, row.id, 'recipient_bounced', email, {'provider': provider, 'details': details})
        elif event_type == 'unsubscribe':
            conn.execute(text("UPDATE campaign_recipients_v2 SET status='unsubscribed', updated_at=:t WHERE id=:id"), updates)
            _log_campaign_event(conn, row.campaign_id, row.id, 'recipient_unsubscribed', email, {'provider': provider, 'details': details})
        _refresh_campaign_stats(conn, row.campaign_id)

    enrollment_rows = []
    if enrollment_id:
        row = conn.execute(text('SELECT * FROM campaign_sequence_enrollments_v2 WHERE id=:id'), {'id': enrollment_id}).fetchone()
        if row:
            enrollment_rows = [row]
    else:
        enrollment_rows = conn.execute(text("SELECT * FROM campaign_sequence_enrollments_v2 WHERE LOWER(email)=:e AND status NOT IN ('completed','replied','unsubscribed','bounced') ORDER BY id DESC LIMIT 50"), {'e': email}).fetchall()

    for row in enrollment_rows:
        if event_type == 'reply':
            conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET status='replied', replied_at=:t, last_event='reply', updated_at=:t WHERE id=:id"), {'id': row.id, 't': now})
            _log_sequence_event(conn, row.sequence_id, row.id, int(getattr(row, 'current_step', 0) or 0), 'recipient_replied', email, {'provider': provider, 'details': details})
        elif event_type == 'bounce':
            conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET status='bounced', error_message=:err, last_event='bounce', updated_at=:t WHERE id=:id"), {'id': row.id, 't': now, 'err': (details or 'Bounce received')[:1000]})
            _log_sequence_event(conn, row.sequence_id, row.id, int(getattr(row, 'current_step', 0) or 0), 'recipient_bounced', email, {'provider': provider, 'details': details})
        elif event_type == 'unsubscribe':
            conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET status='unsubscribed', last_event='unsubscribe', updated_at=:t WHERE id=:id"), {'id': row.id, 't': now})
            _log_sequence_event(conn, row.sequence_id, row.id, int(getattr(row, 'current_step', 0) or 0), 'recipient_unsubscribed', email, {'provider': provider, 'details': details})
        _refresh_sequence_stats(conn, row.sequence_id)

    if event_type == 'reply':
        try:
            _create_reply_task(conn, username=username, email=email, details=details, campaign_id=int(campaign_id or 0), sequence_id=int(sequence_id or 0))
        except Exception:
            pass
    return True


def _send_recipient(conn, campaign, profile, template_row, recipient, request: Optional[Request]):
    suppressed = _is_suppressed(conn, recipient.email)
    if suppressed:
        conn.execute(text("UPDATE campaign_recipients_v2 SET status='unsubscribed', error_message=:err, updated_at=:t WHERE id=:id"), {
            'err': f"Suppressed: {suppressed.reason or 'manual'}",
            't': time.time(),
            'id': recipient.id,
        })
        _log_campaign_event(conn, campaign.id, recipient.id, 'recipient_suppressed', recipient.email or '', {'reason': suppressed.reason or 'manual'})
        return False

    merge_data = _json_load(getattr(recipient, 'merge_data_json', '{}'), {})
    sender_name = (profile.from_name or profile.from_email or campaign.created_by or '').strip()
    subject = _render_merge(template_row.subject_template or '', merge_data, sender_name)
    body = _render_merge(template_row.body_html or '', merge_data, sender_name)
    signature = profile.signature_html or ''
    base_url = _Deps.build_tracking_base_url(request) if _Deps.build_tracking_base_url else ''
    if base_url:
        body = _rewrite_links_for_tracking(body, _campaign_click_url_builder(base_url, campaign.id, recipient))
        pixel = f'<img src="{_campaign_open_url(base_url, campaign.id, recipient)}" width="1" height="1">'
        body = body + '<br><br>' + signature + _create_unsubscribe_block(base_url, recipient.email or '', campaign_id=campaign.id, recipient_id=recipient.id) + pixel
    else:
        body = body + '<br><br>' + signature
    provider = (profile.provider or 'smtp').lower()
    if _Deps.send_with_profile:
        _Deps.send_with_profile(profile, recipient.email, subject, body)
    else:
        send_mode = 'smtp' if provider in {'smtp', 'microsoft_graph', 'google_api'} else 'outlook'
        _Deps.send_email(recipient.email, subject, body, send_mode)
    conn.execute(text("""
        UPDATE campaign_recipients_v2
        SET status='sent', error_message='', provider_message_id=:pmid, last_attempt_at=:t, sent_at=:t, updated_at=:t
        WHERE id=:id
    """), {
        'pmid': f'{provider}:{int(time.time())}:{recipient.id}',
        't': time.time(),
        'id': recipient.id,
    })
    _log_attempt(conn, campaign.id, recipient.id, provider, 'sent', 'ok', 'Sent successfully')
    _log_campaign_event(conn, campaign.id, recipient.id, 'recipient_sent', recipient.email or '', {'provider': provider})
    if _Deps.log_activity:
        _Deps.log_activity(campaign.created_by, 'campaign_send_v2', 'campaign_v2', campaign.id, recipient.email, conn=conn)
    return True


def _active_sequence_steps(conn, sequence_id: int):
    return conn.execute(text('SELECT * FROM campaign_sequence_steps_v2 WHERE sequence_id=:sid AND is_active=1 ORDER BY step_order ASC, id ASC'), {'sid': sequence_id}).fetchall()


def _send_sequence_step(conn, sequence, profile, enrollment, step, request: Optional[Request]):
    suppressed = _is_suppressed(conn, enrollment.email)
    if suppressed:
        conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET status='unsubscribed', last_event='unsubscribe', error_message=:err, updated_at=:t WHERE id=:id"), {
            'id': enrollment.id,
            'err': f"Suppressed: {suppressed.reason or 'manual'}",
            't': time.time(),
        })
        _log_sequence_event(conn, sequence.id, enrollment.id, step.step_order, 'recipient_suppressed', enrollment.email or '', {'reason': suppressed.reason or 'manual'})
        return False

    merge_data = _json_load(getattr(enrollment, 'merge_data_json', '{}'), {})
    sender_name = (profile.from_name or profile.from_email or sequence.created_by or '').strip()
    subject = _render_merge(step.subject_template or '', merge_data, sender_name)
    body = _render_merge(step.body_html or '', merge_data, sender_name)
    signature = profile.signature_html or ''
    base_url = _Deps.build_tracking_base_url(request) if _Deps.build_tracking_base_url else ''
    if base_url:
        body = _rewrite_links_for_tracking(body, _sequence_click_url_builder(base_url, sequence.id, enrollment, step.step_order))
        pixel = f'<img src="{_sequence_open_url(base_url, sequence.id, enrollment, step.step_order)}" width="1" height="1">'
        body = body + '<br><br>' + signature + _create_unsubscribe_block(base_url, enrollment.email or '', sequence_id=sequence.id, enrollment_id=enrollment.id) + pixel
    else:
        body = body + '<br><br>' + signature
    provider = (profile.provider or 'smtp').lower()
    if _Deps.send_with_profile:
        _Deps.send_with_profile(profile, enrollment.email or '', subject, body)
    else:
        send_mode = 'smtp' if provider in {'smtp', 'microsoft_graph', 'google_api'} else 'outlook'
        _Deps.send_email(enrollment.email or '', subject, body, send_mode)
    now = time.time()
    steps = _active_sequence_steps(conn, sequence.id)
    next_step = None
    for candidate in steps:
        if int(candidate.step_order or 0) > int(step.step_order or 0):
            next_step = candidate
            break
    if next_step:
        conn.execute(text("""
            UPDATE campaign_sequence_enrollments_v2
            SET status='waiting', current_step=:next_step, next_run_at=:next_run_at, last_event='sent', provider_message_id=:pmid,
                last_attempt_at=:t, last_sent_at=:t, error_message='', updated_at=:t
            WHERE id=:id
        """), {
            'next_step': int(next_step.step_order or 0),
            'next_run_at': now + max(0, int(next_step.delay_hours or 0)) * 3600,
            'pmid': f'{provider}:{int(now)}:{enrollment.id}:{step.step_order}',
            't': now,
            'id': enrollment.id,
        })
    else:
        conn.execute(text("""
            UPDATE campaign_sequence_enrollments_v2
            SET status='completed', completed_at=:t, last_event='completed', provider_message_id=:pmid,
                last_attempt_at=:t, last_sent_at=:t, error_message='', updated_at=:t
            WHERE id=:id
        """), {
            'pmid': f'{provider}:{int(now)}:{enrollment.id}:{step.step_order}',
            't': now,
            'id': enrollment.id,
        })
    _log_sequence_event(conn, sequence.id, enrollment.id, step.step_order, 'step_sent', enrollment.email or '', {'provider': provider, 'subject': subject})
    if _Deps.log_activity:
        _Deps.log_activity(sequence.created_by, 'sequence_step_send_v2', 'sequence_v2', sequence.id, enrollment.email or '', conn=conn)
    return True




def _graph_oauth_config(request: Optional[Request] = None) -> dict:
    tenant = (os.getenv('MICROSOFT_TENANT_ID') or 'common').strip() or 'common'
    client_id = (os.getenv('MICROSOFT_CLIENT_ID') or '').strip()
    client_secret = (os.getenv('MICROSOFT_CLIENT_SECRET') or '').strip()
    redirect_uri = (os.getenv('MICROSOFT_REDIRECT_URI') or '').strip()
    if not redirect_uri and request is not None:
        redirect_uri = str(request.url_for('campaign_v2_microsoft_callback'))
    return {
        'tenant': tenant,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri,
        'scopes': ['offline_access', 'openid', 'profile', 'User.Read', 'Mail.Send', 'Mail.Read', 'Mail.ReadBasic'],
    }


def _graph_refresh_access_token(refresh_token: str, request: Optional[Request] = None) -> dict | None:
    cfg = _graph_oauth_config(request)
    if not (cfg['client_id'] and cfg['client_secret'] and refresh_token):
        return None
    token_url = f"https://login.microsoftonline.com/{cfg['tenant']}/oauth2/v2.0/token"
    data = urllib.parse.urlencode({
        'client_id': cfg['client_id'],
        'client_secret': cfg['client_secret'],
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'redirect_uri': cfg['redirect_uri'],
        'scope': ' '.join(cfg['scopes']),
    }).encode('utf-8')
    req = urllib.request.Request(token_url, data=data, headers={'Content-Type': 'application/x-www-form-urlencoded'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception:
        return None


def _resolve_profile_graph_token(conn, profile, request: Optional[Request] = None) -> str:
    token = (getattr(profile, 'graph_access_token', '') or '').strip()
    if token:
        return token
    oauth_account_id = int(getattr(profile, 'oauth_account_id', 0) or 0)
    if not oauth_account_id:
        return ''
    account = conn.execute(text('SELECT * FROM oauth_accounts WHERE id=:id'), {'id': oauth_account_id}).fetchone()
    if not account:
        return ''
    access_token = (getattr(account, 'access_token', '') or '').strip()
    expires_at = float(getattr(account, 'token_expires_at', 0) or 0)
    if access_token and expires_at > time.time() + 60:
        return access_token
    refresh_token = (getattr(account, 'refresh_token', '') or '').strip()
    refreshed = _graph_refresh_access_token(refresh_token, request=request)
    if refreshed and refreshed.get('access_token'):
        new_access = refreshed.get('access_token', '')
        new_refresh = refreshed.get('refresh_token') or refresh_token
        expires_in = int(refreshed.get('expires_in', 3600) or 3600)
        conn.execute(text('UPDATE oauth_accounts SET access_token=:a, refresh_token=:r, token_expires_at=:e, updated_at=:t WHERE id=:id'), {
            'a': new_access,
            'r': new_refresh,
            'e': time.time() + expires_in,
            't': time.time(),
            'id': oauth_account_id,
        })
        return new_access
    return access_token


def _graph_get_json(access_token: str, url: str) -> dict:
    req = urllib.request.Request(url, headers={'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _sync_graph_inbox(conn, *, username: str, profile, request: Optional[Request] = None, max_messages: int = 20) -> dict:
    token = _resolve_profile_graph_token(conn, profile, request=request)
    if not token:
        return {'processed': 0, 'logged': 0, 'error': 'No Microsoft Graph access token found for this profile.'}
    try:
        payload = _graph_get_json(token, f'https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$top={max(1, min(max_messages, 50))}&$select=id,subject,bodyPreview,receivedDateTime,from,internetMessageId')
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='ignore')
        return {'processed': 0, 'logged': 0, 'error': f'Graph inbox sync failed: {detail or exc.reason}'}
    except Exception as exc:
        return {'processed': 0, 'logged': 0, 'error': f'Graph inbox sync failed: {exc}'}
    values = payload.get('value', []) or []
    logged = 0
    for item in values:
        sender = (((item.get('from') or {}).get('emailAddress') or {}).get('address') or '').strip().lower()
        if not sender:
            continue
        message_id = (item.get('internetMessageId') or item.get('id') or '').strip()
        if message_id:
            existing = conn.execute(text('SELECT id FROM inbox_events_v2 WHERE email=:e AND details LIKE :needle LIMIT 1'), {'e': sender, 'needle': f'%{message_id}%'}).fetchone()
            if existing:
                continue
        details = f"{(item.get('subject') or '').strip()} | {(item.get('bodyPreview') or '').strip()} | message:{message_id}"
        recipient = conn.execute(text("SELECT * FROM campaign_recipients_v2 WHERE LOWER(email)=:e ORDER BY id DESC LIMIT 1"), {'e': sender}).fetchone()
        enrollment = conn.execute(text("SELECT * FROM campaign_sequence_enrollments_v2 WHERE LOWER(email)=:e ORDER BY id DESC LIMIT 1"), {'e': sender}).fetchone()
        applied = _apply_mail_event(
            conn,
            username=username,
            event_type='reply',
            email=sender,
            campaign_id=int(getattr(recipient, 'campaign_id', 0) or 0),
            recipient_id=int(getattr(recipient, 'id', 0) or 0),
            sequence_id=int(getattr(enrollment, 'sequence_id', 0) or 0),
            enrollment_id=int(getattr(enrollment, 'id', 0) or 0),
            provider='microsoft_graph',
            details=details[:1000],
            payload={'internetMessageId': message_id, 'receivedDateTime': item.get('receivedDateTime', ''), 'subject': item.get('subject', '')},
        )
        if applied:
            logged += 1
    return {'processed': len(values), 'logged': logged, 'error': ''}

def init_email_campaign_module(app, engine, templates, require_login, is_mobile_request, send_email, build_tracking_base_url, add_notification, log_activity, send_with_profile=None, generate_agent_content=None):
    _Deps.engine = engine
    _Deps.templates = templates
    _Deps.require_login = require_login
    _Deps.is_mobile_request = is_mobile_request
    _Deps.send_email = send_email
    _Deps.send_with_profile = send_with_profile
    _Deps.generate_agent_content = generate_agent_content
    _Deps.build_tracking_base_url = build_tracking_base_url
    _Deps.add_notification = add_notification
    _Deps.log_activity = log_activity
    _ensure_tables(engine)
    _seed_default_templates(engine)

    @app.get('/campaign-v2/profiles')
    def campaign_v2_profiles(request: Request):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            profiles = conn.execute(text("SELECT p.*, oa.account_email FROM campaign_profiles_v2 p LEFT JOIN oauth_accounts oa ON oa.id=p.oauth_account_id WHERE p.username=:u ORDER BY p.is_default DESC, p.id DESC"), {'u': user.username}).fetchall()
            oauth_accounts = conn.execute(text("SELECT * FROM oauth_accounts WHERE username=:u ORDER BY id DESC"), {'u': user.username}).fetchall()
        return templates.TemplateResponse('campaign_profiles_v2.html', _template_context(request, user, {'profiles': profiles, 'oauth_accounts': oauth_accounts, 'title': 'Sending Profiles'}))

    @app.post('/campaign-v2/oauth/mock-add')
    def campaign_v2_oauth_mock_add(request: Request, provider: str = Form('microsoft'), account_email: str = Form(...), display_name: str = Form('')):
        user = _Deps.require_login(request)
        now = time.time()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO oauth_accounts
                (username, provider, account_email, display_name, access_token, refresh_token, token_expires_at, scopes, is_active, created_at, updated_at)
                VALUES (:u,:p,:e,:d,'','',0,'',1,:t,:t)
            """), {'u': user.username, 'p': provider.strip().lower(), 'e': account_email.strip().lower(), 'd': display_name.strip(), 't': now})
        return _redirect('/campaign-v2/profiles?success=oauth_saved')

    @app.post('/campaign-v2/profiles/add')
    def campaign_v2_profile_add(request: Request,
                                profile_name: str = Form(...),
                                provider: str = Form('smtp'),
                                oauth_account_id: int = Form(0),
                                from_name: str = Form(''),
                                from_email: str = Form(''),
                                reply_to: str = Form(''),
                                signature_html: str = Form(''),
                                smtp_host: str = Form(''),
                                smtp_port: int = Form(587),
                                smtp_username: str = Form(''),
                                smtp_password: str = Form(''),
                                smtp_use_tls: int = Form(1),
                                graph_access_token: str = Form(''),
                                daily_limit: int = Form(150),
                                hourly_limit: int = Form(40),
                                min_delay_seconds: int = Form(20),
                                max_delay_seconds: int = Form(90),
                                is_default: int = Form(0)):
        user = _Deps.require_login(request)
        now = time.time()
        with engine.begin() as conn:
            if is_default:
                conn.execute(text('UPDATE campaign_profiles_v2 SET is_default=0 WHERE username=:u'), {'u': user.username})
            conn.execute(text("""
                INSERT INTO campaign_profiles_v2
                (username, oauth_account_id, profile_name, provider, from_name, from_email, reply_to, signature_html,
                 smtp_host, smtp_port, smtp_username, smtp_password, smtp_use_tls, graph_access_token,
                 daily_limit, hourly_limit, min_delay_seconds, max_delay_seconds, is_default, is_active, created_at, updated_at)
                VALUES (:u,:oa,:n,:p,:fn,:fe,:rt,:sig,:sh,:sp,:su,:spw,:stls,:gat,:dl,:hl,:mind,:maxd,:def,1,:t,:t)
            """), {
                'u': user.username,
                'oa': oauth_account_id or 0,
                'n': profile_name.strip(),
                'p': provider.strip().lower() or 'smtp',
                'fn': from_name.strip(),
                'fe': from_email.strip(),
                'rt': reply_to.strip(),
                'sig': signature_html,
                'sh': smtp_host.strip(),
                'sp': max(1, int(smtp_port or 587)),
                'su': smtp_username.strip(),
                'spw': smtp_password,
                'stls': 1 if int(smtp_use_tls or 0) else 0,
                'gat': graph_access_token.strip(),
                'dl': max(1, min(int(daily_limit or 150), 5000)),
                'hl': max(1, min(int(hourly_limit or 40), 500)),
                'mind': max(0, int(min_delay_seconds or 0)),
                'maxd': max(int(max_delay_seconds or 0), int(min_delay_seconds or 0)),
                'def': 1 if is_default else 0,
                't': now,
            })
        return _redirect('/campaign-v2/profiles?success=profile_created')


    @app.get('/campaign-v2/oauth/microsoft/start')
    def campaign_v2_microsoft_oauth_start(request: Request):
        user = _Deps.require_login(request)
        cfg = _graph_oauth_config(request)
        if not (cfg['client_id'] and cfg['client_secret'] and cfg['redirect_uri']):
            return _redirect('/campaign-v2/profiles?error=microsoft_oauth_not_configured')
        state = secrets.token_urlsafe(24)
        auth_url = (
            f"https://login.microsoftonline.com/{cfg['tenant']}/oauth2/v2.0/authorize?" +
            urllib.parse.urlencode({
                'client_id': cfg['client_id'],
                'response_type': 'code',
                'redirect_uri': cfg['redirect_uri'],
                'response_mode': 'query',
                'scope': ' '.join(cfg['scopes']),
                'state': f"{user.username}:{state}",
                'prompt': 'select_account',
            })
        )
        response = RedirectResponse(auth_url, status_code=302)
        response.set_cookie('ms_oauth_state', state, httponly=True, samesite='lax')
        return response

    @app.get('/campaign-v2/oauth/microsoft/callback', name='campaign_v2_microsoft_callback')
    def campaign_v2_microsoft_callback(request: Request, code: str = '', state: str = '', error: str = '', error_description: str = ''):
        user = _Deps.require_login(request)
        if error:
            return _redirect('/campaign-v2/profiles?error=' + urllib.parse.quote((error_description or error)[:160]))
        cookie_state = request.cookies.get('ms_oauth_state', '')
        state_user, _, state_token = (state or '').partition(':')
        if not code or state_user != user.username or not cookie_state or cookie_state != state_token:
            return _redirect('/campaign-v2/profiles?error=invalid_oauth_state')
        cfg = _graph_oauth_config(request)
        token_url = f"https://login.microsoftonline.com/{cfg['tenant']}/oauth2/v2.0/token"
        body = urllib.parse.urlencode({
            'client_id': cfg['client_id'],
            'client_secret': cfg['client_secret'],
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': cfg['redirect_uri'],
            'scope': ' '.join(cfg['scopes']),
        }).encode('utf-8')
        req = urllib.request.Request(token_url, data=body, headers={'Content-Type': 'application/x-www-form-urlencoded'}, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                token_payload = json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode('utf-8', errors='ignore')
            return _redirect('/campaign-v2/profiles?error=' + urllib.parse.quote((detail or exc.reason)[:180]))
        access_token = token_payload.get('access_token', '')
        refresh_token = token_payload.get('refresh_token', '')
        expires_in = int(token_payload.get('expires_in', 3600) or 3600)
        try:
            me = _graph_get_json(access_token, 'https://graph.microsoft.com/v1.0/me?$select=displayName,mail,userPrincipalName')
        except Exception:
            me = {}
        account_email = (me.get('mail') or me.get('userPrincipalName') or '').strip().lower()
        display_name = (me.get('displayName') or '').strip()
        now = time.time()
        with engine.begin() as conn:
            existing = conn.execute(text('SELECT id FROM oauth_accounts WHERE username=:u AND provider=:p AND account_email=:e ORDER BY id DESC LIMIT 1'), {
                'u': user.username, 'p': 'microsoft', 'e': account_email,
            }).fetchone()
            if existing:
                conn.execute(text('UPDATE oauth_accounts SET display_name=:d, access_token=:a, refresh_token=:r, token_expires_at=:exp, scopes=:s, is_active=1, updated_at=:t WHERE id=:id'), {
                    'd': display_name, 'a': access_token, 'r': refresh_token, 'exp': now + expires_in,
                    's': ' '.join(cfg['scopes']), 't': now, 'id': existing.id,
                })
            else:
                conn.execute(text("""
                    INSERT INTO oauth_accounts
                    (username, provider, account_email, display_name, access_token, refresh_token, token_expires_at, scopes, is_active, created_at, updated_at)
                    VALUES (:u,'microsoft',:e,:d,:a,:r,:exp,:s,1,:t,:t)
                """), {
                    'u': user.username, 'e': account_email, 'd': display_name, 'a': access_token, 'r': refresh_token,
                    'exp': now + expires_in, 's': ' '.join(cfg['scopes']), 't': now,
                })
        response = _redirect('/campaign-v2/profiles?success=microsoft_connected')
        response.delete_cookie('ms_oauth_state', samesite='lax')
        return response

    @app.get('/campaign-v2/inbox-sync')
    def campaign_v2_inbox_sync(request: Request):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            profiles = conn.execute(text("SELECT p.*, oa.account_email FROM campaign_profiles_v2 p LEFT JOIN oauth_accounts oa ON oa.id=p.oauth_account_id WHERE p.username=:u ORDER BY p.is_default DESC, p.id DESC"), {'u': user.username}).fetchall()
            inbox_rows = conn.execute(text("SELECT * FROM inbox_events_v2 WHERE provider='microsoft_graph' ORDER BY id DESC LIMIT 80")).fetchall()
        return templates.TemplateResponse('campaign_inbox_sync_v2.html', _template_context(request, user, {'profiles': profiles, 'inbox_rows': inbox_rows, 'title': 'Inbox Sync'}))

    @app.post('/campaign-v2/inbox-sync/run')
    def campaign_v2_inbox_sync_run(request: Request, profile_id: int = Form(...), max_messages: int = Form(20)):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            profile = conn.execute(text("SELECT * FROM campaign_profiles_v2 WHERE id=:id AND username=:u"), {'id': profile_id, 'u': user.username}).fetchone()
            if not profile:
                return _redirect('/campaign-v2/inbox-sync?error=profile_not_found')
            result = _sync_graph_inbox(conn, username=user.username, profile=profile, request=request, max_messages=max_messages)
        if result.get('error'):
            return _redirect('/campaign-v2/inbox-sync?error=' + urllib.parse.quote(result['error'][:180]))
        return _redirect(f"/campaign-v2/inbox-sync?success=processed_{result.get('processed',0)}_logged_{result.get('logged',0)}")

    @app.post('/campaign-v2/profiles/{profile_id}/test-send')
    def campaign_v2_profile_test_send(request: Request, profile_id: int, to_email: str = Form(...)):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            profile = conn.execute(text('SELECT * FROM campaign_profiles_v2 WHERE id=:id AND username=:u'), {'id': profile_id, 'u': user.username}).fetchone()
            if not profile:
                return _redirect('/campaign-v2/profiles?error=profile_not_found')
            subject = 'TradeFlow CRM test email'
            body = f"<p>Hello,</p><p>This is a live test from <b>{profile.profile_name}</b>.</p><p>If you received this email, website sending is working correctly.</p>"
            try:
                if _Deps.send_with_profile:
                    _Deps.send_with_profile(profile, to_email.strip(), subject, body)
                else:
                    _Deps.send_email(to_email.strip(), subject, body, 'smtp')
            except Exception as exc:
                return _redirect('/campaign-v2/profiles?error=' + urllib.parse.quote(str(exc)[:180]))
        return _redirect('/campaign-v2/profiles?success=test_sent')

    @app.get('/campaign-v2/agent')
    def campaign_v2_agent(request: Request, company: str = '', contact_name: str = '', country: str = '', goal: str = 'Start a conversation for export sales'):
        user = _Deps.require_login(request)
        result = None
        if company or contact_name or country or goal:
            if _Deps.generate_agent_content:
                result = _Deps.generate_agent_content(company, contact_name, country, goal)
            else:
                result = {'subject': f"Trade opportunity with {company or 'your company'}", 'body': f"Hello {contact_name or 'team'},<br><br>We would like to open a business discussion with {company or 'your company'} regarding export opportunities.<br><br>Best regards"}
        return templates.TemplateResponse('campaign_agent_v2.html', _template_context(request, user, {'result': result, 'company': company, 'contact_name': contact_name, 'country': country, 'goal': goal, 'title': 'AI Outreach Agent'}))

    @app.get('/campaign-v2/templates')
    def campaign_v2_templates(request: Request):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            rows = conn.execute(text("SELECT * FROM campaign_templates_v2 WHERE owner_scope='team' OR owner_username=:u ORDER BY id DESC"), {'u': user.username}).fetchall()
        return templates.TemplateResponse('campaign_templates_v2.html', _template_context(request, user, {'rows': rows, 'title': 'Template Library'}))

    @app.post('/campaign-v2/templates/add')
    def campaign_v2_template_add(request: Request,
                                 owner_scope: str = Form('user'),
                                 team_name: str = Form(''),
                                 template_name: str = Form(...),
                                 subject_template: str = Form(...),
                                 body_html: str = Form(...),
                                 body_text: str = Form(''),
                                 variables_json: str = Form('[]')):
        user = _Deps.require_login(request)
        now = time.time()
        variables = _json_load(variables_json, [])
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO campaign_templates_v2
                (owner_scope, owner_username, team_name, template_name, subject_template, body_html, body_text, variables_json, is_active, created_at, updated_at)
                VALUES (:scope,:owner,:team,:name,:subject,:html,:txt,:vars,1,:t,:t)
            """), {
                'scope': (owner_scope or 'user').strip(),
                'owner': user.username if (owner_scope or 'user') == 'user' else '',
                'team': team_name.strip(),
                'name': template_name.strip(),
                'subject': subject_template,
                'html': body_html,
                'txt': body_text,
                'vars': json.dumps(variables, ensure_ascii=False),
                't': now,
            })
        return _redirect('/campaign-v2/templates?success=template_created')

    @app.get('/campaign-v2/campaigns')
    def campaign_v2_campaigns(request: Request):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            profiles = conn.execute(text("SELECT * FROM campaign_profiles_v2 WHERE username=:u AND is_active=1 ORDER BY is_default DESC, id DESC"), {'u': user.username}).fetchall()
            templates_rows = conn.execute(text("SELECT * FROM campaign_templates_v2 WHERE (owner_scope='team' OR owner_username=:u) AND is_active=1 ORDER BY id DESC"), {'u': user.username}).fetchall()
            leads = conn.execute(text("SELECT id, company, contact_person, email, country, stage FROM leads WHERE COALESCE(email,'')<>'' ORDER BY id DESC LIMIT 500"), {'u': user.username}).fetchall()
            campaigns = conn.execute(text("SELECT * FROM campaigns_v2 WHERE created_by=:u ORDER BY id DESC LIMIT 100"), {'u': user.username}).fetchall()
            suppression_count = conn.execute(text("SELECT COUNT(*) FROM suppression_list_v2 WHERE is_active=1")).scalar() or 0
        return templates.TemplateResponse('campaigns_v2.html', _template_context(request, user, {
            'profiles': profiles,
            'templates_rows': templates_rows,
            'leads': leads,
            'campaigns': campaigns,
            'suppression_count': int(suppression_count),
            'title': 'Campaigns',
        }))

    @app.post('/campaign-v2/campaigns/create')
    def campaign_v2_create(request: Request,
                           campaign_name: str = Form(...),
                           profile_id: int = Form(...),
                           template_id: int = Form(...),
                           notes: str = Form(''),
                           lead_ids: list[str] = Form([])):
        user = _Deps.require_login(request)
        selected = [int(x) for x in (lead_ids or []) if str(x).isdigit()]
        now = time.time()
        with engine.begin() as conn:
            campaign_id = _insert_and_get_id(conn, engine, """
                INSERT INTO campaigns_v2
                (created_by, campaign_name, profile_id, template_id, audience_source, status, total_recipients, notes, created_at, updated_at)
                VALUES (:u,:name,:pid,:tid,'manual','draft',:total,:notes,:t,:t)
            """, {
                'u': user.username,
                'name': campaign_name.strip(),
                'pid': profile_id,
                'tid': template_id,
                'total': len(selected),
                'notes': notes,
                't': now,
            })
            if selected:
                rows = conn.execute(text("SELECT id, company, contact_person, email, country, stage FROM leads WHERE id IN (%s)" % ','.join(str(i) for i in selected))).fetchall()
                for row in rows:
                    merge_data = {
                        'company': row.company or '',
                        'contact_name': row.contact_person or '',
                        'country': row.country or '',
                        'email': row.email or '',
                        'stage': row.stage or '',
                    }
                    conn.execute(text("""
                        INSERT INTO campaign_recipients_v2
                        (campaign_id, lead_id, email, contact_name, company, merge_data_json, status, created_at, updated_at)
                        VALUES (:cid,:lid,:email,:name,:company,:merge,'queued',:t,:t)
                    """), {
                        'cid': campaign_id,
                        'lid': row.id,
                        'email': row.email or '',
                        'name': row.contact_person or '',
                        'company': row.company or '',
                        'merge': json.dumps(merge_data, ensure_ascii=False),
                        't': now,
                    })
            _log_campaign_event(conn, campaign_id, 0, 'campaign_created', campaign_name.strip(), {})
        return _redirect(f'/campaign-v2/campaign/{campaign_id}')

    @app.get('/campaign-v2/campaign/{campaign_id}')
    def campaign_v2_detail(request: Request, campaign_id: int):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            campaign = conn.execute(text("""
                SELECT c.*, p.profile_name, p.provider, p.from_email, p.from_name, t.template_name
                FROM campaigns_v2 c
                LEFT JOIN campaign_profiles_v2 p ON p.id=c.profile_id
                LEFT JOIN campaign_templates_v2 t ON t.id=c.template_id
                WHERE c.id=:id AND c.created_by=:u
            """), {'id': campaign_id, 'u': user.username}).fetchone()
            recipients = conn.execute(text('SELECT * FROM campaign_recipients_v2 WHERE campaign_id=:id ORDER BY id ASC'), {'id': campaign_id}).fetchall()
            events = conn.execute(text('SELECT * FROM campaign_events_v2 WHERE campaign_id=:id ORDER BY id DESC LIMIT 60'), {'id': campaign_id}).fetchall()
            suppression = conn.execute(text('SELECT COUNT(*) FROM campaign_recipients_v2 WHERE campaign_id=:id AND status IN (\'unsubscribed\',\'bounced\')'), {'id': campaign_id}).scalar() or 0
        if not campaign:
            return _redirect('/campaign-v2/campaigns')
        return templates.TemplateResponse('campaign_detail_v2.html', _template_context(request, user, {
            'campaign': campaign,
            'recipients': recipients,
            'events': events,
            'suppression_count': int(suppression),
            'title': f'Campaign #{campaign_id}',
        }))

    @app.post('/campaign-v2/campaign/{campaign_id}/send-now')
    def campaign_v2_send_now(request: Request, campaign_id: int, limit: int = Form(50)):
        user = _Deps.require_login(request)
        now = time.time()
        sent = 0
        failed = 0
        skipped = 0
        limit = max(1, min(int(limit or 50), 500))
        with engine.begin() as conn:
            campaign = conn.execute(text('SELECT * FROM campaigns_v2 WHERE id=:id AND created_by=:u'), {'id': campaign_id, 'u': user.username}).fetchone()
            if not campaign:
                return _redirect('/campaign-v2/campaigns?error=campaign_not_found')
            profile = conn.execute(text('SELECT * FROM campaign_profiles_v2 WHERE id=:id AND username=:u'), {'id': campaign.profile_id, 'u': user.username}).fetchone()
            template_row = conn.execute(text('SELECT * FROM campaign_templates_v2 WHERE id=:id'), {'id': campaign.template_id}).fetchone()
            if not profile or not template_row:
                return _redirect(f'/campaign-v2/campaign/{campaign_id}?error=missing_profile_or_template')
            conn.execute(text("UPDATE campaigns_v2 SET status='sending', started_at=CASE WHEN started_at=0 THEN :t ELSE started_at END, updated_at=:t WHERE id=:id"), {'t': now, 'id': campaign_id})
            recipients = conn.execute(text("SELECT * FROM campaign_recipients_v2 WHERE campaign_id=:cid AND status IN ('queued','failed') ORDER BY id ASC LIMIT :lim"), {'cid': campaign_id, 'lim': limit}).fetchall()
            for recipient in recipients:
                try:
                    result = _send_recipient(conn, campaign, profile, template_row, recipient, request)
                    if result:
                        sent += 1
                    else:
                        skipped += 1
                except Exception as exc:
                    failed += 1
                    provider = (profile.provider or 'smtp').lower()
                    conn.execute(text("UPDATE campaign_recipients_v2 SET status='failed', error_message=:err, last_attempt_at=:t, updated_at=:t WHERE id=:id"), {
                        'err': str(exc)[:1000],
                        't': time.time(),
                        'id': recipient.id,
                    })
                    _log_attempt(conn, campaign.id, recipient.id, provider, 'failed', 'error', str(exc))
                    _log_campaign_event(conn, campaign.id, recipient.id, 'recipient_failed', recipient.email or '', {'error': str(exc)[:300]})
            _refresh_campaign_stats(conn, campaign_id)
            remaining = conn.execute(text("SELECT COUNT(*) FROM campaign_recipients_v2 WHERE campaign_id=:cid AND status='queued'"), {'cid': campaign_id}).scalar() or 0
            new_status = 'completed' if remaining == 0 else 'partial'
            conn.execute(text("UPDATE campaigns_v2 SET status=:s, finished_at=CASE WHEN :rem=0 THEN :t ELSE finished_at END, updated_at=:t WHERE id=:id"), {
                's': new_status,
                'rem': remaining,
                't': time.time(),
                'id': campaign_id,
            })
            _log_campaign_event(conn, campaign_id, 0, 'campaign_processed', f'Sent {sent}, failed {failed}, skipped {skipped}', {'sent': sent, 'failed': failed, 'skipped': skipped, 'remaining': int(remaining)})
            if _Deps.add_notification:
                _Deps.add_notification(f"{user.username} processed campaign #{campaign_id}: sent {sent}, failed {failed}, skipped {skipped}", kind='campaign', related_type='campaign', related_id=campaign_id, conn=conn)
        return _redirect(f'/campaign-v2/campaign/{campaign_id}?sent={sent}&failed={failed}&skipped={skipped}')

    @app.post('/campaign-v2/campaign/{campaign_id}/event/{recipient_id}')
    def campaign_v2_manual_event(request: Request, campaign_id: int, recipient_id: int, event_type: str = Form('reply'), details: str = Form('')):
        user = _Deps.require_login(request)
        event_type = (event_type or 'reply').strip().lower()
        if event_type not in MAIL_EVENT_TYPES:
            return _redirect(f'/campaign-v2/campaign/{campaign_id}?error=invalid_event')
        with engine.begin() as conn:
            campaign = conn.execute(text('SELECT id FROM campaigns_v2 WHERE id=:id AND created_by=:u'), {'id': campaign_id, 'u': user.username}).fetchone()
            recipient = conn.execute(text('SELECT * FROM campaign_recipients_v2 WHERE id=:rid AND campaign_id=:cid'), {'rid': recipient_id, 'cid': campaign_id}).fetchone()
            if campaign and recipient:
                _apply_mail_event(conn, username=user.username, event_type=event_type, email=recipient.email or '', campaign_id=campaign_id, recipient_id=recipient_id, provider='manual', details=details)
        return _redirect(f'/campaign-v2/campaign/{campaign_id}?event={event_type}')

    @app.get('/campaign-v2/sequences')
    def campaign_v2_sequences(request: Request):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            profiles = conn.execute(text("SELECT * FROM campaign_profiles_v2 WHERE username=:u AND is_active=1 ORDER BY is_default DESC, id DESC"), {'u': user.username}).fetchall()
            leads = conn.execute(text("SELECT id, company, contact_person, email, country, stage FROM leads WHERE COALESCE(email,'')<>'' ORDER BY id DESC LIMIT 500")).fetchall()
            rows = conn.execute(text("SELECT * FROM campaign_sequences_v2 WHERE created_by=:u ORDER BY id DESC LIMIT 100"), {'u': user.username}).fetchall()
        return templates.TemplateResponse('campaign_sequences_v2.html', _template_context(request, user, {
            'profiles': profiles,
            'leads': leads,
            'rows': rows,
            'title': 'Sequences',
        }))

    @app.post('/campaign-v2/sequences/create')
    def campaign_v2_sequences_create(request: Request,
                                     sequence_name: str = Form(...),
                                     profile_id: int = Form(...),
                                     notes: str = Form(''),
                                     stop_on_reply: int = Form(1),
                                     stop_on_click: int = Form(0),
                                     stop_on_open: int = Form(0),
                                     step1_name: str = Form('Intro email'),
                                     step1_delay_hours: int = Form(0),
                                     step1_subject: str = Form(...),
                                     step1_body: str = Form(...),
                                     step2_name: str = Form('Follow-up 1'),
                                     step2_delay_hours: int = Form(48),
                                     step2_subject: str = Form(''),
                                     step2_body: str = Form(''),
                                     step3_name: str = Form('Follow-up 2'),
                                     step3_delay_hours: int = Form(96),
                                     step3_subject: str = Form(''),
                                     step3_body: str = Form(''),
                                     lead_ids: list[str] = Form([])):
        user = _Deps.require_login(request)
        selected = [int(x) for x in (lead_ids or []) if str(x).isdigit()]
        now = time.time()
        steps_payload = [
            {'order': 1, 'name': step1_name.strip() or 'Step 1', 'delay_hours': max(0, int(step1_delay_hours or 0)), 'subject': step1_subject, 'body': step1_body},
            {'order': 2, 'name': step2_name.strip() or 'Step 2', 'delay_hours': max(0, int(step2_delay_hours or 0)), 'subject': step2_subject, 'body': step2_body},
            {'order': 3, 'name': step3_name.strip() or 'Step 3', 'delay_hours': max(0, int(step3_delay_hours or 0)), 'subject': step3_subject, 'body': step3_body},
        ]
        steps_payload = [s for s in steps_payload if (s['subject'] or '').strip() and (s['body'] or '').strip()]
        if not steps_payload:
            return _redirect('/campaign-v2/sequences?error=missing_steps')
        with engine.begin() as conn:
            sequence_id = _insert_and_get_id(conn, engine, """
                INSERT INTO campaign_sequences_v2
                (created_by, sequence_name, profile_id, status, stop_on_reply, stop_on_click, stop_on_open, notes,
                 total_enrollments, active_enrollments, completed_enrollments, created_at, updated_at)
                VALUES (:u,:name,:pid,'active',:sr,:sc,:so,:notes,0,0,0,:t,:t)
            """, {
                'u': user.username,
                'name': sequence_name.strip(),
                'pid': profile_id,
                'sr': 1 if stop_on_reply else 0,
                'sc': 1 if stop_on_click else 0,
                'so': 1 if stop_on_open else 0,
                'notes': notes,
                't': now,
            })
            for step in steps_payload:
                conn.execute(text("""
                    INSERT INTO campaign_sequence_steps_v2 (sequence_id, step_order, step_name, delay_hours, subject_template, body_html, is_active, created_at, updated_at)
                    VALUES (:sid,:ord,:name,:delay,:subject,:body,1,:t,:t)
                """), {
                    'sid': sequence_id,
                    'ord': step['order'],
                    'name': step['name'],
                    'delay': step['delay_hours'],
                    'subject': step['subject'],
                    'body': step['body'],
                    't': now,
                })
            if selected:
                rows = conn.execute(text("SELECT id, company, contact_person, email, country, stage FROM leads WHERE id IN (%s)" % ','.join(str(i) for i in selected))).fetchall()
                for row in rows:
                    merge_data = {
                        'company': row.company or '',
                        'contact_name': row.contact_person or '',
                        'country': row.country or '',
                        'email': row.email or '',
                        'stage': row.stage or '',
                    }
                    conn.execute(text("""
                        INSERT INTO campaign_sequence_enrollments_v2
                        (sequence_id, lead_id, email, contact_name, company, merge_data_json, current_step, status, next_run_at, last_event, created_at, updated_at)
                        VALUES (:sid,:lid,:email,:name,:company,:merge,1,'active',:next,'enrolled',:t,:t)
                    """), {
                        'sid': sequence_id,
                        'lid': row.id,
                        'email': row.email or '',
                        'name': row.contact_person or '',
                        'company': row.company or '',
                        'merge': json.dumps(merge_data, ensure_ascii=False),
                        'next': now,
                        't': now,
                    })
            _log_sequence_event(conn, sequence_id, 0, 0, 'sequence_created', sequence_name.strip(), {'enrolled': len(selected), 'steps': len(steps_payload)})
            _refresh_sequence_stats(conn, sequence_id)
        return _redirect(f'/campaign-v2/sequence/{sequence_id}')

    @app.get('/campaign-v2/sequence/{sequence_id}')
    def campaign_v2_sequence_detail(request: Request, sequence_id: int):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            sequence = conn.execute(text("""
                SELECT s.*, p.profile_name, p.provider, p.from_email, p.from_name
                FROM campaign_sequences_v2 s
                LEFT JOIN campaign_profiles_v2 p ON p.id=s.profile_id
                WHERE s.id=:id AND s.created_by=:u
            """), {'id': sequence_id, 'u': user.username}).fetchone()
            if not sequence:
                return _redirect('/campaign-v2/sequences')
            steps = conn.execute(text('SELECT * FROM campaign_sequence_steps_v2 WHERE sequence_id=:sid ORDER BY step_order ASC, id ASC'), {'sid': sequence_id}).fetchall()
            enrollments = conn.execute(text('SELECT * FROM campaign_sequence_enrollments_v2 WHERE sequence_id=:sid ORDER BY id ASC'), {'sid': sequence_id}).fetchall()
            events = conn.execute(text('SELECT * FROM campaign_sequence_events_v2 WHERE sequence_id=:sid ORDER BY id DESC LIMIT 80'), {'sid': sequence_id}).fetchall()
        return templates.TemplateResponse('campaign_sequence_detail_v2.html', _template_context(request, user, {
            'sequence': sequence,
            'steps': steps,
            'enrollments': enrollments,
            'events': events,
            'title': f'Sequence #{sequence_id}',
        }))

    @app.post('/campaign-v2/sequence/{sequence_id}/run')
    def campaign_v2_sequence_run(request: Request, sequence_id: int, limit: int = Form(50)):
        user = _Deps.require_login(request)
        limit = max(1, min(int(limit or 50), 500))
        processed = 0
        failed = 0
        stopped = 0
        now = time.time()
        with engine.begin() as conn:
            sequence = conn.execute(text('SELECT * FROM campaign_sequences_v2 WHERE id=:id AND created_by=:u'), {'id': sequence_id, 'u': user.username}).fetchone()
            if not sequence:
                return _redirect('/campaign-v2/sequences?error=sequence_not_found')
            profile = conn.execute(text('SELECT * FROM campaign_profiles_v2 WHERE id=:id AND username=:u'), {'id': sequence.profile_id, 'u': user.username}).fetchone()
            if not profile:
                return _redirect(f'/campaign-v2/sequence/{sequence_id}?error=missing_profile')
            steps = _active_sequence_steps(conn, sequence_id)
            steps_map = {int(step.step_order or 0): step for step in steps}
            due = conn.execute(text("""
                SELECT * FROM campaign_sequence_enrollments_v2
                WHERE sequence_id=:sid AND status IN ('active','waiting','failed') AND next_run_at <= :t
                ORDER BY next_run_at ASC, id ASC LIMIT :lim
            """), {'sid': sequence_id, 't': now, 'lim': limit}).fetchall()
            for enrollment in due:
                if int(sequence.stop_on_reply or 0) and float(enrollment.replied_at or 0) > 0:
                    conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET status='replied', updated_at=:t WHERE id=:id"), {'t': time.time(), 'id': enrollment.id})
                    stopped += 1
                    continue
                if int(sequence.stop_on_open or 0) and float(enrollment.opened_at or 0) > 0:
                    conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET status='completed', completed_at=:t, last_event='stopped_on_open', updated_at=:t WHERE id=:id"), {'t': time.time(), 'id': enrollment.id})
                    _log_sequence_event(conn, sequence_id, enrollment.id, int(enrollment.current_step or 0), 'stopped_on_open', enrollment.email or '', {})
                    stopped += 1
                    continue
                if int(sequence.stop_on_click or 0) and float(enrollment.clicked_at or 0) > 0:
                    conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET status='completed', completed_at=:t, last_event='stopped_on_click', updated_at=:t WHERE id=:id"), {'t': time.time(), 'id': enrollment.id})
                    _log_sequence_event(conn, sequence_id, enrollment.id, int(enrollment.current_step or 0), 'stopped_on_click', enrollment.email or '', {})
                    stopped += 1
                    continue
                step = steps_map.get(int(enrollment.current_step or 0))
                if not step:
                    conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET status='completed', completed_at=:t, last_event='completed', updated_at=:t WHERE id=:id"), {'t': time.time(), 'id': enrollment.id})
                    _log_sequence_event(conn, sequence_id, enrollment.id, int(enrollment.current_step or 0), 'completed', enrollment.email or '', {'reason': 'no_more_steps'})
                    stopped += 1
                    continue
                try:
                    result = _send_sequence_step(conn, sequence, profile, enrollment, step, request)
                    if result:
                        processed += 1
                    else:
                        stopped += 1
                except Exception as exc:
                    failed += 1
                    conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET status='failed', error_message=:err, last_attempt_at=:t, last_event='failed', updated_at=:t WHERE id=:id"), {
                        'err': str(exc)[:1000],
                        't': time.time(),
                        'id': enrollment.id,
                    })
                    _log_sequence_event(conn, sequence_id, enrollment.id, int(enrollment.current_step or 0), 'step_failed', enrollment.email or '', {'error': str(exc)[:300]})
            _refresh_sequence_stats(conn, sequence_id)
            conn.execute(text("UPDATE campaign_sequences_v2 SET updated_at=:t WHERE id=:id"), {'t': time.time(), 'id': sequence_id})
            if _Deps.add_notification:
                _Deps.add_notification(f"{user.username} processed sequence #{sequence_id}: sent {processed}, failed {failed}, stopped {stopped}", kind='campaign', related_type='campaign', related_id=sequence_id, conn=conn)
        return _redirect(f'/campaign-v2/sequence/{sequence_id}?processed={processed}&failed={failed}&stopped={stopped}')

    @app.post('/campaign-v2/sequence/{sequence_id}/status')
    def campaign_v2_sequence_status(request: Request, sequence_id: int, status: str = Form('active')):
        user = _Deps.require_login(request)
        status = (status or 'active').strip().lower()
        if status not in {'active', 'paused'}:
            status = 'active'
        with engine.begin() as conn:
            conn.execute(text('UPDATE campaign_sequences_v2 SET status=:s, updated_at=:t WHERE id=:id AND created_by=:u'), {'s': status, 't': time.time(), 'id': sequence_id, 'u': user.username})
            _log_sequence_event(conn, sequence_id, 0, 0, 'sequence_status', status, {'by': user.username})
        return _redirect(f'/campaign-v2/sequence/{sequence_id}?status={status}')

    @app.post('/campaign-v2/sequence/{sequence_id}/event/{enrollment_id}')
    def campaign_v2_sequence_event(request: Request, sequence_id: int, enrollment_id: int, event_type: str = Form('reply'), details: str = Form('')):
        user = _Deps.require_login(request)
        event_type = (event_type or 'reply').strip().lower()
        if event_type not in MAIL_EVENT_TYPES:
            return _redirect(f'/campaign-v2/sequence/{sequence_id}?error=invalid_event')
        with engine.begin() as conn:
            sequence = conn.execute(text('SELECT id FROM campaign_sequences_v2 WHERE id=:id AND created_by=:u'), {'id': sequence_id, 'u': user.username}).fetchone()
            enrollment = conn.execute(text('SELECT * FROM campaign_sequence_enrollments_v2 WHERE id=:eid AND sequence_id=:sid'), {'eid': enrollment_id, 'sid': sequence_id}).fetchone()
            if sequence and enrollment:
                _apply_mail_event(conn, username=user.username, event_type=event_type, email=enrollment.email or '', sequence_id=sequence_id, enrollment_id=enrollment_id, provider='manual', details=details)
        return _redirect(f'/campaign-v2/sequence/{sequence_id}?event={event_type}')

    @app.get('/campaign-v2/replies')
    def campaign_v2_replies(request: Request):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            inbox_rows = conn.execute(text('SELECT * FROM inbox_events_v2 ORDER BY id DESC LIMIT 120')).fetchall()
            suppression_rows = conn.execute(text('SELECT * FROM suppression_list_v2 WHERE is_active=1 ORDER BY id DESC LIMIT 120')).fetchall()
            recent_campaigns = conn.execute(text('SELECT id, campaign_name FROM campaigns_v2 WHERE created_by=:u ORDER BY id DESC LIMIT 20'), {'u': user.username}).fetchall()
            recent_sequences = conn.execute(text('SELECT id, sequence_name FROM campaign_sequences_v2 WHERE created_by=:u ORDER BY id DESC LIMIT 20'), {'u': user.username}).fetchall()
        return templates.TemplateResponse('campaign_replies_v2.html', _template_context(request, user, {
            'inbox_rows': inbox_rows,
            'suppression_rows': suppression_rows,
            'recent_campaigns': recent_campaigns,
            'recent_sequences': recent_sequences,
            'title': 'Replies & Suppression',
        }))

    @app.post('/campaign-v2/replies/log')
    def campaign_v2_replies_log(request: Request,
                                event_type: str = Form('reply'),
                                email: str = Form(...),
                                provider: str = Form('manual'),
                                details: str = Form(''),
                                campaign_id: int = Form(0),
                                recipient_id: int = Form(0),
                                sequence_id: int = Form(0),
                                enrollment_id: int = Form(0)):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            _apply_mail_event(conn, username=user.username, event_type=event_type, email=email, campaign_id=campaign_id, recipient_id=recipient_id, sequence_id=sequence_id, enrollment_id=enrollment_id, provider=provider, details=details)
        return _redirect('/campaign-v2/replies?success=event_logged')

    @app.post('/campaign-v2/suppression/add')
    def campaign_v2_suppression_add(request: Request, email: str = Form(...), reason: str = Form('manual'), notes: str = Form('')):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            _add_suppression(conn, email, (reason or 'manual').strip().lower(), user.username, source='manual', notes=notes)
        return _redirect('/campaign-v2/replies?success=suppressed')

    @app.post('/campaign-v2/suppression/{row_id}/reactivate')
    def campaign_v2_suppression_reactivate(request: Request, row_id: int):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            conn.execute(text('UPDATE suppression_list_v2 SET is_active=0, updated_at=:t, notes=COALESCE(notes,\'\') || :suffix WHERE id=:id'), {
                't': time.time(),
                'suffix': f' | reactivated by {user.username}',
                'id': row_id,
            })
        return _redirect('/campaign-v2/replies?success=reactivated')

    @app.get('/track/v2/open')
    def campaign_v2_track_open(campaign_id: int = 0, recipient_id: int = 0, sequence_id: int = 0, enrollment_id: int = 0, step_order: int = 0, email: str = '', lead_id: int = 0):
        now = time.time()
        with engine.begin() as conn:
            if campaign_id and recipient_id:
                conn.execute(text("UPDATE campaign_recipients_v2 SET opened_at=CASE WHEN opened_at=0 THEN :t ELSE opened_at END, updated_at=:t WHERE id=:rid"), {'t': now, 'rid': recipient_id})
                _log_campaign_event(conn, campaign_id, recipient_id, 'recipient_opened', email or '', {'lead_id': int(lead_id or 0)})
                _refresh_campaign_stats(conn, campaign_id)
            if sequence_id and enrollment_id:
                conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET opened_at=CASE WHEN opened_at=0 THEN :t ELSE opened_at END, last_event='open', updated_at=:t WHERE id=:eid"), {'t': now, 'eid': enrollment_id})
                _log_sequence_event(conn, sequence_id, enrollment_id, int(step_order or 0), 'recipient_opened', email or '', {'lead_id': int(lead_id or 0)})
                _refresh_sequence_stats(conn, sequence_id)
        tiny_gif = b'GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;'
        return Response(content=tiny_gif, media_type='image/gif')

    @app.get('/track/v2/click')
    def campaign_v2_track_click(url: str, campaign_id: int = 0, recipient_id: int = 0, sequence_id: int = 0, enrollment_id: int = 0, step_order: int = 0, email: str = '', lead_id: int = 0):
        now = time.time()
        target = urllib.parse.unquote(url or '')
        with engine.begin() as conn:
            if campaign_id and recipient_id:
                conn.execute(text("UPDATE campaign_recipients_v2 SET clicked_at=CASE WHEN clicked_at=0 THEN :t ELSE clicked_at END, updated_at=:t WHERE id=:rid"), {'t': now, 'rid': recipient_id})
                _log_campaign_event(conn, campaign_id, recipient_id, 'recipient_clicked', target, {'lead_id': int(lead_id or 0)})
                _refresh_campaign_stats(conn, campaign_id)
            if sequence_id and enrollment_id:
                conn.execute(text("UPDATE campaign_sequence_enrollments_v2 SET clicked_at=CASE WHEN clicked_at=0 THEN :t ELSE clicked_at END, last_event='click', updated_at=:t WHERE id=:eid"), {'t': now, 'eid': enrollment_id})
                _log_sequence_event(conn, sequence_id, enrollment_id, int(step_order or 0), 'recipient_clicked', target, {'lead_id': int(lead_id or 0)})
                _refresh_sequence_stats(conn, sequence_id)
        return _redirect(target or '/')

    @app.get('/unsubscribe', response_class=HTMLResponse)
    def campaign_v2_unsubscribe(email: str = '', campaign_id: int = 0, recipient_id: int = 0, sequence_id: int = 0, enrollment_id: int = 0):
        email_clean = _email_norm(email)
        with engine.begin() as conn:
            _apply_mail_event(conn, username='public', event_type='unsubscribe', email=email_clean, campaign_id=campaign_id, recipient_id=recipient_id, sequence_id=sequence_id, enrollment_id=enrollment_id, provider='public_link', details='Public unsubscribe link')
        html = f"""
        <html><head><meta charset='utf-8'><title>Unsubscribed</title>
        <style>body{{font-family:Arial,sans-serif;background:#0b1220;color:#fff;display:grid;place-items:center;min-height:100vh;margin:0}}.card{{max-width:640px;background:#111827;padding:28px;border-radius:18px;border:1px solid rgba(255,255,255,.08)}}a{{color:#facc15}}</style>
        </head><body><div class='card'><h1>You have been unsubscribed</h1><p>{email_clean or 'Your email'} has been added to the suppression list for future outreach.</p><p>You can now close this page.</p></div></body></html>
        """
        return HTMLResponse(html)

    @app.get('/campaign-v2/logs')
    def campaign_v2_logs(request: Request):
        user = _Deps.require_login(request)
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT a.*, c.campaign_name, r.email
                FROM campaign_send_attempts_v2 a
                LEFT JOIN campaigns_v2 c ON c.id=a.campaign_id
                LEFT JOIN campaign_recipients_v2 r ON r.id=a.recipient_id
                WHERE c.created_by=:u
                ORDER BY a.id DESC
                LIMIT 200
            """), {'u': user.username}).fetchall()
            sequence_rows = conn.execute(text("""
                SELECT e.*, s.sequence_name, en.email
                FROM campaign_sequence_events_v2 e
                LEFT JOIN campaign_sequences_v2 s ON s.id=e.sequence_id
                LEFT JOIN campaign_sequence_enrollments_v2 en ON en.id=e.enrollment_id
                WHERE s.created_by=:u
                ORDER BY e.id DESC
                LIMIT 200
            """), {'u': user.username}).fetchall()
        return templates.TemplateResponse('campaign_logs_v2.html', _template_context(request, user, {'rows': rows, 'sequence_rows': sequence_rows, 'title': 'Activity Logs'}))
