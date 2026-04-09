import os
import time
import requests

try:
    import win32com.client  # type: ignore
except Exception as exc:
    raise SystemExit("pywin32 is required. Run: pip install pywin32") from exc

BASE_URL = os.getenv("BASE_URL", "").strip().rstrip("/")
DEVICE_TOKEN = os.getenv("CRM_DEVICE_TOKEN", "").strip()
POLL_SECONDS = int(os.getenv("CRM_BRIDGE_POLL_SECONDS", "20") or "20")


def heartbeat():
    requests.post(f"{BASE_URL}/api/bridge/heartbeat", json={"device_token": DEVICE_TOKEN}, timeout=30).raise_for_status()


def get_jobs():
    r = requests.get(f"{BASE_URL}/api/bridge/jobs", headers={"X-Device-Token": DEVICE_TOKEN}, timeout=60)
    r.raise_for_status()
    return r.json().get("jobs", [])


def set_send_account(mail, smtp_address: str):
    if not smtp_address:
        return
    outlook = mail.Application
    session = outlook.Session
    for account in session.Accounts:
        try:
            if (account.SmtpAddress or "").lower() == smtp_address.lower():
                mail.SendUsingAccount = account
                return
        except Exception:
            continue


def send_job(job):
    outlook = win32com.client.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)
    mail.To = job.get("to_email", "")
    mail.Subject = job.get("subject", "")
    mail.HTMLBody = job.get("html_body", "")
    set_send_account(mail, job.get("from_email", ""))
    mail.Send()


def post_result(job_id: int, status: str, message: str = ""):
    requests.post(
        f"{BASE_URL}/api/bridge/job-result",
        json={
            "device_token": DEVICE_TOKEN,
            "job_id": job_id,
            "status": status,
            "message": message[:1000],
            "provider_message_id": "",
        },
        timeout=30,
    ).raise_for_status()


def main():
    if not BASE_URL:
        raise SystemExit("BASE_URL env var is missing")
    if not DEVICE_TOKEN:
        raise SystemExit("CRM_DEVICE_TOKEN env var is missing")

    print("Outlook Bridge Agent started")
    while True:
        try:
            heartbeat()
            jobs = get_jobs()
            if jobs:
                print(f"Fetched {len(jobs)} job(s)")
            for job in jobs:
                try:
                    send_job(job)
                    post_result(job["id"], "sent", "Sent via Outlook Desktop")
                    print("Sent job", job["id"])
                except Exception as exc:
                    post_result(job["id"], "failed", str(exc))
                    print("Failed job", job["id"], exc)
        except Exception as exc:
            print("Bridge loop error:", exc)
        time.sleep(max(POLL_SECONDS, 5))


if __name__ == "__main__":
    main()
