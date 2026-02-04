import os
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from simple_salesforce import Salesforce
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# ==========================================
# 1. SETUP & CONFIGURATION
# ==========================================
load_dotenv()

SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_TOKEN = os.getenv('SF_TOKEN')
DEBOUNCE_API_KEY = os.getenv('DEBOUNCE_API_KEY')
INSTANTLY_API_KEY = os.getenv('INSTANTLY_API_KEY')

CAM_PRICING = os.getenv('CAMPAIGN_ID_PRICING')
CAM_BLOGS = os.getenv('CAMPAIGN_ID_BLOGS')
CAM_COMPARE = os.getenv('CAMPAIGN_ID_COMPARE')
CAM_HOME = os.getenv('CAMPAIGN_ID_HOME')

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def get_salesforce_client():
    try:
        return Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)
    except Exception as e:
        print(f"❌ Salesforce Connection Failed: {e}")
        return None

def get_tracker_sheets():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        # GitHub Actions create karega is file ko
        creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open("Instantly_Sync_Tracker")
        return spreadsheet.worksheet("Emails"), spreadsheet.worksheet("CompanyCampaigns")
    except Exception as e:
        print(f"❌ Google Sheet Connection Error: {e}")
        return None, None

def determine_campaign(url):
    if not url: return CAM_HOME, "Home/General"
    url = url.lower()
    if '/pricing' in url: return CAM_PRICING, "Pricing"
    elif '/customer-stories/' in url: return CAM_BLOGS, "Blogs/Stories"
    elif '/compare/' in url: return CAM_COMPARE, "Compare"
    else: return CAM_HOME, "Home/General"

def validate_email(email):
    if not email: return {}
    try:
        response = requests.get("https://api.debounce.io/v1/", params={'api': DEBOUNCE_API_KEY, 'email': email})
        return response.json().get('debounce', {})
    except:
        return {}

def add_to_instantly(campaign_id, email, first_name, last_name, person_type="Lead"):
    url = "https://api.instantly.ai/api/v2/leads"
    headers = {"Authorization": f"Bearer {INSTANTLY_API_KEY}", "Content-Type": "application/json"}
    
    payload = {
        "email": email.strip(),
        "first_name": (first_name or "").strip(),
        "last_name": (last_name or "").strip(),
        "campaign_id": campaign_id.strip(),
        "skip_if_in_workspace": False,
        "skip_if_in_campaign": False,
        "custom_variables": {"source": "SF_Automation", "type": person_type}
    }
    
    try:
        res = requests.post(url, json=payload, headers=headers)
        if res.status_code == 200:
            print(f"   ✅ SUCCESS: Added {email} ({person_type}) to Instantly")
            return True
        print(f"   ❌ Instantly Error: {res.text}")
        return False
    except Exception as e:
        print(f"   ❌ Request Error: {e}")
        return False

def fetch_related_contacts(sf, company_name):
    if not company_name or len(company_name) < 2: return []
    safe_company = company_name.replace("'", "\\'")
    query = f"SELECT Email, FirstName, LastName, Status__c FROM Contact WHERE Account.Name = '{safe_company}'"
    try:
        return sf.query(query)['records']
    except:
        return []

# ==========================================
# 3. MAIN LOGIC
# ==========================================
def run_sync():
    sf = get_salesforce_client()
    email_sheet, company_sheet = get_tracker_sheets()
    
    if not all([sf, email_sheet, company_sheet]): return

    # Load tracking data from Sheets
    processed_emails = [e.lower() for e in email_sheet.col_values(1)]
    existing_company_campaigns = [f"{row[0]}|{row[1]}" for row in company_sheet.get_all_values()]

    # TEST & RUN WINDOW: 30 DAYS
    check_time = datetime.now(timezone.utc) - timedelta(days=30)
    print(f"🕒 Scanning Salesforce Leads since: {check_time}")

    query = f"""
        SELECT Id, Email, FirstName, LastName, Company, Last_Page_Seen__c, Owner.Name 
        FROM Lead 
        WHERE CreatedDate > {check_time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        AND Sub_Channel__c = 'Website Visit'
        AND (Owner.Name = 'Vipul Babbar' OR Owner.Name = 'Anirudh Vashishth')
    """
    
    try:
        results = sf.query(query)
        leads = results['records']
        print(f"📄 Found {len(leads)} potential leads.")
    except Exception as e:
        print(f"❌ Salesforce Query Error: {e}")
        return

    for lead in leads:
        email = lead.get('Email', '').lower().strip()
        company = lead.get('Company', '').strip()
        last_page = lead.get('Last_Page_Seen__c')
        target_cam_id, cam_name = determine_campaign(last_page)
        
        if not email: continue

        # Rule 1: Email Duplicate Check
        if email in processed_emails:
            print(f"⏭️ Skipping {email}: Person already processed.")
            continue

        # Rule 2: Company-Campaign Check (Same company, same landing page/campaign = Skip)
        comp_cam_key = f"{company}|{target_cam_id}"
        if comp_cam_key in existing_company_campaigns:
            print(f"⏭️ Skipping {email}: Company '{company}' already represented in {cam_name} campaign.")
            continue

        print(f"\n⚡ Processing: {email} ({company}) -> {cam_name}")
        
        # 4. Validate & Add Main Lead
        val = validate_email(email)
        if val.get('result') in ['Accept All', 'Deliverable', 'Safe to Send']:
            if add_to_instantly(target_cam_id, email, lead.get('FirstName'), lead.get('LastName'), "Main Lead"):
                # Track in Sheets
                email_sheet.append_row([email, datetime.now().isoformat()])
                company_sheet.append_row([company, target_cam_id, datetime.now().isoformat()])
                processed_emails.append(email)
                existing_company_campaigns.append(comp_cam_key)

                # 5. Add Colleagues
                if company:
                    contacts = fetch_related_contacts(sf, company)
                    for c in contacts:
                        c_email = (c.get('Email') or '').lower().strip()
                        # Colleague duplicate check
                        if c_email and c_email not in processed_emails and c.get('Status__c') != 'Left the Company':
                            c_val = validate_email(c_email)
                            if c_val.get('result') in ['Accept All', 'Deliverable', 'Safe to Send']:
                                if add_to_instantly(target_cam_id, c_email, c.get('FirstName'), c.get('LastName'), "Colleague"):
                                    email_sheet.append_row([c_email, datetime.now().isoformat()])
                                    processed_emails.append(c_email)
        else:
            print(f"🚫 {email} failed validation.")

if __name__ == "__main__":
    run_sync()
