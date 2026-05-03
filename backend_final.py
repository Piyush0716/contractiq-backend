from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests as req
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import json
from groq import Groq
from datetime import datetime, timedelta
from typing import Optional

# ── Config ─────────────────────────────────────────────────────
SUPABASE_URL = "https://wqaiziymroogggjyalqe.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndxYWl6aXltcm9vZ2dnanlhbHFlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NzY5NDc0MSwiZXhwIjoyMDkzMjcwNzQxfQ.yr_zuS5wXB2KkabMAmZbE0dvqvGz2VMWNrf6U__qUrg"
GROQ_KEY     = "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"
MODEL        = "llama-3.3-70b-versatile"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}
HEADERS_MIN = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

groq_client = Groq(api_key=GROQ_KEY)
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── DB helpers ─────────────────────────────────────────────────
def run_sql(sql: str) -> list:
    """Run raw SQL via Supabase RPC function"""
    for attempt in range(3):
        try:
            r = req.post(
                f"{SUPABASE_URL}/rest/v1/rpc/run_query",
                headers=HEADERS,
                json={"query": sql},
                timeout=60,
                verify=False
            )
            if r.status_code == 200:
                result = r.json()
                return result if isinstance(result, list) else []
            print(f"run_sql failed: {r.status_code} {r.text[:200]}")
            return []
        except Exception as e:
            print(f"run_sql attempt {attempt} error: {e}")
            if attempt == 2:
                return []
            import time; time.sleep(2)
    return []

def sb_get(table: str, params: dict) -> list:
    try:
        r = req.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, params=params, timeout=15, verify=False)
        return r.json() if r.status_code == 200 and isinstance(r.json(), list) else []
    except: return []

def sb_post(table: str, data: dict) -> dict:
    try:
        r = req.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=data, timeout=15, verify=False)
        result = r.json()
        return result[0] if isinstance(result, list) and result else result
    except: return {}

def sb_patch(table: str, match_col: str, match_val: str, data: dict) -> int:
    try:
        r = req.patch(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=HEADERS_MIN,
            params={match_col: f"eq.{match_val}"},
            json=data, timeout=15, verify=False
        )
        return r.status_code
    except: return 500

# ── Schema for LLM ─────────────────────────────────────────────
SCHEMA = """
PostgreSQL table: contracts (500K rows, one row = one contract line item)

KEY COLUMNS (use exact names, all lowercase):
contract_id, contract_line_number, previous_contract_id, renewal_sequence,
original_hardware_order_id, parent_company_id, parent_company_name,
contract_type (Hardware-led/Service-led), selling_type (Direct/Indirect),
channel_code (DIR-001/IND-D-001/IND-D-R-001/IND-D-R-R-001),
contract_tier (Enterprise/Commercial/SMB), master_agreement_id,
contract_start_date (TEXT YYYY-MM-DD), contract_end_date (TEXT YYYY-MM-DD),
contract_term_months (TEXT), contract_header_status (Active/Pending Approval),
contract_line_status, auto_renew_flag (Y/N — N means DQ issue),
dq_score (TEXT — cast to FLOAT), dq_failed_rules,
renewal_probability_score (TEXT),
contract_total_value_usd (TEXT), contract_line_value_usd (TEXT),
contract_annualised_value_usd (TEXT),
customer_id, customer_name, customer_tier, customer_region,
customer_subregion, customer_country, customer_city,
distributor_id, distributor_name, distributor_contact_email,
reseller_id, reseller_name, reseller_contact_email,
product_id, product_description,
product_line (Compute/Storage/Networking/Support/Cloud/Managed/Security/Analytics),
service_portfolio, coverage_type, coverage_response_sla,
asset_serial_number, asset_criticality, asset_install_date,
asset_age_months (TEXT), asset_city, asset_state, asset_country_code,
customer_contact_email (NULL in 47% = DQ violation),
hardware_user_contact_email (NULL in 47% = DQ violation),
reseller_contact_email (NULL in 38% = DQ violation),
distributor_contact_email (NULL in 3%),
hardware_order_id, hardware_order_status, hardware_delivery_date,
hardware_warranty_start_date, hardware_warranty_end_date,
hardware_unit_cost_usd (TEXT), hardware_total_cost_usd (TEXT),
quote_id, quote_date, quote_status,
case_id (NULL if no case), case_contract_ref, case_asset_serial_ref,
case_status, case_severity (1-Critical/2-High/3-Normal/4-Low),
case_group, case_reason, case_origin, case_resolution_type,
case_resolution_code, case_sla_breached (Y/N),
case_response_time_hours (TEXT), case_reopen_count (TEXT),
case_closed_date, case_service_portfolio, case_coverage_type,
case_location_city (may differ from asset_city = DQ issue),
case_location_country, case_contact_email, case_contact_full_name,
case_count_on_asset (TEXT)

CRITICAL SQL RULES:
- ALL numeric columns are TEXT — always cast: CAST(dq_score AS FLOAT), CAST(contract_annualised_value_usd AS FLOAT)
- Date filter: contract_end_date::DATE < NOW()::DATE + INTERVAL '90 days'
- NULL check: column IS NULL OR column = ''
- Always LIMIT 50
- Always WHERE contract_header_status='Active'
- Never use JOIN — all data is in one table, use GROUP BY
"""

# ── Persona prompts ─────────────────────────────────────────────
PROMPTS = {
    "customer": """You are a contract assistant helping enterprise customers understand their service contracts.

YOUR JOB:
- Answer questions about their contracts, asset serial numbers, coverage details, renewal dates
- Proactively identify gaps: missing contact emails, open cases, expiring coverage
- Recommend actions at renewal time: term options, coverage upgrades, what to watch out for
- Be helpful, clear, and action-oriented

STRICT RULES:
- Only show this customer's own data — never other customers
- NEVER mention: dq_score, auto_renew_flag, distributor names, reseller names, internal system IDs
- NEVER mention data quality issues in technical terms — frame as "we need to update your contact details"
- Answer ONLY what was asked — do not add unrequested analysis
- Keep responses concise and actionable""",

    "rep": """You are an AI assistant for sales representatives managing enterprise accounts.

YOUR JOB:
- Show customer portfolio view — contracts, renewal status, ARR, coverage gaps
- Give regional breakdowns when asked
- Recommend add-ons and upsell opportunities based on contract gaps or expiring coverage
- Flag accounts needing urgent attention (expiring, DQ issues affecting renewal)
- Help prepare renewal handoffs

STRICT RULES:
- Stay within your own account portfolio — no cross-rep visibility
- Answer ONLY what was asked — do not force DQ breakdowns when not relevant
- When recommending add-ons, base it on actual product gaps visible in the data
- Keep it commercial and customer-focused — not technical ops language""",

    "ops": """You are the Contract Intelligence Agent for Services Operations.

YOU HAVE TWO MODES based on what is asked:

MODE 1 — REQUEST PROCESSING:
When asked about renewal requests, approvals, or customer/rep submissions:
- Show pending requests, their status, routing details
- Help prepare SAP update details for approvals
- Track request flow: customer → rep → ops

MODE 2 — CONTRACT & DQ INTELLIGENCE:
When asked about data quality, contracts, cases, contacts, or analysis:
- Run deep analysis across all contracts
- For email/contact gaps: identify which field is missing, how many affected, valid fix sources only
  (case_contact_email is best source for hw_user_contact_email — NEVER suggest reseller/distributor email as hw_user fix)
- Detect patterns by customer, region, product line, distributor
- Give fixability breakdown ONLY when asked about contact/email issues
- For other questions: answer directly without forcing the fix framework

STRICT RULES:
- Answer ONLY what was asked — do not add unrequested DQ breakdowns
- Base all numbers on actual query results — never estimate or assume
- If data shows 0 rows or unexpected results, say so clearly rather than guessing
- End with one specific recommended action relevant to the question""",

    "leadership": """You are the executive Contract Intelligence Agent for Services leadership.

YOU HAVE TWO MODES:

MODE 1 — DATA SUMMARY (/datasummary):
When asked for DQ overview, confidence index, or data health:
- Overall DQ confidence score across all contracts
- Rule-by-rule breakdown: which rules are failing and by how much
- Regional heatmap: which geographies have worst data quality
- Trend direction: improving or declining
- Revenue at risk from DQ failures

MODE 2 — BUSINESS STATUS (/businessstatus):
When asked for renewal outlook, contract scoring, or business health:
- Renewal pipeline: contracts expiring in 90/180 days, ARR at risk
- Contract health scoring by customer tier, region, product line
- Patterns and trends: what's improving, what's declining
- Forward-looking recommendations: where to focus to protect revenue

GENERAL RULES:
- Always connect data findings to business impact (revenue, renewal probability)
- Give strategic recommendations — not operational task lists
- Answer ONLY what was asked — do not dump all metrics every time
- Be concise: executives want insight, not data tables
- If asked about specific customers or contracts, provide that detail"""
}

# ── Pre-built queries ──────────────────────────────────────────
def prebuilt(key: str, customer_filter: str = "") -> list:
    cf = f"AND customer_id='{customer_filter}'" if customer_filter else ""
    queries = {
        "dq_customers": f"""
            SELECT customer_name, customer_region, customer_tier,
                   COUNT(DISTINCT contract_id) as contracts,
                   SUM(CASE WHEN auto_renew_flag='N' THEN 1 ELSE 0 END) as dq_failures,
                   SUM(CASE WHEN auto_renew_flag='Y' THEN 1 ELSE 0 END) as dq_pass,
                   COUNT(*) as total_rows
            FROM contracts WHERE contract_header_status='Active' {cf}
            GROUP BY customer_name,customer_region,customer_tier
            ORDER BY dq_failures DESC LIMIT 15""",

        "dq_distributors": f"""
            SELECT distributor_name,
                   COUNT(DISTINCT contract_id) as contracts,
                   COUNT(DISTINCT customer_name) as customers,
                   ROUND(AVG(NULLIF(dq_score,'')::FLOAT)::NUMERIC,1) as avg_dq,
                   SUM(CASE WHEN auto_renew_flag='N' THEN 1 ELSE 0 END) as failures,
                   ROUND(SUM(NULLIF(contract_annualised_value_usd,'')::FLOAT)::NUMERIC,0) as arr
            FROM contracts WHERE contract_header_status='Active'
            AND distributor_name IS NOT NULL AND distributor_name != ''
            GROUP BY distributor_name ORDER BY failures DESC LIMIT 10""",

        "renewal_pipeline": f"""
            SELECT customer_name, customer_region, contract_id,
                   contract_end_date,
                   auto_renew_flag, dq_score, renewal_probability_score,
                   contract_annualised_value_usd as arr,
                   selling_type, channel_code
            FROM contracts WHERE contract_header_status='Active' {cf}
            AND contract_end_date IS NOT NULL AND contract_end_date != ''
            AND contract_end_date <= TO_CHAR(NOW() + INTERVAL '180 days','YYYY-MM-DD')
            ORDER BY contract_end_date ASC LIMIT 25""",

        "email_dq": f"""
            SELECT
                COUNT(*) as total_active,
                SUM(CASE WHEN customer_contact_email IS NULL OR customer_contact_email='' THEN 1 ELSE 0 END) as customer_email_blank,
                SUM(CASE WHEN hardware_user_contact_email IS NULL OR hardware_user_contact_email='' THEN 1 ELSE 0 END) as hw_user_blank,
                SUM(CASE WHEN reseller_contact_email IS NULL OR reseller_contact_email='' THEN 1 ELSE 0 END) as reseller_blank,
                SUM(CASE WHEN (hardware_user_contact_email IS NULL OR hardware_user_contact_email='')
                    AND (case_contact_email IS NOT NULL AND case_contact_email != '') THEN 1 ELSE 0 END) as fixable_from_case,
                SUM(CASE WHEN (hardware_user_contact_email IS NULL OR hardware_user_contact_email='')
                    AND (case_contact_email IS NULL OR case_contact_email='')
                    AND (customer_contact_email IS NOT NULL AND customer_contact_email != '') THEN 1 ELSE 0 END) as fixable_from_customer,
                SUM(CASE WHEN (hardware_user_contact_email IS NULL OR hardware_user_contact_email='')
                    AND (case_contact_email IS NULL OR case_contact_email='')
                    AND (customer_contact_email IS NULL OR customer_contact_email='') THEN 1 ELSE 0 END) as truly_orphaned
            FROM contracts WHERE contract_header_status='Active' {cf}""",

        "case_by_product": f"""
            SELECT product_line, product_description,
                   COUNT(DISTINCT case_id) as cases,
                   COUNT(DISTINCT contract_id) as contracts_affected,
                   SUM(CASE WHEN case_sla_breached='Y' THEN 1 ELSE 0 END) as sla_breaches,
                   ROUND(AVG(NULLIF(case_response_time_hours,'')::FLOAT)::NUMERIC,1) as avg_response_hrs,
                   ROUND(AVG(NULLIF(case_reopen_count,'')::FLOAT)::NUMERIC,2) as avg_reopens
            FROM contracts WHERE case_id IS NOT NULL AND case_id != '' {cf}
            GROUP BY product_line,product_description
            ORDER BY cases DESC LIMIT 20""",

        "location_mismatch": f"""
            SELECT customer_region,
                   COUNT(*) as total_cases,
                   SUM(CASE WHEN LOWER(TRIM(case_location_city)) != LOWER(TRIM(asset_city)) THEN 1 ELSE 0 END) as mismatches,
                   ROUND(100.0*SUM(CASE WHEN LOWER(TRIM(case_location_city)) != LOWER(TRIM(asset_city)) THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0)::NUMERIC,1) as mismatch_pct
            FROM contracts
            WHERE case_id IS NOT NULL AND case_location_city IS NOT NULL AND asset_city IS NOT NULL {cf}
            GROUP BY customer_region ORDER BY mismatch_pct DESC""",

        "portfolio_summary": f"""
            SELECT COUNT(DISTINCT contract_id) as total_contracts,
                   COUNT(DISTINCT customer_name) as total_customers,
                   COUNT(DISTINCT distributor_name) as total_distributors,
                   COUNT(DISTINCT reseller_name) as total_resellers,
                   ROUND(AVG(NULLIF(dq_score,'')::FLOAT)::NUMERIC,1) as avg_dq,
                   SUM(CASE WHEN auto_renew_flag='N' THEN 1 ELSE 0 END) as dq_failures,
                   SUM(CASE WHEN case_sla_breached='Y' THEN 1 ELSE 0 END) as sla_breaches,
                   ROUND(SUM(NULLIF(contract_annualised_value_usd,'')::FLOAT)::NUMERIC,0) as total_arr
            FROM contracts WHERE contract_header_status='Active' {cf}""",

        "regional_breakdown": f"""
            SELECT customer_region,
                   COUNT(DISTINCT contract_id) as contracts,
                   COUNT(DISTINCT customer_name) as customers,
                   ROUND(AVG(NULLIF(dq_score,'')::FLOAT)::NUMERIC,1) as avg_dq,
                   SUM(CASE WHEN auto_renew_flag='N' THEN 1 ELSE 0 END) as failures,
                   SUM(CASE WHEN customer_contact_email IS NULL OR customer_contact_email='' THEN 1 ELSE 0 END) as email_blank,
                   ROUND(SUM(CAST(contract_annualised_value_usd AS FLOAT))::NUMERIC,0) as arr
            FROM contracts WHERE contract_header_status='Active' {cf}
            GROUP BY customer_region ORDER BY failures DESC""",

        "parent_company": f"""
            SELECT parent_company_name,
                   COUNT(DISTINCT customer_name) as subsidiaries,
                   COUNT(DISTINCT contract_id) as contracts,
                   ROUND(AVG(NULLIF(dq_score,'')::FLOAT)::NUMERIC,1) as avg_dq,
                   SUM(CASE WHEN auto_renew_flag='N' THEN 1 ELSE 0 END) as failures,
                   ROUND(SUM(CAST(contract_annualised_value_usd AS FLOAT))::NUMERIC,0) as arr
            FROM contracts WHERE contract_header_status='Active'
            AND parent_company_name IS NOT NULL AND parent_company_name != ''
            GROUP BY parent_company_name ORDER BY failures DESC LIMIT 15""",

        "product_summary": f"""
            SELECT product_line,
                   COUNT(DISTINCT contract_id) as contracts,
                   ROUND(SUM(CAST(contract_annualised_value_usd AS FLOAT))::NUMERIC,0) as arr,
                   ROUND(AVG(NULLIF(dq_score,'')::FLOAT)::NUMERIC,1) as avg_dq,
                   SUM(CASE WHEN case_id IS NOT NULL THEN 1 ELSE 0 END) as total_cases,
                   SUM(CASE WHEN case_sla_breached='Y' THEN 1 ELSE 0 END) as sla_breaches
            FROM contracts WHERE contract_header_status='Active' {cf}
            GROUP BY product_line ORDER BY contracts DESC""",
    }
    return run_sql(queries[key]) if key in queries else []

# ── Intent classifier ──────────────────────────────────────────
def classify(q: str) -> str:
    q = q.lower()
    if any(w in q for w in ['parent company','parent companies','holding','global group']): return 'parent_company'
    if any(w in q for w in ['region','geography','apac','emea','americas','india','country']): return 'regional_breakdown'
    if any(w in q for w in ['product','what do customer buy','product type','product line']): return 'product_summary'
    if any(w in q for w in ['distributor','channel partner']) and any(w in q for w in ['dq','quality','issue','problem','failure']): return 'dq_distributors'
    if any(w in q for w in ['worst dq','dq score','data quality','dq failure','failing']) and any(w in q for w in ['customer','company','account']): return 'dq_customers'
    if any(w in q for w in ['renew','expir','pipeline','upcoming','due','90 day','180 day']): return 'renewal_pipeline'
    if any(w in q for w in ['email','contact','blank','missing','hw user','hardware user','contact email']): return 'email_dq'
    if any(w in q for w in ['case','incident','ticket','raised']) and any(w in q for w in ['product','type']): return 'case_by_product'
    if any(w in q for w in ['location','city','address','mismatch','different location','wrong city']): return 'location_mismatch'
    if any(w in q for w in ['summary','overview','portfolio','total','how many','overall']): return 'portfolio_summary'
    return 'llm'

# ── LLM SQL generation with retry ─────────────────────────────
def llm_sql(question: str, persona: str, customer_id: str = None) -> list:
    cf = f"AND customer_id='{customer_id}'" if customer_id and persona=='customer' else ""
    persona_note = ""
    if persona == 'customer':
        persona_note = f"IMPORTANT: Only return data for customer_id='{customer_id}'. Do NOT include dq_score, auto_renew_flag, distributor_name, reseller_name."
    elif persona == 'leadership':
        persona_note = "Return aggregated totals only — no individual contract_id or customer details."

    for attempt in range(3):
        prompt = f"""{SCHEMA}

Question: {question}
{persona_note}

Generate ONE PostgreSQL SELECT query to answer this question.
- Always WHERE contract_header_status='Active' {cf}
- Cast all numeric TEXT columns to FLOAT
- Use date::DATE for date comparisons
- LIMIT 50
- Return ONLY the SQL, no markdown, no explanation."""

        resp = groq_client.chat.completions.create(
            model=MODEL,
            messages=[{"role":"user","content":prompt}],
            max_tokens=400, temperature=0.1
        )
        sql = resp.choices[0].message.content.strip().replace("```sql","").replace("```","").strip()
        result = run_sql(sql)
        if result is not None and len(result) > 0:
            return result
        # If empty/failed, retry with simpler prompt
        if attempt < 2:
            question = f"Simple version: {question} — use only basic GROUP BY and COUNT"

    return []

# ── Core agent ─────────────────────────────────────────────────
def agent_answer(question: str, persona: str, customer_id: str, history: list) -> str:
    intent = classify(question)
    cf = customer_id if persona == 'customer' else None
    data = []

    # Try pre-built first
    if intent != 'llm':
        data = prebuilt(intent, cf or "")

    # If pre-built failed or empty, try LLM SQL
    if not data:
        data = llm_sql(question, persona, customer_id)

    # If still empty, use REST API direct for basic summary
    if not data:
        rest_data = sb_get("contracts", {
            "contract_header_status": "eq.Active",
            "select": "customer_name,customer_region,customer_tier,auto_renew_flag,dq_score,contract_annualised_value_usd,distributor_name,product_line,case_id,case_sla_breached,contract_end_date",
            "limit": "500",
            "order": "dq_score.asc"
        })
        if rest_data:
            # Aggregate in Python
            from collections import defaultdict
            if any(w in question.lower() for w in ['customer','worst','dq','quality']):
                agg = defaultdict(lambda:{'contracts':0,'failures':0,'arr':0,'region':''})
                for c in rest_data:
                    n = c.get('customer_name','Unknown')
                    agg[n]['contracts'] += 1
                    agg[n]['failures'] += 1 if c.get('auto_renew_flag')=='N' else 0
                    agg[n]['arr'] += float(c.get('contract_annualised_value_usd') or 0)
                    agg[n]['region'] = c.get('customer_region','')
                data = [{"customer_name":k,"contracts":v['contracts'],"dq_failures":v['failures'],
                         "arr":round(v['arr'],0),"region":v['region']}
                        for k,v in sorted(agg.items(), key=lambda x:-x[1]['failures'])[:15]]
            elif any(w in question.lower() for w in ['product','case']):
                agg = defaultdict(lambda:{'cases':0,'sla':0})
                for c in rest_data:
                    pl = c.get('product_line','Unknown')
                    agg[pl]['cases'] += 1 if c.get('case_id') else 0
                    agg[pl]['sla'] += 1 if c.get('case_sla_breached')=='Y' else 0
                data = [{"product_line":k,"cases":v['cases'],"sla_breaches":v['sla']}
                        for k,v in sorted(agg.items(), key=lambda x:-x[1]['cases'])]
            else:
                data = rest_data[:20]

    context = f"Query intent: {intent}\nRows returned: {len(data)}\nData:\n{json.dumps(data[:25], default=str, indent=2)}"

    messages = [{"role":"system","content":PROMPTS[persona]}]
    for h in history[-6:]:
        messages.append(h)
    messages.append({"role":"user","content":f"""Question: {question}

{context}

Answer the question directly using the data above.
- Use exact numbers from the data
- Only answer what was asked — do not add analysis about unrelated fields
- If question is about products: answer about products
- If question is about location: answer about location
- If question is about emails/contacts: then mention fix sources
- End with one specific recommended action related to the question"""})

    resp = groq_client.chat.completions.create(model=MODEL, messages=messages, max_tokens=900)
    return resp.choices[0].message.content

# ── Models ─────────────────────────────────────────────────────
class QueryReq(BaseModel):
    question: str
    persona: str = "ops"
    customer_id: Optional[str] = None
    chat_history: Optional[list] = []

class RenewalReq(BaseModel):
    contract_id: str
    customer_id: str
    customer_name: str
    requested_by: str
    renewal_term_months: int = 24
    selling_type: str = "Direct"
    channel_code: Optional[str] = None
    notes: Optional[str] = None

class ApprovalReq(BaseModel):
    request_id: int
    action: str
    ops_user: str = "Ops Team"
    notes: Optional[str] = None

class ForwardReq(BaseModel):
    request_id: int
    rep_user: str = "Sales Rep"

# ── Endpoints ──────────────────────────────────────────────────
@app.post("/query")
def query(r: QueryReq):
    answer = agent_answer(r.question, r.persona, r.customer_id or "", r.chat_history or [])
    return {"answer": answer, "persona": r.persona}

@app.post("/raise-request")
def raise_request(r: RenewalReq):
    data = {
        "request_type": "renewal",
        "contract_id": r.contract_id,
        "customer_id": r.customer_id,
        "customer_name": r.customer_name,
        "requested_by": r.requested_by,
        "selling_type": r.selling_type,
        "channel_code": r.channel_code,
        "renewal_term_months": r.renewal_term_months,
        "notes": r.notes,
        "status": "sent_to_ops" if r.selling_type=="Direct" else "forwarded_to_rep",
        "requested_at": datetime.now().isoformat()
    }
    result = sb_post("requests", data)
    request_id = result.get('id') if result else None

    persona = "ops" if r.selling_type=="Direct" else "rep"
    sb_post("notifications", {
        "persona": persona,
        "type": "renewal_request",
        "title": f"Renewal — {r.customer_name}",
        "message": f"{r.customer_name} raised {r.renewal_term_months}m renewal for {r.contract_id}. {'Direct → Ops.' if r.selling_type=='Direct' else f'Indirect via {r.channel_code} → Rep review.'}",
        "contract_id": r.contract_id,
        "customer_id": r.customer_id,
        "request_id": request_id,
        "priority": "high",
        "is_read": False
    })
    if r.selling_type != "Direct":
        sb_post("notifications", {
            "persona": "rep",
            "type": "renewal_request",
            "title": f"Customer renewal — {r.customer_name}",
            "message": f"Indirect customer {r.customer_name} raised renewal for {r.contract_id}. Your review needed before Ops.",
            "contract_id": r.contract_id,
            "customer_id": r.customer_id,
            "priority": "high",
            "is_read": False
        })
    return {"success": True, "request_id": request_id, "routed_to": persona}

@app.post("/forward-request")
def forward_request(r: ForwardReq):
    sb_patch("requests", "id", str(r.request_id), {"status": "sent_to_ops"})
    reqs = sb_get("requests", {"id": f"eq.{r.request_id}", "select": "*"})
    if reqs:
        rd = reqs[0]
        sb_post("notifications", {
            "persona": "ops",
            "type": "renewal_request",
            "title": f"Forwarded renewal — {rd.get('customer_name')}",
            "message": f"Rep forwarded renewal for {rd.get('contract_id')} from {rd.get('customer_name')}. Ready for Ops approval.",
            "contract_id": rd.get('contract_id'),
            "customer_id": rd.get('customer_id'),
            "request_id": r.request_id,
            "priority": "high",
            "is_read": False
        })
    return {"success": True}

@app.post("/approve-request")
def approve_request(r: ApprovalReq):
    sb_patch("requests", "id", str(r.request_id), {
        "status": "approved" if r.action=="approve" else "rejected",
        "assigned_ops": r.ops_user,
        "notes": r.notes
    })
    reqs = sb_get("requests", {"id": f"eq.{r.request_id}", "select": "*"})
    if reqs:
        rd = reqs[0]
        sb_post("notifications", {
            "persona": "customer",
            "type": "approval_update",
            "title": f"Renewal {r.action}d — {rd.get('contract_id')}",
            "message": f"Your renewal request for contract {rd.get('contract_id')} has been {r.action}d by our Services team.",
            "contract_id": rd.get('contract_id'),
            "customer_id": rd.get('customer_id'),
            "priority": "high",
            "is_read": False
        })
        sb_post("notifications", {
            "persona": "rep",
            "type": "approval_update",
            "title": f"Ops {r.action}d — {rd.get('customer_name')}",
            "message": f"Ops {r.action}d renewal for {rd.get('contract_id')} ({rd.get('customer_name')}).",
            "contract_id": rd.get('contract_id'),
            "customer_id": rd.get('customer_id'),
            "priority": "normal",
            "is_read": False
        })
    return {"success": True, "action": r.action}

@app.post("/notifications/read/{notif_id}")
def mark_read(notif_id: int):
    sb_patch("notifications", "id", str(notif_id), {"is_read": True})
    return {"success": True}

@app.get("/notifications/{persona}")
def get_notifications(persona: str):
    return sb_get("notifications", {
        "persona": f"eq.{persona}",
        "is_read": "eq.false",
        "order": "created_at.desc",
        "limit": "20"
    })

@app.get("/requests")
def get_requests(status: Optional[str] = None):
    params = {"order": "requested_at.desc", "limit": "50", "select": "*"}
    if status: params["status"] = f"eq.{status}"
    return sb_get("requests", params)

@app.patch("/requests/{request_id}")
def patch_request(request_id: int, data: dict):
    sb_patch("requests", "id", str(request_id), data)
    return {"success": True}

@app.get("/contracts/{customer_id}")
def get_contracts(customer_id: str):
    return sb_get("contracts", {
        "customer_id": f"eq.{customer_id}",
        "contract_header_status": "eq.Active",
        "select": "contract_id,contract_line_number,contract_type,contract_start_date,contract_end_date,contract_term_months,contract_annualised_value_usd,renewal_probability_score,product_description,service_portfolio,coverage_type,coverage_response_sla,selling_type,channel_code,asset_serial_number",
        "order": "contract_end_date.asc",
        "limit": "100"
    })

@app.get("/customers")
def get_customers():
    result = run_sql("""
        SELECT DISTINCT customer_id, customer_name, customer_tier,
               customer_region, parent_company_name
        FROM contracts
        WHERE contract_header_status='Active'
        AND customer_id IS NOT NULL AND customer_id != ''
        ORDER BY customer_name LIMIT 100
    """)
    return result or []

@app.get("/dq/summary")
def dq_summary():
    result = run_sql("""
        SELECT
            COUNT(*) as total_active,
            ROUND(AVG(NULLIF(dq_score,'')::FLOAT)::NUMERIC,1) as avg_dq,
            SUM(CASE WHEN auto_renew_flag='N' THEN 1 ELSE 0 END) as dq_failures,
            SUM(CASE WHEN customer_contact_email IS NULL OR customer_contact_email='' THEN 1 ELSE 0 END) as customer_email_blank,
            SUM(CASE WHEN hardware_user_contact_email IS NULL OR hardware_user_contact_email='' THEN 1 ELSE 0 END) as hw_user_email_blank,
            SUM(CASE WHEN reseller_contact_email IS NULL OR reseller_contact_email='' THEN 1 ELSE 0 END) as reseller_email_blank,
            SUM(CASE WHEN case_sla_breached='Y' THEN 1 ELSE 0 END) as sla_breaches,
            SUM(CASE WHEN contract_end_date::DATE < NOW()::DATE + INTERVAL '90 days' THEN 1 ELSE 0 END) as renewal_due_90d,
            SUM(CASE WHEN contract_end_date::DATE < NOW()::DATE + INTERVAL '180 days' THEN 1 ELSE 0 END) as renewal_due_180d,
            ROUND(SUM(NULLIF(contract_annualised_value_usd,'')::FLOAT)::NUMERIC,0) as total_arr
        FROM contracts WHERE contract_header_status='Active'
    """)
    return result[0] if result else {}

@app.get("/health")
def health():
    r = req.get(f"{SUPABASE_URL}/rest/v1/contracts", headers=HEADERS, params={"limit":"1","select":"contract_id"}, timeout=10)
    return {"status": "ok" if r.status_code==200 else "db_error", "model": MODEL, "db": r.status_code}

@app.get("/dq/rules")
def dq_rules():
    # Query 1: Blank/null checks
    q1 = run_sql("""
        SELECT COUNT(*) as total,
        SUM(CASE WHEN customer_id IS NOT NULL AND customer_id!='' THEN 1 ELSE 0 END) as customer_id_ok,
        SUM(CASE WHEN customer_name IS NOT NULL AND customer_name!='' THEN 1 ELSE 0 END) as customer_name_ok,
        SUM(CASE WHEN customer_contact_email IS NOT NULL AND customer_contact_email!='' THEN 1 ELSE 0 END) as customer_email_ok,
        SUM(CASE WHEN hardware_user_contact_email IS NOT NULL AND hardware_user_contact_email!='' THEN 1 ELSE 0 END) as hw_user_email_ok,
        SUM(CASE WHEN reseller_contact_email IS NOT NULL AND reseller_contact_email!='' THEN 1 ELSE 0 END) as reseller_email_ok,
        SUM(CASE WHEN distributor_contact_email IS NOT NULL AND distributor_contact_email!='' THEN 1 ELSE 0 END) as distributor_email_ok,
        SUM(CASE WHEN product_id IS NOT NULL AND product_id!='' THEN 1 ELSE 0 END) as product_id_ok,
        SUM(CASE WHEN asset_serial_number IS NOT NULL AND asset_serial_number!='' THEN 1 ELSE 0 END) as asset_serial_ok,
        SUM(CASE WHEN asset_city IS NOT NULL AND asset_city!='' THEN 1 ELSE 0 END) as asset_city_ok,
        SUM(CASE WHEN contract_start_date IS NOT NULL AND contract_start_date!='' THEN 1 ELSE 0 END) as start_date_ok,
        SUM(CASE WHEN contract_end_date IS NOT NULL AND contract_end_date!='' THEN 1 ELSE 0 END) as end_date_ok,
        SUM(CASE WHEN service_portfolio IS NOT NULL AND service_portfolio!='' THEN 1 ELSE 0 END) as portfolio_ok,
        SUM(CASE WHEN coverage_type IS NOT NULL AND coverage_type!='' THEN 1 ELSE 0 END) as coverage_ok
        FROM contracts WHERE contract_header_status='Active'
    """)

    # Query 2: Mismatch checks
    q2 = run_sql("""
        SELECT COUNT(*) as total,
        SUM(CASE WHEN contract_type='Service-led' AND (quote_id IS NOT NULL AND quote_id!='') THEN 1
                 WHEN contract_type!='Service-led' THEN 1 ELSE 0 END) as quote_match_ok,
        SUM(CASE WHEN contract_type='Hardware-led' AND (hardware_order_id IS NOT NULL AND hardware_order_id!='') THEN 1
                 WHEN contract_type!='Hardware-led' THEN 1 ELSE 0 END) as hw_order_match_ok,
        SUM(CASE WHEN case_id IS NULL THEN 1
                 WHEN case_id IS NOT NULL AND case_location_city IS NOT NULL AND asset_city IS NOT NULL
                      AND LOWER(TRIM(case_location_city))=LOWER(TRIM(asset_city)) THEN 1
                 ELSE 0 END) as location_match_ok,
        SUM(CASE WHEN case_id IS NULL THEN 1
                 WHEN case_id IS NOT NULL AND case_contact_email IS NOT NULL AND case_contact_email!='' THEN 1
                 ELSE 0 END) as case_contact_ok,
        SUM(CASE WHEN selling_type='Direct' AND (distributor_id IS NULL OR distributor_id='') THEN 1
                 WHEN selling_type!='Direct' AND distributor_id IS NOT NULL AND distributor_id!='' THEN 1
                 ELSE 0 END) as channel_distributor_ok,
        SUM(CASE WHEN distributor_id IS NULL OR reseller_id IS NULL OR distributor_id!=reseller_id THEN 1 ELSE 0 END) as dist_ne_reseller_ok,
        SUM(CASE WHEN distributor_id IS NULL OR distributor_id!=customer_id THEN 1 ELSE 0 END) as dist_ne_customer_ok
        FROM contracts WHERE contract_header_status='Active'
    """)

    if not q1 or not q2:
        return {"error": "Query failed"}

    d1 = q1[0]; d2 = q2[0]
    total = int(d1.get('total') or 1)
    def sc(v): return round(int(v or 0)/total*100, 1)

    rules = [
        # Completeness
        {"category":"Completeness","code":"R01","name":"Customer ID not blank","score":sc(d1['customer_id_ok'])},
        {"category":"Completeness","code":"R02","name":"Customer name not blank","score":sc(d1['customer_name_ok'])},
        {"category":"Completeness","code":"R03","name":"Customer contact email not blank","score":sc(d1['customer_email_ok'])},
        {"category":"Completeness","code":"R04","name":"Hardware user email not blank","score":sc(d1['hw_user_email_ok'])},
        {"category":"Completeness","code":"R05","name":"Reseller contact email not blank","score":sc(d1['reseller_email_ok'])},
        {"category":"Completeness","code":"R06","name":"Distributor contact email not blank","score":sc(d1['distributor_email_ok'])},
        {"category":"Completeness","code":"R07","name":"Product ID not blank","score":sc(d1['product_id_ok'])},
        {"category":"Completeness","code":"R08","name":"Asset serial number not blank","score":sc(d1['asset_serial_ok'])},
        {"category":"Completeness","code":"R09","name":"Asset city not blank","score":sc(d1['asset_city_ok'])},
        {"category":"Completeness","code":"R10","name":"Contract start date not blank","score":sc(d1['start_date_ok'])},
        {"category":"Completeness","code":"R11","name":"Contract end date not blank","score":sc(d1['end_date_ok'])},
        {"category":"Completeness","code":"R12","name":"Service portfolio not blank","score":sc(d1['portfolio_ok'])},
        {"category":"Completeness","code":"R13","name":"Coverage type not blank","score":sc(d1['coverage_ok'])},
        # Mismatch
        {"category":"Mismatch","code":"R14","name":"Service-led contract has quote","score":sc(d2['quote_match_ok'])},
        {"category":"Mismatch","code":"R15","name":"Hardware-led contract has HW order","score":sc(d2['hw_order_match_ok'])},
        {"category":"Mismatch","code":"R16","name":"Case location city matches asset city","score":sc(d2['location_match_ok'])},
        {"category":"Mismatch","code":"R17","name":"Case has contact email","score":sc(d2['case_contact_ok'])},
        # Channel
        {"category":"Channel","code":"R18","name":"Direct/Indirect distributor alignment","score":sc(d2['channel_distributor_ok'])},
        {"category":"Channel","code":"R19","name":"Distributor ≠ Reseller","score":sc(d2['dist_ne_reseller_ok'])},
        {"category":"Channel","code":"R20","name":"Distributor ≠ End Customer","score":sc(d2['dist_ne_customer_ok'])},
    ]

    from collections import defaultdict
    cats = defaultdict(list)
    for r in rules: cats[r['category']].append(r['score'])
    categories = [{"category":cat,"overall_score":round(sum(s)/len(s),1),
                   "rules":[r for r in rules if r['category']==cat]} for cat,s in cats.items()]
    overall = round(sum(r['score'] for r in rules)/len(rules),1)

    return {
        "total_contracts": total,
        "overall_dq_score": overall,
        "critical_rules":  len([r for r in rules if r['score']<70]),
        "warning_rules":   len([r for r in rules if 70<=r['score']<90]),
        "healthy_rules":   len([r for r in rules if r['score']>=90]),
        "categories": categories,
        "all_rules": rules
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
