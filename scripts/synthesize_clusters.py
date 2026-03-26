"""
synthesize_clusters.py — Batch-update cluster_ids across all context records.

Applies:
1. Same-URL dedup links
2. Same-event dedup links
3. Existing cluster expansions (records joining previously-defined clusters)
4. Thematic clusters (cross-source and narrative groupings)

Usage:
    python scripts/synthesize_clusters.py             # apply all updates
    python scripts/synthesize_clusters.py --dry-run   # preview changes
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STORE = PROJECT_ROOT / "context-store"


def load_record(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_record(path, record):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Define all cluster assignments
# ---------------------------------------------------------------------------

# SAME-URL DEDUP GROUPS — link records from the same source URL
SAME_URL_DEDUP = {
    "DEDUP-URL-WEB-SOUTH-AVE-BRIDGE": [
        "CTX-WEB-2026-00001", "CTX-WEB-2026-00017",
        "CTX-WEB-2026-00026", "CTX-WEB-2026-00028",
    ],
    "DEDUP-URL-WEB-VEO-RETURN": [
        "CTX-WEB-2026-00005", "CTX-WEB-2026-00020", "CTX-WEB-2026-00029",
    ],
    "DEDUP-URL-WEB-SENATE-BUDGET": [
        "CTX-WEB-2026-00003", "CTX-WEB-2026-00018", "CTX-WEB-2026-00027",
    ],
    "DEDUP-URL-WEB-KIRKPATRICK-BRIDGE": [
        "CTX-WEB-2026-00008", "CTX-WEB-2026-00023", "CTX-WEB-2026-00030",
    ],
    "DEDUP-URL-WEB-BRAVEST-FINEST": [
        "CTX-WEB-2026-00004", "CTX-WEB-2026-00019",
    ],
    "DEDUP-URL-WEB-ST-PATRICKS": [
        "CTX-WEB-2026-00006", "CTX-WEB-2026-00021",
    ],
    "DEDUP-URL-WEB-COMMUNITY-SOLAR": [
        "CTX-WEB-2026-00009", "CTX-WEB-2026-00024",
    ],
    "DEDUP-URL-WEB-LEAD-STANDARD": [
        "CTX-WEB-2026-00010", "CTX-WEB-2026-00025",
    ],
    "DEDUP-URL-WEB-FINANCE-SEARCH": [
        "CTX-WEB-2026-00007", "CTX-WEB-2026-00022",
    ],
    "DEDUP-URL-NEWS-COUNCIL-BALLOT": [
        "CTX-NEWS-2025-00177", "CTX-NEWS-2025-00179",
    ],
    "DEDUP-URL-NEWS-WAGE-ORDINANCE": [
        "CTX-NEWS-2025-00207", "CTX-NEWS-2025-00257",
    ],
    "DEDUP-URL-NEWS-STATE-AID": [
        "CTX-NEWS-2025-00176", "CTX-NEWS-2025-00178",
    ],
    "DEDUP-URL-NEWS-PLAN-A-VISIT": [
        "CTX-NEWS-2022-00001", "CTX-NEWS-2022-00002",
    ],
}

# SAME-EVENT DEDUP — different articles covering the same event
SAME_EVENT_DEDUP = {
    "DEDUP-EVENT-SANCTUARY-CITY": [
        "CTX-NEWS-2025-00190", "CTX-NEWS-2025-00271",
    ],
    "DEDUP-EVENT-SURGE-LINK": [
        "CTX-NEWS-2025-00078", "CTX-NEWS-2025-00079",
    ],
    "DEDUP-EVENT-AIRPORT-NOEM-VIDEO": [
        "CTX-NEWS-2025-00331",  # Airport isn't showing Noem video
        "CTX-NEWS-2025-00347",  # Why airport rejected Noem video (follow-up)
    ],
}

# EXISTING CLUSTER EXPANSIONS — records joining previously-defined clusters
CLUSTER_EXPANSIONS = {
    "CLUSTER-PAYROLL-MODERNIZATION-2026Q1": [
        "CTX-NEWS-2025-00038", "CTX-NEWS-2025-00057",
        "CTX-NEWS-2025-00061", "CTX-NEWS-2025-00129",
        "CTX-NEWS-2025-00317",  # Hundreds of teachers shorted on pay after $11M tech screwup
        "CTX-NEWS-2025-00324",  # NY AG ends whistleblower review of Syracuse IT project
        "CTX-NEWS-2025-00360",  # Consultants, waivers, lack of bids in IT overhaul
        "CTX-NEWS-2025-00006",  # Council questions Walsh administration over unfinished IT project
        "CTX-COUNCIL-2026-00021",  # IT Position Transfer from Parks to IT
        "CTX-COUNCIL-2026-00101",  # IT Vendor Extension Pending RFP
        "CTX-COUNCIL-2026-00133",  # IT Vendor License RFP Waiver
    ],
    "CLUSTER-VEO-MICROMOBILITY-2026Q1": [
        "CTX-WEB-2026-00005",
        "CTX-WEB-2026-00093",  # Return of Full Fleet of Veo
    ],
    "CLUSTER-LEAD-REMEDIATION-2026Q1": [
        "CTX-WEB-2026-00010", "CTX-WEB-2026-00025", "CTX-WEB-2026-00033",
        "CTX-NEWS-2025-00368",  # County/city unprecedented lead contamination investment
        "CTX-WEB-2026-00069",  # Water Filter Distribution Progress
        "CTX-WEB-2026-00071",  # Water Meets EPA Lead Standard
        "CTX-WEB-2026-00076",  # Water Service Inventory Release
        "CTX-WEB-2026-00092",  # Water Dept Public Meeting April 4
    ],
    "CLUSTER-AXON-LPR-2026Q1": [
        "CTX-COUNCIL-2026-00039", "CTX-COUNCIL-2026-00040",
        "CTX-NEWS-2025-00300",  # Politicians want to cut ties with Flock Safety LPR
    ],
    "CLUSTER-FIRST-RESPONDER-WELLNESS-2026Q1": [
        "CTX-COUNCIL-2026-00016", "CTX-COUNCIL-2026-00017",
        "CTX-COUNCIL-2026-00018", "CTX-COUNCIL-2026-00144",
    ],
    "CLUSTER-EV-CHARGING-PILOT-2026Q1": [
        "CTX-COUNCIL-2026-00036", "CTX-COUNCIL-2026-00129",
        "CTX-WEB-2026-00009",
    ],
    "CLUSTER-OCRRA-BATTERY-RECYCLING-2026Q1": [
        "CTX-COUNCIL-2026-00014", "CTX-COUNCIL-2026-00143",
        "CTX-COUNCIL-2026-00146",
    ],
}

# THEMATIC CLUSTERS
NEW_CLUSTERS = {
    # -----------------------------------------------------------------------
    # Existing clusters (from prior sessions) — expanded with new records
    # -----------------------------------------------------------------------
    "CLUSTER-BUDGET-BATTLE-2025Q2": [
        "CTX-NEWS-2025-00001", "CTX-NEWS-2025-00002", "CTX-NEWS-2025-00007",
        "CTX-NEWS-2025-00014", "CTX-NEWS-2025-00020", "CTX-NEWS-2025-00021",
        "CTX-NEWS-2025-00024", "CTX-NEWS-2025-00025", "CTX-NEWS-2025-00026",
        "CTX-NEWS-2025-00027", "CTX-NEWS-2025-00029", "CTX-NEWS-2025-00032",
        "CTX-NEWS-2025-00034", "CTX-NEWS-2025-00035", "CTX-NEWS-2025-00039",
        "CTX-NEWS-2025-00040", "CTX-NEWS-2025-00041", "CTX-NEWS-2025-00042",
        "CTX-NEWS-2025-00043", "CTX-NEWS-2025-00044", "CTX-NEWS-2025-00045",
        "CTX-NEWS-2025-00046", "CTX-NEWS-2025-00186", "CTX-NEWS-2025-00224",
        "CTX-NEWS-2025-00269", "CTX-NEWS-2025-00270", "CTX-NEWS-2025-00292",
        "CTX-NEWS-2025-00349",  # Residents worried about $1.8M property reassessment
        "CTX-NEWS-2025-00359",  # Councilors oppose tax hike amid $120M reserve surplus
        "CTX-NEWS-2025-00388",  # Council will override all 45 mayoral budget vetoes
        "CTX-COUNCIL-2026-00059",  # State Aid Increase Resolution
    ],
    "CLUSTER-SCHOOL-ZONE-CAMERAS-2025Q4": [
        "CTX-NEWS-2025-00097", "CTX-NEWS-2025-00114", "CTX-NEWS-2025-00131",
        "CTX-NEWS-2025-00147", "CTX-NEWS-2025-00211", "CTX-NEWS-2025-00228",
        "CTX-NEWS-2025-00267", "CTX-NEWS-2025-00287",
        "CTX-WEB-2026-00038", "CTX-WEB-2026-00039",
        "CTX-NEWS-2025-00332",  # Can't afford to forgo speeding tickets (Your Letters)
        "CTX-NEWS-2025-00380",  # Parents feel extra safe with school zone speedometers
        "CTX-COUNCIL-2026-00102",  # Out-of-State Plates, School Zone Cameras
        "CTX-NEWS-2025-00133",  # School zone camera news
        "CTX-NEWS-2025-00134",  # School zone camera news
        "CTX-NEWS-2025-00170",  # School zone camera news
        "CTX-NEWS-2025-00251",  # School zone camera news
        "CTX-NEWS-2025-00254",  # School zone camera news
    ],
    "CLUSTER-PBA-ARMORY-SQUARE-2025Q3": [
        "CTX-NEWS-2025-00073", "CTX-NEWS-2025-00074", "CTX-NEWS-2025-00075",
        "CTX-NEWS-2025-00087", "CTX-NEWS-2025-00089", "CTX-NEWS-2025-00109",
        "CTX-NEWS-2025-00196", "CTX-NEWS-2025-00248",
        "CTX-NEWS-2025-00319",  # Is Armory Square really so chaotic? (field investigation)
        "CTX-NEWS-2025-00320",  # Duplicate of 00319
    ],
    "CLUSTER-CRIME-TRENDS-2025Q3": [
        "CTX-NEWS-2025-00076", "CTX-NEWS-2025-00111", "CTX-NEWS-2025-00140",
        "CTX-NEWS-2025-00156", "CTX-NEWS-2025-00167", "CTX-NEWS-2025-00172",
        "CTX-NEWS-2025-00238", "CTX-NEWS-2025-00252",
        "CTX-NEWS-2025-00366",  # Community activist on youth violent crime surge
        "CTX-NEWS-2025-00003",  # Safer Streets Programs initiative
        "CTX-NEWS-2025-00068",  # Crime data/stats
        "CTX-NEWS-2025-00112",  # Crime data/stats
        "CTX-NEWS-2025-00113",  # Crime data/stats
    ],
    "CLUSTER-DPW-WINTER-OPS-2025Q4": [
        "CTX-NEWS-2025-00158", "CTX-NEWS-2025-00163", "CTX-NEWS-2025-00168",
        "CTX-NEWS-2025-00173",
        "CTX-WEB-2026-00035", "CTX-WEB-2026-00036",
        "CTX-NEWS-2025-00323",  # Snow removal downtown streets (Your Letters)
        "CTX-NEWS-2025-00340",  # New snowplows named
        "CTX-WEB-2026-00060",  # Trash/Recycling Collection Delays (weather)
        "CTX-WEB-2026-00068",  # Snow Safety Alert for Winter Storm
    ],
    "CLUSTER-LEAD-WATER-2025Q3": [
        "CTX-NEWS-2025-00096", "CTX-NEWS-2025-00102",
        "CTX-NEWS-2025-00063", "CTX-NEWS-2025-00233",
        "CTX-WEB-2026-00040", "CTX-WEB-2026-00042",
        "CTX-NEWS-2025-00394",  # OCWA seeking water line info from 48K customers (EPA mandate)
        "CTX-NEWS-2025-00016",  # DPW budget hearing on water
        "CTX-NEWS-2025-00086",  # Lead remediation news
        "CTX-NEWS-2025-00197",  # Lead remediation news
        "CTX-NEWS-2025-00244",  # Lead remediation news
        "CTX-NEWS-2025-00281",  # Lead remediation news
    ],
    "CLUSTER-FISCAL-AUDIT-2026Q1": [
        "CTX-COUNCIL-2026-00100", "CTX-COUNCIL-2026-00147",
        "CTX-COUNCIL-2026-00148", "CTX-COUNCIL-2026-00149",
        "CTX-COUNCIL-2026-00150", "CTX-COUNCIL-2026-00151",
        "CTX-WEB-2026-00003", "CTX-WEB-2026-00018",
        "CTX-NEWS-2025-00375",  # City fiscal audit / comptroller oversight
        "CTX-COUNCIL-2026-00069",  # Police and Fire Budget Audit Resolution
    ],
    "CLUSTER-SPD-1153-MOVE-2026Q1": [
        "CTX-COUNCIL-2026-00065", "CTX-COUNCIL-2026-00078",
        "CTX-COUNCIL-2026-00091", "CTX-COUNCIL-2026-00092",
        "CTX-COUNCIL-2026-00095", "CTX-COUNCIL-2026-00130",
        "CTX-COUNCIL-2026-00145",
    ],
    "CLUSTER-PEDESTRIAN-SAFETY-2026Q1": [
        "CTX-COUNCIL-2026-00048", "CTX-COUNCIL-2026-00049",
        "CTX-COUNCIL-2026-00050", "CTX-COUNCIL-2026-00051",
        "CTX-COUNCIL-2026-00063", "CTX-COUNCIL-2026-00082",
        "CTX-WEB-2026-00032",
        "CTX-WEB-2026-00055",  # Mayor Announces Vision Zero Open Houses
    ],
    "CLUSTER-MAYORAL-TRANSITION-2025Q4": [
        "CTX-NEWS-2025-00160", "CTX-NEWS-2025-00165", "CTX-NEWS-2025-00175",
        "CTX-NEWS-2025-00206", "CTX-NEWS-2025-00215", "CTX-NEWS-2025-00247",
        "CTX-NEWS-2025-00280",
        "CTX-NEWS-2025-00393",  # Mayoral transition / Owens administration
    ],
    "CLUSTER-CRB-OVERSIGHT-2025": [
        "CTX-NEWS-2025-00159", "CTX-NEWS-2025-00182",
        "CTX-NEWS-2025-00272",
        "CTX-COUNCIL-2026-00011",
    ],
    "CLUSTER-SANCTUARY-CITY-2025Q2": [
        "CTX-NEWS-2025-00085", "CTX-NEWS-2025-00190",
        "CTX-NEWS-2025-00233", "CTX-NEWS-2025-00271",
        "CTX-NEWS-2025-00285", "CTX-NEWS-2025-00286",
        "CTX-NEWS-2025-00316",  # How ICE's campaign in Syracuse breeds fear and resistance
        "CTX-NEWS-2025-00385",  # Sanctuary city / immigration enforcement
        "CTX-COUNCIL-2026-00002",  # NY For All Act State Support Resolution
    ],
    "CLUSTER-SHA-GOVERNANCE-CRISIS-2025": [
        "CTX-NEWS-2025-00008",  # Resident calls to replace Bill Simmons
        "CTX-NEWS-2025-00009",  # I-Team: SHA-City Hall email thread on East Adams
        "CTX-NEWS-2025-00012",  # Simmons insists he won't resign
        "CTX-NEWS-2025-00018",  # Silence from SHA board of commissioners
        "CTX-NEWS-2025-00022",  # 20-minute public hearing, no leadership
        "CTX-NEWS-2025-00023",  # SHA annual plan hearing
        "CTX-NEWS-2025-00028",  # Board chair addresses Open Meetings violation
        "CTX-NEWS-2025-00091",  # Eastwood Heights deterioration
        "CTX-NEWS-2025-00217",  # Calvin Corriders steps down as board president
        "CTX-NEWS-2025-00256",  # SHA assumes landlord role, leadership concerns
        "CTX-NEWS-2025-00298",  # City accuses SHA of "shooting holes in their own boat"
        "CTX-NEWS-2025-00367",  # SHA governance / leadership accountability
        "CTX-NEWS-2025-00384",  # SHA board dysfunction / oversight failure
    ],
    "CLUSTER-SKYLINE-APARTMENTS-2025": [
        "CTX-NEWS-2025-00141",  # Skyline renovation timeline
        "CTX-NEWS-2025-00209",  # Skyline code violations / lawsuit
        "CTX-NEWS-2025-00304",  # After 2 years, owner aims to rent
        "CTX-NEWS-2025-00327",  # Owners hope to talk way out of lawsuit
        "CTX-NEWS-2025-00330",  # Settlement, promises to finish repairs
        "CTX-NEWS-2025-00338",  # City sues company, "failed to invest"
        "CTX-NEWS-2025-00355",  # Skyline Apartments water woes / violations
        "CTX-NEWS-2025-00356",  # Skyline residents still waiting for repairs
        "CTX-NEWS-2025-00358",  # Skyline Apartments inspection/enforcement update
        "CTX-NEWS-2025-00004",  # 'Baffled': Company running former Skyline responds to lawsuit
    ],
    "CLUSTER-SPD-USE-OF-FORCE-2025": [
        "CTX-NEWS-2025-00045",  # Cop's trial settlement debate
        "CTX-NEWS-2025-00302",  # Should they have settled? (follow-up)
        "CTX-NEWS-2025-00313",  # Family seeks new trial in fatal shooting lawsuit
        "CTX-NEWS-2025-00314",  # Duplicate of 00313
        "CTX-NEWS-2025-00321",  # Jury rules excessive force in Father's Day shooting
        "CTX-NEWS-2025-00362",  # SPD use of force / civil rights case
        "CTX-NEWS-2025-00010",  # SPD explains use of "tactical strikes" in viral arrest video
    ],
    "CLUSTER-DOWNTOWN-DEVELOPMENT-2025": [
        "CTX-NEWS-2025-00311",  # Hotel lawsuit pauses construction
        "CTX-NEWS-2025-00322",  # Armory Square building project on hold
        "CTX-NEWS-2025-00333",  # City closes downtown garage (safety)
        "CTX-NEWS-2025-00342",  # City wants to sell prime site — 15+ month delay
    ],
    "CLUSTER-EAST-ADAMS-TRANSFORMATION-2025": [
        "CTX-NEWS-2025-00009",  # SHA board failed to vote on $2M East Adams funding
        "CTX-NEWS-2025-00081",  # Project delay, 18-month extension
        "CTX-NEWS-2025-00082",  # $30M federal grant cancellation
        "CTX-NEWS-2025-00098",  # Council land swap for Children Rising Center
        "CTX-NEWS-2025-00155",  # Phase 2 brownfield remediation begins
        "CTX-NEWS-2025-00160",  # Phase 1 groundbreaking, Walsh/Owens
        "CTX-NEWS-2025-00201",  # Blueprint 15 interim ED appointment
        "CTX-NEWS-2025-00246",  # Pioneer Homes HUD relocation vouchers
        "CTX-NEWS-2025-00256",  # SHA assumes landlord role for 1,400 units
        "CTX-NEWS-2025-00274",  # City seeks $10M state replacement for lost federal funds
        "CTX-NEWS-2025-00275",  # I-81 streetscape near East Adams
        "CTX-NEWS-2025-00298",  # City vs SHA on $2M environmental testing funding
        "CTX-NEWS-2025-00299",  # McKinney Manor demolition begins ($50M HUD project)
    ],
    "CLUSTER-MARIA-REGINA-CONGEL-2025Q2": [
        "CTX-NEWS-2025-00350",  # City maintains neglected convent for absent property owner
        "CTX-NEWS-2025-00351",  # Impending demolition elicits memories of convent history
        "CTX-NEWS-2025-00352",  # Landowner's absence fuels frustration at town meeting
        "CTX-NEWS-2025-00354",  # Tensions boil over Maria Regina property (Mayor vs councilors)
        "CTX-NEWS-2025-00373",  # Sisters reflect on demolition of Maria Regina Motherhouse
        "CTX-NEWS-2025-00374",  # Chapel's fate uncertain as convent deconstruction concludes
        "CTX-NEWS-2025-00386",  # North side residents air frustrations over fire-damaged building
    ],
    "CLUSTER-SCSD-GOVERNANCE-2025": [
        "CTX-NEWS-2025-00376",  # Syracuse schools brace for cuts from spending bill
        "CTX-NEWS-2025-00382",  # SCSD to pay outgoing superintendent $125K for consulting
        "CTX-NEWS-2025-00383",  # SCSD spends 50% more per student, ranks near bottom
        "CTX-NEWS-2025-00389",  # STEAM High School to open this September
        "CTX-NEWS-2025-00083",  # STEAM school / innovative education
        "CTX-NEWS-2025-00094",  # STEAM school / innovative education
        "CTX-NEWS-2025-00202",  # STEAM school / innovative education
        "CTX-NEWS-2025-00236",  # STEAM school / innovative education
    ],

    # -----------------------------------------------------------------------
    # New clusters — added in cluster analysis pass 3
    # -----------------------------------------------------------------------
    "CLUSTER-HOMELESSNESS-SERVICES-2025": [
        "CTX-NEWS-2025-00056",  # Rescue Mission fence / homeless services
        "CTX-NEWS-2025-00059",  # Homelessness / shelter services
        "CTX-NEWS-2025-00188",  # Homeless services / encampment
        "CTX-NEWS-2025-00193",  # Homeless services / shelter
        "CTX-NEWS-2025-00213",  # Drop-in center / homeless services
        "CTX-NEWS-2025-00245",  # Homelessness / service gaps
        "CTX-NEWS-2025-00290",  # Rescue Mission / homeless services
    ],
    "CLUSTER-COLUMBUS-STATUE-2025": [
        "CTX-NEWS-2025-00136",  # Columbus statue / historic preservation debate
        "CTX-NEWS-2025-00139",  # Columbus statue / removal debate
        "CTX-NEWS-2025-00198",  # Columbus statue / council deliberation
        "CTX-NEWS-2025-00214",  # Historic preservation / landmark decision
        "CTX-NEWS-2025-00232",  # Columbus statue / community reaction
        "CTX-NEWS-2025-00318",  # Columbus statue / final resolution
    ],
    "CLUSTER-SUSTAINABILITY-CLIMATE-2025": [
        "CTX-NEWS-2025-00015",  # Mayor Walsh launches 'Sustainable Syracuse'
        "CTX-NEWS-2025-00080",  # Sustainability / greenhouse gas
        "CTX-NEWS-2025-00092",  # Climate action plan
        "CTX-NEWS-2025-00183",  # Sustainability / environment
        "CTX-NEWS-2025-00194",  # Greenhouse gas / climate
        "CTX-NEWS-2025-00392",  # Sustainability / environment
        "CTX-WEB-2026-00002",   # Stormwater Management comment period
        "CTX-WEB-2026-00051",   # SWMP and MS4 comment period
    ],
    "CLUSTER-VACANT-BUILDING-FIRES-2025": [
        "CTX-NEWS-2025-00106",  # Vacant building fire / audit
        "CTX-NEWS-2025-00107",  # Vacant structures / fire risk
        "CTX-NEWS-2025-00108",  # Vacant building fire response
    ],
    "CLUSTER-PARKWAY-BRIDGE-STRIKES-2025": [
        "CTX-NEWS-2025-00105",  # Parkway bridge strike
        "CTX-NEWS-2025-00161",  # Bridge strike / "the clanker"
        "CTX-NEWS-2025-00337",  # Bridge strike incident
        "CTX-NEWS-2025-00370",  # Bridge strike / infrastructure
    ],
    "CLUSTER-FIRE-APPARATUS-2026Q1": [
        "CTX-NEWS-2025-00017",  # Fire and Police Chiefs budget hearing
        "CTX-COUNCIL-2026-00012",  # Emergency Signal Preemption Miovision Contract
        "CTX-COUNCIL-2026-00064",  # SFD Bryer Code Compliance Software
        "CTX-COUNCIL-2026-00112",  # Fire Apparatus Training Truck Status
        "CTX-COUNCIL-2026-00113",  # Fire Apparatus: Decades of Deferred Maintenance
        "CTX-COUNCIL-2026-00131",  # Hazmat Apparatus and Ground Ladder Procurement
    ],
    "CLUSTER-DPW-CAPITAL-2026Q1": [
        "CTX-COUNCIL-2026-00060",  # SU West Campus Street Conversion
        "CTX-COUNCIL-2026-00061",  # DPW Capital Package (Salt Barn $4M, Fleet)
        "CTX-COUNCIL-2026-00062",  # James Street TIP, Chaffi Bridge, Parks Capital
        "CTX-COUNCIL-2026-00076",  # Bond Ordinances Revised Schedule
        "CTX-COUNCIL-2026-00081",  # Midland Ave Lot Acquisition and DPW Batch
        "CTX-COUNCIL-2026-00094",  # Midland Avenue Paving $620K Overrun
        "CTX-COUNCIL-2026-00125",  # DPW Capital Bond Authorizations and CHIPS Reform
    ],
    "CLUSTER-SPD-GRANTS-PROGRAMS-2026Q1": [
        "CTX-COUNCIL-2026-00019",  # ATF Task Force Overtime Reimbursement
        "CTX-COUNCIL-2026-00066",  # Reduced Guns DOJ Grant Reprogramming
        "CTX-COUNCIL-2026-00067",  # STRIVE Grant Year 2 (IPV Reduction)
        "CTX-COUNCIL-2026-00068",  # SPD PAL Program $50K DCJS Grant
    ],
    "CLUSTER-BESS-MORATORIUM-2026Q1": [
        "CTX-COUNCIL-2026-00035",  # BESS Moratorium Vote
        "CTX-COUNCIL-2026-00106",  # BESS Six-Month Moratorium and Working Group
    ],
    "CLUSTER-GOOD-CAUSE-EVICTION-2026Q1": [
        "CTX-COUNCIL-2026-00031",  # Good Cause Eviction Defeated 4-4
        "CTX-COUNCIL-2026-00128",  # Good Cause Eviction re-introduction
    ],
    "CLUSTER-SENIOR-TAX-EXEMPTION-2026Q1": [
        "CTX-COUNCIL-2026-00087",  # Senior Property Tax Exemption Expansion
        "CTX-COUNCIL-2026-00137",  # Senior Exemption Held for Data Analysis
    ],
    "CLUSTER-PARKS-CAPITAL-2025": [
        "CTX-WEB-2026-00047",   # Elmwood Park Sediment Removal
        "CTX-WEB-2026-00048",   # Kirk Park Canoe Launch
        "CTX-WEB-2026-00049",   # Phase 3 Creekwalk Extension
        "CTX-WEB-2026-00056",   # Kirk Park Pedestrian Bridge
        "CTX-COUNCIL-2026-00013",  # Magnarelli Center Sound System Grant
        "CTX-COUNCIL-2026-00096",  # Parks Omnibus (Breanna Stewart, Landscape)
    ],
    "CLUSTER-ROAD-RECONSTRUCTION-2026Q1": [
        "CTX-WEB-2026-00082",   # Road reconstruction announcement
        "CTX-WEB-2026-00087",   # Road reconstruction announcement
        "CTX-WEB-2026-00094",   # Road reconstruction announcement
        "CTX-WEB-2026-00110",   # Road reconstruction announcement
    ],
    "CLUSTER-DEER-MANAGEMENT-2025": [
        "CTX-NEWS-2025-00126",  # Urban deer management
        "CTX-NEWS-2025-00203",  # Deer management / hunting program
        "CTX-NEWS-2025-00381",  # Deer management update
    ],
    "CLUSTER-MICRON-ECONOMIC-DEV-2025": [
        "CTX-NEWS-2025-00230",  # Micron / economic development
        "CTX-NEWS-2025-00242",  # Micron / Clay fab investment
        "CTX-NEWS-2025-00243",  # Micron / semiconductor economic impact
        "CTX-NEWS-2025-00341",  # Micron / regional development
    ],
    "CLUSTER-CODE-ENFORCEMENT-REFORM-2026Q1": [
        "CTX-NEWS-2025-00005",  # Syracuse Code Enforcement in the spotlight
        "CTX-NEWS-2025-00013",  # Syracuse Mayor taking Nob Hill to Supreme Court
        "CTX-COUNCIL-2026-00022",  # NYS Building Code Update, Housing Corrections
        "CTX-COUNCIL-2026-00023",  # Foreclosure Redemption and Outreach
        "CTX-COUNCIL-2026-00088",  # Third-Party Building Plan Review Renewal
        "CTX-COUNCIL-2026-00134",  # Property Transfers, Foreclosure, Building Code Hold
    ],
    "CLUSTER-HUD-HOUSING-PROGRAMS-2026Q1": [
        "CTX-COUNCIL-2026-00089",  # HUD Environmental Review and Property Disposition
        "CTX-COUNCIL-2026-00103",  # Items 36-39 HUD and Code Items Introduced
        "CTX-COUNCIL-2026-00104",  # HUD CDBG Action Plan, Senior Tax Exemption
    ],
}


def main():
    dry_run = "--dry-run" in sys.argv

    # Build a map of record_id -> list of cluster_ids to add
    updates = {}  # record_id -> set of new cluster_ids

    # Process all assignment groups
    all_groups = {}
    all_groups.update(SAME_URL_DEDUP)
    all_groups.update(SAME_EVENT_DEDUP)
    all_groups.update(CLUSTER_EXPANSIONS)
    all_groups.update(NEW_CLUSTERS)

    for cluster_id, record_ids in all_groups.items():
        for rid in record_ids:
            if rid not in updates:
                updates[rid] = set()
            updates[rid].add(cluster_id)

    print(f"Records to update: {len(updates)}")
    print(f"Cluster/dedup groups: {len(all_groups)}")

    # Load and update records
    modified = 0
    missing = 0
    already = 0

    for record_id, new_clusters in sorted(updates.items()):
        # Find the record file
        fname = f"{record_id}.json"
        fpath = STORE / fname
        if not fpath.exists():
            print(f"  MISSING: {record_id}")
            missing += 1
            continue

        record = load_record(fpath)
        existing = set(record.get("cluster_ids", []))
        to_add = new_clusters - existing

        if not to_add:
            already += 1
            continue

        record["cluster_ids"] = sorted(existing | new_clusters)

        if dry_run:
            print(f"  [DRY RUN] {record_id}: +{len(to_add)} clusters -> {sorted(to_add)}")
        else:
            save_record(fpath, record)

        modified += 1

    print(f"\nSummary:")
    print(f"  Modified: {modified}")
    print(f"  Already linked: {already}")
    print(f"  Missing files: {missing}")

    if dry_run:
        print("\n(Dry run — no files changed)")


if __name__ == "__main__":
    main()
