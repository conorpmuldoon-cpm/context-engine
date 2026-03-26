# Context Briefing: Payroll Modernization & IT Systems

**Generated:** 2026-03-24
**Briefing Type:** Cluster + thematic (department)
**Records Reviewed:** 32
**Core Records:** 28
**Relevance Score Range:** 0.65–0.95
**Primary Cluster:** CLUSTER-PAYROLL-MODERNIZATION-2026Q1 (16 records)

---

## Executive Summary

The payroll modernization project is the most politically charged IT initiative in Syracuse city government — **$7.8M spent over 5.5 years** with the core payroll system still not operational as of March 2026. The project carries heavy political baggage from the Walsh administration (EY procurement scandal, AG investigation, whistleblower allegations, abandoned Oracle Fusion), but the Owens administration has restarted with new partners (Mosaic Consulting + UKG Pro) and a 7-segment Finance Committee presentation in February 2026 produced a **unanimous but explicitly conditional** council vote to proceed. The dominant political signal is sustained, high-confidence scrutiny (12 instances). Key milestones: **July 1, 2026** (AS400 timekeeping decommission target) and **June/July 2027** (UKG Pro implementation complete, AS400 payroll sunset). This is directly relevant to the Innovation Office — the API department is on the project ownership group, and the CIO role is active in the broader IT modernization landscape.

---

## The Core Problem

Syracuse runs a $330M+ operating budget on a system architecture from the 1980s:

- **AS400 mainframe** handles payroll, HR demographics, water billing, and assessment
- Employee status changes require: printing an AS400 screenshot → handwriting changes → scanning → DocuSigning (10-15 minutes vs. seconds in a modern system)
- 2,000-person organization uses **paper time sheets**
- Payroll staff: 2 retirees + 3 employees
- Police and fire spend **$15M in overtime without analytical reports** — no on-demand overtime listing is possible
- 9 union contracts with complex deduction cascades
- Demographic data "incomplete at best" and "isolated"
- Grant cost allocation is **100% manual journal entries** ($36M+ DOT/CHIPS expenditures, only $30M claimed — a significant cash management gap)
- Applicant tracking is entirely paper-based; city lost Indeed full suite (limited to 3 postings/month with 90 vacancies)

As HR Assistant Director Mike Smith put it at the February 3 Finance Committee: *"We're sick of it. We are so tired of dealing with AS400 and processing paperwork like we're still running the Erie Canal. We're a modern city... and a major part of our office equipment is a pen. We're done."*

---

## Project History (2020–2025)

| Date | Event | Cost Impact |
|------|-------|-------------|
| **2020** | Project begins under Walsh administration | — |
| **Oct 2022** | EY holds private "wavespace" brainstorming with senior officials | — |
| **Jan 2023** | SWC first "go-live" (minimal: 1 person, 7-8 personas, no employees) | — |
| **Oct 2023** | Annemarie Deegan named First Deputy Commissioner of Finance (new position) | — |
| **Late 2024** | EY relationship terminated; costs ballooned 2024 | Major cost escalation |
| **Jan 2025** | Walsh formally terminates EY contract | — |
| **Apr 2025** | Council questions $8.1M spent; AG investigating | AG investigation |
| **May 2025** | Whistleblower Susan Fahrenkrug alleges EY used Advanced IT as "pass-through" | — |
| **Jun 2025** | Foxpointe/Bonadio audit: no fraud found, but oversight failures, non-cooperating staff | $88K audit cost |
| **Jun 2025** | Bonadio recommends establishing CTO position | — |
| **Sept 2025** | Mosaic Consulting $900K request presented to council | — |
| **Dec 2025** | Walsh term ends; Owens inherits unfinished project | **$7.8M total spent** |

**Key takeaway:** ~$3.9M went to SWC timekeeping (partially working), ~$3.9M to Oracle Fusion and EY (total loss). Oracle described as "Android phone with iPhone charger" — never went live. Contractors terminated: EY, ERP1, Dear Alien Bait, Cherro Technologies. Active legacy contractors: Kathleen LLC (former AS400 programmer), Central City Data (former API employee).

---

## Current Plan: Mosaic + UKG Pro

### What Was Approved (Feb 9, 2026 — Unanimous Vote)

- **Partner:** Mosaic Consulting Group (Nashville) — selected via competitive RFP as "most qualified by far"
- **Platform:** UKG Pro (SaaS, fully cloud-based) — selected over Oracle Fusion and Workday
- **Cost:** ~$1M implementation + ~$750K recurring annual. No new funding — $2.5M remains from $6.25M previously approved
- **Contract term:** 18 months from execution (corrected to August 2027 expiration)
- **Ownership group:** Deputy Mayor, API Director, HR Director, CFO, First Deputy Commissioner of Finance

### Implementation Timeline

| Target Date | Milestone |
|-------------|-----------|
| **Current** | SWC digital timekeeping at 70% live (max ~250 employees/month) |
| **Jul 1, 2026** | AS400 timekeeping decommissioned; SWC rollout complete |
| **Mar 2027** | SWC end of life (UKG stops supporting updates) |
| **Jun/Jul 2027** | UKG Pro implementation complete; AS400 payroll/HR demographics sunset |

### Module Priority (if underfunded)

| Priority | Module | Rationale |
|----------|--------|-----------|
| **Must-have** | Demographic data | Foundation for everything |
| **Must-have** | Payroll | Core function |
| **Must-have** | Timekeeping | Already partially live via SWC |
| **Must-have** | Position management | Staffing/budgeting |
| **Nice-to-have** | Performance management | First to cut |
| **Excluded** | AI chatbot | Deliberately excluded from scope |

---

## The February 3 Finance Committee (7 Segments)

The pivotal event — a comprehensive deep-dive that gave the new council full institutional knowledge of the project. Key revelations:

1. **Scale of manual work:** 24 police officer retropay corrections (dating to 2021) consumed 40 staff hours. Weekly + bi-weekly + SURA payroll cycles all run manually.
2. **Oracle comparison:** Syracuse City School District spending an additional $900K to get Oracle operational. Rochester school district spent $30M+.
3. **SWC limitations:** Aging software, multiple license types, no mobile punch, county server dependency. New hires delayed weeks.
4. **Culture change:** Water Department 5 months live — fewer paycheck errors, employees have overtime visibility. Initial resistance giving way to compliance.
5. **No peer:** No NY municipality currently live on UKG Pro. State of Indiana and Boulder, CO cited as references.
6. **The key question:** Finance Committee Chair Jones-Rowser asked *"why is it different now?"* — Answer: better team structure, more active leadership, correct software, right partner via RFP.

### Council's Verdict

Councilor Williams (Finance Committee Chair) delivered a public statement at the February 9 vote: council has "expressed our concern and our hesitation with allocating additional funds" but "we are at a point that we need to move forward." The items passed unanimously, but with a clear message: **"Prove us wrong."** The council demanded benchmarks in the contract before fund disbursement.

---

## Broader IT Landscape

Beyond payroll, the context store reveals a wider IT modernization picture:

### Systems in Play
| System | Status | Department |
|--------|--------|------------|
| **Surge Link** (municipal broadband) | Expanding to 9,200+ households ($10.8M state grant) | Mayor's Office / IT |
| **Assessment platform** (Image Mate → Beacon) | Migrating; ~250K hits/year; county aligned | API / IT |
| **Bryer** (fire code compliance) | New; 3-year agreement; no cost | SFD |
| **iSol Talent Acquisition** | Bridge recruiting until UKG Pro applicant tracking | HR |
| **Community center fiber** | Spectrum coax → fiber upgrade for e-gaming | IT / Parks |
| **1153 HQ IT infrastructure** | NAN Associates ($200K); door locks, cameras, network | SPD / IT |

### Key IT Personnel
| Name | Role |
|------|------|
| **Vinnie Scipion** | Chief Information Officer (presented assessment migration) |
| **Dave Prowak** | Director of IT (managing citywide IT RFP, vendor licenses, 1153 infrastructure) |
| **Josh Syrson** | Project Manager, API Department (payroll modernization day-to-day lead) |
| **Mike Smith** | Assistant Director of HR (payroll modernization advocate) |
| **Annemarie Deegan** | First Deputy Commissioner of Finance (project ownership group) |
| **Evan Loving** | Budget Director (clarified funding availability) |

---

## Political Signals

| Signal | Count | Key Instances |
|--------|-------|---------------|
| **Scrutiny** | 12 (high) | AG investigation, whistleblower, external audits, "why is it different?", $15M overtime without reports, "prove us wrong" |
| **Championship** | 2 | Mike Smith's "Erie Canal" statement; Josh Syrson's detailed presentation |
| **Priority Alignment** | 1 | CTO position recommended in Bonadio audit |
| **Budget Commitment** | 1 | $6.25M previously approved, $2.5M remaining |

This is the **highest scrutiny-to-commitment ratio** of any topic in the Context Engine. The council's conditional approval puts the Owens administration on notice — benchmarks must be met or political consequences follow.

---

## Innovation Office Relevance

This briefing is directly relevant to the Office of Analytics, Performance & Innovation:

1. **API is on the project ownership group** alongside Deputy Mayor, HR, CFO, and Finance
2. **CIO Vinnie Scipion** is actively presenting IT items to council
3. **Overtime analytics** — the $15M without reports is exactly the kind of data governance gap API could help address once UKG Pro is live
4. **Grant cost allocation** — manual journal entries for $36M+ in expenditures represents a data pipeline problem
5. **Demographic data** — "incomplete at best" → once UKG Pro captures clean demographic data, API can build workforce analytics
6. **Position management** — currently no centralized view of staffing across departments

---

## Open Questions

1. **UKG Pro implementation status** — As of the March 16 audit, the system is "contracted but not yet operational." Has Mosaic begun active work since the February 9 approval?
2. **SWC rollout (remaining 30%)** — On track for July 1, 2026 AS400 timekeeping decommission?
3. **AG investigation outcome** — No update since April 2025.
4. **CIO vs. CTO** — Bonadio recommended a CTO. Scipion holds CIO title. Was the recommendation adopted?
5. **IT RFP award** — 26-28 responses received (August 2025). Still being scored as of February 18. Awarded?
6. **Oracle residual costs** — Any ongoing licensing/contractual obligations?
7. **AS400 for water billing and assessment** — What's the plan after payroll leaves?
8. **No NY municipal peer on UKG Pro** — Implementation risk. Has Mosaic done comparable NY public-sector implementations?
9. **Mosaic contract milestones** — Council demanded benchmarks before fund disbursement. What are they?
10. **Police/fire overtime analytics** — When will the system deliver on-demand overtime cost analysis? Is this in the initial implementation scope?
11. **Grant reimbursement automation** — Manual journal entries for $36M+ in DOT/CHIPS. When does the new system fix this?
12. **iSol → UKG applicant tracking timeline** — City has 90 vacancies and 3 free Indeed postings/month.

---

*Context Engine Briefing | 32 records reviewed | Generated by Librarian*
