# Oracle Cloud Always Free Research (Mind You)

## Quick Answer

**No.**

The commonly cited Oracle Cloud Always Free allocation of **4 OCPU / 24 GB RAM** is no longer current.

The current verified Always Free allocation is:

- 2 OCPUs
- 12 GB RAM

Always Free remains permanent, but it is subject to operational caveats including idle reclamation, no SLA, home-region restrictions, and the possibility of future quota changes.

The remainder of this document provides the detailed findings, including current Oracle Always Free resources, caveats, operational limitations, advantages, disadvantages, deployment suitability, and the final recommendation. For readers requiring the complete engineering rationale, methodology, supporting evidence, and implementation guidance, refer to the supporting documents listed at the end of this document.

## 1. Research Objective

This investigation was conducted to verify a claim circulating in the development community: that Oracle Cloud Infrastructure (OCI) Always Free tier provides 4 OCPU and 24 GB of RAM at no cost, indefinitely. The claim was evaluated in the context of Mind You AI Council, an internal engineering intelligence dashboard built on Next.js 16, Supabase PostgreSQL, and GitHub Actions-based scheduled ingestion.

Mind You currently runs on Vercel (hosting) and hosted Supabase (database, auth, storage). The infrastructure cost is approximately $0-45 per month depending on the plan tier. The investigation explored whether Oracle Always Free could replace this arrangement at zero cost while preserving the application's existing architecture and operational requirements. The evaluation compared Oracle Always Free against the current setup and four alternative free-tier platforms: Vercel Hobby, Railway, and Render.

The research was motivated by the potential to reduce infrastructure costs to $0 per month while gaining full server control and commercial use permission, which is restricted on Vercel Hobby. The evaluation assessed compute capacity, storage, networking, migration effort, cost, risk, and compatibility with the existing Mind You architecture.

The original research question was:

> "Can you check if totoo yung rumor na Oracle Cloud Always Free provides 4 OCPU / 24 GB RAM free forever? I need details, caveats, pros, cons, limits, everything you can find."

### Methodology

Research was conducted using a four-tier source hierarchy. Tier 1 comprised Oracle's official documentation, verified against live pages on 2026-07-23. Tier 2 included Oracle blog posts and technical documentation. Tier 3 encompassed community reports and third-party documentation from Supabase, Vercel, Railway, Render, Docker, and Caddy. Tier 4 covered third-party blog posts and reviews. Findings were validated through structured section reviews, cross-reference verification, gap analysis between documentation sources, unsupported-claims checks, and confidence assessments for each finding.

The evaluation comprised thirteen sections: always-free resource specifications, operational limitations, deployment architecture options, a comparison matrix against alternative platforms, migration effort estimates, cost analysis, risk assessment, performance benchmark plans, step-by-step setup instructions, a production readiness checklist, and a go/no-go decision framework. Each finding was assigned a confidence level (High, Medium, or Low) based on source quality and verification status.

This document presents the complete answer, synthesizing findings from a thirteen-section engineering evaluation and a structured executive summary. A reader should be able to understand the full answer from this document alone, without needing to consult the supporting research files. The evaluation was completed on 2026-07-23 and all Tier 1 sources were verified against live documentation on that date.

---

## 2. Executive Answer

**The claim is no longer accurate.** Oracle reduced the Always Free ARM A1 compute allocation from 4 OCPUs / 24 GB RAM to 2 OCPUs / 12 GB RAM in June 2026. The change was made without prior announcement and was corroborated by community reports before Oracle's official documentation was updated to reflect the new figures.

The current verified Always Free allocation for ARM-based Ampere A1 instances is 2 OCPUs and 12 GB of RAM. On the ARM A1 shape, 1 OCPU equals 1 vCPU (not 2, as on x86 shapes), so the allocation is equivalent to 2 vCPUs and 12 GB of RAM. The commonly cited "4 vCPU" figure may stem from confusion between x86 and ARM OCPU definitions, where x86 defines 1 OCPU as 2 vCPUs.

The "free forever" characterization is broadly accurate — Always Free services do not expire — but is subject to significant operational limitations: idle resource reclamation after a 7-day inactivity window, no SLA or uptime guarantee, home-region restrictions, community-only support, and the possibility of further quota reductions without notice.

Oracle Always Free is the only free-tier platform evaluated that simultaneously provides sufficient RAM for the Mind You stack, always-on compute, self-hosted PostgreSQL, commercial use permission, and permanent free availability. It is technically suitable for the project, subject to the documented caveats.

The evaluation also assessed two deployment architectures. Architecture A deploys the full Mind You stack on a single Oracle Always Free VM using Docker Compose, requiring zero application code changes and an estimated 12 to 14 hours of migration effort. Architecture B retains Supabase Cloud for the database while deploying only the Next.js application to Oracle, but requires a complete rewrite of all database queries, authentication logic, and storage integration, with an estimated effort of 45 to 80 hours. Architecture A is recommended over Architecture B.

---

## 3. Current Oracle Always Free Resources

The following summarizes the Always Free resources relevant to hosting Mind You. All figures were verified against Oracle's official documentation on 2026-07-23.

### 3.1 Compute

ARM-based Ampere A1 instances (VM.Standard.A1.Flex) with a combined total of 2 OCPUs and 12 GB of RAM. One or two instances may be created, up to the combined quota. The processor is an Ampere Altra Q80-30 at 3.0 GHz maximum frequency. Network bandwidth scales with OCPU count at 1 Gbps per OCPU, up to 40 Gbps maximum. Connection tracking is 12,000 per OCPU. Supported images include Oracle Linux, Ubuntu, and Oracle Linux Cloud Developer (which requires at least 8 GB of memory, constraining instance configurations on 12 GB).

AMD Micro instances (VM.Standard.E2.1.Micro) are also available at 1/8 OCPU and 1 GB RAM each, with up to 2 instances. These are insufficient for the Mind You workload but could serve supplementary purposes such as lightweight monitoring or jump hosts.

### 3.2 Storage

200 GB of combined boot volume and block volume storage. Boot volumes have a minimum of 50 GB, leaving 150 GB for application data and database storage. Five volume backups are included (boot and block combined). Object Storage provides 20 GB across Standard, Infrequent Access, and Archive tiers for Always Free-only accounts, with 50,000 API requests per month.

A documentation discrepancy exists: the Object Storage Limits page states 10 GB while the Always Free Resources page states 20 GB. The 20 GB figure from the more specific Always Free Resources page was verified on 2026-07-23.

### 3.3 Networking

Up to 2 Virtual Cloud Networks (VCNs), one flexible load balancer at 10 Mbps (with 16 listeners, 16 backend sets, and 1,024 backend servers), and 10 TB of monthly outbound data transfer. Oracle announced the elimination of all outbound data transfer charges in February 2026. A reserved public IP is available at no cost and should be used to prevent IP loss on VM reboot. Email outbound through port 25 is blocked by default.

Site-to-Site VPN supports up to 50 IPSec connections, and Bastion service is free for both free and paid accounts. VCN Flow Logs provide up to 10 GB per month shared across the OCI Logging service.

### 3.4 Database

Always Free includes two Oracle Autonomous AI Database instances at 20 GB each with 1 OCPU and 20 simultaneous sessions. Mind You would not use these directly — instead, self-hosted PostgreSQL 15 via Docker is the recommended approach, as it preserves the existing Supabase client pattern with zero application code changes. The self-hosted approach also avoids the Autonomous Database OCPU definition, which differs from the compute OCPU and is a dedicated database processing unit.

### 3.5 Support

Community support only. OCI documentation and community forums are available. My Oracle Support, technical account managers, live chat, and phone support are not available for Always Free accounts. Troubleshooting relies entirely on self-service resources.

### 3.6 Other Services

Additional Always Free services include Vault (software-protected keys, all versions free), Vault Secrets (150 secrets), Certificates (5 CAs, 150 certificates), Resource Manager/Terraform (100 stacks), Monitoring (500 million ingestion data points), Notifications (1 million HTTPS notifications per month), Email Delivery (3,000 emails per month), and Connector Hub (2 connectors). These are not directly relevant to Mind You's deployment but are available at no cost.

---

## 4. Advantages

Oracle Always Free offers several material advantages for Mind You that are not available from any other free-tier platform evaluated.

**Highest RAM allocation.** 12 GB of always-on RAM at zero monthly cost. This is the highest among all free-tier platforms evaluated. Vercel Hobby provides a maximum of 2 GB per function, Railway offers 0.5 GB per service, and Render provides 512 MB. The 12 GB allocation is sufficient to run the full Mind You stack — Next.js, PostgreSQL, GoTrue, PostgREST, Studio, Realtime, and Caddy — on a single VM with estimated usage of 4.5 to 7.5 GB, leaving headroom for growth and supplementary services.

**Commercial use permitted.** Vercel Hobby explicitly prohibits commercial use, which would require upgrading to Vercel Pro at $20 per seat per month. Oracle Always Free imposes no such limitation, making it suitable for production internal tools that may be used by team members in a commercial context.

**Full server control.** SSH, Docker, and custom configuration with root access. This enables self-hosted Supabase deployment using Docker Compose, preserving the existing `@supabase/supabase-js` client pattern with no application code changes. Migration effort for this architecture is estimated at 12 to 14 hours of engineering work for a developer with Docker and Linux experience. The existing auth logic, RBAC, and all API routes remain identical.

**Substantial storage.** 200 GB block plus 20 GB object storage exceeds all alternatives. Supabase Free provides 500 MB database and 1 GB file storage. Vercel provides 1 GB blob storage. Railway provides 0.5 GB volume storage. Render provides 1 GB. The 200 GB allocation is sufficient to host the database, application logs, file uploads, and backup archives on a single VM without external storage dependencies.

**Generous egress.** 10 TB per month is far more than sufficient for an internal dashboard with scheduled ingestion from RSS, Hacker News, GitHub Trending, and repository radar sources.

**Simple rollback.** DNS can be re-pointed to Vercel within minutes, and Vercel remains as a fallback throughout and after migration. The original codebase requires no modification under Architecture A, so reverting is a DNS change rather than a code deployment. Total rollback time is estimated at under 1 hour of active effort plus DNS propagation time. This provides a safety net during the migration period and afterwards if Oracle Always Free proves unsuitable.

**Low vendor lock-in.** The architecture uses standard PostgreSQL, the `@supabase/supabase-js` client works with any Supabase-compatible endpoint, and GitHub Actions workflows are independent of Oracle Cloud. If Oracle changes Always Free terms, migration to another platform is feasible without codebase rewrite. The original code can be preserved in a separate Git branch before migration, enabling quick reversion if needed. This contrasts with Architecture B, which would create Oracle-specific dependencies and make rollback more complex.

**Flexible instance configuration.** The ARM A1 shape allows flexible allocation of OCPUs and memory within the Always Free quota. A single 2-OCPU / 12 GB instance can be created, or two 1-OCPU / 6 GB instances can be created for workload separation. This flexibility allows the deployment to be tuned to the specific requirements of the Mind You stack.

**Cost savings potential.** If currently on Vercel Pro ($20/month) plus Supabase Pro ($25/month), Oracle Always Free would eliminate $540 in annual hosting costs. If on Vercel Hobby plus Supabase Free (both free but Vercel Hobby prohibits commercial use), the primary savings would be the ability to use the stack commercially without upgrading to paid tiers. Domain registration costs (~$10-15/year) are independent of hosting choice and remain unchanged. The primary non-monetary cost is the estimated 2-4 hours per month of infrastructure management overhead, which includes updates, monitoring, backups, and troubleshooting.

---

## 5. Caveats and Operational Limitations

The following caveats are operationally significant and must be understood before deployment. While Oracle Always Free provides substantial compute and storage resources at no cost, the absence of an SLA, the idle reclamation policy, and the possibility of quota changes introduce operational risks that differ materially from managed platforms. Each caveat has documented mitigations where applicable.

**Idle reclamation.** Oracle may reclaim compute instances that are deemed idle. Idle is defined as CPU, network, and memory utilization each below 20 percent over a 7-day rolling window. The memory threshold applies only to A1 shapes — AMD Micro instances are not evaluated on memory. Reclaimed instances are terminated, and all data on the instance is lost unless backed up externally. There is no documented notification period before reclamation. The standard community mitigation is a health-ping cron job running every 48 to 72 hours that generates sufficient CPU and network activity to keep utilization above the 20 percent threshold.

**No SLA.** Always Free services are provided as-is with no service level agreement. There is no guaranteed uptime percentage, no credits or compensation for downtime, and no priority support for availability issues. Oracle may perform maintenance that disrupts Always Free instances without advance notice. Estimated uptime based on community reports is 95 to 99 percent, which is lower than managed platforms such as Vercel (99.99%) or Supabase (99.9%). This is acceptable for Mind You as an internal tool but would be a meaningful risk for customer-facing SaaS.

**Account inactivation.** Oracle may deactivate accounts that are inactive for 90 days. New accounts without a valid payment method may be suspended after 30 days. A credit card is required during sign-up for identity verification but is not charged on Always Free accounts. Suspended accounts may result in termination of Always Free resources and data loss. Signing in at least once every 60 days and maintaining a valid payment method on file are sufficient mitigations.

**Home-region restriction.** Always Free resources can only be used in the home region of the tenancy, and region selection is permanent. ARM A1 instances cannot be created in South Korea North (Chuncheon). The home region should be chosen carefully during account creation — ideally a region with known A1 capacity such as US East (Ashburn) or US West (Phoenix). Once selected, the home region cannot be changed. If the team's primary users are in a different region, latency may be a factor, though for an internal dashboard the impact is typically acceptable.

**Quota stability.** Oracle reduced the ARM A1 allocation from 4/24 to 2/12 without prior announcement. Always Free limits are not contractually guaranteed to remain unchanged. This is a documented risk with no mitigation other than maintaining Vercel as a fallback and keeping off-site backups independent of Oracle Cloud. The June 2026 reduction demonstrates that Always Free terms may change at Oracle's discretion.

**ARM architecture.** The Ampere A1 shape uses ARM64 architecture. Not all Docker images or pre-built binaries are available for ARM64. Image compatibility should be verified before migration. Most modern Docker images, including the official Node.js and PostgreSQL images, support ARM64, but edge cases may exist for less common packages or native modules. The Mind You codebase uses standard npm packages without native dependencies, which reduces the likelihood of ARM64 compatibility issues, but Docker image compatibility for the full Supabase stack should be tested on the target VM before migration.

**Documentation inconsistencies.** Oracle's documentation contains several discrepancies: the marketing page previously stated 4 OCPUs / 24 GB (now updated), the Object Storage Limits page states 10 GB while the Always Free Resources page states 20 GB, and boot volume minimums are stated as both 47 GB and 50 GB on the same page. The Always Free Resources page is the most specific and recently verified source. Always Free compute quotas are documented exclusively on this page and are not cross-referenced on the Limits by Service page. These inconsistencies suggest that Oracle's documentation may not always be kept in sync across all pages.

**Outbound data transfer.** While the Always Free documentation states 10 TB per month of outbound data transfer, Oracle announced in February 2026 the elimination of all outbound data transfer charges across all regions. This may effectively remove the egress cap, though the Always Free documentation has not been updated to reflect this change. The practical impact is that egress is unlikely to be a constraint for Mind You.

**Support limitations.** No live chat, phone support, or technical account manager is available. Troubleshooting relies entirely on documentation and community forums. This is adequate for standard Docker and PostgreSQL deployments but may be insufficient for Oracle Cloud-specific networking or IAM issues. Budget additional time for self-service troubleshooting compared to managed platforms.

**Accidental charges.** While Always Free resources are free, accidental provisioning of paid-shape resources may incur charges. Examples include creating a paid VM shape, exceeding Object Storage or Block Volume quotas ($0.0255/GB/month overage), or accidentally upgrading to a paid account. A billing alert at $0.10 is recommended, and the "Upgrade to Paid" button should not be clicked unless intentional. Oracle Cloud provides budget and alerting tools in the OCI Console under Billing > Budgets & Alerts.

---

## 6. Resource Limits

The following limits are the most relevant for engineering decisions when deploying Mind You on Oracle Always Free. These are hard caps that cannot be increased without upgrading to a paid OCI tier.

| Resource                      | Always Free Cap             | Practical Impact                                                                         |
| ----------------------------- | --------------------------- | ---------------------------------------------------------------------------------------- |
| ARM A1 Compute                | 2 OCPUs / 12 GB total       | Cannot exceed this across all instances; sufficient for Mind You at 4.5-7.5 GB estimated |
| Block Volume                  | 200 GB total (boot + block) | 50 GB boot leaves 150 GB for application data and database                               |
| Object Storage                | 20 GB combined tiers        | Sufficient for backup archives and log storage                                           |
| Outbound Data Transfer        | 10 TB/month                 | Far exceeds internal dashboard needs; charges eliminated Feb 2026                        |
| Load Balancer                 | 1 instance, 10 Mbps         | Single load balancer; sufficient for single-VM deployment                                |
| Autonomous Database           | 2 instances, 20 GB each     | Not used if self-hosting PostgreSQL via Docker                                           |
| Volume Backups                | 5 total (boot + block)      | Insufficient for comprehensive backup strategy; off-site pg_dump required                |
| API Requests (Object Storage) | 50,000/month                | Sufficient for automated backup uploads                                                  |

The Always Free allocation is fixed and cannot be increased. Unlike paid OCI tiers, there is no option to purchase additional Always Free capacity. If the workload grows beyond the 12 GB RAM or 200 GB storage limits, migration to a paid OCI tier or another platform would be required.

---

## 7. Pros and Cons

**Pros:**

- $0/month with no time limit — Always Free services do not expire and remain available for the life of the account
- 12 GB RAM always-on — highest among all free-tier options evaluated, sufficient for the full Mind You stack
- Commercial use permitted without restriction — suitable for production internal tools
- 200 GB storage — substantially exceeds Supabase Free (500 MB), Vercel (1 GB), Railway (0.5 GB), and Render (1 GB)
- 10 TB/month egress — sufficient for internal and production workloads with scheduled ingestion
- Full server control — SSH, Docker, custom stack configuration with root access
- Zero application code changes under Architecture A — the existing Supabase client works with self-hosted Supabase
- Standard PostgreSQL — no vendor lock-in; data can be exported to any PostgreSQL host
- Simple rollback via DNS re-pointing to Vercel within hours

**Cons:**

- No SLA — 95-99% estimated uptime with no compensation for downtime or guaranteed availability
- Idle reclamation risk — VM may be terminated if utilization falls below 20% over a 7-day window
- Account inactivity risk — 90-day suspension policy for inactive accounts
- Self-managed infrastructure — Docker, PostgreSQL, backups, monitoring, and security patches require manual effort
- ARM architecture — potential image compatibility issues requiring pre-migration testing
- Quota may change without notice — demonstrated by the June 2026 reduction from 4/24 to 2/12
- Community support only — no official troubleshooting assistance for Oracle Cloud-specific issues
- Home-region locked — resources cannot be moved to another region after tenancy creation
- Infrastructure management overhead — estimated 2-4 hours/month for ongoing maintenance

The advantages collectively outweigh the disadvantages for Mind You's use case as an internal tool. The 12 GB RAM allocation, zero cost, and commercial use permission are decisive factors. The caveats are manageable through documented mitigations and are acceptable trade-offs for a non-customer-facing application. The primary risk — quota reduction without notice — is mitigated by maintaining Vercel as a fallback, enabling DNS re-pointing within hours if Oracle changes Always Free terms.

---

## 8. Suitability for Mind You

Oracle Always Free is suitable for the current Mind You architecture under Architecture A, which deploys the full stack on a single ARM A1 VM using Docker Compose.

### 8.1 RAM Budget

The estimated RAM requirement for the Mind You stack is 4.5 to 7.5 GB, well within the 12 GB allocation. The stack includes PostgreSQL (2-3 GB), GoTrue authentication (256-512 MB), PostgREST REST API (256-512 MB), Supabase Studio (256-512 MB), Realtime (256-512 MB), the Next.js application (512 MB-1 GB), Caddy reverse proxy (50-100 MB), and OS plus Docker overhead (500 MB-1 GB).

### 8.2 Migration Effort

Migration effort is estimated at 12 to 14 hours for a developer with Docker and Linux experience. No application code changes are required — the existing `@supabase/supabase-js` client library works with self-hosted Supabase without modification. The primary work involves creating a Dockerfile for the Next.js ARM64 build, a docker-compose.yml for the Supabase stack, a Caddyfile for reverse proxy configuration, and updating environment variables and DNS records.

The migration can be performed with zero downtime by running the Oracle VM alongside the existing Vercel deployment during the transition period. DNS can be switched when the Oracle deployment is verified and ready. Rollback is a DNS re-point if issues are discovered after migration.

### 8.3 Deployment Considerations

The primary deployment considerations are: ARM64 image compatibility testing before migration, idle reclamation prevention via health-ping cron, off-site backup configuration using pg_dump to Object Storage or a separate location, and security hardening including SSH key authentication, firewall rules restricted to ports 22, 80, and 443, and regular OS updates.

A billing alert should be configured at $0.10 in the OCI Console to catch accidental charges. The public IP should be reserved to prevent loss on VM reboot. The home region should be selected with care, as it cannot be changed after tenancy creation. DNS TTL should be lowered to 300 seconds at least 24 hours before migration to minimize propagation delay.

### 8.4 Major Risks

Major risks include idle reclamation (mitigated by cron), account suspension (mitigated by regular sign-in and valid payment method), quota reduction (mitigated by Vercel fallback and off-site backups), and the absence of an SLA (accepted as a trade-off for internal tooling). Three risks are rated critical impact; all have documented mitigations with low residual risk.

The risk profile is acceptable for Mind You because the application is internal and non-customer-facing. Downtime would affect team productivity but would not directly impact revenue or reputation. The estimated 95-99% uptime, while lower than managed platforms, is sufficient for an internal dashboard used during business hours.

### 8.5 Architecture B

Architecture B, which retains Supabase Cloud for the database while deploying only Next.js to Oracle, is not recommended. It requires a complete rewrite of all database queries, authentication logic, and storage integration, with an estimated effort of 45 to 80 hours. The operational benefit of a managed database does not justify the migration cost for an internal tool.

### 8.6 Comparison with Alternatives

Oracle Always Free was compared against four alternatives: the current Vercel and hosted Supabase arrangement, Vercel Hobby (free), Railway (trial/free), and Render (free). Oracle Always Free is the only evaluated option that simultaneously provides sufficient RAM for the full stack, always-on compute without sleep or cold starts, self-hosted PostgreSQL, commercial use permission, and permanent free availability.

Vercel Hobby comes closest but is restricted to non-commercial use and provides a maximum of 2 GB of RAM per function. Railway's free tier offers only 0.5 GB per service and services sleep after approximately 5 minutes of inactivity. Render's free tier provides 512 MB of RAM with 30-60 second cold starts after 15 minutes of inactivity. None of these alternatives meet all of Mind You's requirements simultaneously.

The comparison also considered Vercel Pro plus Supabase Pro at $45 per month as the managed alternative. This option provides 99.99% SLA, zero operational overhead, and no migration required, but incurs $540 in annual costs and does not provide the same level of server control or commercial use flexibility as Oracle Always Free.

---

## 9. Final Recommendation

Oracle Always Free with Architecture A is recommended for Mind You, subject to five conditions: the team accepts self-managed infrastructure; the team accepts the absence of an SLA; idle reclamation prevention is implemented; off-site backups are maintained independent of Oracle Cloud; and Vercel is retained as a fallback.

The recommendation is conditional and carries a medium-high confidence level. Factual claims regarding compute, storage, and cost are rated high confidence based on Oracle's official documentation verified on 2026-07-23. Engineering estimates for migration effort and RAM budget are rated medium confidence based on codebase inspection without validation through actual migration. Risk assessments and uptime estimates carry medium confidence based on community reports without systematic measurement.

The recommendation reflects a trade-off: Oracle Always Free provides the highest RAM allocation and zero cost among free-tier options, but introduces operational responsibilities that managed platforms handle automatically. For Mind You as an internal tool with a technically capable team, this trade-off is favorable. The estimated 12-14 hour migration effort and 2-4 hours/month ongoing overhead are reasonable for the $0/month cost and commercial use flexibility gained.

If any condition is unacceptable, the alternative is Vercel Pro combined with Supabase Pro at $45 per month, which provides managed infrastructure, an SLA, and zero operational overhead. This alternative preserves all current functionality with no migration required, but incurs $540 in annual hosting costs and does not provide the full server control or commercial use flexibility of Oracle Always Free.

---

## 10. Supporting Research

The complete investigation is documented in the following files. These documents provide the detailed evidence base for all conclusions presented in this summary.

### Executive Summary

`oracle-cloud-always-free-executive-summary.md`

A concise management summary covering the evaluation's purpose, methodology, key findings, recommendation, and conclusion. Intended for technical reviewers, advisers, and project stakeholders who need the essential findings without the full technical detail. Approximately 850 words.

### Complete Engineering Evaluation

`oracle-cloud-always-free-evaluation.md`

The complete technical report containing the full methodology, all thirteen sections of analysis (resource specifications, operational limitations, deployment architecture, comparison matrix, migration effort, cost analysis, risk assessment, performance benchmarks, setup instructions, production readiness checklist, and go/no-go decision), references by source tier, and all supporting evidence. This document is the primary evidence base for all conclusions presented in this summary. Approximately 1,670 lines.

Both documents were created on 2026-07-23 and have been reviewed for accuracy, consistency, and completeness. Tier 1 source citations include verified URLs and access dates. Engineering estimates are clearly labeled as such and should be validated through actual migration testing.
