import { getDb, closeDb } from "@/src/db/client";
import { createHash } from "node:crypto";

const COMMUNITY_JSON_URL =
  "https://raw.githubusercontent.com/BELYAGOUBIABDELILAH/open-prompt-library/main/data/prompts.json";

const FOLDER_CATEGORY_MAP: Record<string, string> = {
  "coding-development": "code_review",
  "ai-automation": "architecture",
  "writing-content": "documentation",
  "data-analytics": "debugging",
  "marketing-social": "stakeholder_emails",
  "business-career": "stakeholder_emails",
  "education-learning": "intern_mentoring",
  "security": "security_audit",
  "image-design": "code_review",
  "general": "documentation",
  "documentation": "documentation",
  "research-analysis": "architecture",
  "health-wellness": "documentation",
  "sales-business": "stakeholder_emails",
  "games-fun": "documentation",
  "food-recipes": "documentation",
};

function categoryFromFolder(folder: string): string {
  return FOLDER_CATEGORY_MAP[folder] ?? "documentation";
}

function hashContent(content: string): string {
  return createHash("sha256").update(content).digest("hex").slice(0, 16);
}

const CURATED_EXTRAS = [
  {
    title: "System Design Whiteboard Session",
    content: "I'm designing {system_name} for {scale} users. Walk me through a whiteboard-style system design: (1) requirements (functional/non-functional), (2) high-level architecture diagram (text), (3) data model, (4) API design, (5) key trade-offs, (6) scaling bottlenecks and how to address them. Be opinionated about technology choices.",
    category: "architecture",
    description: "Whiteboard-style system design interview or planning session",
    input_fields: JSON.stringify(["system name", "expected scale", "key requirements"]),
    output_description: "Full system design walkthrough with architecture, data model, APIs, trade-offs, and scaling plan",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "API Error Response Standardization",
    content: "Review the API error responses in {codebase_path}. Identify inconsistencies, then propose a standardized error response format following RFC 7807 (Problem Details). Include: (1) current state analysis, (2) proposed schema, (3) migration plan, (4) middleware/error handler implementation. All errors should include: type, title, status, detail, instance, and optional extensions.",
    category: "refactoring",
    description: "Standardize API error responses across a codebase",
    input_fields: JSON.stringify(["codebase path or API spec"]),
    output_description: "Error format audit, RFC 7807 schema, migration plan, and error handler implementation",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "Docker Compose Development Environment",
    content: "Create a docker-compose.yml for developing {project_name}. Services needed: {services}. Include: (1) service definitions with health checks, (2) volume mounts for live reload, (3) environment variables with sensible defaults, (4) network configuration, (5) wait-for-it dependency ordering, (6) a Makefile with common commands (up, down, logs, shell, test). Use multi-stage builds where applicable.",
    category: "documentation",
    description: "Generate a production-grade Docker Compose setup for local development",
    input_fields: JSON.stringify(["project name", "comma-separated list of services"]),
    output_description: "Complete docker-compose.yml, Dockerfiles, env file, Makefile, and README",
    model_recommendation: "Claude Sonnet 4, GPT-4o, Claude Haiku",
  },
  {
    title: "CI/CD Pipeline Audit",
    content: "Audit the CI/CD pipeline defined in {ci_file}. Evaluate: (1) build time and caching strategy, (2) test coverage and parallelism, (3) security scanning gaps, (4) deployment safety (canary/blue-green), (5) rollback capability, (6) secret management. For each issue found, provide: severity, impact, and a concrete fix with YAML diff.",
    category: "code_review",
    description: "Audit CI/CD pipeline for performance, security, and reliability",
    input_fields: JSON.stringify(["path to CI config file"]),
    output_description: "Pipeline audit with severity-graded findings and concrete YAML fixes",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "Log Analysis Pattern Detection",
    content: "Analyze these logs and identify patterns: ```{logs}```. For each pattern: (1) describe the pattern, (2) count occurrences, (3) time distribution, (4) root cause hypothesis, (5) correlation with other patterns. Output as a structured report with severity rankings. Highlight any patterns that indicate systemic issues vs. one-off errors.",
    category: "incident_analysis",
    description: "Detect and analyze patterns in application logs",
    input_fields: JSON.stringify(["log snippets or log file path"]),
    output_description: "Structured pattern analysis report with severity rankings and root cause hypotheses",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "TypeScript Migration Planner",
    content: "Plan a migration from {source_lang} to TypeScript for {project_name}. Include: (1) file-by-file migration order (leaf modules first), (2) tsconfig.json strategy (strict vs. permissive), (3) third-party type package needs (DefinitelyTyped), (4) build pipeline changes, (5) common migration pitfalls for this specific source language, (6) rollout strategy (allowJs, incremental adoption). Estimate effort per phase.",
    category: "refactoring",
    description: "Plan a phased migration to TypeScript with risk mitigation",
    input_fields: JSON.stringify(["source language (JS/Python/etc)", "project name", "approximate file count"]),
    output_description: "Phased migration plan with file ordering, config strategy, effort estimates, and common pitfalls",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "API Rate Limiting Design",
    content: "Design a rate limiting strategy for {api_name}. Requirements: (1) support tiered limits (free/pro/enterprise), (2) sliding window algorithm, (3) distributed-friendly, (4) clear error responses with Retry-After header, (5) rate limit headers on every response (X-RateLimit-Limit, Remaining, Reset), (6) admin overrides for specific keys. Compare Redis-based vs. in-memory approaches. Provide implementation sketches in {preferred_language}.",
    category: "architecture",
    description: "Design a distributed rate limiting system for APIs",
    input_fields: JSON.stringify(["API name", "preferred implementation language"]),
    output_description: "Rate limiting strategy with algorithm comparison, header spec, error format, and code sketches",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "Post-Mortem Template Filler",
    content: "Fill out a post-mortem for this incident: {incident_description}. Follow the template: (1) Date & Duration, (2) Summary (one sentence), (3) Timeline (all times in UTC), (4) Root Cause, (5) Impact (users affected, revenue, data), (6) Detection (how was it found, how long to detect), (7) Response (who, what actions, how long to mitigate), (8) Resolution, (9) Five Whys analysis, (10) Action Items (owner, deadline, type: prevent/detect/process). Be brutally honest about gaps.",
    category: "incident_analysis",
    description: "Generate a structured post-mortem from an incident description",
    input_fields: JSON.stringify(["incident description", "approximate duration"]),
    output_description: "Complete post-mortem with timeline, five whys, and action items with owners",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "Dependency Upgrade Impact Analysis",
    content: "Analyze upgrading {package_name} from {current_version} to {target_version}. For each breaking change: (1) what changed, (2) how many files affected, (3) migration pattern, (4) test gaps. Include: changelog summary, API diff, deprecation warnings, and a validated upgrade script that runs the existing test suite at each intermediate version.",
    category: "refactoring",
    description: "Analyze impact of upgrading a critical dependency",
    input_fields: JSON.stringify(["package name", "current version", "target version"]),
    output_description: "Breaking change analysis with affected files, migration patterns, and upgrade script",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "Code Review: Security-Focused",
    content: "Review this code specifically for security issues: ```{code}```. Check for: (1) injection vulnerabilities (SQL, NoSQL, command, LDAP), (2) authentication/authorization bypasses, (3) sensitive data exposure, (4) XML external entities (XXE), (5) broken access control, (6) security misconfiguration, (7) cross-site scripting (XSS), (8) insecure deserialization, (9) known vulnerable components, (10) insufficient logging/monitoring. Rate each finding using CVSS-like scoring.",
    category: "security_audit",
    description: "Security-focused code review against OWASP Top 10",
    input_fields: JSON.stringify(["code snippet or diff"]),
    output_description: "OWASP-grounded security review with CVSS-like severity scores and remediation",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "Performance Regression Investigation",
    content: "We have a performance regression in {component_name}. Before: {before_metric}. After: {after_metric}. Profile data: {profile_data}. Investigate: (1) isolate the commit that introduced the regression using git bisect, (2) analyze the diff for likely causes, (3) suggest the minimal fix, (4) add a performance regression test to CI. Include before/after flamegraph interpretation.",
    category: "debugging",
    description: "Investigate and fix a performance regression",
    input_fields: JSON.stringify(["component name", "before metric", "after metric", "profile data or commit range"]),
    output_description: "Bisect result, root cause analysis, fix suggestion, and regression test",
    model_recommendation: "Claude Sonnet 4, GPT-4o",
  },
  {
    title: "Code Documentation Generator",
    content: "Generate comprehensive documentation for {codebase_path}. Focus on: (1) module-level READMEs explaining purpose and ownership, (2) function/class-level JSDoc comments for all public APIs, (3) architecture decision records for significant design choices, (4) setup/contribution guide, (5) API reference (auto-generated from types), (6) troubleshooting FAQ. Use JSDoc/TSDoc format. Include examples for every public API.",
    category: "documentation",
    description: "Generate comprehensive code documentation from source",
    input_fields: JSON.stringify(["codebase path or module name"]),
    output_description: "README files, JSDoc comments, ADRs, setup guide, API reference, and FAQ",
    model_recommendation: "Claude Sonnet 4, GPT-4o, Claude Haiku",
  },
];

const UI_DESIGN_PROMPTS = [
  {
    title: "SaaS Landing Page Hero",
    content: "Create a landing page hero section for {product_name}, a {product_description}. Include: (1) headline (max 10 words) with a highlighted benefit, (2) subheadline (max 20 words) explaining who it's for, (3) two CTAs (primary: 'Get Started Free', secondary: 'See Demo'), (4) a mock product screenshot or dashboard preview on the right, (5) subtle background pattern or gradient. Use modern sans-serif typography, generous whitespace, and a single accent color. Primary CTA should be the accent color. Style: clean, professional, conversion-focused.",
    category: "code_review",
    description: "Generate a conversion-optimized SaaS landing page hero section",
    input_fields: JSON.stringify(["product name", "product description"]),
    output_description: "HTML/CSS for a hero section with headline, CTAs, and mockup layout",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "GenDesigns",
    source_url: "https://gendesigns.ai/blog/ai-design-prompt-library-100-prompts",
  },
  {
    title: "Dark Dashboard with Sidebar",
    content: "Create a dark-themed analytics dashboard layout. Include: (1) fixed left sidebar (240px) with logo, 5 nav items (Dashboard, Analytics, Settings, Team, Billing), user avatar at bottom, (2) top bar with search, notifications bell (with badge), and date range selector, (3) main content area with 4 stat cards (Revenue, Users, Sessions, Conversion Rate) in a row, (4) a line chart below the stats, (5) a right sidebar or panel for recent activity feed. Background: very dark (#0a0a0f). Cards: slightly lighter (#14141f) with 1px subtle borders. Accent color: teal/cyan. All text: white/light gray hierarchy.",
    category: "code_review",
    description: "Generate a complete dark analytics dashboard layout",
    input_fields: JSON.stringify(["dashboard name", "primary metric labels (optional)"]),
    output_description: "HTML/CSS/JS for a responsive dark dashboard with sidebar, charts, and stat cards",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0, Cursor",
    source: "GenDesigns",
    source_url: "https://gendesigns.ai/blog/ai-design-prompt-library-100-prompts",
  },
  {
    title: "Pricing Table — 3 Tiers",
    content: "Design a pricing section with 3 tiers: Free, Pro ($29/mo), Enterprise (custom). Style: clean cards with subtle shadows, rounded corners (12px). Pro tier should be visually emphasized (highlighted border or slightly elevated). Each card includes: plan name, price, description, feature list (5-7 items with checkmarks), and a CTA button. Include an annual/monthly toggle at the top that updates prices (20% annual discount). Add a 'Most Popular' badge on the Pro tier. Footer text: 'All plans include a 14-day free trial. No credit card required.'",
    category: "code_review",
    description: "Generate a 3-tier SaaS pricing table with annual/monthly toggle",
    input_fields: JSON.stringify(["product name", "custom pricing (optional)"]),
    output_description: "HTML/CSS/JS for responsive pricing table with toggle and highlighted pro tier",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "iToolVerse",
    source_url: "https://www.itoolverse.com/generator/ui-prompts",
  },
  {
    title: "Feature Grid — Bento Layout",
    content: "Create a bento-grid feature showcase section for {product_name}. Use a 3-column grid with varying cell sizes (some 1x1, one 2x1 featured cell, one 1x2). Each cell has: icon (top-left), title, short description (max 2 lines), and a subtle hover effect (scale 1.02 + shadow). Include a section header with 'Everything you need to {value_prop}' as H2 and a short supporting paragraph. Style: minimal, plenty of whitespace, icons in a consistent outline style with an accent color. Responsive: collapse to 2-col on tablet, 1-col on mobile.",
    category: "code_review",
    description: "Generate a bento-grid feature showcase section",
    input_fields: JSON.stringify(["product name", "value proposition", "number of features"]),
    output_description: "Responsive bento-grid HTML/CSS with varying cell sizes and hover effects",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "GenDesigns",
    source_url: "https://gendesigns.ai/blog/ai-design-prompt-library-100-prompts",
  },
  {
    title: "Login & Register Page (Auth Flow)",
    content: "Create a modern authentication page with both Login and Register tabs/slides. Left half: brand area with logo, tagline, and a subtle illustration or gradient. Right half: the form. Login form: email, password, 'Forgot password?' link, 'Sign in' button (primary color), and 'No account? Sign up' link. Register form: name, email, password, confirm password, terms checkbox, 'Create account' button. Include social login buttons (Google, GitHub) with icons above the form divider ('or continue with'). Form validation states: default, focused, error (red border + message), success. Subtle micro-animations on tab switch.",
    category: "code_review",
    description: "Generate a complete login/register auth page with split layout",
    input_fields: JSON.stringify(["brand name", "accent color (optional)"]),
    output_description: "HTML/CSS/JS for auth page with login/register tabs, social buttons, and validation states",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "TypeUI",
    source_url: "https://www.typeui.sh/prompts",
  },
  {
    title: "SaaS Dashboard — Analytics Overview",
    content: "Design an analytics overview page for a SaaS dashboard. Top: date range picker and 'Export' button. Content area: 4 stat cards (MRR, Active Users, Churn Rate, NPS Score) with trend indicators (green up / red down arrows and percentage). Below: a line chart (revenue over time) on the left (2/3 width) and a donut chart (user segments) on the right (1/3 width). Below that: a data table (Recent Transactions) with columns: User, Plan, Amount, Status (paid/pending/refunded), Date. Include pagination. Use a clean data typography (tabular numbers for metrics). Responsive: stack cards on mobile, full-width charts.",
    category: "code_review",
    description: "Generate a data-rich analytics dashboard page",
    input_fields: JSON.stringify(["app name", "time range (default: Last 30 days)"]),
    output_description: "HTML/CSS/JS for analytics dashboard with stat cards, charts, and data table",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "iToolVerse",
    source_url: "https://www.itoolverse.com/generator/ui-prompts",
  },
  {
    title: "Testimonials / Social Proof Section",
    content: "Create a testimonials section for {company_name}. Layout: centered H2 'Loved by {industry} teams', subtitle, then a grid of testimonial cards (3-6). Each card: quote text (in quotation marks), author avatar (placeholder circle), author name, title/company, and star rating (5 stars). Two layout options: (a) even card grid, or (b) masonry-style with varying quote lengths. Include a carousel variant for mobile. Add a subtle background element (dots pattern or soft gradient). Cards should have white/light backgrounds with shadows. Make quotes the focal point — large, readable text.",
    category: "code_review",
    description: "Generate a testimonials/social proof section with multiple layout options",
    input_fields: JSON.stringify(["company name", "industry", "number of testimonials"]),
    output_description: "HTML/CSS for testimonial grid with star ratings, avatars, and carousel variant",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "TypeUI",
    source_url: "https://www.typeui.sh/prompts",
  },
  {
    title: "404 Page with Personality",
    content: "Create a 404 error page that's helpful and on-brand. Include: (1) large illustration or icon (keep it simple — a broken grid, floating 404, or abstract shape), (2) 'Page not found' heading, (3) brief explanation text, (4) 'Back to home' primary CTA button, (5) search bar as secondary option, (6) a small easter egg (e.g., a hidden theme toggle or a funny tooltip). Keep the illustration minimal — no hand-drawn SVG. Use the brand's existing colors. Add a subtle CSS animation (floating or drift) on the illustration.",
    category: "code_review",
    description: "Generate a branded 404 error page with helpful navigation",
    input_fields: JSON.stringify(["company/brand name", "brand color (optional)"]),
    output_description: "HTML/CSS for 404 page with illustration, search, CTAs, and subtle animation",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0, Cursor",
    source: "TypeUI",
    source_url: "https://www.typeui.sh/prompts",
  },
  {
    title: "Mobile-First Landing Page",
    content: "Design a mobile-first landing page for {app_name}, a mobile app. Section order (mobile: single column): (1) Top bar with app logo and 'Download' button, (2) Hero with app mockup (phone frame), headline, subheadline, and app store badge buttons, (3) Feature list — 3 features with icon + text, each on its own row with alternating left/right layout on desktop, (4) Screenshots carousel/grid showing the app UI, (5) Pricing or 'Free forever' callout, (6) Footer with links and social icons. All touch targets ≥44px. Use system fonts for performance. Fast-loading, minimal JS. Progressive enhancement: basic HTML works without CSS.",
    category: "code_review",
    description: "Generate a mobile-first landing page optimized for app downloads",
    input_fields: JSON.stringify(["app name", "app description", "platform (iOS/Android/both)"]),
    output_description: "Responsive landing page HTML/CSS with app store buttons, feature list, and screenshots section",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "GenDesigns",
    source_url: "https://gendesigns.ai/blog/ai-design-prompt-library-100-prompts",
  },
  {
    title: "Newsletter / Blog Section",
    content: "Create a newsletter signup + blog preview section. Left: newsletter card with headline 'Stay in the loop', description, email input field with 'Subscribe' button (inline), and a small privacy note. Right: 2-3 recent blog post previews (stacked vertically) — each with category tag, title, publish date, excerpt (2 lines), and 'Read more' link. Style: clean, readable typography. The newsletter card should have a subtle background tint or a left accent border to distinguish it from blog posts. On mobile: stack vertically.",
    category: "code_review",
    description: "Generate a combined newsletter signup and blog preview section",
    input_fields: JSON.stringify(["blog name", "accent color"]),
    output_description: "HTML/CSS for newsletter card + blog post list with responsive layout",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0, Claude Haiku",
    source: "TypeUI",
    source_url: "https://www.typeui.sh/prompts",
  },
  {
    title: "E-commerce Product Page",
    content: "Design an e-commerce product page for {product_name}. Layout: Left (50%): product image gallery — main image with 4 thumbnail selectors below, zoom on hover. Right (50%): product title, rating stars with review count, price (with original price strikethrough if on sale and discount badge), color/size selector buttons, quantity stepper, 'Add to Cart' button (primary, full-width), 'Buy Now' button (secondary), accordion below for: Description, Shipping Info, Returns. Include trust signals: secure checkout badge, free shipping over $X. Mobile: stack vertically (images top, details bottom).",
    category: "code_review",
    description: "Generate a conversion-optimized e-commerce product page",
    input_fields: JSON.stringify(["product name", "price", "category"]),
    output_description: "HTML/CSS/JS for full product page with gallery, selectors, add-to-cart, and accordion details",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "TypeUI",
    source_url: "https://www.typeui.sh/prompts",
  },
  {
    title: "Bento Grid Dashboard — Personal Stats",
    content: "Design a personal stats dashboard using a bento grid layout. Use a 4-column grid with irregular cell sizes: (1) Profile card (1×1) — avatar, name, member since, (2) Streak card (1×1) — current streak number with fire icon, (3) Activity chart (2×1) — mini line chart showing 30-day activity, (4) Top items (1×2) — ranked list of top 5 with counts, (5) Calendar heatmap (2×1) — mini GitHub-style contribution grid, (6) Achievements (1×1) — badges earned. All cells have rounded corners (12px), subtle borders, and consistent padding. Background: light with very subtle dot pattern.",
    category: "code_review",
    description: "Generate a bento-grid personal stats dashboard",
    input_fields: JSON.stringify(["user type (developer, writer, etc)", "primary metric name"]),
    output_description: "HTML/CSS for bento-grid dashboard with chart, heatmap, stats, and badges",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0, Cursor",
    source: "iToolVerse",
    source_url: "https://www.itoolverse.com/generator/ui-prompts",
  },
  {
    title: "Settings Page with Tab Navigation",
    content: "Design a settings/preferences page. Left sidebar: vertical tab navigation (Profile, Account, Notifications, Appearance, Privacy, Billing) with active tab highlighted. Right content area changes based on selected tab. Profile tab: avatar upload, name, bio, location fields. Notifications tab: toggle switches for each notification type grouped by category (email, push, in-app). Appearance tab: theme selector (Light/Dark/System), font size slider, density selector (Comfortable/Compact). Each field/section should have clear labels and helper text below inputs. Show a save confirmation toast on changes. Tab state persists in URL hash.",
    category: "code_review",
    description: "Generate a multi-tab settings page with various input types",
    input_fields: JSON.stringify(["app name", "list of settings tabs (optional)"]),
    output_description: "HTML/CSS/JS for settings page with sidebar tabs, toggles, theme picker, and toast notifications",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "GenDesigns",
    source_url: "https://gendesigns.ai/blog/ai-design-prompt-library-100-prompts",
  },
  {
    title: "FAQ Accordion Section",
    content: "Create an FAQ section with centered H2 'Frequently asked questions'. Below: accordion list of 6-8 Q&A items. Each item: clickable question row with chevron icon (rotates on open), answer panel slides open below. Smooth animation (max-height transition, 300ms ease). Only one item open at a time (accordion behavior). Style: clean borders between items (no background on items), generous padding, readable font sizes. Include a 'Still have questions?' footer with a contact link or button. Add schema.org FAQ structured data markup (hidden) for SEO. Accessible: items are <button> elements, aria-expanded, keyboard navigable.",
    category: "code_review",
    description: "Generate an accessible FAQ accordion section with SEO markup",
    input_fields: JSON.stringify(["product/service name"]),
    output_description: "HTML/CSS/JS for FAQ accordion with accessible markup, SEO schema, and smooth animation",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0, Claude Haiku",
    source: "TypeUI",
    source_url: "https://www.typeui.sh/prompts",
  },
  {
    title: "Data Table with Filters",
    content: "Design a data table component for displaying tabular data. Include: (1) toolbar above with search input, column visibility dropdown, and 'Export CSV' button, (2) sortable column headers (click to sort asc/desc, show arrow indicator), (3) row hover highlight, (4) checkbox column for multi-select, (5) pagination below (Previous/Next, page numbers, 'Showing X of Y' text), (6) empty state when no results match filter. Columns should be resizable (drag handle on header borders). Use tabular/oldstyle figures for number columns so they align properly. Responsive: horizontal scroll on mobile with sticky first column.",
    category: "code_review",
    description: "Generate a feature-rich data table with sorting, filtering, and pagination",
    input_fields: JSON.stringify(["column names", "data type (e.g., users, orders)"]),
    output_description: "HTML/CSS/JS for data table with sort, search, column toggle, pagination, and empty state",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0, Cursor",
    source: "TypeUI",
    source_url: "https://www.typeui.sh/prompts",
  },
  {
    title: "Onboarding Flow — Step-by-Step",
    content: "Design an onboarding wizard with 4 steps. Layout: step indicator at top (circles with numbers, connected by lines; current step highlighted, completed steps have checkmarks), content area in the middle (changes per step), navigation buttons at bottom ('Back' and 'Next/Continue'). Step 1: Welcome screen with illustration, headline, and 'Get Started' button. Step 2: Personalization — 3-4 option cards to select preferences. Step 3: Connect/Integrate — app/service selection with search. Step 4: Done — confetti animation, 'You're all set!' message, and 'Go to Dashboard' button. Smooth slide transitions between steps (left/right). Skip link available on early steps.",
    category: "code_review",
    description: "Generate a multi-step onboarding wizard with progress indicator",
    input_fields: JSON.stringify(["product name", "onboarding steps (optional)"]),
    output_description: "HTML/CSS/JS for 4-step onboarding with animations, progress indicator, and personalization cards",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "iToolVerse",
    source_url: "https://www.itoolverse.com/generator/ui-prompts",
  },
  {
    title: "Contact / Support Page",
    content: "Create a contact and support page. Layout: Left (40%): contact info with icon list (email, phone, location, business hours) and a small illustration or brand mark. Right (60%): contact form with fields: Name, Email, Subject (dropdown: General, Support, Billing, Partnership), Message (textarea), and 'Send Message' button. Below the fold: an embedded FAQ or 'Common questions' section with 3-4 quick links. Form should show inline validation errors. On submit, show a success state replacing the form with a checkmark and 'We'll get back to you within 24 hours' message. Style: warm, trustworthy.",
    category: "code_review",
    description: "Generate a contact page with form, info, and inline validation",
    input_fields: JSON.stringify(["company name", "support email", "business hours"]),
    output_description: "HTML/CSS/JS for contact page with form validation, success state, and FAQ links",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "GenDesigns",
    source_url: "https://gendesigns.ai/blog/ai-design-prompt-library-100-prompts",
  },
  {
    title: "Waitlist / Pre-launch Landing Page",
    content: "Create a pre-launch waitlist landing page for {product_name}. Sections: (1) Full-screen hero with centered content — brand logo, headline ('{product_name} — coming soon'), subheadline, email input with 'Join the waitlist' button, social proof text ('Join {N} others'), (2) Feature preview — 3 simple benefit cards with icons, (3) Built with / Powered by logos (if relevant), (4) Footer with links. Style: minimal, large typography, one accent color, subtle background gradient or animation. No hero image — let the typography carry the page. On submit: show 'You're on the list!' confirmation with a share link. Add referrer count tracking.",
    category: "code_review",
    description: "Generate a pre-launch waitlist landing page with confirmation flow",
    input_fields: JSON.stringify(["product name", "tagline"]),
    output_description: "HTML/CSS/JS for waitlist page with email capture, confirmation, and social proof",
    model_recommendation: "Claude Sonnet 4, GPT-4o, v0",
    source: "GenDesigns",
    source_url: "https://gendesigns.ai/blog/ai-design-prompt-library-100-prompts",
  },
];

export async function ingestCommunityPrompts(): Promise<number> {
  console.log("  Fetching community prompts from open-prompt-library...");

  let entries: unknown[];
  try {
    const res = await fetch(COMMUNITY_JSON_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    entries = (await res.json()) as unknown[];
  } catch (err) {
    console.error("  ❌ Failed to fetch open-prompt-library:", err);
    return 0;
  }

  const MAX_COMMUNITY = 300;
  await getDb().from("prompts").delete().eq("source", "community");

  const toInsert: Record<string, unknown>[] = [];
  for (const entry of entries.slice(0, MAX_COMMUNITY)) {
    const e = entry as { act?: string; prompt?: string; folder?: string; slug?: string; source?: string; path?: string };
    if (!e.prompt || !e.act) continue;

    const title = e.act.slice(0, 200);
    const content = e.prompt.slice(0, 5000);
    const folder = e.folder ?? "general";
    const category = categoryFromFolder(folder);
    const externalId = hashContent(content);
    const sourceUrl = e.path
      ? `https://github.com/BELYAGOUBIABDELILAH/open-prompt-library/blob/main/${e.path}`
      : null;
    const description = `From the ${category} category in the open-prompt-library community collection`;
    const modelRec = e.source && e.source !== "Awesome ChatGPT Prompts" ? e.source : null;

    toInsert.push({
      title,
      content,
      category,
      description,
      source: "community",
      external_id: externalId,
      source_url: sourceUrl,
      output_description: null,
      model_recommendation: modelRec,
    });
  }

  if (toInsert.length > 0) {
    const { error } = await getDb().from("prompts").insert(toInsert);
    if (error) console.error("  ❌ Failed to insert community prompts:", error.message);
  }

  console.log(`  ✅ Inserted ${toInsert.length} community prompts (capped at ${MAX_COMMUNITY})`);
  return toInsert.length;
}

export async function ingestCuratedExtras(): Promise<number> {
  let count = 0;
  for (const p of CURATED_EXTRAS) {
    const { error } = await getDb().from("prompts").upsert({
      title: p.title,
      content: p.content,
      category: p.category,
      description: p.description,
      input_fields: p.input_fields,
      output_description: p.output_description,
      model_recommendation: p.model_recommendation,
      source: "curated",
      external_id: hashContent(p.title + p.content),
      is_featured: 0,
    }, { onConflict: "source, external_id", ignoreDuplicates: true });
    if (!error) count++;
  }
  console.log(`  ✅ Inserted ${count} additional curated prompts`);
  return count;
}

export async function ingestUiDesignPrompts(): Promise<number> {
  await getDb().from("prompts").delete().eq("source", "ui_design");

  const toInsert = UI_DESIGN_PROMPTS.map((p) => ({
    title: p.title,
    content: p.content,
    category: p.category,
    description: p.description,
    input_fields: p.input_fields,
    output_description: p.output_description,
    model_recommendation: p.model_recommendation,
    source: "ui_design",
    source_url: p.source_url,
    external_id: hashContent(p.title + p.content),
    is_featured: 0,
  }));

  const { error } = await getDb().from("prompts").insert(toInsert);
  if (error) console.error("  ❌ Failed to insert UI design prompts:", error.message);

  const count = error ? 0 : toInsert.length;
  console.log(`  ✅ Inserted ${count} UI design prompts`);
  return count;
}

async function main() {
  console.log("=".repeat(60));
  console.log("📝 Ingesting prompts");
  console.log("=".repeat(60));

  await ingestCuratedExtras();
  await ingestUiDesignPrompts();
  await ingestCommunityPrompts();

  const { count: total } = await getDb()
    .from("prompts")
    .select("*", { count: "exact", head: true });
  const { data: bySource } = await getDb()
    .from("prompts")
    .select("source");

  const sourceCounts = new Map<string, number>();
  for (const row of bySource ?? []) {
    sourceCounts.set(row.source, (sourceCounts.get(row.source) ?? 0) + 1);
  }

  console.log("\n📊 Prompt Library Summary:");
  console.log(`  Total prompts: ${total ?? 0}`);
  for (const [source, count] of sourceCounts) {
    console.log(`  ${source.padEnd(12)} ${count}`);
  }

  closeDb();
  console.log("\n✅ Prompt ingestion complete");
}

const isMain = process.argv[1]?.replace(/\\/g, "/").endsWith("index.ts");
if (isMain) {
  main().catch((err) => {
    console.error("❌ Fatal error:", err);
    process.exit(1);
  });
}

export {
  main as ingestPrompts,
  CURATED_EXTRAS,
  UI_DESIGN_PROMPTS,
};
