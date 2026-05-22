# NoesisFood Functional Specification

## Purpose
This document is the current source-of-authority functional specification for NoesisFood.

It describes:
- what the product does;
- which user-facing flows are in scope;
- which backend and frontend behaviors are intentionally supported;
- key safety, wording, and launch-readiness constraints;
- which areas must be treated as stable unless explicitly changed.

This document is intentionally functional rather than implementation-deep. Runtime code, tests, and operational checklists remain the authoritative sources for exact technical behavior.

## Current Product Summary
NoesisFood is a consumer packaged-food analysis app for supermarket products.

It supports:
- barcode/product lookup;
- manual product analysis;
- photo/OCR-assisted analysis;
- Vita Score and explanation rendering;
- allergen detection from available label data;
- dietary signals from available label data;
- usage-context messaging for special product types such as seasonings;
- safety/trust messaging from external and local safety sources;
- correction and feedback flows;
- multilingual UI in `EL`, `EN`, `DE`, and `FR`;
- PWA installability and Google Play packaging support.

NoesisFood is an informational product-analysis service. It does not provide medical advice.

## Functional Scope
### In scope
- Consumer-facing food-product analysis for packaged supermarket products.
- Product lookup via barcode and external product data sources.
- Manual fallback flows where barcode/product data is missing or incomplete.
- Photo/OCR flows for nutrition and ingredient extraction support.
- Scoring and explanation presentation through the NoesisFood Vita Score model.
- Label-data-based allergen, dietary, and usage-context messaging.
- Safety and recall-awareness overlays where supported by configured sources.
- Correction and feedback collection for product-quality improvement.
- Public privacy and data-deletion support pages for store-review readiness.

### Out of scope
- Medical diagnosis or treatment recommendations.
- Religious or dietary suitability guarantees.
- Offline product-analysis caching.
- Push notifications, background sync, periodic background sync, or advanced PWA permissions.
- User-account management or login.
- Admin dashboards or in-app moderation tooling.

## Supported User Flows
### 1. Home / scan entry
Users can start analysis from the home screen using:
- barcode/product identifier lookup;
- manual entry;
- photo-based flows.

The home screen also includes:
- market-ready informational transparency notice;
- language selector;
- recent/home auxiliary UI where available.

### 2. Barcode / product lookup flow
Users submit a barcode or product identifier.

Expected behavior:
- the app attempts product lookup through Open Food Facts-backed flows;
- normalized product data is analyzed and rendered;
- if data is incomplete, confidence and messaging remain cautious;
- safety overlays may be attached where source evidence supports them.

### 3. Manual analysis flow
Users can submit manual nutrition and product information.

Expected behavior:
- the app returns a result without requiring an external product match;
- confidence is lower or more cautious where source quality is limited;
- manual results still support explanations, usage context, and feedback paths.

### 4. Photo / OCR flow
Users can upload product-related images for OCR-assisted recovery.

Expected behavior:
- photos may support nutrition extraction and fallback rescue paths;
- failures should degrade to recovery-oriented errors, not silent crashes;
- OCR-backed results remain more cautious than verified structured lookups.

### 5. Result page
The result page presents:
- product identity block;
- Final Vita Score;
- Basic Nutrition Score when available;
- score explanation and adjustments;
- confidence / data-quality messaging;
- allergens;
- dietary signals;
- usage context;
- additives / ingredient intelligence and related detail areas;
- comparison and related secondary actions where supported;
- correction and feedback entry points.

Section ordering and existing result architecture are considered stable unless explicitly changed.

### 6. Correction and feedback
Users can:
- correct nutrition values in-session;
- submit correction feedback;
- submit improvement-oriented feedback where available.

Expected behavior:
- corrected-session recalculation refreshes score and explanation state;
- no-change and unrealistic-value submissions are rejected;
- duplicate feedback protections remain active.

## Functional Constraints
### Informational-only positioning
NoesisFood must remain an informational product-analysis app.

It must not claim:
- medical authority;
- guaranteed safety;
- guaranteed dietary or religious suitability;
- definitive compliance from incomplete or inferred data alone.

### Dietary-signals constraints
`dietary_signals` is a cautious label-data feature, not a suitability checker.

Current active v1 signals:
- `vegan`
- `vegetarian`
- `halal`

Not active:
- `kosher`
- `gluten_free`
- `lactose_free`

Allowed statuses:
- `halal`: `certified | unclear | possible_not_suitable`
- `vegan`: `labeled | unclear | possible_not_suitable`
- `vegetarian`: `labeled | unclear | possible_not_suitable`

Important wording constraints:
- no `safe`, `approved`, `guaranteed suitable`, `probably_vegan`, or `possible_halal`;
- no positive dietary claims from absence-of-concern terms alone;
- one shared caution warning per dietary card in the UI.

### Allergen constraints
Allergen output must remain cautious and source-aware.

The feature must not overclaim absence or guaranteed safety.
Localized display terms exist for major allergen groups and raw-tag mapping where relevant in the UI.

### Usage-context constraints
`usage_context` is a contextual interpretation layer for special product types such as seasonings.

It may adjust messaging context, but must not change underlying scoring math unless explicitly designed to do so.

### Score explanation constraints
The app distinguishes between:
- `Basic Nutrition Score`
- `Final Vita Score`
- score adjustments, caps, floors, and confidence/guard notes

User-facing wording should remain professional, non-medical, and understandable in all supported UI languages.

## External Data and Safety Sources
### Product data
Primary product lookup is backed by Open Food Facts-related flows.

Coverage may vary by country and by product completeness. The product experience is therefore globally possible, but not globally uniform.

### Safety sources
Current safety/trust integrations include:
- `RASFF`
- Germany-specific `lebensmittelwarnung.de`
- Greece-specific `EFET`

This means safety-source richness is not geographically identical across all markets.

## Supported Languages
Current supported UI languages:
- Greek (`EL`)
- English (`EN`)
- German (`DE`)
- French (`FR`)

Critical strings and functional UI paths are expected to work in these four languages.

## Public Routes and Launch-Readiness Pages
Current important public routes include:
- `/`
- `/manifest.webmanifest`
- `/service-worker.js`
- `/.well-known/assetlinks.json`
- `/privacy`
- `/data-deletion`
- `/health`

Store-readiness pages:
- `/privacy`: public privacy policy
- `/data-deletion`: public deletion/correction request page

These pages are launch-readiness/legal support surfaces and should remain isolated from the core app flow.

## PWA and Packaging Scope
Current PWA/packaging posture is intentionally conservative.

Included:
- manifest
- branded icons
- conservative service worker
- versioned shell cache
- Android Digital Asset Links support for TWA packaging

Not included:
- push notifications
- background sync
- periodic background sync
- offline scan/API caching
- advanced permission expansion

These exclusions are intentional and should not be changed casually.

## Repo-Specific Implementation Boundaries
### Stable runtime areas
The following are treated as stable unless explicitly approved for change:
- scan flow;
- camera/photo flow;
- OCR flow;
- manual barcode/product ID flow;
- product lookup logic;
- scoring logic and thresholds;
- allergen logic;
- dietary-signals logic;
- usage-context logic;
- feedback logic;
- monitoring/debug behavior;
- API contracts;
- existing data structures;
- PWA icons, manifest behavior, and service worker behavior;
- multilingual behavior;
- section ordering and navigation/state behavior.

### Frontend structure
The main consumer UI lives primarily in:
- [app/frontend/index.html](/C:/Users/pflem/noesisfood/app/frontend/index.html)

This file carries:
- core layout;
- theme and styling;
- client-side UI rendering hooks;
- PWA registration hooks.

Because the frontend is intentionally centralized, CSS-first changes are often low risk, while structural render-flow changes are higher risk.

## Observability and Quality Expectations
Functional changes should preserve:
- manual regression stability;
- launch-readiness behavior;
- recovery-oriented error handling for incomplete or failed OCR/photo flows;
- correction/feedback routes and validation;
- live deploy verification patterns.

## Related Operational Docs
This spec should be read alongside:
- [docs/regression_test_checklist.md](/C:/Users/pflem/noesisfood/docs/regression_test_checklist.md)
- [docs/soft_beta_smoke_checklist.md](/C:/Users/pflem/noesisfood/docs/soft_beta_smoke_checklist.md)
- [docs/beta_daily_review.md](/C:/Users/pflem/noesisfood/docs/beta_daily_review.md)
- [docs/correction_feedback_review.md](/C:/Users/pflem/noesisfood/docs/correction_feedback_review.md)

Those documents are operational checklists. This file is the functional overview and source-of-authority summary.

## Recommended Future Documentation Split
One single spec file is the right choice for now.

Split later only if the document becomes hard to maintain. The first clean split, if needed, should likely be:
- functional spec;
- safety / dietary / trust rules;
- launch-readiness / PWA / store packaging notes;
- QA / regression procedures.

For the current repo size and stability needs, one top-level functional spec is the lowest-risk choice.
