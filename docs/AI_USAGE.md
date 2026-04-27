# AI Usage Disclosure

## Overview

AI tools were used as development aids during this assessment, 
similar to how they would be used in a professional engineering 
environment. All architectural decisions, design choices, and 
implementation strategies are my own.

---

## Tools Used

| Tool | Model | Purpose |
|------|-------|---------|
| VS Code Antigravity | Claude Sonnet 4.6 | Debugging, brainstorming, code commenting |
| OpenAI Codex | ChatGPT 5.4 | Code review and feedback |

---

## How AI Was Used

### Debugging
- Troubleshooting Docker and PostgreSQL connectivity issues
- Resolving dbt model compilation errors
- Diagnosing AWS Lambda package compatibility issues

### Brainstorming
- Discussing star schema design options for datasets 
  with no natural foreign keys
- Evaluating imputation strategies (mean vs median vs 
  group-based median) for NULL income values
- Exploring POPIA compliance considerations for 
  healthcare data

### Code Commenting
- Refining docstrings and inline comments for clarity
- Ensuring comments accurately reflect implementation

### Code Review
- Final review of all tasks against assessment criteria
- Identifying documentation-to-implementation mismatches

---

## What I Decided and Built Myself

- **Architecture:** Star schema design with bridge table 
  approach for missing foreign keys (AD-006)
- **Imputation strategy:** Province-level median based on 
  South African regional income patterns (AD-002)
- **POPIA compliance:** Decision to exclude PII from 
  analytical layer (AD-007)
- **dbt structure:** Staging + marts layer organisation
- **Error handling patterns:** Lambda error-to-prefix flow 
  and structured CloudWatch logging
- **All SQL:** DDL, transformations, and data quality queries

---

## Principle

AI tools were part of my development workflow for this 
assessment, just as they would be in a professional 
engineering environment. The architectural decisions, 
design trade-offs, and implementation choices documented 
in the Technical Decisions Record reflect my own judgment 
based on my own experience, past projects and roles 
and understanding of the domain.