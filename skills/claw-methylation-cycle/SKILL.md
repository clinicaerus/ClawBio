---
name: methylation-cycle
version: 0.1.1
description: >
  Analyses methylation cycle gene variants (MTHFR, MTRR, MTR, CBS, BHMT,
  SHMT1, COMT, AHCY) from raw genotype data and produces a structured clinical
  report with enzymatic activity estimates, Net Methylation Capacity (NMC),
  BH4 axis capacity, compound heterozygosity detection, and prioritised
  supplementation recommendations.
author: Samuel Carmona Aguirre <samuel@unimed-consulting.es>
organization: UNIMED Consulting · CAPS Digital
framework: Holomedicina® (Samuel Carmona Aguirre, 2014/UNESCO 2016)
license: MIT
tags:
  - genomics
  - methylation
  - nutrigenomics
  - MTHFR
  - neurodevelopment
  - CAPS-Digital
trigger_keywords:
  - methylation
  - MTHFR
  - folate cycle
  - BH4
  - homocysteine
  - methylation cycle
  - metilación
  - ciclo de metilación
  - neurotransmitter synthesis
  - dopamine synthesis upstream

inputs:
  - name: genotype_file
    type: file
    formats: [23andme, adntro, ancestry]
    description: Raw SNP genotype file in 23andMe / ADNTRO tab-delimited format.
  - name: snp_dict
    type: dict
    description: >
      Alternative programmatic input — mapping of rsID → genotype string
      (e.g. {"rs1801133": "CT"}). Used when the upstream pipeline has already
      parsed the raw file.

outputs:
  - name: report_md
    type: file
    format: markdown
    description: Human-readable clinical methylation cycle report.
  - name: result_json
    type: file
    format: json
    description: >
      Structured JSON output for downstream integration with 13MIL v6.0,
      Escáner Semántico Clínico, and N-GENE complementary PRS layer.

dependencies:
  python: ">=3.9"
  packages:
    - pytest>=7.0
  optional:
    - pandas
---

# claw-methylation-cycle

Methylation cycle analysis skill for ClawBio. Produces enzymatic activity
profiles, Net Methylation Capacity (NMC), BH4 axis estimates, compound
heterozygosity detection, and clinical recommendations from raw SNP genotype
data — integrated into the CAPS Digital / Holomedicina® clinical framework.

---

## Trigger

**Fire this skill when:**

- The user asks about methylation, MTHFR variants, folate cycle, or
  homocysteine risk from a genotype file.
- A raw SNP file (23andMe / ADNTRO / Ancestry format) is provided and the
  clinical question involves methylation, BH4, dopamine/serotonin synthesis
  capacity, or neurodevelopmental contexts (ADHD, depression, anxiety).
- The upstream workflow (PharmGx Reporter, NutriGx Advisor) has flagged
  MTHFR or MTRR and the clinician needs the full methylation panel.
- Integration with a holonic clinical report (13MIL v6.0 / CAPS Digital)
  requires the methylation layer.
- Keywords present: `methylation`, `MTHFR`, `BH4`, `folate cycle`,
  `metilación`, `ciclo de metilación`, `homocysteine`, `dopamine upstream`,
  `neurotransmitter synthesis`, `5-MTHF`, `methylcobalamin`.

**Do NOT fire this skill when:**

- The question is purely about folic acid dietary supplementation without
  a genotype file available.
- The user is asking about MTHFR in the context of thrombophilia/clotting
  only — use the PharmGx Reporter for warfarin/anticoagulation questions.
- Only N-GENE polygenic risk report data is available (no raw SNP file) —
  the skill requires genotype-level input; PRS percentiles are not sufficient.
- The SNP file format is VCF, FASTQ, BAM, or PLINK binary — these require
  preprocessing before this skill can run.
- The clinical question is exclusively pharmacogenomic (CYP enzymes,
  drug metabolism) — use PharmGx Reporter instead.

---

## Workflow

1. **Receive input** — Accept either a raw genotype file path or a pre-parsed
   `snp_dict`. If a file is provided, call `parse_genotype_file()` to extract
   the rsID → genotype mapping.

2. **Panel coverage check** — Compare detected rsIDs against the 9-gene
   methylation panel. Log missing SNPs. For any SNP absent from the input,
   mark the corresponding gene as `not_assess
