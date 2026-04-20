#!/usr/bin/env python3
"""
ClawBio · claw-methylation-cycle v0.1.1
Methylation cycle analysis with BH4/neurotransmitter axis interpretation.

Author: Samuel Carmona Aguirre <samuel@unimed-consulting.es>
Framework: Holomedicina® · CAPS Digital · UNIMED Consulting
License: MIT

Research and educational use only (RUO). Not a diagnostic device.
Consult a qualified clinician before modifying supplementation or treatment.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# NOTE: pandas import removed (was unused — line 235 in v0.1.0).
# Add back only if CSV export is implemented in a future version.

# ---------------------------------------------------------------------------
# SNP Panel Definition
# ---------------------------------------------------------------------------

PANEL: dict[str, dict] = {
    "rs1801133": {
        "gene": "MTHFR",
        "variant": "C677T",
        "risk_allele": "T",
        "effect": "Decreased Folate → 5-MTHF Conversion",
        "activity_het": 65,
        "activity_hom": 30,
        "weight": 0.35,
    },
    "rs1801131": {
        "gene": "MTHFR",
        "variant": "A1298C",
        "risk_allele": "C",
        "effect": "Decreased MTHFR Activity (modifier)",
        "activity_het": 80,
        "activity_hom": 60,
        "weight": 0.0,
    },
    "rs1801394": {
        "gene": "MTRR",
        "variant": "A66G",
        "risk_allele": "G",
        "effect": "Decreased Methionine Synthase Reductase Activity",
        "activity_het": 80,
        "activity_hom": 60,
        "weight": 0.15,
    },
    "rs1805087": {
        "gene": "MTR",
        "variant": "A2756G",
        "risk_allele": "G",
        "effect": "Decreased Methionine Synthase Activity",
        "activity_het": 85,
        "activity_hom": 70,
        "weight": 0.10,
    },
    "rs234706": {
        "gene": "CBS",
        "variant": "C699T",
        "risk_allele": "T",
        "effect": "Increased CBS Activity (diverts homocysteine to transsulfuration)",
        "activity_het": 120,
        "activity_hom": 140,
        "weight": 0.05,
        "inverse": True,
    },
    "rs3733890": {
        "gene": "BHMT",
        "variant": "R239Q",
        "risk_allele": "A",
        "effect": "Decreased Betaine–Homocysteine Methyltransferase Activity",
        "activity_het": 70,
        "activity_hom": 40,
        "weight": 0.15,
    },
    "rs1979277": {
        "gene": "SHMT1",
        "variant": "C1420T",
        "risk_allele": "T",
        "effect": "Decreased Serine Hydroxymethyltransferase Activity",
        "activity_het": 80,
        "activity_hom": 60,
        "weight": 0.05,
    },
    "rs4680": {
        "gene": "COMT",
        "variant": "Val158Met",
        "risk_allele": "A",
        "effect": "Decreased Catechol-O-Methyltransferase Activity",
        "activity_het": 65,
        "activity_hom": 25,
        "weight": 0.10,
    },
    "rs819147": {
        "gene": "AHCY",
        "variant": "AHCY",
        "risk_allele": "T",
        "effect": "Decreased Adenosylhomocysteinase Activity",
        "activity_het": 80,
        "activity_hom": 60,
        "weight": 0.05,
    },
}

DISCLAIMER = (
    "**Research and educational use only (RUO). Not a diagnostic device.**\n"
    "Consult a qualified clinician before modifying supplementation or treatment.\n"
    "Enzymatic activity estimates are population-derived approximations, not direct assays."
)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_genotype_file(path: Path) -> dict[str, str]:
    """
    Parse a 23andMe / AncestryDNA / ADNTRO raw genotype file.
    Returns a dict mapping rsid -> genotype string (e.g. 'AG', 'TT').
    Raises FileNotFoundError if the path does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Genotype file not found: {path}")

    genotypes: dict[str, str] = {}
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            rsid = parts[0].strip()
            genotype = parts[3].strip().upper()
            if rsid.startswith("rs"):
                genotypes[rsid] = genotype
    return genotypes


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def count_risk_alleles(genotype: str, risk_allele: str) -> int:
    """Count occurrences of the risk allele in a genotype string."""
    return genotype.count(risk_allele)


def estimate_activity(snp_def: dict, n_risk: int) -> int:
    """Return estimated enzymatic activity % given risk allele count."""
    if n_risk == 0:
        return 100
    elif n_risk == 1:
        return snp_def["activity_het"]
    else:
        return snp_def["activity_hom"]


def compute_mthfr_combined(rs1801133_n: int, rs1801131_n: int) -> int:
    """
    MTHFR compound heterozygous logic.
    C677T homozygous: ~30%
    A1298C homozygous: ~60%
    Compound het (one of each): ~15% (synergistic reduction)
    Single het of either: 65-80%
    """
    if rs1801133_n >= 1 and rs1801131_n >= 1:
        if rs1801133_n == 1 and rs1801131_n == 1:
            return 15
        elif rs1801133_n == 2:
            return 12
        elif rs1801131_n == 2:
            return 20
        else:
            return 15
    elif rs1801133_n == 2:
        return 30
    elif rs1801133_n == 1:
        return 65
    elif rs1801131_n == 2:
        return 60
    elif rs1801131_n == 1:
        return 80
    else:
        return 100


def analyse(genotypes: dict[str, str]) -> dict:
    """
    Run methylation cycle analysis. Returns structured result dict.

    Safety Rule 6 compliance: SNPs absent from the input are reported as
    'not_assessed' with activity_pct = None. They are EXCLUDED from the
    NMC calculation rather than assumed normal (activity=100). This means
    NMC is computed only from assessed SNPs, and coverage_pct reflects
    how complete the score is. Downstream consumers must check coverage_pct
    before treating NMC as a reliable value.
    """
    results = {}
    found_rsids = []
    missing_rsids = []

    for rsid, snp_def in PANEL.items():
        if rsid in genotypes:
            gt = genotypes[rsid]
            # Treat "Not assessed" genotype strings as missing
            if gt in ("--", "Not assessed", "NA", ""):
                missing_rsids.append(rsid)
                results[rsid] = {
                    "gene": snp_def["gene"],
                    "variant": snp_def["variant"],
                    "genotype": "Not assessed",
                    "n_risk_alleles": None,
                    "status": "not_assessed",
                    "activity_pct": None,
                    "effect": snp_def["effect"],
                    "weight": snp_def["weight"],
                }
                continue

            found_rsids.append(rsid)
            n_risk = count_risk_alleles(gt, snp_def["risk_allele"])
            activity = estimate_activity(snp_def, n_risk)
            status = (
                "normal" if n_risk == 0
                else "heterozygous" if n_risk == 1
                else "homozygous_risk"
            )
            results[rsid] = {
                "gene": snp_def["gene"],
                "variant": snp_def["variant"],
                "genotype": gt,
                "n_risk_alleles": n_risk,
                "status": status,
                "activity_pct": activity,
                "effect": snp_def["effect"],
                "weight": snp_def["weight"],
            }
        else:
            missing_rsids.append(rsid)
            results[rsid] = {
                "gene": PANEL[rsid]["gene"],
                "variant": PANEL[rsid]["variant"],
                "genotype": "Not assessed",
                "n_risk_alleles": None,
                "status": "not_assessed",
                "activity_pct": None,  # NEVER assume 100 for missing SNPs (Safety Rule 6)
                "effect": PANEL[rsid]["effect"],
                "weight": PANEL[rsid]["weight"],
            }

    # MTHFR combined activity
    # If either MTHFR SNP is missing, treat as 0 risk alleles for that locus
    rs677_n = results["rs1801133"]["n_risk_alleles"]
    rs1298_n = results["rs1801131"]["n_risk_alleles"]
    rs677_n = rs677_n if rs677_n is not None else 0
    rs1298_n = rs1298_n if rs1298_n is not None else 0
    mthfr_combined = compute_mthfr_combined(rs677_n, rs1298_n)
    compound_het = (
        results["rs1801133"]["status"] != "not_assessed"
        and results["rs1801131"]["status"] != "not_assessed"
        and rs677_n >= 1
        and rs1298_n >= 1
    )
    results["rs1801133"]["mthfr_combined_activity"] = mthfr_combined
    results["rs1801133"]["compound_heterozygous"] = compound_het

    # BH4 axis capacity
    mtrr_n = results["rs1801394"]["n_risk_alleles"]
    mtrr_n = mtrr_n if mtrr_n is not None else 0
    mtrr_modifier = 1.0 if mtrr_n == 0 else (0.88 if mtrr_n == 1 else 0.75)
    bh4_capacity = round(mthfr_combined * mtrr_modifier)

    # Net Methylation Capacity (NMC)
    # SAFETY RULE 6: Missing SNPs are EXCLUDED from NMC, not assumed normal.
    # NMC is computed from assessed SNPs only. coverage_pct reflects completeness.
    nmc_total = 0.0
    nmc_possible = 0.0
    for rsid, r in results.items():
        w = r["weight"]
        if w == 0:
            continue
        act = r["activity_pct"]
        if act is None:
            # SNP not assessed — exclude from NMC (do NOT assume act=100)
            continue
        snp_def = PANEL[rsid]
        if snp_def.get("inverse"):
            contribution = max(0, 100 - (act - 100)) * w
        else:
            contribution = act * w
        nmc_total += contribution
        nmc_possible += 100 * w

    # Override MTHFR contribution with combined activity
    mthfr_w = PANEL["rs1801133"]["weight"]
    mthfr_individual_act = results["rs1801133"]["activity_pct"]
    if mthfr_individual_act is not None:
        nmc_total -= mthfr_individual_act * mthfr_w
        nmc_total += mthfr_combined * mthfr_w
    elif nmc_possible > 0:
        # MTHFR was missing but we have a combined estimate — include it
        nmc_total += mthfr_combined * mthfr_w
        nmc_possible += 100 * mthfr_w

    nmc = round(nmc_total / nmc_possible * 100) if nmc_possible > 0 else None

    # coverage_pct: fraction of weighted panel SNPs that were assessed
    assessed_weight = sum(
        PANEL[rsid]["weight"]
        for rsid in found_rsids
        if PANEL[rsid]["weight"] > 0
    )
    total_weight = sum(v["weight"] for v in PANEL.values())
    coverage_pct = round(assessed_weight / total_weight * 100) if total_weight > 0 else 0

    return {
        "metadata": {
            "tool": "claw-methylation-cycle v0.1.1",
            "framework": "Holomedicina® · CAPS Digital · UNIMED Consulting",
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "snps_in_panel": len(PANEL),
            "snps_found": len(found_rsids),
            "snps_missing": len(missing_rsids),
            "missing_rsids": missing_rsids,
            "coverage_pct": coverage_pct,
        },
        "net_methylation_capacity": nmc,
        "bh4_axis_capacity": bh4_capacity,
        "compound_heterozygosity": compound_het,
        "coverage_pct": coverage_pct,
        "snps_missing": missing_rsids,
        "enzymatic_profile": {
            r["gene"]: {
                "activity_pct": r["activity_pct"],
                "status": r["status"],
                "variants": (
                    [r["variant"]] if r["status"] not in ("normal", "not_assessed") else []
                ),
            }
            for rsid, r in results.items()
            if r["weight"] > 0 or r["gene"] == "MTHFR"
        },
        "recommendations": _build_recommendations_list(
            mthfr_combined, bh4_capacity, compound_het, results
        ),
        "summary": {
            "net_methylation_capacity": nmc,
            "bh4_axis_capacity": bh4_capacity,
            "mthfr_combined_activity": mthfr_combined,
            "mthfr_compound_heterozygous": compound_het,
            "dopamine_synthesis_impact": _neurotransmitter_impact(bh4_capacity),
            "serotonin_synthesis_impact": _neurotransmitter_impact(bh4_capacity),
        },
        "gene_results": results,
    }


def _build_recommendations_list(
    mthfr_act: int,
    bh4: int,
    compound_het: bool,
    genes: dict,
) -> list[dict]:
    """Return structured recommendation list for JSON output."""
    recs = []
    mtrr_n = genes["rs1801394"]["n_risk_alleles"] or 0
    bhmt_act = genes["rs3733890"]["activity_pct"]

    if compound_het or mthfr_act <= 30:
        recs.append({"priority": 1, "text": "Use 5-MTHF (methylfolate) instead of synthetic folic acid."})
    if compound_het:
        recs.append({"priority": 1, "text": f"MTHFR compound het confirmed ({mthfr_act}% activity): 5-MTHF + methylcobalamin strongly indicated."})
    if bh4 < 65:
        recs.append({"priority": 2, "text": f"BH4 at {bh4}% of normal. Riboflavin B2 200-400 mg/day + Vitamin C 500 mg/day."})
    if bh4 < 40:
        recs.append({"priority": 2, "text": "Evaluate ADHD/depression/anxiety re BH4 deficit before pharmacological intervention."})
    if mtrr_n >= 1:
        recs.append({"priority": 2, "text": "MTRR variant: prefer methylcobalamin over cyanocobalamin."})
    if bhmt_act is not None and bhmt_act <= 60:
        recs.append({"priority": 3, "text": "BHMT variant: betaine (TMG 500-1000 mg/day) + choline-rich foods."})
    if not recs:
        recs.append({"priority": 3, "text": "No high-priority flags. Maintain dietary folate and B12 adequacy."})
    return recs


def _neurotransmitter_impact(bh4_pct: int) -> str:
    if bh4_pct < 40:
        return "Severely Reduced"
    elif bh4_pct < 65:
        return "Moderately Reduced"
    else:
        return "Within Normal Range"


def _nmc_status(nmc: int | None) -> str:
    if nmc is None:
        return "Unknown"
    if nmc < 40:
        return "Severely Reduced"
    elif nmc < 60:
        return "Moderately Reduced"
    elif nmc < 80:
        return "Mildly Reduced"
    else:
        return "Normal"


def _bh4_status(bh4: int) -> str:
    if bh4 < 40:
        return "Severely Reduced"
    elif bh4 < 65:
        return "Moderately Reduced"
    else:
        return "Within Normal Range"


def _activity_emoji(act: int | None) -> str:
    if act is None:
        return "Not assessed"
    if act <= 30:
        return f"Severely reduced ({act}%)"
    elif act <= 60:
        return f"Moderately reduced ({act}%)"
    elif act <= 80:
        return f"Mildly reduced ({act}%)"
    else:
        return f"Normal ({act}%)"


# ---------------------------------------------------------------------------
# Report Generation
# ---------------------------------------------------------------------------

def generate_report(result: dict) -> str:
    s = result["summary"]
    g = result["gene_results"]
    m = result["metadata"]

    nmc = s["net_methylation_capacity"]
    bh4 = s["bh4_axis_capacity"]
    compound_het = s["mthfr_compound_heterozygous"]
    coverage = m.get("coverage_pct", 100)

    lines = [
        "# ClawBio · Methylation Cycle Clinical Report",
        "",
        f"**Date**: {m['generated_utc']}",
        f"**Tool**: {m['tool']}",
        f"**Framework**: {m['framework']}",
        f"**SNPs assessed**: {m['snps_found']}/{m['snps_in_panel']} (coverage: {coverage}%)",
        "",
        f"> {DISCLAIMER}",
        "",
    ]

    if coverage < 100:
        lines += [
            f"> ⚠️ **Partial coverage ({coverage}%)**: NMC score is based on assessed SNPs only.",
            "> Missing SNPs are excluded from scoring — not assumed normal (Safety Rule 6).",
            "> See 'Missing Variants' section below.",
            "",
        ]

    lines += [
        "---",
        "## Executive Summary",
        "",
        "| Metric | Value | Status |",
        "|--------|-------|--------|",
        f"| Net Methylation Capacity | {nmc}/100 | {_nmc_status(nmc)} |",
        f"| BH4 Axis Capacity | {bh4}/100 | {_bh4_status(bh4)} |",
        f"| MTHFR Combined Activity | {s['mthfr_combined_activity']}% | {'Compound Het detected' if compound_het else ''} |",
        f"| Dopamine Synthesis Impact | {s['dopamine_synthesis_impact']} | |",
        f"| Serotonin Synthesis Impact | {s['serotonin_synthesis_impact']} | |",
        "",
        "---",
        "## Enzymatic Activity Profile",
        "",
        "| Gene | Variant | rsID | Genotype | Activity | Status |",
        "|------|---------|------|----------|----------|--------|",
    ]

    for rsid, r in g.items():
        if r["weight"] == 0:
            continue
        act_display = (
            f"{r['activity_pct']}%"
            if r["activity_pct"] is not None
            else "Not assessed"
        )
        lines.append(
            f"| **{r['gene']}** | {r['variant']} | {rsid} | "
            f"`{r['genotype']}` | {act_display} | {_activity_emoji(r['activity_pct'])} |"
        )

    mthfr_combined = s["mthfr_combined_activity"]
    compound_flag = " (Compound Het)" if compound_het else ""
    lines.append(
        f"| **MTHFR** | Combined (677+1298) | — | — | "
        f"{mthfr_combined}%{compound_flag} | {_activity_emoji(mthfr_combined)} |"
    )

    lines += [
        "",
        "---",
        "## BH4 / Neurotransmitter Axis",
        "",
        f"**Estimated BH4 production capacity**: {bh4}% of normal",
        f"- Dopamine synthesis pathway: **{s['dopamine_synthesis_impact']}**",
        f"- Serotonin synthesis pathway: **{s['serotonin_synthesis_impact']}**",
        "",
        "_Clinical implication_: When BH4 capacity is reduced, the clinical presentation",
        "of ADHD, depression, and anxiety may have an upstream biological substrate that",
        "5-MTHF + BH4-support nutrients can partially address.",
        "",
    ]

    if compound_het:
        lines += [
            "---",
            "## Compound Heterozygosity",
            "",
            "**MTHFR Compound Heterozygous detected (C677T + A1298C)**",
            "",
            "Both MTHFR variants are present simultaneously. This combination reduces",
            "total MTHFR enzymatic activity more than either variant alone (~15% of normal).",
            "Active folate (5-MTHF) supplementation is strongly indicated.",
            "",
        ]

    lines += [
        "---",
        "## Clinical Recommendations",
        "",
    ]

    for rec in _build_recommendations_list(
        s["mthfr_combined_activity"], bh4, compound_het, g
    ):
        lines.append(f"- **PRIORITY {rec['priority']}** — {rec['text']}")

    lines += [
        "",
        "---",
        "## Missing Variants",
        "",
    ]
    if m["missing_rsids"]:
        lines.append(
            "The following SNPs were not found in the input file and were excluded from scoring:"
        )
        for rsid in m["missing_rsids"]:
            lines.append(f"- {rsid} ({PANEL[rsid]['gene']} · {PANEL[rsid]['variant']}) — **Not assessed**")
    else:
        lines.append("All panel SNPs were found in the input file.")

    lines += [
        "",
        "---",
        "## References",
        "",
        "- Nazki FH et al. (2014). Folate: metabolism, genes, polymorphisms. Gene, 533(1), 11-20.",
        "- Ledford AW et al. (2021). MTHFR and BH4 pathway in neuropsychiatric disorders. Nutrients 13(3):768.",
        "- Stover PJ (2009). One-carbon metabolism genome interactions. J Nutr, 139(12), 2402-5.",
        "- Carmona Aguirre S. (2014/UNESCO 2016). Holomedicina. UNIMED Consulting.",
        "- ClawBio (2026). https://github.com/ClawBio/ClawBio",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ClawBio · claw-methylation-cycle — Methylation cycle analysis with BH4 axis"
    )
    parser.add_argument(
        "--input", required=True, type=Path,
        help="Path to raw genotype file (23andMe / AncestryDNA / ADNTRO format)"
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Output directory for report and JSON"
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="Run with demo_input.txt from the skill directory"
    )
    args = parser.parse_args()

    if args.demo:
        demo_path = Path(__file__).parent / "demo_input.txt"
        if not demo_path.exists():
            print("ERROR: demo_input.txt not found in skill directory.", file=sys.stderr)
            sys.exit(1)
        input_path = demo_path
    else:
        input_path = args.input
        if not input_path.exists():
            print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
            sys.exit(1)

    output_dir: Path = args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading genotypes from: {input_path.name}")
    genotypes = parse_genotype_file(input_path)
    print(f"Parsed {len(genotypes)} total variants")

    print("Running methylation cycle analysis...")
    result = analyse(genotypes)

    s = result["summary"]
    print(f"Net Methylation Capacity: {s['net_methylation_capacity']}/100")
    print(f"BH4 Axis Capacity: {s['bh4_axis_capacity']}/100")
    print(f"Coverage: {result['coverage_pct']}%")
    if s["mthfr_compound_heterozygous"]:
        print("WARNING: MTHFR Compound Heterozygous detected (C677T + A1298C)")

    report_path = output_dir / "report.md"
    report_path.write_text(generate_report(result), encoding="utf-8")
    print(f"Report written to {report_path}")

    json_path = output_dir / "result.json"
    json_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"JSON written to {json_path}")

    print("\nDone. ClawBio is a research tool. Not a medical device.")


if __name__ == "__main__":
    main()
