import pytest

try:
    from skills.methylation_cycle.methylation_cycle import analyse, parse_genotype_file
except ImportError:
    from methylation_cycle import analyse, parse_genotype_file

FULL_PANEL_SNPS = {
    "rs1801133": "CT",
    "rs1801131": "AC",
    "rs1801394": "AG",
    "rs1805087": "GG",
    "rs234706":  "CC",
    "rs3733890": "AG",
    "rs1979277": "CT",
    "rs4680":    "AG",
    "rs819147":  "GG",
}

ALL_NORMAL_SNPS = {
    "rs1801133": "GG",
    "rs1801131": "AA",
    "rs1801394": "AA",
    "rs1805087": "AA",
    "rs234706":  "CC",
    "rs3733890": "GG",
    "rs1979277": "CC",
    "rs4680":    "GG",
    "rs819147":  "GG",
}


class TestParsing:
    def test_parse_returns_dict(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("rs1801133\t1\t11856378\tCT\n")
        result = parse_genotype_file(str(f))
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_parse_handles_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            parse_genotype_file(str(tmp_path / "nonexistent.txt"))


class TestAnalyseOutputStructure:
    def test_returns_dict(self):
        result = analyse(FULL_PANEL_SNPS)
        assert isinstance(result, dict)

    def test_required_keys(self):
        result = analyse(FULL_PANEL_SNPS)
        required = {
            "net_methylation_capacity",
            "bh4_axis_capacity",
            "compound_heterozygosity",
            "enzymatic_profile",
            "recommendations",
        }
        assert not required - result.keys()

    def test_nmc_in_range(self):
        result = analyse(FULL_PANEL_SNPS)
        assert 0 <= result["net_methylation_capacity"] <= 100

    def test_bh4_in_range(self):
        result = analyse(FULL_PANEL_SNPS)
        assert 0 <= result["bh4_axis_capacity"] <= 100

    def test_recommendations_is_list(self):
        result = analyse(FULL_PANEL_SNPS)
        assert isinstance(result["recommendations"], list)


class TestCompoundHeterozygosity:
    def test_detected_when_both_present(self):
        result = analyse(FULL_PANEL_SNPS)
        assert result["compound_heterozygosity"] is True

    def test_not_detected_all_normal(self):
        result = analyse(ALL_NORMAL_SNPS)
        assert result["compound_heterozygosity"] is False

    def test_not_detected_single_variant(self):
        snps = {**ALL_NORMAL_SNPS, "rs1801133": "CT"}
        result = analyse(snps)
        assert result["compound_heterozygosity"] is False


class TestScoreCalculations:
    def test_all_normal_high_nmc(self):
        result = analyse(ALL_NORMAL_SNPS)
        assert result["net_methylation_capacity"] >= 80

    def test_compound_het_reduces_nmc(self):
        normal = analyse(ALL_NORMAL_SNPS)
        het = analyse(FULL_PANEL_SNPS)
        assert het["net_methylation_capacity"] < normal["net_methylation_capacity"]

    def test_compound_het_reduces_bh4(self):
        normal = analyse(ALL_NORMAL_SNPS)
        het = analyse(FULL_PANEL_SNPS)
        assert het["bh4_axis_capacity"] < normal["bh4_axis_capacity"]


class TestMissingSNPHandling:
    def test_empty_snps_does_not_crash(self):
        result = analyse({})
        assert isinstance(result, dict)

    def test_partial_input_has_coverage_flag(self):
        result = analyse({"rs1801133": "CT"})
        has_flag = (
            result.get("coverage_pct") is not None
            or result.get("snps_missing") is not None
        )
        assert has_flag
