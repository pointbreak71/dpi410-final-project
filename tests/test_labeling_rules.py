from src.utils import label_jel_codes


def test_labeling_rules():
    assert label_jel_codes(["L12"]) == "market"
    assert label_jel_codes(["D21"]) == "firm"
    assert label_jel_codes(["L12", "D21"]) == "both"
    assert label_jel_codes([]) == "unclear"
