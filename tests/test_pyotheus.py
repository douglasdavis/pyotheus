import pyotheus
from prometheus_client.parser import text_string_to_metric_families


def reshape_samples(samples):
    result = {}
    for sample in samples:
        name = sample.name
        if name.endswith("count") or name.endswith("sum"):
            key = name
        elif name.endswith("bucket"):
            labels = sample.labels
            le = labels["le"]
            key = f"{name}_le_{le}"
        else:
            raise ValueError("whoops")
        result[key] = sample
    return result


def test_basic_histogram_and_encoding_result():
    registry = pyotheus.Registry()
    registry.histogram_add(
        name="my_hist",
        help="some histogram metric",
        buckets=[500, 1000, 2000, 3000, 5000],
    )
    registry.histogram_observe("my_hist", [("foo", "bar"), ("baz", "qux")], 1100)
    encoded = registry.encode()
    families = list(text_string_to_metric_families(encoded))
    assert len(families) == 1
    samples = reshape_samples(families[0].samples)
    assert "my_hist_count" in samples
    assert "my_hist_sum" in samples
    assert samples["my_hist_bucket_le_500.0"].labels == {"le": "500.0", "foo": "bar", "baz": "qux"}
    assert samples["my_hist_bucket_le_1000.0"].value == 0
    assert samples["my_hist_bucket_le_2000.0"].value == 1
