import pyotheus
from prometheus_client.parser import text_string_to_metric_families


def reshape_families(families):
    result = {}
    for family in families:
        result[family.name] = family
    return result


def reshape_samples(samples):
    result = {}
    for sample in samples:
        name = sample.name
        if name.endswith("count") or name.endswith("sum") or name.endswith("_total"):
            key = name
        elif name.endswith("bucket"):
            labels = sample.labels
            le = labels["le"]
            key = f"{name}_le_{le}"
        else:
            key = name
        result[key] = sample
    return result


def test_basic():
    registry = pyotheus.Registry()
    histogram = pyotheus.Histogram(
        name="my_hist",
        documentation="some histogram metric",
        buckets=[500, 1000, 2000, 3000, 5000],
        registry=registry,
    )
    counter = pyotheus.Counter(
        name="my_counter",
        documentation="some counter metric",
        registry=registry,
    )
    histogram.observe([("foo", "bar"), ("baz", "qux")], 1100)
    counter.inc({"foo": "bar"})
    encoded = registry.encode()
    families = list(text_string_to_metric_families(encoded.decode()))
    families = reshape_families(families)

    hist_samples = reshape_samples(families["my_hist"].samples)
    assert "my_hist_count" in hist_samples
    assert "my_hist_sum" in hist_samples
    assert hist_samples["my_hist_bucket_le_500.0"].labels == {
        "le": "500.0",
        "foo": "bar",
        "baz": "qux",
    }
    assert hist_samples["my_hist_bucket_le_1000.0"].value == 0
    assert hist_samples["my_hist_bucket_le_2000.0"].value == 1

    counter_total_samples = reshape_samples(families["my_counter_total"].samples)
    assert counter_total_samples["my_counter_total"].value == 1


def test_basic_global():
    histogram = pyotheus.Histogram(
        "my_hist", "some histogram metric", [50, 100, 200, 300, 500]
    )
    counter = pyotheus.Counter("my_counter", "some counter metric")
    gauge = pyotheus.Gauge("my_gauge", "some gauge metric")
    histogram.observe([("foo", "bar"), ("baz", "qux")], 400)
    counter.inc({"foo": "bar"})
    i = counter.inc({"foo": "bar"})
    assert i == 1
    gauge.set([("baz", "qux")], 171)
    old = gauge.set({"baz": "qux"}, 172)
    assert old == 171

    encoded = pyotheus.encode_global_registry()
    families = list(text_string_to_metric_families(encoded.decode()))
    families = reshape_families(families)

    hist_samples = reshape_samples(families["my_hist"].samples)
    assert "my_hist_count" in hist_samples
    assert "my_hist_sum" in hist_samples
    assert hist_samples["my_hist_bucket_le_500.0"].labels == {
        "le": "500.0",
        "foo": "bar",
        "baz": "qux",
    }
    assert hist_samples["my_hist_bucket_le_300.0"].value == 0
    assert hist_samples["my_hist_bucket_le_500.0"].value == 1

    counter_total_samples = reshape_samples(families["my_counter_total"].samples)
    assert counter_total_samples["my_counter_total"].value == 2

    gauge_samples = reshape_samples(families["my_gauge"].samples)
    assert gauge_samples["my_gauge"].value == 172
