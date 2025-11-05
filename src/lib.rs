use std::sync::{Mutex, OnceLock};

use indexmap::IndexMap;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::{Family, MetricConstructor};
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::prelude::*;

static MODULE_REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

#[derive(Clone)]
struct HistogramConstructor {
    buckets: &'static [f64],
}

impl MetricConstructor<Histogram> for HistogramConstructor {
    fn new_metric(&self) -> Histogram {
        Histogram::new(self.buckets.iter().copied())
    }
}

type HistogramFamily = Family<Vec<(String, String)>, Histogram, HistogramConstructor>;
type CounterFamily = Family<Vec<(String, String)>, Counter>;
type GaugeFamily = Family<Vec<(String, String)>, Gauge>;

fn normalize_labels(labels: Bound<'_, PyAny>) -> PyResult<Vec<(String, String)>> {
    labels
        .extract::<IndexMap<String, String>>()
        .map(|m| m.into_iter().collect())
        .or_else(|_| labels.extract::<Vec<(String, String)>>())
        .map_err(|_| {
            PyTypeError::new_err("labels must be list[tuple[str, str]] or dict[str, str].")
        })
}

fn encode_registry(registry: &Registry) -> PyResult<String> {
    let mut buffer = String::new();
    encode(&mut buffer, registry).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to encode registry ({err})"))
    })?;
    Ok(buffer)
}

#[pyclass(name = "Histogram")]
struct PyHistogram(HistogramFamily);

#[pymethods]
impl PyHistogram {
    /// Create a histogram metric.
    ///
    /// This triggers a small, necessary memory leak. The Histogram
    /// metric from the prometheus_client crate requires a constructor
    /// with 'static bin edges ("buckets"). From Python we can only
    /// accept a dynamically defined sequence of floats (a Python
    /// `list[float]` that resolves to a Rust `Vec<f64>`). We leak the
    /// `Vec<f64>` to create a static reference to a slice of f64;
    /// this is used to instantiate all required variants of the
    /// Histogram dynamically, as different labels come through the
    /// program.
    ///
    /// # Examples
    ///
    /// ```python
    /// import pyotheus
    /// h = pyotheus.Histogram(
    ///     "response_time_ns",
    ///     "response time in nanoseconds",
    ///     [0.5e6, 1.0e6, 2.0e6, 5.0e6, 10.0e6],
    /// )
    /// ```
    #[new]
    #[pyo3(signature = (name, documentation, buckets, registry=None))]
    fn __init__(
        name: &str,
        documentation: &str,
        buckets: Vec<f64>,
        registry: Option<Bound<'_, PyRegistry>>,
    ) -> Self {
        let buckets: &'static [f64] = Box::leak(buckets.into_boxed_slice());
        let cons = HistogramConstructor { buckets };
        let family = HistogramFamily::new_with_constructor(cons);
        if let Some(pyreg) = registry {
            pyreg
                .borrow_mut()
                .0
                .register(name, documentation, family.clone());
        } else {
            let mut reg = MODULE_REGISTRY.get().unwrap().lock().unwrap();
            reg.register(name, documentation, family.clone());
        }
        Self(family)
    }

    fn observe(&mut self, labels: Bound<'_, PyAny>, value: f64) -> PyResult<()> {
        let labels = normalize_labels(labels)?;
        self.0.get_or_create(&labels).observe(value);
        Ok(())
    }
}

#[pyclass(name = "Counter")]
struct PyCounter(CounterFamily);

#[pymethods]
impl PyCounter {
    #[new]
    #[pyo3(signature = (name, documentation, registry=None))]
    fn __init__(
        name: &str,
        documentation: &str,
        registry: Option<Bound<'_, PyRegistry>>,
    ) -> Self {
        let family = CounterFamily::default();
        if let Some(pyreg) = registry {
            pyreg
                .borrow_mut()
                .0
                .register(name, documentation, family.clone());
        } else {
            let mut reg = MODULE_REGISTRY.get().unwrap().lock().unwrap();
            reg.register(name, documentation, family.clone());
        }
        Self(family)
    }

    fn inc(&mut self, labels: Bound<'_, PyAny>) -> PyResult<u64> {
        let labels = normalize_labels(labels)?;
        Ok(self.0.get_or_create(&labels).inc())
    }
}

#[pyclass(name = "Gauge")]
struct PyGauge(GaugeFamily);

#[pymethods]
impl PyGauge {
    #[new]
    #[pyo3(signature = (name, documentation, registry=None))]
    fn __init__(
        name: &str,
        documentation: &str,
        registry: Option<Bound<'_, PyRegistry>>,
    ) -> Self {
        let family = GaugeFamily::default();
        if let Some(pyreg) = registry {
            pyreg
                .borrow_mut()
                .0
                .register(name, documentation, family.clone());
        } else {
            let mut reg = MODULE_REGISTRY.get().unwrap().lock().unwrap();
            reg.register(name, documentation, family.clone());
        }
        Self(family)
    }

    fn set(&mut self, labels: Bound<'_, PyAny>, value: i64) -> PyResult<i64> {
        let labels = normalize_labels(labels)?;
        Ok(self.0.get_or_create(&labels).set(value))
    }
}

#[pyclass(name = "Registry")]
struct PyRegistry(Registry);

#[pymethods]
impl PyRegistry {
    #[new]
    fn __init__() -> Self {
        Self(<Registry>::default())
    }

    fn __repr__(&self) -> &'static str {
        "Registry()"
    }

    fn __str__(&self) -> &'static str {
        self.__repr__()
    }

    /// Encode the regitry's metrics
    ///
    /// This method will release the GIL while encoding the registry
    fn encode(&mut self, py: Python<'_>) -> PyResult<Vec<u8>> {
        py.detach(|| encode_registry(&self.0).map(String::into_bytes))
    }
}

#[pymodule(gil_used = false)]
mod pyotheus {

    #[pymodule_export]
    use super::PyCounter;
    #[pymodule_export]
    use super::PyGauge;
    #[pymodule_export]
    use super::PyHistogram;
    #[pymodule_export]
    use super::PyRegistry;
    use super::*;

    #[pyfunction]
    fn encode_global_registry(py: Python<'_>) -> PyResult<Vec<u8>> {
        py.detach(|| {
            let registry = MODULE_REGISTRY.get().unwrap().lock().unwrap();
            encode_registry(&registry).map(String::into_bytes)
        })
    }

    #[pyfunction]
    fn init_tracing(level: &str) {
        let level_filter = level.parse::<tracing::Level>().expect("Invalid level");
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_filter(Targets::new().with_target("pyotheus", level_filter)),
            )
            .init();
    }

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        MODULE_REGISTRY.get_or_init(|| Mutex::new(Registry::default()));

        m.add("__version__", "0.1.0.dev3")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_one() {
        assert_eq!(1, 0 + 1);
    }
}
