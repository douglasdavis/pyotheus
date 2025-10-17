use std::collections::HashMap;

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::family::MetricConstructor;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use pyo3::exceptions::{PyKeyError, PyRuntimeError};
use pyo3::prelude::*;

use pyo3::types::PyList;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::prelude::*;

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

#[pyclass(name = "Registry")]
#[derive(Debug)]
struct PyRegistry {
    registry: Registry,
    histograms: HashMap<String, HistogramFamily>,
}

#[pymethods]
impl PyRegistry {
    #[new]
    fn __init__() -> Self {
        PyRegistry {
            registry: <Registry>::default(),
            histograms: HashMap::new(),
        }
    }

    fn __repr__(&self) -> &'static str {
        "Registry()"
    }

    fn __str__(&self) -> &'static str {
        self.__repr__()
    }

    /// Add a histogram metric to the registry.
    ///
    /// This method triggers a small, necessary memory leak. The
    /// Histogram metric from the prometheus_client crate requires a
    /// constructor with 'static bin edges ("buckets"). From Python we
    /// can only accept a dynamically defined sequence of floats (a
    /// Python `list[float]` that resolves to a Rust `Vec<f64>`). We
    /// leak the `Vec<f64>` to create a static reference to a slice of
    /// f64; this is used to instantiate all required variants of the
    /// Histogram dynamically, as different labels come through the
    /// program.
    ///
    /// # Examples
    ///
    /// ```python
    /// import pyotheus
    /// r = pyotheus.Registry()
    /// r.histogram(
    ///     "response_time_ns",
    ///     "response time in nanoseconds",
    ///     [0.5e6, 1.0e6, 2.0e6, 5.0e6, 10.0e6],
    /// )
    /// ```
    ///
    #[pyo3(signature = (*, name, help, buckets))]
    fn histogram_add(&mut self, name: &str, help: &str, buckets: Vec<f64>) -> PyResult<()> {
        // fail early, without incurring the Box::leak
        if self.histograms.contains_key(name) {
            return Err(PyKeyError::new_err(format!(
                "Histogram with name {name} already exists"
            )));
        }
        let buckets: &'static [f64] = Box::leak(buckets.into_boxed_slice());
        let cons = HistogramConstructor { buckets };
        let family = HistogramFamily::new_with_constructor(cons);
        self.histograms.insert(name.to_string(), family.clone());
        self.registry.register(name, help, family);
        tracing::debug!("Added histogram '{name}'");
        Ok(())
    }

    /// Observe a single event to be histogrammed.
    fn histogram_observe(
        &mut self,
        name: &str,
        labels: Bound<'_, PyList>,
        val: f64,
    ) -> PyResult<()> {
        // First check that we have a histogram with the given name;
        // we want to fail early without incurring the Python list ->
        // Rust Vec conversion cost when unncessary.
        let family = self.histograms
            .get(name)
            .ok_or_else(|| PyKeyError::new_err(format!("Histogram '{}' not found", name)))?;
        // Now extract and observe
        let labels: Vec<(String, String)> = labels.extract()?;
        family.get_or_create(&labels).observe(val);
        Ok(())
    }

    /// Retrieve a list of all histogram names
    fn histogram_list(&self) -> Vec<String> {
        self.histograms.keys().cloned().collect()
    }

    /// Encode the regitry's metrics
    ///
    /// This method will release the GIL while encoding the registry
    fn encode(&mut self, py: Python<'_>) -> PyResult<String> {
        py.detach(|| {
            let mut buffer = String::new();
            encode(&mut buffer, &self.registry).map_err(|err| {
                PyRuntimeError::new_err(format!("Failed to encode registry ({err})"))
            })?;
            Ok(buffer)
        })
    }
}

#[pymodule]
mod pyotheus {

    use super::*;

    #[pymodule_export]
    use super::PyRegistry;

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
    fn init(_m: &Bound<'_, PyModule>) -> PyResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_list_len() {
        let mut registry = PyRegistry::__init__();
        let add1 = registry.histogram_add("hist0", "help str", vec![100.0, 200.0, 300.0]);
        let add2 = registry.histogram_add("hist1", "help str", vec![100.0, 200.0, 400.0]);
        assert!(add1.is_ok());
        assert!(add2.is_ok());
        let mut hist_list = registry.histogram_list();
        hist_list.sort();
        let mut hist_expected = vec!["hist0", "hist1"];
        hist_expected.sort();
        assert_eq!(hist_list, hist_expected);
    }

    #[test]
    fn test_histogram_exists() {
        let mut registry = PyRegistry::__init__();
        let add1 = registry.histogram_add("hist0", "help str", vec![100.0, 200.0, 300.0]);
        assert!(add1.is_ok());
        let add2 = registry.histogram_add("hist0", "help str", vec![100.0, 200.0]);
        assert!(add2.is_err());
    }
}
