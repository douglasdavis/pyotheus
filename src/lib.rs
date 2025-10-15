use std::collections::HashMap;

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::family::MetricConstructor;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use pyo3::exceptions::{PyKeyError, PyRuntimeError};
use pyo3::prelude::*;

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

    /// Add a histogram to the internal registry.
    fn add_histogram(&mut self, name: &str, help: &str, buckets: Vec<f64>) -> PyResult<()> {
        let buckets: &'static [f64] = Box::leak(buckets.into_boxed_slice());
        let cons = HistogramConstructor { buckets };
        let family = HistogramFamily::new_with_constructor(cons);
        self.histograms.insert(name.to_string(), family.clone());
        self.registry.register(name, help, family);
        Ok(())
    }

    /// Retrieve a list of all histogram names
    fn histogram_names(&self) -> Vec<String> {
        self.histograms.keys().cloned().collect()
    }

    /// Observe a single event to be histogrammed.
    fn observe_histogram(
        &mut self,
        name: &str,
        labels: Vec<(String, String)>,
        val: f64,
    ) -> PyResult<()> {
        self.histograms
            .get(name)
            .ok_or_else(|| PyKeyError::new_err(format!("Histogram '{}' not found", name)))?
            .get_or_create(&labels)
            .observe(val);
        Ok(())
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
    #[pymodule_export]
    use super::PyRegistry;
}
