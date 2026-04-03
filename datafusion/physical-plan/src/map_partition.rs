// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`MapPartitionExec`] applies a user-defined function to each partition of data

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{
    DisplayAs, DisplayFormatType, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::ExecutionPlan;
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};

use futures::stream::{Stream, StreamExt};
use log::trace;

/// Function type for map_partition operations
/// Takes partition index and a vector of RecordBatches, returns Result<Vec<RecordBatch>>
pub type MapPartitionFn =
    Arc<dyn Fn(usize, Vec<RecordBatch>) -> Result<Vec<RecordBatch>> + Send + Sync>;

/// SO function definition for map_partition
/// Contains path to SO library and function name to call
#[derive(Debug, Clone)]
pub struct SoFunction {
    /// Path to the SO library file
    pub so_path: String,
    /// Name of the function to call in the SO library
    pub function_name: String,
}

/// MapPartition function: either a closure or a SO function
#[derive(Clone)]
enum MapPartitionFunction {
    /// Closure function (existing)
    Closure(MapPartitionFn),
    /// SO library function (new)
    So(SoFunction),
}

/// MapPartitionExec applies a user-defined function to each partition of data.
///
/// This is similar to Spark's `mapPartitions` - it allows processing all data
/// in a partition at once, enabling custom processing logic that may:
/// - Change the schema (output schema can differ from input)
/// - Change the number of rows (filter, aggregate, expand)
/// - Maintain state across all rows in a partition)
///
/// Supports two types of functions:
/// 1. Closure function (in-process)
/// 2. SO library function (for distributed execution via Ballista)
///
/// # Example (Closure)
/// ```ignore
/// let exec = MapPartitionExec::try_new(
///     input_exec,
///     Arc::new(output_schema),
///     Arc::new(|partition_idx, batches| {
///         // Custom processing of all batches in this partition
///         Ok(processed_batches)
///     }),
/// )?;
/// ```
///
/// # Example (SO)
/// ```ignore
/// let exec = MapPartitionExec::try_new_with_so(
///     input_exec,
///     Arc::new(output_schema),
///     "/path/to/libmy_transform.so".to_string(),
///     "transform_partition".to_string(),
/// )?;
/// ```
pub struct MapPartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// User-defined function (closure or SO)
    func: MapPartitionFunction,
    /// Output schema (may differ from input schema)
    output_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached properties
    cache: Arc<PlanProperties>,
}

impl fmt::Debug for MapPartitionExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapPartitionExec")
            .field("input", &self.input)
            .field("output_schema", &self.output_schema)
            .field("cache", &self.cache)
            .finish_non_exhaustive()
    }
}

impl MapPartitionExec {
    /// Create a new MapPartitionExec with a closure function
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        func: MapPartitionFn,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input, Arc::clone(&output_schema));

        Ok(Self {
            input,
            func: MapPartitionFunction::Closure(func),
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        })
    }

    /// Create a new MapPartitionExec with an SO library function
    ///
    /// This is used for distributed execution via Ballista where the function
    /// is loaded from a shared library at runtime.
    pub fn try_new_with_so(
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        so_path: String,
        function_name: String,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input, Arc::clone(&output_schema));

        Ok(Self {
            input,
            func: MapPartitionFunction::So(SoFunction {
                so_path,
                function_name,
            }),
            output_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        })
    }

    /// Get the input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the output schema
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Get the SO function if this is an SO-based map partition
    pub fn so_function(&self) -> Option<&SoFunction> {
        match &self.func {
            MapPartitionFunction::So(so) => Some(so),
            MapPartitionFunction::Closure(_) => None,
        }
    }

    /// Check if this is an SO-based map partition
    pub fn is_so(&self) -> bool {
        matches!(self.func, MapPartitionFunction::So(_))
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> PlanProperties {
        // Equivalence properties are reset since we can't reason about the
        // output ordering/equivalence after arbitrary transformation
        let eq_properties = EquivalenceProperties::new(output_schema);

        // Output partitioning matches input partitioning
        let output_partitioning = input.output_partitioning().clone();

        PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        let new_input = children.swap_remove(0);
        // Recompute cache since input may affect partitioning
        let cache = Arc::new(Self::compute_properties(
            &new_input,
            Arc::clone(&self.output_schema),
        ));

        Self {
            input: new_input,
            func: self.func.clone(),
            output_schema: Arc::clone(&self.output_schema),
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }
}

impl DisplayAs for MapPartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MapPartitionExec: schema={}",
                    self.output_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(
                    f,
                    "output_schema={}",
                    self.output_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}

impl ExecutionPlan for MapPartitionExec {
    fn name(&self) -> &'static str {
        "MapPartitionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // We cannot guarantee order is maintained since the user function
        // may reorder rows
        vec![false]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // MapPartitionExec has no expressions
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            self.with_new_children_and_same_properties(children),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start MapPartitionExec::execute for partition {partition}");

        let input_stream = self.input.execute(partition, context.clone())?;
        let metrics = MapPartitionMetrics::new(&self.metrics, partition);

        // Get output schema for IPC deserialization
        let output_schema = Arc::clone(&self.output_schema);

        // Create the actual function to use based on type
        let func: MapPartitionFn = match &self.func {
            MapPartitionFunction::Closure(f) => Arc::clone(f),
            MapPartitionFunction::So(so_func) => {
                // For SO functions, load the library and create a wrapper closure
                // This uses the SO loader module to dynamically load the function
                let so_path = so_func.so_path.clone();
                let fn_name = so_func.function_name.clone();

                Arc::new(move |partition_idx, batches| {
                    // Load the SO library and get the function
                    // Use the global SO loader for caching
                    let loader = &super::so_loader::GLOBAL_SO_LOADER;
                    
                    let so_func = match loader.load_function(&so_path, &fn_name) {
                        Ok(f) => f,
                        Err(e) => return Err(e),
                    };

                    // Serialize input RecordBatches to Arrow IPC format
                    let input_ipc = match super::so_loader::serialize_batches_to_ipc(&batches) {
                        Ok(data) => data,
                        Err(e) => return Err(e),
                    };

                    // Call the SO function with IPC data
                    let output_ipc = match super::so_loader::execute_so_function(
                        so_func,
                        partition_idx as i32,
                        &input_ipc,
                    ) {
                        Ok(data) => data,
                        Err(e) => return Err(e),
                    };

                    // Deserialize output IPC data back to RecordBatches
                    super::so_loader::deserialize_batches_from_ipc(&output_ipc, Arc::clone(&output_schema))
                })
            }
        };

        Ok(Box::pin(MapPartitionExecStream {
            input: input_stream,
            func,
            output_schema: Arc::clone(&self.output_schema),
            partition,
            metrics,
            batches: Vec::new(),
            state: StreamState::Collecting,
            output_batches: Vec::new(),
            output_index: 0,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        // Since the user function can arbitrarily transform data,
        // we can't provide meaningful statistics
        Ok(Arc::new(Statistics::new_unknown(&self.output_schema)))
    }
}

/// Metrics for MapPartitionExec
struct MapPartitionMetrics {
    baseline_metrics: BaselineMetrics,
}

impl MapPartitionMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
        }
    }
}

/// State of the stream
enum StreamState {
    /// Still collecting input batches
    Collecting,
    /// Applying user function to collected batches
    Processing,
    /// Emitting output batches
    Emitting,
    /// Done
    Finished,
}

/// Stream implementation for MapPartitionExec
struct MapPartitionExecStream {
    /// Input stream
    input: SendableRecordBatchStream,
    /// User function
    func: MapPartitionFn,
    /// Output schema
    output_schema: SchemaRef,
    /// Partition index
    partition: usize,
    /// Metrics
    metrics: MapPartitionMetrics,
    /// Collected batches from input
    batches: Vec<RecordBatch>,
    /// Current state
    state: StreamState,
    /// Processed output batches
    output_batches: Vec<RecordBatch>,
    /// Current output batch index
    output_index: usize,
}

impl Stream for MapPartitionExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.metrics.baseline_metrics.elapsed_compute().clone();

        loop {
            match self.state {
                StreamState::Collecting => {
                    // Collect all batches from input
                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            self.batches.push(batch);
                            // Continue collecting
                        }
                        Poll::Ready(Some(Err(e))) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            // Input exhausted, move to processing
                            self.state = StreamState::Processing;
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                StreamState::Processing => {
                    // Apply user function to collected batches
                    let timer = elapsed_compute.timer();
                    let partition = self.partition;
                    let batches = std::mem::take(&mut self.batches);
                    let func = Arc::clone(&self.func);

                    match func(partition, batches) {
                        Ok(output_batches) => {
                            timer.done();
                            self.output_batches = output_batches;
                            self.state = StreamState::Emitting;
                        }
                        Err(e) => {
                            timer.done();
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                StreamState::Emitting => {
                    // Emit output batches one by one
                    if self.output_index < self.output_batches.len() {
                        let batch = self.output_batches[self.output_index].clone();
                        self.output_index += 1;
                        self.metrics
                            .baseline_metrics
                            .record_output(batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    } else {
                        // All output emitted
                        self.state = StreamState::Finished;
                    }
                }
                StreamState::Finished => {
                    return Poll::Ready(None);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Unknown since we can't predict user function output
        (0, None)
    }
}

impl RecordBatchStream for MapPartitionExecStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect;
    use crate::test::TestMemoryExec;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> Schema {
        Schema::new(vec![Field::new("a", DataType::Int32, false)])
    }

    fn make_batch(values: Vec<i32>) -> RecordBatch {
        let schema = test_schema();
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int32Array::from(values))])
            .unwrap()
    }

    #[tokio::test]
    async fn test_map_partition_identity() {
        let schema = Arc::new(test_schema());
        let batch1 = make_batch(vec![1, 2, 3]);
        let batch2 = make_batch(vec![4, 5, 6]);

        let input = TestMemoryExec::try_new_exec(
            &[vec![batch1.clone(), batch2.clone()]],
            Arc::clone(&schema),
            None,
        )
        .unwrap();

        let exec = MapPartitionExec::try_new(
            input,
            Arc::clone(&schema),
            Arc::new(|partition_idx, batches| {
                println!("=== Partition {partition_idx} ===");
                println!("  Received {} batches", batches.len());
                for (i, batch) in batches.iter().enumerate() {
                    let array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    println!("  Batch {i}: {:?}", array.iter().collect::<Vec<_>>());
                }
                Ok(batches.clone())
            }),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let results = collect(Arc::new(exec), ctx).await.unwrap();

        println!("=== Output Results ===");
        println!("Total output batches: {}", results.len());
        for (i, batch) in results.iter().enumerate() {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            println!("  Result batch {i}: {:?}", array.iter().collect::<Vec<_>>());
        }

        assert_eq!(results.len(), 2);
        // Verify data
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6);
    }

    #[tokio::test]
    async fn test_map_partition_transform() {
        let schema = Arc::new(test_schema());
        let batch = make_batch(vec![1, 2, 3, 4, 5]);

        let input =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
                .unwrap();

        // Double all values
        let exec = MapPartitionExec::try_new(
            input,
            Arc::clone(&schema),
            Arc::new(|partition_idx, batches| {
                println!("=== Partition {partition_idx} (Transform: Double values) ===");
                println!("  Received {} batches", batches.len());
                let mut results = Vec::new();
                for batch in batches {
                    let array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let original: Vec<_> = array.iter().collect();
                    println!("  Input values: {original:?}");
                    let doubled: Int32Array =
                        array.iter().map(|v| v.map(|x| x * 2)).collect();
                    let doubled_collected: Vec<_> = doubled.iter().collect();
                    println!("  Output values: {doubled_collected:?}");
                    results.push(
                        RecordBatch::try_new(
                            Arc::new(test_schema()),
                            vec![Arc::new(doubled)],
                        )
                        .unwrap(),
                    );
                }
                Ok(results)
            }),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let results = collect(Arc::new(exec), ctx).await.unwrap();

        println!("=== Output Results ===");
        for (i, batch) in results.iter().enumerate() {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            println!("  Result batch {i}: {:?}", array.iter().collect::<Vec<_>>());
        }

        assert_eq!(results.len(), 1);
        let result = &results[0];
        let array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(array.value(0), 2);
        assert_eq!(array.value(4), 10);
    }

    #[tokio::test]
    async fn test_map_partition_filter_rows() {
        let schema = Arc::new(test_schema());
        let batch = make_batch(vec![1, 2, 3, 4, 5]);

        let input =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
                .unwrap();

        // Keep only odd values
        let exec = MapPartitionExec::try_new(
            input,
            Arc::clone(&schema),
            Arc::new(|partition_idx, batches| {
                println!("=== Partition {partition_idx} (Filter: Keep odd values) ===");
                println!("  Received {} batches", batches.len());
                let mut results = Vec::new();
                for batch in batches {
                    let array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    println!("  Input values: {:?}", array.iter().collect::<Vec<_>>());
                    let filtered: Int32Array = array
                        .iter()
                        .filter_map(|v| v.filter(|&x| x % 2 == 1))
                        .collect();
                    println!(
                        "  Output values (odd only): {:?}",
                        filtered.iter().collect::<Vec<_>>()
                    );
                    if !filtered.is_empty() {
                        results.push(
                            RecordBatch::try_new(
                                Arc::new(test_schema()),
                                vec![Arc::new(filtered)],
                            )
                            .unwrap(),
                        );
                    }
                }
                Ok(results)
            }),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let results = collect(Arc::new(exec), ctx).await.unwrap();

        println!("=== Output Results ===");
        for (i, batch) in results.iter().enumerate() {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            println!("  Result batch {i}: {:?}", array.iter().collect::<Vec<_>>());
        }

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // 1, 3, 5
    }

    #[tokio::test]
    async fn test_map_partition_change_schema() {
        let input_schema = Arc::new(test_schema());
        let batch = make_batch(vec![1, 2, 3]);

        let input =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&input_schema), None)
                .unwrap();

        // Create output schema with additional column
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("a_doubled", DataType::Int32, false),
        ]));

        let output_schema_clone = Arc::clone(&output_schema);
        let exec = MapPartitionExec::try_new(
            input,
            Arc::clone(&output_schema),
            Arc::new(move |_idx, batches| {
                let mut results = Vec::new();
                for batch in batches {
                    let array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let doubled: Int32Array =
                        array.iter().map(|v| v.map(|x| x * 2)).collect();
                    results.push(
                        RecordBatch::try_new(
                            Arc::clone(&output_schema_clone),
                            vec![Arc::new(array.clone()), Arc::new(doubled)],
                        )
                        .unwrap(),
                    );
                }
                Ok(results)
            }),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let results = collect(Arc::new(exec), ctx).await.unwrap();

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_map_partition_multiple_partitions() {
        let schema = Arc::new(test_schema());
        let batch1 = make_batch(vec![1, 2, 3]);
        let batch2 = make_batch(vec![4, 5, 6]);

        // Two partitions
        let input = TestMemoryExec::try_new_exec(
            &[vec![batch1], vec![batch2]],
            Arc::clone(&schema),
            None,
        )
        .unwrap();

        let exec = MapPartitionExec::try_new(
            input,
            Arc::clone(&schema),
            Arc::new(|partition_idx, batches| {
                println!("=== Partition {partition_idx} ===");
                println!("  Received {} batches", batches.len());
                for (i, batch) in batches.iter().enumerate() {
                    let array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    println!("  Batch {i}: {:?}", array.iter().collect::<Vec<_>>());
                }
                Ok(batches.clone())
            }),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let results = collect(Arc::new(exec), ctx).await.unwrap();

        println!("=== Output Results ===");
        println!("Total output batches: {}", results.len());
        for (i, batch) in results.iter().enumerate() {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            println!("  Result batch {i}: {:?}", array.iter().collect::<Vec<_>>());
        }

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6);
    }
}
