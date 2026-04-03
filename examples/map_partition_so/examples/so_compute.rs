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

//! MapPartition with SO library example
//!
//! This example demonstrates using SO library for distributed computation:
//! 1. Read CSV data
//! 2. Create physical execution plan with MapPartitionExec using SO function
//! 3. Execute the plan - SO function is called for each partition

use std::sync::Arc;
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use datafusion::prelude::*;
use datafusion::physical_plan::map_partition::MapPartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== MapPartition with SO Library Example ===\n");

    let ctx = SessionContext::new();

    // Step 1: Read CSV file
    println!("Step 1: Reading CSV file...");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    ctx.register_csv("input", "data/input.csv",
        CsvReadOptions::new().has_header(true).schema(&schema)).await?;

    let df = ctx.table("input").await?;
    println!("Original data:");
    df.clone().show().await?;

    // Step 2: Create physical plan with MapPartitionExec using SO
    println!("\nStep 2: Creating physical plan with MapPartitionExec using SO...");
    println!("  - Input: CSV file");
    println!("  - SO Library: libmap_partition_so.so");
    println!("  - Function: identity");

    let logical_plan = df.clone().into_optimized_plan()?;
    let input_plan = ctx.state().create_physical_plan(&logical_plan).await?;

    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let so_path = "../../target/release/libmap_partition_so.so";
    let function_name = "identity";

    let map_partition_exec: Arc<dyn ExecutionPlan> = Arc::new(
        MapPartitionExec::try_new_with_so(
            input_plan, output_schema, so_path.to_string(), function_name.to_string(),
        )?
    );

    println!("\nStep 3: Executing distributed computation...");
    
    let exec = map_partition_exec.clone();
    let task_ctx = ctx.task_ctx();
    
    let stream = exec.execute(0, task_ctx).unwrap();
    let result = stream.try_collect::<Vec<RecordBatch>>().await;
    
    match result {
        Ok(batches) => {
            println!("\n=== Execution completed ===");
            println!("Total batches: {}", batches.len());
            for (i, batch) in batches.iter().enumerate() {
                println!("  Batch {}: {} rows, {} columns", i, batch.num_rows(), batch.num_columns());
            }
            println!("\nNote: SO function '{}' was called with Arrow IPC data!", function_name);
        }
        Err(e) => {
            println!("\n=== Execution error ===");
            println!("Error: {:?}", e);
        }
    }

    println!("\n=== To implement business logic in SO ===");
    println!("1. Edit src/lib.rs to implement your business logic");
    println!("2. Rebuild: cargo build --release");
    println!("3. Use function name in MapPartitionExec::try_new_with_so()");
    println!("\nExample - doubling values:");
    println!("   let function_name = \"double_values\";");
    
    Ok(())
}