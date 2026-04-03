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

//! MapPartition distributed computation example using SO library
//!
//! This example demonstrates:
//! 1. Reading CSV data
//! 2. Using DataFrame.map_partition_so for distributed computation
//! 3. Business logic is implemented in SO library

use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Distributed MapPartition with SO Example ===\n");

    let ctx = SessionContext::new();

    // Step 1: Read CSV file
    println!("Step 1: Reading CSV file...");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    ctx.register_csv(
        "input",
        "data/input.csv",
        CsvReadOptions::new()
            .has_header(true)
            .schema(&schema),
    ).await?;

    let df = ctx.table("input").await?;

    println!("Original data:");
    df.clone().show().await?;

    // Step 2: Use map_partition_so for distributed computation
    println!("\nStep 2: Applying map_partition_so for distributed computation...");

    // SO library configuration
    let so_path = "../../target/release/libmap_partition_so.so".to_string();
    let function_name = "identity".to_string();

    // Check if SO file exists
    if !std::path::Path::new(&so_path).exists() {
        println!("\n=== ERROR: SO library not found ===");
        println!("Please build first:");
        println!("  cd examples/map_partition_so");
        println!("  cargo build --release");
        return Ok(());
    }

    println!("  SO Library: {}", so_path);
    println!("  Function: {}", function_name);

    // Output schema - must match what SO function produces
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    // Use map_partition_so - distributed computation via SO library
    let df_result = df.map_partition_so(
        output_schema,
        so_path,
        function_name,
    ).await?;

    println!("\nStep 3: Showing results...");
    df_result.show().await?;

    println!("\n=== Test completed ===");
    println!("SO function processed all partitions via DataFrame.map_partition_so()");
    println!("\nTo implement custom business logic:");
    println!("1. Edit src/lib.rs in the SO library");
    println!("2. Implement your transform function with Arrow IPC format");
    println!("3. Rebuild: cargo build --release");
    println!("4. Update function_name in this example");

    Ok(())
}