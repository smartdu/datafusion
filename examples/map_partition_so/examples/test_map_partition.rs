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

//! MapPartition distributed computation example

use std::sync::Arc;
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{Schema, Field, DataType};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Closure-based MapPartition Example ===\n");
    
    let ctx = SessionContext::new();
    
    // Create test data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let test_data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )?;
    
    ctx.register_batch("test_data", test_data)?;
    
    let df = ctx.table("test_data").await?;
    
    println!("Original data:");
    df.clone().show().await?;
    
    // Define output schema and create a clone for the closure
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
        Field::new("value_doubled", DataType::Int32, false),
    ]));
    let output_schema_in_closure = output_schema.clone();
    let output_schema_for_call = output_schema.clone();
    
    // Use map_partition with closure
    let df_transformed = df.map_partition(
        move |_partition_idx, batches| {
            let mut results = Vec::new();
            for batch in batches {
                let id_col = batch.column(0);
                let value_col = batch.column(1);
                
                let value_array = value_col.as_any().downcast_ref::<Int32Array>()
                    .expect("value column should be Int32Array");
                
                let num_rows = batch.num_rows();
                
                // Double values
                let mut doubled = Vec::with_capacity(num_rows);
                for i in 0..num_rows {
                    doubled.push(value_array.value(i) * 2);
                }
                
                let result_batch = RecordBatch::try_new(
                    output_schema_in_closure.clone(),
                    vec![
                        id_col.clone(),
                        value_col.clone(),
                        Arc::new(Int32Array::from(doubled)),
                    ],
                ).unwrap();
                
                results.push(result_batch);
            }
            Ok(results)
        },
        output_schema_for_call,
    ).await?;
    
    println!("\nAfter transform (closure-based):");
    df_transformed.show().await?;
    
    println!("\n=== SO-based MapPartition Example ===\n");
    println!("To use SO-based MapPartition:");
    println!("1. Build the SO library:");
    println!("   cd examples/map_partition_so");
    println!("   cargo build --release");
    println!("");
    println!("2. Use in code:");
    println!("   let so_func = SoFunction {{");
    println!("       so_path: \"target/release/libmap_partition_so.so\".to_string(),");
    println!("       function_name: \"double_values\".to_string(),");
    println!("   }};");
    println!("");
    println!("   // Note: Full SO support requires Arrow IPC serialization");
    println!("   // Current status: infrastructure in place, serialization TBD");
    println!("");
    println!("=== Test completed ===");
    
    Ok(())
}