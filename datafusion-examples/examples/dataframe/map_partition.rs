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

//! MapPartition Demo
//!
//! This example demonstrates how to use `DataFrame::map_partition` to apply
//! custom processing to each partition of data.

use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::config::CsvOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use tempfile::tempdir;

/// This example shows how to use map_partition for custom processing.
///
/// It demonstrates:
/// 1. Reading data from a single CSV file
/// 2. Force repartitioning data into 2 partitions
/// 3. Using map_partition to process each partition separately
pub async fn main() -> Result<()> {
    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // Create a temporary directory and CSV file with test data
    let dir = tempdir()?;
    let file_path = dir.path().join("test_data.csv");
    {
        let mut file = File::create(&file_path)?;
        file.write_all(
            b"a,b,c
1,2,3
5,3,1
3,1,4
8,4,2
2,6,5
10,5,8
7,2,6
4,9,7
",
        )?;
    }
    let file_path_str = file_path.to_str().unwrap();

    println!("=== Reading CSV file: {file_path_str} ===\n");

    // Create a session context with small batch size to create multiple batches
    // This allows RoundRobinBatch repartition to distribute data across partitions
    let config = SessionConfig::new().with_batch_size(2);
    let ctx = SessionContext::new_with_config(config);

    // Read CSV file with specified schema
    let df = ctx
        .read_csv(
            file_path_str,
            CsvReadOptions::new().has_header(true).schema(&schema),
        )
        .await?;

    println!("=== Original Data ===");
    df.clone().show().await?;

    // Force repartition into 2 partitions using RoundRobinBatch
    let df = df.repartition(Partitioning::RoundRobinBatch(2))?;

    println!("=== Data force repartitioned into 2 partitions ===\n");

    // Use map_partition to process each partition
    let output_schema: SchemaRef = df.schema().inner().clone();
    let schema_for_closure = Arc::clone(&output_schema);

    let df_result = df
        .map_partition(
            move |partition_idx, batches| {
                println!("=== Processing Partition {partition_idx} ===");
                println!("  Received {} batches", batches.len());

                for (i, batch) in batches.iter().enumerate() {
                    let a_array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let b_array = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let c_array = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    println!(
                        "  Batch {i}: a={:?}, b={:?}, c={:?}",
                        a_array.iter().collect::<Vec<_>>(),
                        b_array.iter().collect::<Vec<_>>(),
                        c_array.iter().collect::<Vec<_>>()
                    );
                }

                let mut results = Vec::new();
                for batch in batches {
                    let a_array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();

                    // Double column 'a'
                    let doubled: Int32Array =
                        a_array.iter().map(|v| v.map(|x| x * 2)).collect();

                    println!(
                        "  Output: a (doubled)={:?}\n",
                        doubled.iter().collect::<Vec<_>>()
                    );

                    let new_batch = RecordBatch::try_new(
                        Arc::clone(&schema_for_closure),
                        vec![
                            Arc::new(doubled) as _,
                            batch.column(1).clone(),
                            batch.column(2).clone(),
                        ],
                    )?;
                    results.push(new_batch);
                }
                Ok(results)
            },
            output_schema,
        )
        .await?;

    println!("=== Final Result ===");
    df_result.clone().show().await?;

    // Write result to CSV file for end-to-end verification
    let output_file = dir.path().join("output_result.csv");
    let output_path_str = output_file.to_str().unwrap();

    println!("\n=== Writing result to CSV: {output_path_str} ===");
    df_result
        .write_csv(
            output_path_str,
            DataFrameWriteOptions::new().with_single_file_output(true),
            Some(CsvOptions::default()),
        )
        .await?;

    // Read back and display the written file to verify
    println!("\n=== Verifying: Reading back written CSV ===");
    let df_verify = ctx
        .read_csv(output_path_str, CsvReadOptions::new().has_header(true))
        .await?;
    df_verify.show().await?;

    println!("\n=== End-to-end verification complete! ===");
    println!("Input file:  {file_path_str}");
    println!("Output file: {output_path_str}");

    Ok(())
}
