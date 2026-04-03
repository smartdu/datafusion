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

//! SO (Shared Object) Loader for dynamic function loading
//!
//! This module provides functionality to dynamically load functions from
//! shared libraries (.so files) at runtime. This is used for distributed
//! execution via Ballista where business logic is implemented in separate
//! SO libraries.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use libloading::Library;

use arrow::array::RecordBatch;
use arrow::ipc::{writer::FileWriter, writer::IpcWriteOptions, reader::FileReader};
use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, DataFusionError};

/// Serialize a vector of RecordBatches to Arrow IPC binary format
///
/// # Arguments
/// * `batches` - Vector of RecordBatches to serialize
///
/// # Returns
/// IPC binary data
pub fn serialize_batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let mut cursor = Cursor::new(Vec::new());
    
    // Create IPC writer with default options
    let options = IpcWriteOptions::default();
    let mut writer = FileWriter::try_new_with_options(
        &mut cursor,
        schema.as_ref(),
        options,
    ).map_err(|e| DataFusionError::Execution(format!("Failed to create IPC writer: {}", e)))?;

    // Write record batches directly (schema is written automatically on first write)
    for batch in batches {
        writer.write(batch)
            .map_err(|e| DataFusionError::Execution(format!("Failed to write batch: {}", e)))?;
    }

    writer.finish()
        .map_err(|e| DataFusionError::Execution(format!("Failed to finish IPC writer: {}", e)))?;

    Ok(cursor.into_inner())
}

/// Deserialize Arrow IPC binary format to a vector of RecordBatches
///
/// # Arguments
/// * `data` - IPC binary data
/// * `schema` - Schema for deserialization
///
/// # Returns
/// Vector of RecordBatches
pub fn deserialize_batches_from_ipc(data: &[u8], schema: SchemaRef) -> Result<Vec<RecordBatch>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut cursor = Cursor::new(data);
    
    // Create IPC reader - projection is None to read all columns
    let reader = FileReader::try_new(
        &mut cursor,
        None,
    ).map_err(|e| DataFusionError::Execution(format!("Failed to create IPC reader: {}", e)))?;

    // Create schema field name to index mapping
    let schema_fields: HashMap<&str, usize> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| DataFusionError::Execution(format!("Failed to read batch: {}", e)))?;
        
        // Re-project to match expected schema if needed
        let mut columns = Vec::new();
        for field in schema.fields().iter() {
            let field_name = field.name();
            if let Some(&batch_idx) = schema_fields.get(field_name.as_str()) {
                columns.push(batch.column(batch_idx).clone());
            } else {
                return Err(DataFusionError::Execution(format!(
                    "Field {} not found in IPC data", field_name
                )));
            }
        }
        
        let output_batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .map_err(|e| DataFusionError::Execution(format!("Failed to create output batch: {}", e)))?;
        batches.push(output_batch);
    }

    Ok(batches)
}

/// SO function signature for map_partition
///
/// The function takes:
/// - partition_idx: the partition index
/// - input_data: pointer to IPC data buffer
/// - input_len: length of the IPC data
/// - output_data: pointer to output buffer (caller allocates)
/// - output_len: pointer to output length (filled by callee)
///
/// Returns 0 on success, non-zero on error (error message stored in output if provided)
pub type SoMapPartitionFn = unsafe extern "C" fn(
    partition_idx: i32,
    input_data: *const u8,
    input_len: i64,
    output_data: *mut u8,
    output_len: *mut i64,
) -> i32;

/// Cached SO function with its library to keep it alive
struct CachedFunction {
    /// The library (kept alive by Arc)
    _library: Arc<Library>,
    /// The function pointer
    function: SoMapPartitionFn,
}

/// SO library loader with caching
///
/// Caches loaded libraries to avoid repeated loading overhead
pub struct SoLoader {
    /// Cache of loaded libraries: path -> Arc<Library>
    libraries: Mutex<HashMap<String, Arc<Library>>>,
    /// Cache of loaded functions: (so_path, function_name) -> CachedFunction
    functions: Mutex<HashMap<(String, String), CachedFunction>>,
}

impl SoLoader {
    /// Create a new SO loader
    pub fn new() -> Self {
        Self {
            libraries: Mutex::new(HashMap::new()),
            functions: Mutex::new(HashMap::new()),
        }
    }

    /// Load a function from an SO library
    ///
    /// # Arguments
    /// * `so_path` - Path to the shared library file
    /// * `function_name` - Name of the function to load
    ///
    /// # Returns
    /// A callable function pointer
    pub fn load_function(
        &self,
        so_path: &str,
        function_name: &str,
    ) -> Result<SoMapPartitionFn> {
        // Check function cache first
        let key = (so_path.to_string(), function_name.to_string());
        
        {
            let functions = self.functions.lock().map_err(|e| {
                DataFusionError::Execution(format!("Failed to lock SO cache: {}", e))
            })?;
            
            if let Some(cached) = functions.get(&key) {
                return Ok(cached.function);
            }
        }

        // Load library if not in cache
        let library = {
            let mut libraries = self.libraries.lock().map_err(|e| {
                DataFusionError::Execution(format!("Failed to lock SO cache: {}", e))
            })?;

            if let Some(lib) = libraries.get(so_path) {
                Arc::clone(lib)
            } else {
                let lib = unsafe { Library::new(so_path) }.map_err(|e| {
                    DataFusionError::Execution(format!("Failed to load SO '{}': {}", so_path, e))
                })?;
                let arc_lib = Arc::new(lib);
                libraries.insert(so_path.to_string(), Arc::clone(&arc_lib));
                arc_lib
            }
        };

        // Get the function
        // SAFETY: library.get() returns a valid function pointer from the loaded library
        let raw_func = unsafe { library.get(function_name.as_bytes()) }.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to find function '{}' in SO '{}': {}",
                function_name, so_path, e
            ))
        })?;
        // SAFETY: raw_func is a valid function pointer from the loaded library
        let func: SoMapPartitionFn = unsafe { *raw_func };

        // Cache the function with its library to keep it alive
        {
            let mut functions = self.functions.lock().map_err(|e| {
                DataFusionError::Execution(format!("Failed to lock SO cache: {}", e))
            })?;
            
            functions.insert(key, CachedFunction {
                _library: library,
                function: func,
            });
        }

        // Return the function
        // We need to re-get it from cache since we moved it
        let functions = self.functions.lock().map_err(|e| {
            DataFusionError::Execution(format!("Failed to lock SO cache: {}", e))
        })?;
        
        Ok(functions.get(&(
            so_path.to_string(), 
            function_name.to_string()
        )).unwrap().function)
    }

    /// Clear the library cache
    pub fn clear_cache(&self) -> Result<()> {
        let mut libraries = self.libraries.lock().map_err(|e| {
            DataFusionError::Execution(format!("Failed to lock SO cache: {}", e))
        })?;
        libraries.clear();
        
        let mut functions = self.functions.lock().map_err(|e| {
            DataFusionError::Execution(format!("Failed to lock SO cache: {}", e))
        })?;
        functions.clear();
        
        Ok(())
    }
}

impl Default for SoLoader {
    fn default() -> Self {
        Self::new()
    }
}

/// Global SO loader instance for use across the application
lazy_static::lazy_static! {
    pub static ref GLOBAL_SO_LOADER: SoLoader = SoLoader::new();
}

/// Execute a SO function with IPC data
///
/// # Arguments
/// * `func` - The SO function to call
/// * `partition_idx` - Partition index
/// * `input_data` - Input IPC data
///
/// # Returns
/// Output IPC data on success
pub fn execute_so_function(
    func: SoMapPartitionFn,
    partition_idx: i32,
    input_data: &[u8],
) -> Result<Vec<u8>> {
    // Pre-allocate output buffer with at least the same size as input
    // (for identity functions, output size equals input size)
    let buffer_size = std::cmp::max(input_data.len(), 8192);
    let mut output_data: Vec<u8> = Vec::with_capacity(buffer_size);
    let mut output_len: i64 = 0;

    // Call the SO function
    // The SO function will write output to output_data buffer
    let result = unsafe {
        func(
            partition_idx,
            input_data.as_ptr(),
            input_data.len() as i64,
            output_data.as_mut_ptr(),
            &mut output_len,
        )
    };

    if result != 0 {
        return Err(DataFusionError::Execution(format!(
            "SO function returned error code: {}",
            result
        )));
    }

    // Set the actual size of output data
    if output_len > 0 {
        unsafe {
            output_data.set_len(output_len as usize);
        }
    }

    Ok(output_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_so_loader_creation() {
        let loader = SoLoader::new();
        // Just verify it can be created
        assert!(loader.libraries.lock().is_ok());
    }

    #[test]
    fn test_so_loader_default() {
        let _loader = SoLoader::default();
        // Just verify it can be created
    }
}