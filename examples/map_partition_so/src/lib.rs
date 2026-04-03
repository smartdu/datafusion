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

//! MapPartition SO library - Arrow IPC format support
//!
//! This library provides transform functions that work with Arrow IPC format.
//! The functions receive Arrow IPC serialized data and return Arrow IPC data.

use std::slice;

/// Double all Int32 values in the input Arrow IPC data
///
/// Input: Arrow IPC binary format containing Int32 columns
/// Output: Arrow IPC binary format with all Int32 values doubled
///
/// Returns 0 on success, non-zero on error
#[no_mangle]
pub extern "C" fn double_values(
    _partition_idx: i32,
    input_data: *const u8,
    input_len: i64,
    output_data: *mut u8,
    output_len: *mut i64,
) -> i32 {
    // Safety check
    if input_data.is_null() || output_data.is_null() || output_len.is_null() {
        return -1;
    }

    // Read input IPC data
    let input_bytes = unsafe { slice::from_raw_parts(input_data, input_len as usize) };
    
    // Parse Arrow IPC format
    // Format: Magic bytes "ARROW1" + schema + record batches
    
    // Check magic bytes
    if input_len < 6 || &input_bytes[0..6] != b"ARROW1" {
        return -2;
    }
    
    let mut offset = 6;
    
    // Read schema
    if input_len < offset as i64 + 4 {
        return -3;
    }
    let schema_len = i32::from_le_bytes([input_bytes[offset], input_bytes[offset+1], input_bytes[offset+2], input_bytes[offset+3]]) as usize;
    offset += 4;
    
    if input_len < offset as i64 + schema_len as i64 {
        return -4;
    }
    let _schema_bytes = &input_bytes[offset..offset + schema_len];
    offset += schema_len;
    
    // Process record batches
    let mut output = Vec::new();
    output.extend_from_slice(b"ARROW1");
    
    // We'll copy schema and modify batches
    // For simplicity, just copy the input directly for now
    // A full implementation would parse, transform, and rewrite
    
    // For this demo, we just copy through (placeholder)
    // In production, you would:
    // 1. Parse the IPC message
    // 2. Find Int32 arrays
    // 3. Double the values
    // 4. Write back to IPC format
    
    unsafe {
        let output_slice = slice::from_raw_parts_mut(output_data, input_len as usize);
        output_slice.copy_from_slice(input_bytes);
        *output_len = input_len;
    }
    
    println!("[SO] Processed {} bytes of IPC data", input_len);
    
    0
}

/// Identity function - passes Arrow IPC data through unchanged
///
/// Returns 0 on success, non-zero on error
#[no_mangle]
pub extern "C" fn identity(
    _partition_idx: i32,
    input_data: *const u8,
    input_len: i64,
    output_data: *mut u8,
    output_len: *mut i64,
) -> i32 {
    if input_data.is_null() || output_data.is_null() || output_len.is_null() {
        return -1;
    }
    
    unsafe {
        let input = slice::from_raw_parts(input_data, input_len as usize);
        let output = slice::from_raw_parts_mut(output_data, input_len as usize);
        output.copy_from_slice(input);
        *output_len = input_len;
    }
    
    println!("[SO] Identity passed through {} bytes", input_len);
    
    0
}

/// Filter function - keep only rows where Int32 value > threshold
///
/// Input: Arrow IPC data
/// Output: Arrow IPC data with filtered rows
///
/// Returns 0 on success, non-zero on error
#[no_mangle]
pub extern "C" fn filter_above_threshold(
    _partition_idx: i32,
    input_data: *const u8,
    input_len: i64,
    output_data: *mut u8,
    output_len: *mut i64,
    threshold: i32,
) -> i32 {
    // Safety check
    if input_data.is_null() || output_data.is_null() || output_len.is_null() {
        return -1;
    }
    
    // For now, just copy through as placeholder
    // Full implementation would parse IPC, filter, and rewrite
    let input_bytes = unsafe { slice::from_raw_parts(input_data, input_len as usize) };
    
    unsafe {
        let output = slice::from_raw_parts_mut(output_data, input_len as usize);
        output.copy_from_slice(input_bytes);
        *output_len = input_len;
    }
    
    println!("[SO] Filter (threshold={}) processed {} bytes", threshold, input_len);
    
    0
}