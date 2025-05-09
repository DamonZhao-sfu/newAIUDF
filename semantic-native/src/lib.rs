// Rust Side - src/lib.rs

mod llm;
mod expressions;

use std::sync::Arc;
use jni::{JNIEnv, JavaVM, objects::{JClass, JObject, JString}, sys::{jlong}};
use arrow::array::{ArrayRef, make_array, StructArray, Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::ffi::FFI_ArrowArray;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi::from_ffi;
use arrow::ffi::to_ffi;
use jni::sys::jstring;
use regex::Regex;

// Struct to wrap an Arrow RecordBatch for FFI
struct ArrowRecordBatchWrapper {
    batch: RecordBatch,
}

fn extract_column_names(prompt: &str) -> Result<Vec<String>, String> {
    let re = Regex::new(r"\{([^}]+)\}").map_err(|e| e.to_string())?;
    let matches: Vec<String> = re
        .captures_iter(prompt)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .collect();

    if matches.is_empty() {
        Err(format!("No column names found in prompt: {}", prompt))
    } else {
        Ok(matches)
    }
}

// JNI entry point function to process Arrow data
#[no_mangle]
unsafe extern "system" fn Java_SemOperatorPlugin_utils_JNIProcessor_process(
    mut env: JNIEnv,
    _class: JClass,
    prompt: jstring,
    input_array_ptr: jlong,
    input_schema_ptr: jlong,
    output_array_ptr: jlong,
    output_schema_ptr: jlong,
) -> jlong {
    // Try to process the Arrow data and handle errors
    match process_arrow_data(
        &mut env, // Pass env by reference
        prompt,
        input_array_ptr,
        input_schema_ptr,
        output_array_ptr,
        output_schema_ptr,
    ) {
        Ok(ptr) => ptr,
        Err(e) => {
            // Log the error
            eprintln!("Error processing Arrow data: {:?}", e);

            // Throw a Java exception
            0 // Return 0 on error
        }
    }
}

unsafe fn process_arrow_data(
    env: &mut JNIEnv,
    prompt: jstring,
    input_array_ptr: jlong,
    input_schema_ptr: jlong,
    output_array_ptr: jlong,
    output_schema_ptr: jlong,
) -> Result<jlong, Box<dyn std::error::Error>> {
    // Convert the input pointers to FFI structs
    let in_array = input_array_ptr as *mut FFI_ArrowArray;
    let in_schema = input_schema_ptr as *mut FFI_ArrowSchema;
    let prompt_str: String = env
        .get_string(&JString::from_raw(prompt))
        .unwrap()
        .into();
    let columns = extract_column_names(&prompt_str);
    println!("{:?}", columns);
    // Safety: The pointers should be valid as they come from the JVM
    unsafe {
        let array = std::ptr::read(in_array);
        let array_data = from_ffi(array, &*in_schema)?;
        let child_array_refs: Vec<ArrayRef> = make_array(array_data).to_data().child_data()
            .iter() // Iterate over the slice of ArrayData references
            .map(|data| make_array(data.clone())) // Clone each ArrayData and convert to ArrayRef
            .collect(); // Collect the results into a Vec<ArrayRef>

        let schema = Arc::new(Schema::try_from(&*in_schema)?);
        let batch = RecordBatch::try_new(schema, child_array_refs)?;

        // filter the batch

        // Create output FFI structs
        let out_array = output_array_ptr as *mut FFI_ArrowArray;
        let out_schema = output_schema_ptr as *mut FFI_ArrowSchema;

        let struct_array = StructArray::from(batch);
        let array_ref: ArrayRef = Arc::new(struct_array);
        let output_array_data = array_ref.to_data();

        // Export the data using to_ffi - this returns a tuple we need to assign to output pointers
        let (ffi_array, ffi_schema) = to_ffi(&output_array_data)?;

        // Copy the result to the output pointers
        *out_array = ffi_array;
        *out_schema = ffi_schema;

        // Create a wrapper to hold the batch and return a pointer to it
        Ok(1)
    }
}
