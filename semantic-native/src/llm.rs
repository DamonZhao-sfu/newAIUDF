use std::error::Error;
use polars_core::prelude::*;
use pyo3_polars::derive::polars_expr;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use reqwest::{Error as ReqwestError};
use std::time::{Instant, Duration};
use pyo3::prelude::*;

// Temporarily copied from expression_lib, remove these when dependencies issue are solved.
#[derive(Serialize)]
pub struct RequestBody<'a> {
    model: &'a str,
    prompt: &'a str,
    max_tokens: u32,
    temperature: f32,
}

#[derive(Serialize)]
pub struct RequestBody2<'a> {
    model: &'a str,
    prompt: Vec<&'a str>,
    max_tokens: u32,
    temperature: f32,
    guided_choice: Option<Vec<String>>, // New field added
}

#[derive(Deserialize)]
pub struct ResponseBody {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
pub struct Choice {
    text: String,
}

#[pyclass]
#[derive(Clone, Deserialize)]
pub struct QueryArgs {
    #[pyo3(get, set)]
    model: String,
    #[pyo3(get, set)]
    url: String,
    #[pyo3(get, set)]
    max_tokens: u32,
    #[pyo3(get, set)]
    temperature: f32,
    #[pyo3(get, set)]
    guided_choice: Option<Vec<String>>,
}

// Implement a method to create default QueryArgs
impl Default for QueryArgs {
    fn default() -> Self {
        QueryArgs {
            model: "/data/models/Qwen2.5-Coder-1.5B-Instruct/".to_string(),
            url: "http://localhost:8000/v1/completions".to_string(),
            max_tokens: 10,
            temperature: 0.0,
            guided_choice: None,
        }
    }
}

#[pymethods]
impl QueryArgs {
    #[new]
    #[pyo3(signature = (
        model = "/data/models/Qwen2.5-Coder-1.5B-Instruct/",
        url = "http://localhost:8000/v1/completions",
        max_tokens = 10,
        temperature = 0.0,
        guided_choice = None
    ))]
    pub fn new(
        model: Option<&str>,
        url: Option<&str>,
        max_tokens: Option<u32>,
        temperature: Option<f32>,
        guided_choice: Option<Vec<String>>,
    ) -> Self {
        QueryArgs {
            model: model.unwrap_or("/data/models/Qwen2.5-Coder-1.5B-Instruct/").to_string(),
            url: url.unwrap_or("http://localhost:8000/v1/completions").to_string(),
            max_tokens: max_tokens.unwrap_or(10),
            temperature: temperature.unwrap_or(0.0),
            guided_choice: guided_choice,
        }
    }

    // Implement the __str__ method for pretty printing
    fn __str__(&self) -> PyResult<String> {
        Ok(format!(
            "QueryArgs(model={}, url={}, max_tokens={}, temperature={}, guided_choice={:?})",
            self.model, self.url, self.max_tokens, self.temperature, self.guided_choice
        ))
    }
}

pub fn llm_query_str(values: &Vec<String>, model: &str, url: &str, max_tokens: u32, temperature: f32, guided_choice: Option<Vec<String>>) -> Vec<String> {
    let timeout = 120;
    // Create a client with a custom timeout
    let client = match Client::builder()
        .timeout(Duration::from_secs(timeout)) // Use the timeout parameter
        .build() {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Failed to create HTTP client: {}", e);
            return Vec::new();
        }
    };
    println!("Client created with timeout: {} seconds", timeout);

    // Create a Vec<&str> from the Vec<String>
    let prompt: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    let request_body = RequestBody2 {
        model,
        prompt,
        max_tokens,
        temperature,
        guided_choice: guided_choice.clone(), // Clone the guided_choice to avoid borrowing issues
    };

    let mut outputs = Vec::new();
    let mut len = values.len();

    match client.post(url)
        .json(&request_body)
        .send() {
        Ok(response) => {
            if response.status().is_success() {
                match response.json::<ResponseBody>() {
                    Ok(body) => {
                        for choice in body.choices {
                            outputs.push(choice.text);
                            len -= 1;
                        }
                    },
                    Err(e) => {
                        eprintln!("Failed to parse JSON response: {}", e);
                        for _ in 0..len {
                            outputs.push(format!("Failed to parse JSON response: {}", e));
                        }
                        return outputs;
                    }
                }
            } else {
                eprintln!("Request failed with status: {}", response.status());
                if let Ok(error_response) = response.text() {
                    eprintln!("Error response: {}", error_response);
                    for _ in 0..len {
                        outputs.push(format!("Error response: {}", error_response));
                    }
                    return outputs;
                } else {
                    eprintln!("Failed to read error response body");
                    for _ in 0..len {
                        outputs.push("Failed to read error response body".to_string());
                    }
                    return outputs;
                }
            }
        },
        Err(e) => {
            eprintln!("Request failed: {}", e);

            // Use source to get the underlying error
            if let Some(inner) = e.source() {
                eprintln!("Underlying error: {}", inner);
            } else {
                for _ in 0..len {
                    outputs.push(format!("Request failed: {}", e));
                }
                return outputs;
            }
        }
    }

    return outputs;
}

#[polars_expr(output_type=String)]
pub fn llm_query(inputs: &[Series], kwargs: QueryArgs) -> PolarsResult<Series> {
    // Ensure that there is at least one input Series
    if inputs.is_empty() {
        return Err(PolarsError::ComputeError("No input series provided".into()));
    }

    // Extract strings from the first input Series
    let ca = inputs[0].str()?;

    // Collect all prompts into a single vector
    let prompt_vec: Vec<String> = ca.into_no_null_iter().map(String::from).collect();

    // Call llm_query_str with the vector of prompts
    let outputs = llm_query_str(&prompt_vec, &kwargs.model, &kwargs.url, kwargs.max_tokens, kwargs.temperature, kwargs.guided_choice.clone());

    // Convert the resulting vector of strings into a StringChunked
    let out: StringChunked = StringChunked::new(PlSmallStr::from("output"), &outputs);

    // Return the resulting Series
    Ok(out.into_series())
}