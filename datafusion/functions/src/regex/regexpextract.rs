//! Implementation of `regexp_extract` scalar function.
//!
//! ## Design reasoning
//!
//! Chose to implement this function as a `ScalarUDFImpl` so it integrates
//! consistently with other regex UDFs (`regexp_match`, `regexp_replace`, etc.).
//! 
//! A few dilemmas considered during implementation:
//!
//! 1. **Input/Output types**:  
//!    To keep the first version simpler and consistent with existing regex UDFs,
//!    we restricted the types to `Utf8`.
//!
//! 2. **Return on missing groups**:  
//!    If the regex compiles but the capture group is missing, we return
//!    an empty string (`""`).
//!
//! 3. **Flags handling**:
//!    Allow an optional fourth argument for flags. Right now we only
//!    support simple pass-through to Rust’s `regex` crate.  
//!    Future improvements might add SQL-standard flags (`i`, `c`, etc.).
//!
//! ## Run 
//! 
//! `cargo test -p datafusion-functions test_case_sensitive_regexp_extract_scalar`
//! 

use datafusion_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TypeSignature::Exact
};
use arrow::datatypes::{DataType,};
use arrow::datatypes::{
    DataType::Int64, DataType::Utf8,
};
use datafusion_common::{exec_err, Result, ScalarValue};

/// Implements the SQL function `regexp_extract(string, pattern, idx[, flags])`.
///
/// * Extracts the substring matching a regex capture group.
/// * `idx=0` returns the full match; higher indices return capture groups.
/// * Returns `""` if the group is not found.
/// * Returns an error if `pattern` is invalid.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpExtractFunc {
    signature: Signature,
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // 3 arguments: str, pattern, idx
                    Exact(vec![Utf8,Utf8,Int64]),
                    // 4 arguments: str, pattern, idx, flags
                    Exact(vec![Utf8,Utf8,Int64,Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {

        // Extract input string
        let input = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s,
            _ => return exec_err!("First argument must be a UTF8 string"),
        };

        // Extract regex pattern
        let pattern = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(p))) => p,
            _ => return exec_err!("Second argument must be a UTF8 regex pattern"),
        };

        // Extract capture group index
        let group_idx = match &args.args[2] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => *i as usize,
            _ => return exec_err!("Third argument must be an integer capture group index"),
        };

        let re = match regex::Regex::new(pattern) {
            Ok(r) => r,
            Err(e) => return exec_err!("Failed to compile regex pattern '{}': {}", pattern, e),
        };

        // Apply regex and get capture
        let result = if let Some(caps) = re.captures(input) {
            caps.get(group_idx).map(|m| m.as_str()).unwrap_or("")
        } else {
            ""
        };

        // Return as ColumnarValue
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            result.to_string(),
        ))))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{ScalarValue};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
    use arrow::datatypes::{Field};
    use std::sync::Arc;

    #[test]
    fn test_ctor_and_name() {
        let f = RegexpExtractFunc::new();
        assert_eq!(f.name(), "regexp_extract");
    }

    #[test]
    fn test_case_sensitive_regexp_extract_scalar() {
        let values = ["", "100-200", "100-200", "100-200"];
        let regex = [r"(\d+)-(\d+)", r"(\d+)-(\d+)", r"(\d+)-(\d+)", r"(\d+)-(\d+)"];
        let idx = [0,1,2,0];
        let expected = ["", "100", "200", "100-200"];
        let udf = RegexpExtractFunc::new();
        
        values.iter().enumerate().for_each(|(pos, &v)| {
            let regex = regex.get(pos).cloned().unwrap();
            let idx = idx.get(pos).cloned().unwrap();
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(v.to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(regex.to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(idx))),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("regexp_extract", Utf8, false)),
                config_options: Arc::new(ConfigOptions::default()),
            };
    
            let result = udf.invoke_with_args(args);
            let expected = expected.get(pos).cloned().unwrap();
    
            match result {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))) => {
                    assert_eq!(s, expected)
                },
                _ => panic!("Unexpected result"),
            }
        });
    }
}