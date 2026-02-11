use crate::input::{
    AdmissionInput, BackoffInput, CacheInput, ClockInput, CountMinSketchInput, EngineInput,
    FrequentAdmissionInput,
};
use proc_macro2::Ident;
use syn::parse::{Parse, ParseStream};
use syn::token::Brace;
use syn::{Error, Expr, Token, braced};

impl Parse for CacheInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut engine = None;
        let mut admission = None;

        while !input.is_empty() {
            let key = input.parse::<Ident>()?;
            let _ = input.parse::<Token![:]>()?;

            match key.to_string().as_str() {
                "engine" => {
                    if engine.is_some() {
                        return Err(Error::new(key.span(), "duplicate 'engine' field"));
                    }
                    let value = input.parse::<EngineInput>()?;
                    engine = Some(value);
                }
                "admission" => {
                    if admission.is_some() {
                        return Err(Error::new(key.span(), "duplicate 'admission' field"));
                    }

                    let value = input.parse::<AdmissionInput>()?;
                    admission = Some(value);
                }
                _ => {
                    return Err(Error::new(
                        key.span(),
                        format!("field '${key}' is not recognized"),
                    ));
                }
            };

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        let engine = engine.ok_or_else(|| Error::new(input.span(), "field 'engine' is missing"))?;
        let admission =
            admission.ok_or_else(|| Error::new(input.span(), "field 'admission' is missing"))?;

        Ok(CacheInput { engine, admission_policy: admission })
    }
}

impl Parse for CountMinSketchInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if !input.peek(Brace) {
            return Err(input.error("missed a braced block"));
        }

        let content;
        braced!(content in input);

        let mut width = None;
        let mut height = None;

        while !content.is_empty() {
            let key: Ident = content.parse()?;
            let _ = content.parse::<Token![:]>();

            match key.to_string().as_str() {
                "width" => {
                    if width.is_some() {
                        return Err(Error::new(key.span(), "duplicate 'width' field"));
                    }

                    let value = content.parse::<Expr>()?;
                    width = Some(value)
                }
                "height" => {
                    if height.is_some() {
                        return Err(Error::new(key.span(), "duplicate 'height' field"));
                    }

                    let value = content.parse::<Expr>()?;
                    height = Some(value)
                }
                _ => {
                    return Err(Error::new(
                        key.span(),
                        format!("field '${key}' is not recognized"),
                    ));
                }
            }

            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
            }
        }

        let width = width.ok_or_else(|| Error::new(input.span(), "field 'width' is missing"))?;
        let height = height.ok_or_else(|| Error::new(input.span(), "field 'height' is missing"))?;

        Ok(Self { width, height })
    }
}

impl Parse for AdmissionInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let admission_type = input.parse::<Ident>()?;

        match admission_type.to_string().as_str() {
            "Always" => Ok(AdmissionInput::Always),
            "Frequent" => {
                if !input.peek(Brace) {
                    return Err(input.error("missing a braced block"));
                }

                let content;
                braced!(content in input);

                let mut count_min_sketch = None;
                let mut decay_threshold = None;

                while !content.is_empty() {
                    let key = content.parse::<Ident>()?;
                    let _ = content.parse::<Token![:]>()?;

                    match key.to_string().as_str() {
                        "count_min_sketch" => {
                            let value = content.parse::<CountMinSketchInput>()?;
                            count_min_sketch = Some(value);
                        }
                        "decay_threshold" => {
                            let value = content.parse::<Expr>()?;
                            decay_threshold = Some(value);
                        }
                        _ => {
                            return Err(Error::new(
                                key.span(),
                                format!("field '${key}' is not recognized"),
                            ));
                        }
                    }

                    if content.peek(Token![,]) {
                        let _ = content.parse::<Token![,]>()?;
                    }
                }

                let count_min_sketch = count_min_sketch.ok_or_else(|| {
                    Error::new(content.span(), "field 'count_min_sketch' is missing")
                })?;
                let decay_threshold = decay_threshold.ok_or_else(|| {
                    Error::new(content.span(), "field 'decay_threshold' is missing")
                })?;

                Ok(AdmissionInput::Frequent(Box::new(FrequentAdmissionInput {
                    count_min_sketch,
                    decay_threshold,
                })))
            }
            _ => Err(Error::new(
                admission_type.span(),
                format!("type '${admission_type}' is not supported"),
            )),
        }
    }
}

impl Parse for EngineInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let engine_type = input.parse::<Ident>()?;

        match engine_type.to_string().as_str() {
            "Clock" => {
                if !input.peek(Brace) {
                    return Err(Error::new(input.span(), "missing the brace block"));
                }

                let content;
                braced!(content in input);

                let mut capacity = None;
                let mut backoff = None;

                while !content.is_empty() {
                    let key = content.parse::<Ident>()?;
                    let _ = content.parse::<Token![:]>()?;

                    match key.to_string().as_str() {
                        "capacity" => {
                            if capacity.is_some() {
                                return Err(Error::new(key.span(), "field capacity is duplicate"));
                            }

                            let value = content.parse::<Expr>()?;
                            capacity = Some(value);
                        }
                        "backoff" => {
                            if backoff.is_some() {
                                return Err(Error::new(key.span(), "field backoff is duplicate"));
                            }

                            let value = content.parse::<BackoffInput>()?;
                            backoff = Some(value);
                        }
                        _ => {
                            return Err(Error::new(
                                key.span(),
                                format!("field '${key}' is not recognized"),
                            ));
                        }
                    }

                    if content.peek(Token![,]) {
                        let _ = content.parse::<Token![,]>()?;
                    }
                }

                let capacity = capacity
                    .ok_or_else(|| Error::new(content.span(), "field 'capacity' is missing"))?;
                let backoff = backoff
                    .ok_or_else(|| Error::new(content.span(), "field 'backoff' is missing"))?;

                Ok(EngineInput::Clock(Box::new(ClockInput {
                    capacity,
                    backoff,
                })))
            }
            _ => {
                Err(Error::new(
                    engine_type.span(),
                    format!("engine type '${engine_type}' is not recognized"),
                ))
            }
        }
    }
}

impl Parse for BackoffInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if !input.peek(Brace) {
            return Err(Error::new(input.span(), "missing the brace block"));
        }

        let content;
        braced!(content in input);

        let mut policy = None;
        let mut limit = None;

        while !content.is_empty() {
            let key = content.parse::<Ident>()?;
            let _ = content.parse::<Token![:]>();

            match key.to_string().as_str() {
                "policy" => {
                    if policy.is_some() {
                        return Err(Error::new(key.span(), "field 'policy' is duplicate"));
                    }

                    let value = content.parse::<Expr>()?;
                    policy = Some(value)
                }
                "limit" => {
                    if limit.is_some() {
                        return Err(Error::new(key.span(), "field 'limit' is duplicate"));
                    }

                    let value = content.parse::<Expr>()?;
                    limit = Some(value)
                }
                _ => {
                    return Err(Error::new(
                        key.span(),
                        format!("field '${key}' doesn't relate to backoff"),
                    ));
                }
            }

            if content.peek(Token![,]) {
                let _ = content.parse::<Token![,]>()?;
            }
        }

        let policy = policy.ok_or_else(|| Error::new(content.span(), "field 'policy' is missing"))?;
        let limit = limit.ok_or_else(|| Error::new(content.span(), "field 'limit' is missing"))?;

        Ok(BackoffInput { policy, limit })
    }
}
