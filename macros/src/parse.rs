use crate::ast::{BackoffInput, CacheInput, ClockInput, EngineInput, MetricsInput, S3FIFOInput};
use proc_macro2::Ident;
use std::string::ToString;
use syn::parse::{Parse, ParseStream};
use syn::token::Brace;
use syn::{Error, Expr, Token, braced, parse_quote};

const DEFAULT_SHARDS: usize = 4;
const DEFAULT_LATENCY_SAMPLES: usize = 1024;
const DEFAULT_BACKOFF: &str = "{ policy: BackoffPolicy::Linear, limit: 10 }";

impl Parse for CacheInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut engine = None;
        let mut backoff = None;

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
                "backoff" => {
                    if backoff.is_some() {
                        return Err(Error::new(key.span(), "duplicate 'backoff' field"));
                    }
                    let value = input.parse::<BackoffInput>()?;
                    backoff = Some(value);
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

        let backoff = match backoff {
            Some(backoff) => backoff,
            None => {
                let stream: proc_macro2::TokenStream = DEFAULT_BACKOFF.parse().map_err(|_| {
                    Error::new(input.span(), "Failed to parse DEFAULT_BACKOFF constant")
                })?;

                syn::parse2::<BackoffInput>(stream)?
            }
        };

        Ok(CacheInput { engine, backoff })
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
                let mut metrics = None;

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
                        "metrics" => {
                            if metrics.is_some() {
                                return Err(Error::new(key.span(), "field metrics is duplicate"));
                            }

                            let value = content.parse::<MetricsInput>()?;
                            metrics = Some(value)
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

                let metrics = metrics.unwrap_or_else(|| MetricsInput {
                    shards: parse_quote!(#DEFAULT_SHARDS),
                    latency_samples: parse_quote!(#DEFAULT_LATENCY_SAMPLES),
                });

                Ok(EngineInput::Clock(Box::new(ClockInput {
                    capacity,
                    metrics,
                })))
            }
            "S3FIFO" => {
                if !input.peek(Brace) {
                    return Err(Error::new(input.span(), "missing the brace block"));
                }

                let content;
                braced!(content in input);

                let mut capacity = None;
                let mut metrics = None;

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
                        "metrics" => {
                            if metrics.is_some() {
                                return Err(Error::new(key.span(), "field metrics is duplicate"));
                            }

                            let value = content.parse::<MetricsInput>()?;
                            metrics = Some(value)
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

                let metrics = metrics.unwrap_or_else(|| MetricsInput {
                    shards: parse_quote!(#DEFAULT_SHARDS),
                    latency_samples: parse_quote!(#DEFAULT_LATENCY_SAMPLES),
                });

                Ok(EngineInput::S3FIFO(Box::new(S3FIFOInput {
                    capacity,
                    metrics,
                })))
            }
            _ => Err(Error::new(
                engine_type.span(),
                format!("engine type '${engine_type}' is not recognized"),
            )),
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

        let policy =
            policy.ok_or_else(|| Error::new(content.span(), "field 'policy' is missing"))?;
        let limit = limit.ok_or_else(|| Error::new(content.span(), "field 'limit' is missing"))?;

        Ok(BackoffInput { policy, limit })
    }
}

impl Parse for MetricsInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if !input.peek(Brace) {
            return Err(Error::new(input.span(), "missing the brace block"));
        }

        let content;
        braced!(content in input);

        let mut shards = None;
        let mut latency_samples = None;

        while !content.is_empty() {
            let key = content.parse::<Ident>()?;
            let _ = content.parse::<Token![:]>()?;

            match key.to_string().as_str() {
                "shards" => {
                    if shards.is_some() {
                        return Err(Error::new(key.span(), ""));
                    }

                    let value = content.parse::<Expr>()?;
                    shards = Some(value);
                }
                "latency_samples" => {
                    if latency_samples.is_some() {
                        return Err(Error::new(key.span(), ""));
                    }

                    let value = content.parse::<Expr>()?;
                    latency_samples = Some(value);
                }
                _ => {
                    return Err(Error::new(
                        key.span(),
                        format!("field '${key}' doesn't relate to metrics"),
                    ));
                }
            }

            if content.peek(Token![,]) {
                let _ = content.parse::<Token![,]>()?;
            }
        }

        Ok(MetricsInput {
            shards: shards.unwrap_or_else(|| parse_quote!(#DEFAULT_SHARDS)),
            latency_samples: latency_samples
                .unwrap_or_else(|| parse_quote!(#DEFAULT_LATENCY_SAMPLES)),
        })
    }
}
