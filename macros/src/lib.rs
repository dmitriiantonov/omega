use crate::ast::{BackoffInput, CacheInput, EngineInput, MetricsInput};
use proc_macro::TokenStream;
use quote::{ToTokens, quote};
use syn::parse_macro_input;

mod ast;
mod parse;

#[proc_macro]
pub fn cache(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as CacheInput);

    let expanded = quote! {#input};

    TokenStream::from(expanded)
}

impl ToTokens for CacheInput {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let engine = &self.engine;
        let backoff_config = &self.backoff;

        tokens.extend(quote! {
            ::omega_cache::Cache::new(#engine, #backoff_config)
        });
    }
}

impl ToTokens for EngineInput {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            EngineInput::Clock(clock) => {
                let capacity = &clock.capacity;
                let metrics = &clock.metrics;

                tokens.extend(quote! {
                    ::omega_cache::clock::ClockCache::new(#capacity, #metrics)
                });
            }
            EngineInput::S3FIFO(s3fifo) => {
                let capacity = &s3fifo.capacity;
                let metrics = &s3fifo.metrics;

                tokens.extend(quote! {
                    ::omega_cache::s3fifo::S3FIFOCache::new(#capacity, #metrics)
                });
            }
        }
    }
}

impl ToTokens for BackoffInput {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let policy = &self.policy;
        let limit = &self.limit;

        let extend = quote! {
            ::omega_cache::core::backoff::BackoffConfig { policy: #policy, limit: #limit }
        };

        tokens.extend(extend);
    }
}

impl ToTokens for MetricsInput {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let shards = &self.shards;
        let latency_samples = &self.latency_samples;

        tokens.extend(quote! {
            ::omega_cache::metrics::MetricsConfig::new(#shards, #latency_samples)
        });
    }
}
