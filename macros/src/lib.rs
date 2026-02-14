use crate::ast::{AdmissionInput, BackoffInput, CacheInput, EngineInput, MetricsInput};
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
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
        let admission_policy = &self.admission_policy;

        tokens.extend(quote! {
            crate::Cache::new(#engine, #admission_policy)
        });
    }
}

impl ToTokens for AdmissionInput {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            AdmissionInput::Always => tokens.extend(quote! { crate::AlwaysAdmission::new() }),
            AdmissionInput::Frequent(frequent) => {
                let frequent = frequent.as_ref();
                let cms_width = &frequent.count_min_sketch.width;
                let cms_height = &frequent.count_min_sketch.height;
                let decay_threshold = &frequent.decay_threshold;

                let extend = quote! {
                    crate::FrequentPolicy::new(#cms_width, #cms_height, #decay_threshold)
                };

                tokens.extend(extend)
            }
        }
    }
}

impl ToTokens for EngineInput {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        match self {
            EngineInput::Clock(clock) => {
                let capacity = &clock.capacity;
                let backoff = &clock.backoff;
                let metrics = &clock.metrics;

                tokens.extend(quote! {
                    crate::clock::ClockCache::new(#capacity, #backoff, #metrics)
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
            crate::core::backoff::BackoffConfig { policy: #policy, limit: #limit }
        };

        tokens.extend(extend);
    }
}

impl ToTokens for MetricsInput {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let shards = &self.shards;
        let latency_samples = &self.latency_samples;

        tokens.extend(quote! {
            crate::metrics::MetricsConfig::new(#shards, #latency_samples)
        });
    }
}
