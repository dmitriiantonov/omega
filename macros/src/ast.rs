use syn::Expr;

pub struct CacheInput {
    pub engine: EngineInput,
    pub backoff: BackoffInput,
}

pub enum EngineInput {
    Clock(Box<ClockInput>),
    S3FIFO(Box<S3FIFOInput>),
}

pub struct ClockInput {
    pub capacity: Expr,
    pub metrics: MetricsInput,
}

pub struct S3FIFOInput {
    pub capacity: Expr,
    pub metrics: MetricsInput,
}

pub struct BackoffInput {
    pub policy: Expr,
    pub limit: Expr,
}

pub struct MetricsInput {
    pub shards: Expr,
    pub latency_samples: Expr,
}
