use syn::Expr;

pub struct CacheInput {
    pub engine: EngineInput,
    pub admission_policy: AdmissionInput,
}

pub enum EngineInput {
    Clock(Box<ClockInput>),
    S3FIFO(Box<S3FIFOInput>),
}

pub struct ClockInput {
    pub capacity: Expr,
    pub backoff: BackoffInput,
    pub metrics: MetricsInput,
}

pub struct S3FIFOInput {
    pub capacity: Expr,
    pub backoff: BackoffInput,
    pub metrics: MetricsInput,
}

pub struct BackoffInput {
    pub policy: Expr,
    pub limit: Expr,
}

pub enum AdmissionInput {
    Always,
    Frequent(Box<FrequentAdmissionInput>),
}

pub struct FrequentAdmissionInput {
    pub count_min_sketch: CountMinSketchInput,
    pub decay_threshold: Expr,
}

pub struct CountMinSketchInput {
    pub width: Expr,
    pub depth: Expr,
}

pub struct MetricsInput {
    pub shards: Expr,
    pub latency_samples: Expr,
}
