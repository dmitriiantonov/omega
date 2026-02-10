use syn::Expr;

/// Represents the top-level cache configuration.
///
/// This struct contains both the `engine` used for caching
/// and the `admission` policy that controls which entries are admitted.
pub struct Cache {
    /// The caching engine configuration.
    pub engine: Engine,

    /// The admission policy used for the cache.
    pub admission: Admission,
}

/// Different types of caching engines that can be used.
pub enum Engine {
    /// A clock-based cache.
    ///
    /// The `Clock` engine uses a clock algorithm for eviction.
    Clock(Box<Clock>),
}

/// Configuration for the `Clock` caching engine.
pub struct Clock {
    /// The capacity of the cache.
    ///
    /// This can be a literal or an expression (`syn::Expr`)
    /// representing the number of entries the cache can hold.
    pub capacity: Expr,

    /// Backoff configuration used by the clock algorithm.
    pub backoff: Backoff,
}

/// Configuration for backoff behavior in the clock cache.
pub struct Backoff {
    /// Maximum number of spins allowed before yielding.
    pub max_spins: Expr,

    /// Number of fast retries before normal backoff is applied.
    pub fast_retries: Expr,
}

/// Policies that control which entries are admitted to the cache.
pub enum Admission {
    /// No admission control; all items are admitted.
    Always,

    /// Frequent Admission policy (e.g., TinyLFU-based).
    ///
    /// This uses a probabilistic counter to limit cache pollution.
    Frequent(Box<FrequentAdmission>),
}

/// Configuration for a frequent-admission policy.
pub struct FrequentAdmission {
    /// Width of the Count-Min Sketch (CMS) used for frequency tracking.
    pub cms_width: Expr,

    /// Height of the CMS.
    pub cms_height: Expr,

    /// Threshold for decaying counts in the CMS.
    pub decay_threshold: Expr,
}
