// q6/q10/q16 of the official BerlinMOD-MobilityDB query set - excluded from
// the 14-query comparable set (see BENCHMARK.md, "tdwithin capability gap")
// because SAQE's tdwithin only supports discrete time-bucket comparison,
// not MobilityDB's continuous-time interpolation - a real capability gap,
// not a translation choice. Kept here, still runnable, as the reproducible
// source behind the documented finding (e.g. q6's "8 vs 820 pairs" at scale
// 0.2) rather than deleted outright.
pub mod q10_sq;
pub mod q16_sq;
pub mod q6_sq;
