use ordered_float::OrderedFloat;
use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
};

use serde::{Deserialize, Serialize};

use crate::metrics::{
    CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric, MetricPostProcessing,
};

pub const HNSW_INDEX_CONFIG_FILE: &str = "hnsw_config.json";
pub const HNSW_GRAPH_FILE: &str = "graph.bin";
pub const HNSW_LINKS_FILE: &str = "links.bin";

pub type PointIdType = u32; //RecordId;
pub type ScoreType = f32; //Vector element type;

pub type VectorElementType = f32;
pub type QueryVector = Vec<VectorElementType>;
pub type Vector = Vec<VectorElementType>;

#[derive(Copy, Clone, PartialEq, Debug, Default)]
pub struct ScoredPointOffset {
    pub idx: PointIdType,
    pub score: ScoreType,
}

impl Eq for ScoredPointOffset {}

impl Ord for ScoredPointOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
    }
}

impl PartialOrd for ScoredPointOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
/// Distance f
pub enum Distance {
    // <https://en.wikipedia.org/wiki/Cosine_similarity>
    Cosine,
    // <https://en.wikipedia.org/wiki/Euclidean_distance>
    Euclid,
    // <https://en.wikipedia.org/wiki/Dot_product>
    Dot,
    // <https://simple.wikipedia.org/wiki/Manhattan_distance>
    Manhattan,
}

impl Distance {
    pub fn postprocess_score(&self, score: ScoreType) -> ScoreType {
        match self {
            Distance::Cosine => CosineMetric::postprocess(score),
            Distance::Euclid => EuclidMetric::postprocess(score),
            Distance::Dot => DotProductMetric::postprocess(score),
            Distance::Manhattan => ManhattanMetric::postprocess(score),
        }
    }
}

impl Display for Distance {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Distance::Cosine => write!(f, "cosine"),
            Distance::Euclid => write!(f, "euclid"),
            Distance::Dot => write!(f, "dot"),
            Distance::Manhattan => write!(f, "manhattan"),
        }
    }
}

pub struct ScoredPoint {
    pub id: PointIdType,
    pub score: ScoreType,
}

impl Eq for ScoredPoint {}

impl Ord for ScoredPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
    }
}

impl PartialOrd for ScoredPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScoredPoint {
    fn eq(&self, other: &Self) -> bool {
        (self.id, &self.score) == (other.id, &other.score)
    }
}
