use ordered_float::OrderedFloat;
use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

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

#[derive(
    Debug,
    Deserialize,
    Serialize,
    /*JsonSchema,*/ Clone,
    Copy,
    /*FromPrimitive,*/ PartialEq,
    Eq,
    Hash,
)]
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

pub trait QueryScorer {
    fn score_stored(&self, idx: PointIdType) -> ScoreType;

    fn score(&self, v2: &Vector) -> ScoreType;

    fn score_internal(&self, point_a: PointIdType, point_b: PointIdType) -> ScoreType;
}
