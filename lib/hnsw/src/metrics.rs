use std::{borrow::Cow, marker::PhantomData};

use crate::{
    types::{PointIdType, QueryScorer, QueryVector, ScoreType, Vector, VectorElementType},
    vector_storage::VectorStorage,
};

/// Defines how to compare vectors
pub trait Metric {
    /// Greater the value - closer the vectors
    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType;

    /// Necessary vector transformations performed before adding it to the collection (like normalization)
    /// If no transformation is needed - returns the same vector
    fn preprocess(vector: Vector) -> Vector;
}

pub trait MetricPostProcessing {
    /// correct metric score for displaying
    fn postprocess(score: ScoreType) -> ScoreType;
}

#[derive(Clone)]
pub struct DotProductMetric;

#[derive(Clone)]
pub struct CosineMetric;

#[derive(Clone)]
pub struct EuclidMetric;

#[derive(Clone)]
pub struct ManhattanMetric;

impl Metric for EuclidMetric {
    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        euclid_similarity(v1, v2)
    }

    fn preprocess(vector: Vector) -> Vector {
        vector
    }
}

impl MetricPostProcessing for EuclidMetric {
    fn postprocess(score: ScoreType) -> ScoreType {
        score.abs().sqrt()
    }
}

impl Metric for ManhattanMetric {
    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        manhattan_similarity(v1, v2)
    }

    fn preprocess(vector: Vector) -> Vector {
        vector
    }
}

impl MetricPostProcessing for ManhattanMetric {
    fn postprocess(score: ScoreType) -> ScoreType {
        score.abs()
    }
}

impl Metric for DotProductMetric {
    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        dot_similarity(v1, v2)
    }

    fn preprocess(vector: Vector) -> Vector {
        vector
    }
}

impl MetricPostProcessing for DotProductMetric {
    fn postprocess(score: ScoreType) -> ScoreType {
        score
    }
}

/// Equivalent to DotProductMetric with normalization of the vectors in preprocessing.
impl Metric for CosineMetric {
    fn similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
        DotProductMetric::similarity(v1, v2)
    }

    fn preprocess(vector: Vector) -> Vector {
        cosine_preprocess(vector)
    }
}

impl MetricPostProcessing for CosineMetric {
    fn postprocess(score: ScoreType) -> ScoreType {
        score
    }
}

pub fn euclid_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (a - b).powi(2))
        .sum::<ScoreType>()
}

pub fn manhattan_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (a - b).abs())
        .sum::<ScoreType>()
}

pub fn cosine_preprocess(vector: Vector) -> Vector {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    if is_length_zero_or_normalized(length) {
        return vector;
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

pub fn dot_similarity(v1: &[VectorElementType], v2: &[VectorElementType]) -> ScoreType {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}

/// Check if the length is zero or normalized enough.
///
/// When checking if normalized, we don't check if it's exactly 1.0 but rather whether it is close
/// enough. It prevents multiple normalization iterations from being unstable due to floating point
/// errors.
///
/// When checking normalized, we use 1.0e-6 as threshold. It should be big enough to make
/// renormalizing stable, while small enough to not affect regular normalizations.
#[inline]
fn is_length_zero_or_normalized(length: f32) -> bool {
    length < f32::EPSILON || (length - 1.0).abs() <= 1.0e-6
}

pub struct MetricQueryScorer<'a, TMetric> {
    query: QueryVector,
    vector_storage: &'a dyn VectorStorage,
    metric: PhantomData<TMetric>,
}

impl<'a, TMetric: Metric> MetricQueryScorer<'a, TMetric> {
    pub fn new(query: QueryVector, vector_storage: &'a dyn VectorStorage) -> Self {
        let preprocessed_vector = TMetric::preprocess(query);
        Self {
            query: Vector::from(Cow::from(preprocessed_vector)),
            vector_storage,
            metric: PhantomData,
        }
    }
}

impl<'a, TMetric: Metric> QueryScorer for MetricQueryScorer<'a, TMetric> {
    #[inline]
    fn score_stored(&self, idx: PointIdType) -> ScoreType {
        TMetric::similarity(&self.query, self.vector_storage.get(idx))
    }

    #[inline]
    fn score(&self, v2: &Vector) -> ScoreType {
        TMetric::similarity(&self.query, v2)
    }

    fn score_internal(&self, point_a: PointIdType, point_b: PointIdType) -> ScoreType {
        let v1 = self.vector_storage.get(point_a);
        let v2 = self.vector_storage.get(point_b);
        TMetric::similarity(v1, v2)
    }
}
