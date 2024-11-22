use super::Result;

use crate::{
    metrics::{
        CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric, Metric, MetricQueryScorer,
    },
    types::{Distance, PointIdType, QueryVector, ScoreType, ScoredPointOffset},
    vector_storage::VectorStorage,
};

pub struct Scorer<'a> {
    pub query_scorer: &'a dyn QueryScorer,
    points_buffer: Vec<ScoredPointOffset>,
}

impl<'a> Scorer<'a> {
    pub fn new(query_scorer: &'a dyn QueryScorer) -> Self {
        Scorer {
            query_scorer,
            points_buffer: Vec::new(),
        }
    }

    pub fn score_points(&mut self, points: &[PointIdType], limit: usize) -> &[ScoredPointOffset] {
        if limit == 0 {
            self.points_buffer
                .resize_with(points.len(), ScoredPointOffset::default);
        } else {
            self.points_buffer
                .resize_with(limit, ScoredPointOffset::default);
        }

        let mut count: usize = 0;
        for point_id in points.iter().copied() {
            self.points_buffer[count] = ScoredPointOffset {
                idx: point_id,
                score: self.query_scorer.score_stored(point_id),
            };

            count += 1;
            if count == self.points_buffer.len() {
                break;
            }
        }

        &self.points_buffer[0..count] //TODO: count -> limit?
    }

    pub fn score_point(&self, point: PointIdType) -> ScoreType {
        self.query_scorer.score_stored(point)
    }

    pub fn score_internal(&self, point_a: PointIdType, point_b: PointIdType) -> ScoreType {
        self.query_scorer.score_internal(point_a, point_b)
    }
}

pub fn new_query_scorer<'a>(
    query: QueryVector,
    vector_storage: &'a dyn VectorStorage,
    distance_metric: Distance,
) -> Result<Box<dyn QueryScorer + 'a>> {
    match distance_metric {
        Distance::Cosine => new_query_scorer_with_metric::<CosineMetric>(query, vector_storage),
        Distance::Euclid => new_query_scorer_with_metric::<EuclidMetric>(query, vector_storage),
        Distance::Dot => new_query_scorer_with_metric::<DotProductMetric>(query, vector_storage),
        Distance::Manhattan => {
            new_query_scorer_with_metric::<ManhattanMetric>(query, vector_storage)
        }
    }
}

fn new_query_scorer_with_metric<'a, TMetric: Metric + 'a>(
    query_vector: QueryVector,
    vector_storage: &'a dyn VectorStorage,
) -> Result<Box<dyn QueryScorer + 'a>> {
    Ok(Box::new(MetricQueryScorer::<TMetric>::new(
        query_vector,
        vector_storage,
    )))
}

pub trait QueryScorer {
    fn score_stored(&self, idx: PointIdType) -> ScoreType;

    fn score_internal(&self, point_a: PointIdType, point_b: PointIdType) -> ScoreType;
}
