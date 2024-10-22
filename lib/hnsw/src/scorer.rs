use super::Result;

use crate::{
    fixed_length_priority_queue::FixedLengthPriorityQueue,
    metrics::{
        CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric, Metric, MetricQueryScorer,
    },
    types::{Distance, PointIdType, QueryScorer, QueryVector, ScoreType, ScoredPointOffset},
    vector_storage::{VectorStorage, VectorStorageImpl},
};

pub struct FilteredScorer<'a> {
    pub raw_scorer: &'a dyn RawScorer,
    points_buffer: Vec<ScoredPointOffset>,
}

impl<'a> FilteredScorer<'a> {
    pub fn new(raw_scorer: &'a dyn RawScorer) -> Self {
        FilteredScorer {
            raw_scorer,
            points_buffer: Vec::new(),
        }
    }

    /// Method filters and calculates scores for the given slice of points IDs
    ///
    /// For performance reasons this function mutates input values.
    /// For result slice allocation this function mutates self.
    ///
    /// # Arguments
    ///
    /// * `point_ids` - list of points to score. *Warn*: This input will be wrecked during the execution.
    /// * `limit` - limits the number of points to process after filtering.
    ///
    pub fn score_points(
        &mut self,
        point_ids: &mut [PointIdType],
        limit: usize, //TODO: CHECK IF LIMIT NEVER HAS 0
    ) -> &[ScoredPointOffset] {
        // apply filter and store filtered ids to source slice memory
        // let filtered_point_ids = match self.filter_context {
        //     None => point_ids,
        //     Some(f) => {
        //         let len = point_ids.len();
        //         let mut filtered_len = 0;
        //         for i in 0..len {
        //             let point_id = point_ids[i];
        //             if f.check(point_id) {
        //                 point_ids[filtered_len] = point_id;
        //                 filtered_len += 1;
        //             }
        //         }
        //         &point_ids[0..filtered_len]
        //     }
        // };
        // if limit == 0 {
        //     self.points_buffer
        //         .resize_with(filtered_point_ids.len(), ScoredPointOffset::default);
        // } else {
        self.points_buffer
            .resize_with(limit, ScoredPointOffset::default);
        // }
        let count = self.raw_scorer.score_points(
            /*filtered_point_ids*/ point_ids,
            &mut self.points_buffer,
        );
        &self.points_buffer[0..count] //TODO: count -> limit?
    }

    pub fn score_point(&self, point_id: PointIdType) -> ScoreType {
        self.raw_scorer.score_point(point_id)
    }

    pub fn score_internal(&self, point_a: PointIdType, point_b: PointIdType) -> ScoreType {
        self.raw_scorer.score_internal(point_a, point_b)
    }
}

pub trait RawScorer {
    fn score_points(&self, points: &[PointIdType], scores: &mut [ScoredPointOffset]) -> usize;

    fn score_point(&self, point: PointIdType) -> ScoreType;

    fn score_internal(&self, point_a: PointIdType, point_b: PointIdType) -> ScoreType;

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset>;
}

pub struct RawScorerImpl<TQueryScorer>
where
    TQueryScorer: QueryScorer,
{
    pub query_scorer: TQueryScorer,
    total_vector_count: usize,
}

impl<TQueryScorer> RawScorer for RawScorerImpl<TQueryScorer>
where
    TQueryScorer: QueryScorer,
{
    fn score_points(&self, points: &[PointIdType], scores: &mut [ScoredPointOffset]) -> usize {
        // if self.is_stopped.load(Ordering::Relaxed) {
        //     return 0;
        // }
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            // if !self.check_vector(point_id) {
            //     continue;
            // }
            scores[size] = ScoredPointOffset {
                idx: point_id,
                score: self.query_scorer.score_stored(point_id),
            };

            size += 1;
            if size == scores.len() {
                return size;
            }
        }
        size
    }

    fn score_point(&self, point: PointIdType) -> ScoreType {
        self.query_scorer.score_stored(point)
    }

    fn score_internal(&self, point_a: PointIdType, point_b: PointIdType) -> ScoreType {
        self.query_scorer.score_internal(point_a, point_b)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.total_vector_count as PointIdType).map(|point_id| {
            let point_id = point_id as PointIdType;
            ScoredPointOffset {
                idx: point_id,
                score: self.query_scorer.score_stored(point_id),
            }
        });
        peek_top_largest_iterable(scores, top)
    }
}

pub fn peek_top_largest_iterable<I, E: Ord>(elements: I, top: usize) -> Vec<E>
where
    I: IntoIterator<Item = E>,
{
    if top == 0 {
        return vec![];
    }

    // If the caller is interested in greatest
    // values coming first, the priority queue should be a max-heap
    let mut pq = FixedLengthPriorityQueue::new(top);
    for element in elements {
        pq.push(element);
    }
    pq.into_vec()
}

pub fn new_raw_scorer<'a>(
    query: QueryVector,
    vector_storage: &'a dyn VectorStorage,
    distance_metric: Distance,
) -> Result<Box<dyn RawScorer + 'a>> {
    match distance_metric {
        Distance::Cosine => new_scorer_with_metric::<CosineMetric>(query, vector_storage),
        Distance::Euclid => new_scorer_with_metric::<EuclidMetric>(query, vector_storage),
        Distance::Dot => new_scorer_with_metric::<DotProductMetric>(query, vector_storage),
        Distance::Manhattan => new_scorer_with_metric::<ManhattanMetric>(query, vector_storage),
    }
}

fn new_scorer_with_metric<'a, TMetric: Metric + 'a>(
    query_vector: QueryVector,
    vector_storage: &'a dyn VectorStorage,
) -> Result<Box<dyn RawScorer + 'a>> {
    raw_scorer_from_query_scorer(
        MetricQueryScorer::<TMetric>::new(
            query_vector, //TODO: Check if this is correct - REMOVED TRY INTO
            vector_storage,
        ),
        vector_storage.total_vector_count(),
    )
}

pub fn raw_scorer_from_query_scorer<'a, TQueryScorer>(
    query_scorer: TQueryScorer,
    total_vector_count: usize,
) -> Result<Box<dyn RawScorer + 'a>>
where
    TQueryScorer: QueryScorer + 'a,
{
    Ok(Box::new(RawScorerImpl::<TQueryScorer> {
        query_scorer,
        total_vector_count,
    }))
}
