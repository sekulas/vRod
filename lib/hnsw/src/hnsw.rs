use super::{Error, Result};
use atomic_refcell::AtomicRefCell;
use rand::thread_rng;
use std::{fs::create_dir_all, ops::Deref, path::Path, sync::Arc, thread};

use crate::{
    config::{HnswConfig, HnswGraphConfig},
    graph_layers::GraphLayers,
    graph_layers_builder::GraphLayersBuilder,
    id_tracker::IdTrackerSS,
    scorer::{new_raw_scorer, FilteredScorer},
    types::{Distance, QueryVector, ScoredPoint, ScoredPointOffset},
    vector_storage::VectorStorageSS,
    visited_pool::POOL_KEEP_LIMIT,
};

const HNSW_USE_HEURISTIC: bool = true;

/// disconnected components in the graph.
#[cfg(debug_assertions)]
const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 32;
#[cfg(not(debug_assertions))]
const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 256;

#[derive(Debug)]
pub struct HnswIndex {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
    config: HnswGraphConfig,
    graph: GraphLayers,
}

pub struct HnswIndexLoadArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
}

pub struct HnswIndexCreateArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
    pub hnsw_config: HnswConfig,
    pub distance: Distance,
}

impl HnswIndex {
    pub fn create(args: HnswIndexCreateArgs<'_>) -> Result<Self> {
        let HnswIndexCreateArgs {
            path,
            id_tracker,
            vector_storage,
            hnsw_config,
            distance,
        } = args;

        create_dir_all(path)?;

        let config_path = HnswGraphConfig::get_config_path(path);
        let graph_path = GraphLayers::get_path(path);

            let (config, graph) = Self::build_index(
                path,
                hnsw_config,
                id_tracker.borrow().deref(),
                vector_storage.borrow().deref(),
                distance,
            )?;

            config.save(&config_path)?;
            graph.save(&graph_path)?;

        Ok(HnswIndex {
            id_tracker,
            vector_storage,
            config,
            graph,
        })
    }

    pub fn load(args: HnswIndexLoadArgs<'_>) -> Result<Self> {
        let HnswIndexLoadArgs {
            path,
            id_tracker,
            vector_storage,
        } = args;

        let config_path = HnswGraphConfig::get_config_path(path);
        let graph_path = GraphLayers::get_path(path);
        let graph_links_path = GraphLayers::get_links_path(path);
        let (config, graph) = if graph_path.exists() {
            let config = if config_path.exists() {
                HnswGraphConfig::load(&config_path)?
            } else {
                return Err(Error::ConfigFileHasNotBeenFound { path: config_path });
            };

            (config, GraphLayers::load(&graph_path, &graph_links_path)?)
        } else {
            return Err(Error::GraphFileHasNotBeenFound { path: graph_path });
        };

        Ok(HnswIndex {
            id_tracker,
            vector_storage,
            config,
            path: path.to_owned(),
            graph,
        })
    }

    fn build_index(
        path: &Path,
        hnsw_config: HnswConfig,
        id_tracker: &IdTrackerSS,
        vector_storage: &VectorStorageSS,
        distance: Distance,
    ) -> Result<(HnswGraphConfig, GraphLayers)> {
        let total_vector_count = vector_storage.total_vector_count();

        // let full_scan_threshold = vector_storage
        //     .available_size_in_bytes()
        //     .checked_div(total_vector_count)
        //     .and_then(|avg_vector_size| {
        //         hnsw_config
        //             .full_scan_threshold
        //             .saturating_mul(BYTES_IN_KB)
        //             .checked_div(avg_vector_size)
        //     })
        //     .unwrap_or(1);

        let mut config = HnswGraphConfig::new(
            hnsw_config.m,
            hnsw_config.ef_construct,
            hnsw_config.max_indexing_threads,
            total_vector_count,
            distance, //TODO:: ?? SURELY ??
        );

        let mut rng = thread_rng();

        let mut graph_layers_builder = GraphLayersBuilder::new(
            total_vector_count,
            config.m,
            config.m0,
            config.ef_construct,
            HNSW_USE_HEURISTIC,
        );

        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("hnsw-build-{idx}"))
            .num_threads(*POOL_KEEP_LIMIT) //TODO: To check
            .spawn_handler(|thread| {
                let mut b = thread::Builder::new();
                if let Some(name) = thread.name() {
                    b = b.name(name.to_owned());
                }
                if let Some(stack_size) = thread.stack_size() {
                    b = b.stack_size(stack_size);
                }
                b.spawn(|| thread.run())?;
                Ok(())
            })
            .build()?;

        for vector_id in id_tracker.iter_internal_ids() {
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(vector_id, level);
        }

        let mut ids_iterator = id_tracker.iter_internal_ids();

        let first_few_ids: Vec<_> = ids_iterator
            .by_ref()
            .take(SINGLE_THREADED_HNSW_BUILD_THRESHOLD)
            .collect();
        let ids: Vec<_> = ids_iterator.collect();

        let indexed_vectors = ids.len() + first_few_ids.len();

        let insert_point = |vector_id| {
            let vector = vector_storage.get_vector(vector_id);
            let vector = vector.as_ref().into();
            let raw_scorer = new_raw_scorer(vector, vector_storage, distance)?; //TODO: Distance Type Selction
            let points_scorer = FilteredScorer::new(raw_scorer.as_ref());

            graph_layers_builder.link_new_point(vector_id, points_scorer);
            Ok::<_, Error>(())
        };

        for vector_id in first_few_ids {
            insert_point(vector_id)?;
        }

        if !ids.is_empty() {
            ids.into_iter().try_for_each(insert_point)?;
            //pool.install(|| ids.into_par_iter().try_for_each(insert_point))?; //TODO: Parallalize!!!
        }

        config.indexed_vector_count.replace(indexed_vectors);

        let graph_links_path = GraphLayers::get_links_path(path);
        let graph: GraphLayers = graph_layers_builder.into_graph_layers(&graph_links_path)?;

        Ok((config, graph))
    }

    fn search_vectors_with_graph(
        &self,
        vectors: &[&QueryVector],
        top: usize,
    ) -> Result<Vec<Vec<ScoredPoint>>> {
        vectors
            .iter()
            .map(|&vector| self.search_with_graph(vector, top))
            .collect()
    }

    fn search_with_graph(&self, vector: &QueryVector, top: usize) -> Result<Vec<ScoredPoint>> {
        // let ef = params
        // TODO: make EF be selectable ??
        //     .and_then(|params| params.hnsw_ef)
        //     .unwrap_or(self.config.ef);

        let vector_storage = self.vector_storage.borrow();

        let raw_scorer = new_raw_scorer(
            vector.to_owned(),
            vector_storage.deref(),
            self.config.distance,
        )?;

        let points_scorer = FilteredScorer::new(raw_scorer.as_ref());

        let search_result = self.graph.search(top, self.config.ef, points_scorer);
        let postprocessed_points = self.postprocess_points(search_result);

        Ok(postprocessed_points)
    }

    fn postprocess_points(&self, points: Vec<ScoredPointOffset>) -> Vec<ScoredPoint> {
        let distance = self.config.distance;
        let id_tracker = self.id_tracker.borrow();

        let postprocessed_points: Vec<ScoredPoint> = points
            .into_iter()
            .map(|scored_point| {
                let external_id = id_tracker.get_external_id(scored_point.idx);
                ScoredPoint {
                    id: external_id,
                    score: distance.postprocess_score(scored_point.score),
                }
            })
            .collect();

        postprocessed_points
    }
}
pub trait VectorIndex {
    fn search(&self, vectors: &[&QueryVector], top: usize) -> Result<Vec<Vec<ScoredPoint>>>;
}

impl VectorIndex for HnswIndex {
    fn search(&self, vectors: &[&QueryVector], top: usize) -> Result<Vec<Vec<ScoredPoint>>> {
        self.search_vectors_with_graph(vectors, top)
    }
}

// Plain search
// vectors
// .iter()
// .map(|&vector| {
// new_stoppable_raw_scorer(
// vector.to_owned(),
// &vector_storage,
// deleted_points,
// &is_stopped,
// )
// .map(|scorer| scorer.peek_top_all(top))
// })
// .collect()

#[cfg(test)]
mod tests {
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;
    use atomic_refcell::AtomicRefCell;

    use crate::{
        config::HnswConfig,
        id_tracker::{IdTracker, IdTrackerImpl, IdTrackerSS},
        types::{Distance, PointIdType, QueryVector, VectorElementType},
        vector_storage::{VectorStorageImpl, VectorStorageSS},
        HnswIndex, HnswIndexCreateArgs, VectorIndex,
    };
    use std::sync::Arc;

    fn generate_fibb_data_set() -> (
        Arc<AtomicRefCell<IdTrackerSS>>,
        Arc<AtomicRefCell<VectorStorageSS>>,
    ) {
        let mut id_tracker = IdTrackerImpl::new();
        let mut vector_storage = VectorStorageImpl::new();

        let fibb: Vec<u32> = vec![
            1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765,
            10946, 17711, 28657, 46368, 75025, 121393,
        ];
        for (i, &f) in fibb.iter().enumerate() {
            let _ = id_tracker.add_new_external_id(i as PointIdType);
            vector_storage.add_vector(vec![f as VectorElementType]);
        }

        let id_tracker = Arc::new(AtomicRefCell::new(id_tracker));
        let vector_storage = Arc::new(AtomicRefCell::new(vector_storage));
        (id_tracker, vector_storage)
    }

    #[test]
    fn hnsw_search_euclid_on_fibb_data_set() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let (id_tracker, vector_storage) = generate_fibb_data_set();

        let hnsw_config = HnswConfig {
            m: 6,
            ef_construct: 12,
            max_indexing_threads: 4,
        };

        let hnsw_index = HnswIndex::create(HnswIndexCreateArgs {
            path,
            id_tracker,
            vector_storage,
            hnsw_config,
            distance: Distance::Euclid,
        })?;

        let query_vectors = [
            QueryVector::from(vec![21.0]),
            QueryVector::from(vec![418.0]),
        ];
        let query_vectors_refs: Vec<&QueryVector> = query_vectors.iter().collect();
        let top = 2;

        //Act
        let search_result = hnsw_index.search(&query_vectors_refs, top)?;

        //Assert
        assert_eq!(search_result.len(), query_vectors.len());
        assert_eq!(search_result[0].len(), top);
        assert_eq!(search_result[1].len(), top);

        let result = search_result[0]
            .iter()
            .map(|p| p.id)
            .collect::<Vec<PointIdType>>();

        let result2 = search_result[1]
            .iter()
            .map(|p| p.id)
            .collect::<Vec<PointIdType>>();

        assert_eq!(result, vec![7, 6]);
        assert_eq!(result2, vec![13, 12]);

        Ok(())
    }
}
