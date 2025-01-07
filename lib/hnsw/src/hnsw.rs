use super::{Error, Result};
use atomic_refcell::AtomicRefCell;
use rand::thread_rng;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{fs::create_dir_all, ops::Deref, path::Path, sync::Arc, thread};

use crate::{
    config::{HnswConfig, HnswGraphConfig},
    graph_layers::GraphLayers,
    graph_layers_builder::GraphLayersBuilder,
    id_tracker::IdTrackerSS,
    scorer::{new_query_scorer, Scorer},
    types::{Distance, QueryVector, ScoredPoint, ScoredPointOffset},
    vector_storage::VectorStorageSS,
    visited_pool::POOL_KEEP_LIMIT,
};

const HNSW_USE_HEURISTIC: bool = true;

/// disconnected components in the graph.
#[cfg(debug_assertions)]
const SINGLE_THREADED_BUILD_THRESHOLD: usize = 32;
#[cfg(not(debug_assertions))]
const SINGLE_THREADED_BUILD_THRESHOLD: usize = 256;

#[derive(Debug)]
pub struct HnswIndex {
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
    config: HnswGraphConfig,
    graph: GraphLayers,
}

pub struct LoadArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
}

pub struct CreateArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
    pub hnsw_config: HnswConfig,
    pub distance: Distance,
}

impl HnswIndex {
    pub fn create(args: CreateArgs<'_>) -> Result<Self> {
        create_dir_all(args.path)?;

        let config_path = HnswGraphConfig::get_config_path(args.path);
        let graph_path = GraphLayers::get_path(args.path);

        let (config, graph) = Self::build_index(
            args.path,
            args.hnsw_config,
            args.id_tracker.borrow().deref(),
            args.vector_storage.borrow().deref(),
            args.distance,
        )?;

        config.save(&config_path)?;
        graph.save(&graph_path)?;

        Ok(HnswIndex {
            id_tracker: args.id_tracker,
            vector_storage: args.vector_storage,
            config,
            graph,
        })
    }

    pub fn load(args: LoadArgs<'_>) -> Result<Self> {
        let config_path = HnswGraphConfig::get_config_path(args.path);
        let graph_path = GraphLayers::get_path(args.path);
        let graph_links_path = GraphLayers::get_links_path(args.path);

        let (config, graph) = match (config_path.exists(), graph_path.exists()) {
            (true, true) => {
                let config = HnswGraphConfig::load(&config_path)?;
                let graph = GraphLayers::load(&graph_path, &graph_links_path)?;
                (config, graph)
            }
            (false, _) => return Err(Error::ConfigFileHasNotBeenFound { path: config_path }),
            (_, false) => return Err(Error::GraphFileHasNotBeenFound { path: graph_path }),
        };

        Ok(HnswIndex {
            id_tracker: args.id_tracker,
            vector_storage: args.vector_storage,
            config,
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

        let config = HnswGraphConfig::new(
            hnsw_config.m,
            hnsw_config.ef_construct,
            total_vector_count,
            distance,
        );

        let thread_pool = Self::create_thread_pool()?;
        let mut graph_builder = Self::initialize_graph_builder(total_vector_count, &config);

        Self::assign_random_layers(&mut graph_builder, id_tracker);
        Self::build_graph_structure(
            &thread_pool,
            &mut graph_builder,
            id_tracker,
            vector_storage,
            distance,
        )?;

        let graph = Self::finalize_graph(path, graph_builder)?;

        Ok((config, graph))
    }

    fn create_thread_pool() -> Result<rayon::ThreadPool> {
        rayon::ThreadPoolBuilder::new()
            .thread_name(|idx| format!("hnsw-build-{idx}"))
            .num_threads(*POOL_KEEP_LIMIT)
            .spawn_handler(|thread| {
                let mut builder = thread::Builder::new();
                if let Some(name) = thread.name() {
                    builder = builder.name(name.to_owned());
                }
                if let Some(stack_size) = thread.stack_size() {
                    builder = builder.stack_size(stack_size);
                }
                builder.spawn(|| thread.run())?;
                Ok(())
            })
            .build()
            .map_err(Error::from)
    }

    fn initialize_graph_builder(
        total_vector_count: usize,
        config: &HnswGraphConfig,
    ) -> GraphLayersBuilder {
        GraphLayersBuilder::new(
            total_vector_count,
            config.m,
            config.m0,
            config.ef_construct,
            HNSW_USE_HEURISTIC,
        )
    }

    fn assign_random_layers(graph_builder: &mut GraphLayersBuilder, id_tracker: &IdTrackerSS) {
        let mut rng = thread_rng();
        for vector_id in id_tracker.iter_internal_ids() {
            let level = graph_builder.get_random_layer(&mut rng);
            graph_builder.set_levels(vector_id, level);
        }
    }

    fn build_graph_structure(
        thread_pool: &rayon::ThreadPool,
        graph_builder: &mut GraphLayersBuilder,
        id_tracker: &IdTrackerSS,
        vector_storage: &VectorStorageSS,
        distance: Distance,
    ) -> Result<()> {
        let mut ids_iter = id_tracker.iter_internal_ids();
        let initial_ids: Vec<_> = ids_iter
            .by_ref()
            .take(SINGLE_THREADED_BUILD_THRESHOLD)
            .collect();
        let remaining_ids: Vec<_> = ids_iter.collect();

        let insert_point = |vector_id| {
            let vector = vector_storage.get_vector(vector_id);
            let vector = vector.as_ref().into();
            let query_scorer = new_query_scorer(vector, vector_storage, distance)?;
            let points_scorer = Scorer::new(query_scorer.as_ref());

            graph_builder.link_new_point(vector_id, points_scorer);
            Ok::<_, Error>(())
        };

        for vector_id in initial_ids {
            insert_point(vector_id)?;
        }

        if !remaining_ids.is_empty() {
            thread_pool.install(|| remaining_ids.into_par_iter().try_for_each(insert_point))?;
        }

        Ok(())
    }

    fn finalize_graph(path: &Path, graph_builder: GraphLayersBuilder) -> Result<GraphLayers> {
        let graph_links_path = GraphLayers::get_links_path(path);
        //graph_builder.print_layer_diagnostics();
        graph_builder.into_graph_layers(&graph_links_path)
    }

    fn search_vectors(
        &self,
        vectors: &[&QueryVector],
        top: usize,
        ef_search: usize,
    ) -> Result<Vec<Vec<ScoredPoint>>> {
        vectors
            .iter()
            .map(|&vector| self.search_single(vector, top, ef_search))
            .collect()
    }

    fn search_single(
        &self,
        vector: &QueryVector,
        top: usize,
        ef_search: usize,
    ) -> Result<Vec<ScoredPoint>> {
        let vector_storage = self.vector_storage.borrow();
        let query_scorer = new_query_scorer(
            vector.to_owned(),
            vector_storage.deref(),
            self.config.distance,
        )?;

        let points_scorer = Scorer::new(query_scorer.as_ref());
        let search_result = self.graph.search(top, ef_search, points_scorer);

        Ok(self.postprocess_points(search_result))
    }

    fn postprocess_points(&self, points: Vec<ScoredPointOffset>) -> Vec<ScoredPoint> {
        let id_tracker = self.id_tracker.borrow();
        points
            .into_iter()
            .map(|point| ScoredPoint {
                id: id_tracker.get_external_id(point.idx),
                score: self.config.distance.postprocess_score(point.score),
            })
            .collect()
    }
}
pub trait VectorIndex {
    fn search(
        &self,
        vectors: &[&QueryVector],
        top: usize,
        ef_search: usize,
    ) -> Result<Vec<Vec<ScoredPoint>>>;
}

impl VectorIndex for HnswIndex {
    fn search(
        &self,
        vectors: &[&QueryVector],
        top: usize,
        ef_search: usize,
    ) -> Result<Vec<Vec<ScoredPoint>>> {
        self.search_vectors(vectors, top, ef_search)
    }
}

#[cfg(test)]
mod tests {
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;
    use atomic_refcell::AtomicRefCell;

    use crate::{
        config::HnswConfig,
        id_tracker::{IdTracker, IdTrackerImpl, IdTrackerSS},
        types::{Distance, PointIdType, QueryVector, VectorElementType},
        vector_storage::{VectorStorageImpl, VectorStorageSS},
        CreateArgs, HnswIndex, VectorIndex,
    };
    use std::sync::Arc;

    fn generate_fibb_data_set() -> (
        Arc<AtomicRefCell<IdTrackerSS>>,
        Arc<AtomicRefCell<VectorStorageSS>>,
    ) {
        let mut id_tracker = IdTrackerImpl::new();
        let mut vector_storage = VectorStorageImpl::new(&Distance::Euclid);

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
        };

        let hnsw_index = HnswIndex::create(CreateArgs {
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
        let search_result = hnsw_index.search(&query_vectors_refs, top, 12)?;

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
