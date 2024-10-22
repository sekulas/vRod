use super::{Error, Result};
use atomic_refcell::AtomicRefCell;
use rand::thread_rng;
use std::{
    fs::create_dir_all,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use crate::{
    config::{HnswConfig, HnswGraphConfig},
    graph_layers::GraphLayers,
    graph_layers_builder::GraphLayersBuilder,
    id_tracker::IdTrackerSS,
    scorer::{new_raw_scorer, FilteredScorer},
    types::{Distance, QueryVector, ScoredPointOffset},
    vector_storage::{VectorStorage, VectorStorageSS},
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
    path: PathBuf,
    graph: GraphLayers,
}

pub struct HnswIndexOpenArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageSS>>,
    pub hnsw_config: HnswConfig,
    pub distance: Distance,
}

impl HnswIndex {
    pub fn open(args: HnswIndexOpenArgs<'_>) -> Result<Self> {
        let HnswIndexOpenArgs {
            path,
            id_tracker,
            vector_storage,
            hnsw_config,
            distance,
        } = args;

        create_dir_all(path)?;

        let config_path = HnswGraphConfig::get_config_path(path);
        let graph_path = GraphLayers::get_path(path);
        let graph_links_path = GraphLayers::get_links_path(path);
        let (config, graph) = if graph_path.exists() {
            let config = if config_path.exists() {
                HnswGraphConfig::load(&config_path)?
            } else {
                let vector_storage = vector_storage.borrow();
                let vector_count = vector_storage.total_vector_count();
                // let full_scan_threshold = vector_storage
                //     .available_size_in_bytes()
                //     .checked_div(available_vectors)
                //     .and_then(|avg_vector_size| {
                //         hnsw_config
                //             .full_scan_threshold
                //             .saturating_mul(BYTES_IN_KB)
                //             .checked_div(avg_vector_size)
                //     })
                //     .unwrap_or(1);

                HnswGraphConfig::new(
                    hnsw_config.m,
                    hnsw_config.ef_construct,
                    //full_scan_threshold,
                    hnsw_config.max_indexing_threads,
                    vector_count,
                    distance,
                )
            };

            (config, GraphLayers::load(&graph_path, &graph_links_path)?)
        } else {
            let (config, graph) = Self::build_index(
                path,
                hnsw_config,
                id_tracker.borrow().deref(),
                vector_storage.borrow().deref(),
                distance,
            )?;

            config.save(&config_path)?;
            graph.save(&graph_path)?;

            (config, graph)
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
        vector_storage: &dyn VectorStorage,
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
            std::cmp::max(
                1,
                // total_vector_count //TODO: To check how many entry points
                //     .checked_div(full_scan_threshold)
                //     .unwrap_or(0)
                //     * 10,
                0,
            ),
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
                b.spawn(|| {
                    // // On Linux, use lower thread priority so we interfere less with serving traffic
                    // #[cfg(target_os = "linux")]
                    // if let Err(err) = linux_low_thread_priority() {
                    //     log::debug!(
                    //         "Failed to set low thread priority for HNSW building, ignoring: {err}"
                    //     );
                    // }

                    thread.run()
                })?;
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
        let graph: GraphLayers = graph_layers_builder.into_graph_layers(Some(&graph_links_path))?;

        Ok((config, graph))
    }

    fn search_vectors_with_graph(
        &self,
        vectors: &[&QueryVector],
        top: usize,
    ) -> Result<Vec<Vec<ScoredPointOffset>>> {
        vectors
            .iter()
            .map(|&vector| self.search_with_graph(vector, top))
            .collect()
    }

    fn search_with_graph(
        &self,
        vector: &QueryVector,
        top: usize,
    ) -> Result<Vec<ScoredPointOffset>> {
        // let ef = params
        // TODO: make EF be selectable ??
        //     .and_then(|params| params.hnsw_ef)
        //     .unwrap_or(self.config.ef);

        //let id_tracker = self.id_tracker.borrow();
        let vector_storage = self.vector_storage.borrow();

        let raw_scorer = new_raw_scorer(
            vector.to_owned(),
            vector_storage.deref(),
            self.config.distance,
        )?;

        let points_scorer = FilteredScorer::new(raw_scorer.as_ref());

        let search_result = self.graph.search(top, self.config.ef, points_scorer);
        Ok(search_result)
    }
}

pub trait VectorIndex {
    fn search(&self, vectors: &[&QueryVector], top: usize) -> Result<Vec<Vec<ScoredPointOffset>>>;
}

impl VectorIndex for HnswIndex {
    fn search(&self, vectors: &[&QueryVector], top: usize) -> Result<Vec<Vec<ScoredPointOffset>>> {
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
