use core::panic;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use super::Result;
use crate::components::collection::types::CollectionSearchResult;
use crate::components::wal::{Wal, WalType};
use crate::types::{Lsn, WAL_FILE};
use atomic_refcell::AtomicRefCell;
use hnsw::config::HnswConfig;
use hnsw::id_tracker::{IdTracker, IdTrackerImpl};
use hnsw::types::Distance;
use hnsw::vector_storage::VectorStorageImpl;
use hnsw::{HnswIndex, HnswIndexOpenArgs, VectorIndex};

use crate::{
    components::collection::Collection,
    cq::{CQAction, CQTarget, CQValidator, Query, Validator},
    types::Dim,
};

const HNSW_DIR_NAME: &str = "vr_hnsw";

pub struct SearchSimilarQuery {
    collection: CQTarget,
    distance: Distance,
    query_vectors: Vec<Vec<Dim>>,
}

impl SearchSimilarQuery {
    pub fn new(collection: CQTarget, distance: Distance, query_vectors: Vec<Vec<Dim>>) -> Self {
        Self {
            collection,
            distance,
            query_vectors,
        }
    }
}

impl Query for SearchSimilarQuery {
    fn execute(&mut self) -> Result<()> {
        CQValidator::target_exists(&self.collection);

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        let mut id_tracker = IdTrackerImpl::new();
        let mut vector_storage = VectorStorageImpl::new();

        {
            let result = collection.search_all()?;
            for (id, record) in result {
                id_tracker.add_new_external_id(id);
                vector_storage.add_vector(record.vector);
            }
        }

        let current_version_number = SearchSimilarQuery::get_version_number(&path)?;
        SearchSimilarQuery::remove_outdated_graph_dir(&path, current_version_number)?; //TODO: When remove outdated graph dir? By default? Or by command?
        let hnsw_index_path = path.join(format!("{HNSW_DIR_NAME}_{current_version_number}"));

        let args = HnswIndexOpenArgs {
            path: &hnsw_index_path,
            id_tracker: Arc::new(AtomicRefCell::new(id_tracker)).clone(),
            vector_storage: Arc::new(AtomicRefCell::new(vector_storage)),
            hnsw_config: HnswConfig {
                m: 24, //TODO: M, EF changeable?
                ef_construct: 64,
                max_indexing_threads: 10, //TODO: To Verify
            },
            distance: self.distance,
        };

        // TODO: Should rebuild be specified?

        println!("Opening HNSW Index...");
        let time = std::time::Instant::now();
        let index = HnswIndex::open(args)?;
        println!("HNSW Index opened in {}s.", time.elapsed().as_secs_f32()); //TODO: Time to remove?
        println!("HNSW Index opened.");

        let query_vectors_ref: Vec<&Vec<Dim>> = self.query_vectors.iter().collect();

        println!("Searching similar vectors...");
        let result = index.search(&query_vectors_ref, 10)?; //TODO: Maybe more vectors for query?
                                                            //TODO: Make top number modifiable?
        for query_result in result {
            println!();
            for scored_point in query_result {
                let search_result = collection.search(scored_point.id)?;
                if let CollectionSearchResult::FoundRecord(record) = search_result {
                    println!(
                        "Id: {}, Payload: {}, Distance: {:?}",
                        scored_point.id, record.payload, scored_point.score
                    );
                }
            }
        }

        Ok(())
    }
}

impl CQAction for SearchSimilarQuery {
    fn to_string(&self) -> String {
        format!(
            "{} {}",
            SEARCH_SIMILAR_Q_STR,
            self.distance.to_string().to_uppercase()
        )
    }
}

impl SearchSimilarQuery {
    fn get_version_number(path: &Path) -> Result<Lsn> {
        let wal_type = Wal::load(&path.join(WAL_FILE))?;

        if let WalType::Consistent(wal) = wal_type {
            Ok(wal.get_last_lsn())
        } else {
            panic!("Wal is not consistent. Cannot perform search similar query.")
        }
    }

    fn remove_outdated_graph_dir(path: &Path, current_version: Lsn) -> Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|name| name.to_str()) {
                    if let Some(version_str) = dir_name.strip_prefix(&format!("{HNSW_DIR_NAME}_")) {
                        if let Ok(version) = version_str.parse::<u64>() {
                            if version != current_version {
                                fs::remove_dir_all(&path)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
