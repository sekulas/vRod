use std::sync::Arc;

use super::Result;
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

pub struct SearchSimilarQuery {
    collection: CQTarget,
    query_vector: Vec<Dim>,
}

impl SearchSimilarQuery {
    pub fn new(collection: CQTarget, query_vector: Vec<Dim>) -> Self {
        Self {
            collection,
            query_vector,
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

        let id_tracker_arc = Arc::new(AtomicRefCell::new(id_tracker));
        let args = HnswIndexOpenArgs {
            path: &path,
            id_tracker: id_tracker_arc.clone(),
            vector_storage: Arc::new(AtomicRefCell::new(vector_storage)),
            hnsw_config: HnswConfig {
                m: 3, //TODO: M, EF changeable?
                ef_construct: 5,
                max_indexing_threads: 3, //TODO: To Verify
            },
            distance: Distance::Euclid, //TODO: Selectable?
        };

        let index = HnswIndex::open(args)?;
        let result = index.search(&[&self.query_vector], 5)?; //TODO: Maybe more vectors for query?
                                                              //TODO: Make top number modifiable?

        let id_tracker = id_tracker_arc.borrow();
        for query_result in result {
            for scored_point in query_result {
                let in_collection_id = id_tracker.get_external_id(scored_point.idx);
                let search_result = collection.search(in_collection_id)?;
                if let crate::components::collection::types::CollectionSearchResult::FoundRecord(
                    record,
                ) = search_result
                {
                    println!(
                        "Id: {}, Payload: {}, Score: {:?}",
                        in_collection_id, record.payload, scored_point.score
                    );
                }
            }
        }

        Ok(())
    }
}

impl CQAction for SearchSimilarQuery {
    fn to_string(&self) -> String {
        "SEARCHSIMILAR".to_string()
    }
}
