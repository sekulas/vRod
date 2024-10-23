use std::sync::Arc;

use super::Result;
use crate::components::collection::types::CollectionSearchResult;
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
        let query_vectors_ref: Vec<&Vec<Dim>> = self.query_vectors.iter().collect();

        let result = index.search(&query_vectors_ref, 5)?; //TODO: Maybe more vectors for query?
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
        let distance_str = match self.distance {
            Distance::Cosine => "COSINE",
            Distance::Euclid => "EUCLID",
            Distance::Dot => "DOT",
            Distance::Manhattan => "MANHATTAN",
        };
        "SEARCHSIMILAR ".to_string() + distance_str
    }
}
