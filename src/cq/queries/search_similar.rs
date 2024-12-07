use std::path::Path;
use std::sync::Arc;

use super::{Error, Result};
use crate::components::collection::types::CollectionSearchResult;
use crate::cq::utils::vector_idx::get_vector_index_path;
use atomic_refcell::AtomicRefCell;
use hnsw::id_tracker::{IdTracker, IdTrackerImpl};
use hnsw::types::Distance;
use hnsw::vector_storage::VectorStorageImpl;
use hnsw::{HnswIndex, LoadArgs as HnswIndexLoadArgs, VectorIndex};

use crate::{
    components::collection::Collection,
    cq::{types::SEARCH_SIMILAR_Q_STR, CQAction, CQTarget, CQValidator, Query, Validator},
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
    fn execute(&self) -> Result<()> {
        CQValidator::target_exists(&self.collection);

        let path = self.collection.get_target_path();
        let hnsw_index_path = get_vector_index_path(&path, &self.distance);

        SearchSimilarQuery::vector_index_exists(&hnsw_index_path)?;

        let mut collection = Collection::load(&path)?;

        let mut id_tracker = IdTrackerImpl::new();
        let mut vector_storage = VectorStorageImpl::new(&self.distance);

        {
            let result = collection.search_all()?;
            for (id, record) in result {
                id_tracker.add_new_external_id(id);
                vector_storage.add_vector(record.vector);
            }
        }

        let args = HnswIndexLoadArgs {
            path: &hnsw_index_path,
            id_tracker: Arc::new(AtomicRefCell::new(id_tracker)).clone(),
            vector_storage: Arc::new(AtomicRefCell::new(vector_storage)),
        };

        println!("Loading HNSW Index...");
        let time = std::time::Instant::now();
        let index = HnswIndex::load(args)?;
        println!("HNSW Index loaded in {}s.", time.elapsed().as_secs_f32());

        let query_vectors_ref: Vec<&Vec<Dim>> = self.query_vectors.iter().collect();

        println!("Searching similar vectors...");
        let result = index.search(&query_vectors_ref, 10, 100)?; //TODO: Maybe more vectors for query?
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
    fn vector_index_exists(index_path: &Path) -> Result<()> {
        if !index_path.exists() {
            return Err(Error::VectorIndexDoesNotExist {
                path: index_path.into(),
            });
        }
        Ok(())
    }
}
