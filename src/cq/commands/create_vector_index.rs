use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::Result;
use crate::components::wal::Wal;
use crate::cq::types::CREATE_VECTOR_INDEX_C_STR;
use crate::cq::utils::vector_idx::get_vector_index_path;
use atomic_refcell::AtomicRefCell;
use hnsw::config::HnswConfig;
use hnsw::id_tracker::{IdTracker, IdTrackerImpl};
use hnsw::types::Distance;
use hnsw::vector_storage::VectorStorageImpl;
use hnsw::{CreateArgs as HnswIndexCreateArgs, HnswIndex};

use crate::{
    components::collection::Collection,
    cq::{CQAction, CQTarget, CQValidator, Command, Validator},
};

const BACKUP_INDEX_DIR_SUFFIX: &str = "_bak";

pub struct CreateVectorIndexCommand {
    collection: CQTarget,
    distance: Distance,
}

impl CreateVectorIndexCommand {
    pub fn new(collection: CQTarget, distance: Distance) -> Self {
        Self {
            collection,
            distance,
        }
    }
}

impl Command for CreateVectorIndexCommand {
    fn execute(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);

        let path: std::path::PathBuf = self.collection.get_target_path();
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

        CreateVectorIndexCommand::save_current_index_as_backup(&path, &self.distance)?; //TODO: When remove outdated graph dir? By default? Or by command?
        let hnsw_index_path = get_vector_index_path(&path, &self.distance);

        let args = HnswIndexCreateArgs {
            path: &hnsw_index_path,
            id_tracker: Arc::new(AtomicRefCell::new(id_tracker)).clone(),
            vector_storage: Arc::new(AtomicRefCell::new(vector_storage)),
            hnsw_config: HnswConfig {
                m: 16, //TODO: M, EF changeable?
                ef_construct: 64,
            },
            distance: self.distance,
        };

        wal.append(self.to_string())?;

        println!("Creating HNSW Index...");
        let time = std::time::Instant::now();

        let _ = HnswIndex::create(args)?;
        wal.commit()?;

        println!("HNSW Index created in {}s.", time.elapsed().as_secs_f32()); //###TODO: Time to remove?

        Ok(())
    }

    fn rollback(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let path: std::path::PathBuf = self.collection.get_target_path();

        wal.append(format!("ROLLBACK {}", self.to_string()))?;

        CreateVectorIndexCommand::make_index_from_backup(&path, &self.distance)?;

        println!("HNSW Index creation rolled back.");

        wal.commit()?;

        Ok(())
    }
}

impl CQAction for CreateVectorIndexCommand {
    fn to_string(&self) -> String {
        format!(
            "{} {}",
            CREATE_VECTOR_INDEX_C_STR,
            self.distance.to_string().to_uppercase()
        )
    }
}

impl CreateVectorIndexCommand {
    fn save_current_index_as_backup(path: &Path, distance: &Distance) -> Result<()> {
        let hnsw_index_path = get_vector_index_path(path, distance);

        if hnsw_index_path.exists() {
            let backup_path = CreateVectorIndexCommand::get_path_for_index_backup(&hnsw_index_path);
            if backup_path.exists() {
                fs::remove_dir_all(&backup_path)?;
            }
            fs::rename(hnsw_index_path, &backup_path)?;
        }

        Ok(())
    }

    fn make_index_from_backup(path: &Path, distance: &Distance) -> Result<()> {
        let hnsw_index_path = get_vector_index_path(path, distance);
        let backup_path = CreateVectorIndexCommand::get_path_for_index_backup(&hnsw_index_path);

        if backup_path.exists() {
            println!("Found backup index. Restoring...");

            if hnsw_index_path.exists() {
                fs::remove_dir_all(&hnsw_index_path)?;
            }
            fs::rename(backup_path, hnsw_index_path)?;
        } else {
            println!("No backup index found. Removing current index...");

            if hnsw_index_path.exists() {
                fs::remove_dir_all(hnsw_index_path)?;
            }
        }

        Ok(())
    }

    fn get_path_for_index_backup(path: &Path) -> PathBuf {
        let mut backup_path = path.to_string_lossy().to_string();
        backup_path.push_str(BACKUP_INDEX_DIR_SUFFIX);

        PathBuf::from(backup_path)
    }
}
