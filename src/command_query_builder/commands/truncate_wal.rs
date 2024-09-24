// pub struct TruncateWalCommand {
//     pub target_path: PathBuf,
// }

// impl TruncateWalCommand {
//     pub fn new(target_path: &Path) -> Self {
//         TruncateWalCommand {
//             target_path: target_path.to_owned(),
//         }
//     }
// }

// impl Command for TruncateWalCommand {
//     fn execute(&mut self, lsn: Lsn) -> Result<()> {
//         let wal_path = self.target_path.join(WAL_FILE);

//         fs::remove_file(&wal_path)?;

//         Wal::create(&wal_path)?;

//         Ok(())
//     }

//     fn rollback(&mut self, lsn: Lsn) -> Result<()> {
//         Ok(())
//     }
// }

// impl CQAction for TruncateWalCommand {
//     fn to_string(&self) -> String {
//         "TRUNCATEWAL".to_string()
//     }
// }
