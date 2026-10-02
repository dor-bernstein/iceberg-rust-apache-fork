// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, MergeManifestProcess, SnapshotProducer};
use super::{
    MANIFEST_MERGE_ENABLED, MANIFEST_MERGE_ENABLED_DEFAULT, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES,
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
};
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus,
    Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProduceOperation;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Iceberg field id of the `file_path` column in position delete files.
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2147483546;

/// Conflicts collected, and named in the error message, before the validation stops looking.
const MAX_CONFLICTS_IN_MESSAGE: usize = 5;

/// Which snapshot [`Operation`] a file replacement records.
///
/// `rewrite_files` and `overwrite_files` differ only in this value
pub(crate) trait ReplaceFilesMode: Send + Sync + 'static {
    const OPERATION: Operation;
}

/// Files were added and removed without changing table data (compaction,
/// changing file format, relocating files).
pub struct Rewrite;

/// Files were added and removed in a logical overwrite.
pub struct Overwrite;

impl ReplaceFilesMode for Rewrite {
    const OPERATION: Operation = Operation::Replace;
}

impl ReplaceFilesMode for Overwrite {
    const OPERATION: Operation = Operation::Overwrite;
}

/// A blanket `impl<M: ReplaceFilesMode> SnapshotProduceOperation for M` would
/// collide with `impl SnapshotProduceOperation for FastAppendOperation`: the
/// compiler cannot prove `FastAppendOperation` will never implement
/// `ReplaceFilesMode`. This wrapper carries the shared implementation instead.
pub(crate) struct ReplaceFilesOperation<M: ReplaceFilesMode>(PhantomData<M>);

impl<M: ReplaceFilesMode> ReplaceFilesOperation<M> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M: ReplaceFilesMode> SnapshotProduceOperation for ReplaceFilesOperation<M> {
    fn operation(&self) -> Operation {
        M::OPERATION
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // generate delete manifest entries from removed files
        let snapshot = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch());

        if let Some(snapshot) = snapshot {
            let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
                let builder = ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(old_entry.snapshot_id().unwrap())
                    .sequence_number(old_entry.sequence_number().unwrap())
                    .file_sequence_number(old_entry.file_sequence_number().unwrap())
                    .data_file(old_entry.data_file().clone());

                builder.build()
            };

            let manifest_list = snapshot
                .load_manifest_list(
                    snapshot_produce.table.file_io(),
                    snapshot_produce.table.metadata(),
                )
                .await?;

            let mut deleted_entries = Vec::new();

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file
                    .load_manifest(snapshot_produce.table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    if entry.content_type() == DataContentType::Data
                        && snapshot_produce
                            .removed_data_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }

                    if (entry.content_type() == DataContentType::PositionDeletes
                        || entry.content_type() == DataContentType::EqualityDeletes)
                        && snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }
                }
            }

            Ok(deleted_entries)
        } else {
            Ok(vec![])
        }
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let table_metadata_ref = snapshot_produce.table.metadata();
        let file_io_ref = snapshot_produce.table.file_io();

        let Some(snapshot) = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(file_io_ref, table_metadata_ref)
            .await?;

        let mut existing_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io_ref).await?;

            let found_deleted_files: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    if snapshot_produce
                        .removed_data_file_paths
                        .contains(entry.data_file().file_path())
                        || snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        Some(entry.data_file().file_path().to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if found_deleted_files.is_empty() {
                existing_files.push(manifest_file.clone());
            } else {
                // Rewrite the manifest file without the deleted data files
                let survives = |entry: &ManifestEntry| {
                    entry.is_alive() && !found_deleted_files.contains(entry.data_file().file_path())
                };

                if manifest.entries().iter().any(|entry| survives(entry)) {
                    let mut manifest_writer = snapshot_produce.new_manifest_writer(
                        manifest_file.content,
                        manifest_file.partition_spec_id,
                    )?;

                    for entry in manifest.entries() {
                        // Carry survivors forward as `Existing`: `add_entry` would
                        // restamp them as `Added` under the new snapshot and drop
                        // their file sequence number.
                        if survives(entry) {
                            manifest_writer.add_existing_entry((**entry).clone())?;
                        }
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
            }
        }

        Ok(existing_files)
    }
}

/// Transaction action that replaces one set of files with another.
///
/// `M` is sealed to [`Rewrite`] and [`Overwrite`] via the [`RewriteFilesAction`] /
/// [`OverwriteFilesAction`] type aliases below; `ReplaceFilesMode` itself stays
/// `pub(crate)` so no other type can be substituted for `M`.
#[allow(private_bounds)]
pub struct ReplaceFilesAction<M: ReplaceFilesMode> {
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,

    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    snapshot_id: Option<i64>,
    new_data_file_sequence_number: Option<i64>,
    target_branch: Option<String>,
    enable_delete_filter_manager: bool,
    check_file_existence: bool,
    validate_from_snapshot_id: Option<i64>,

    _mode: PhantomData<M>,
}

/// Rewrites files without changing table data — compaction and friends.
pub type RewriteFilesAction = ReplaceFilesAction<Rewrite>;

/// Rewrites files as a logical overwrite.
pub type OverwriteFilesAction = ReplaceFilesAction<Overwrite>;

#[allow(private_bounds)]
impl<M: ReplaceFilesMode> ReplaceFilesAction<M> {
    pub fn new() -> Self {
        Self {
            target_size_bytes: MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
            min_count_to_merge: MANIFEST_MIN_MERGE_COUNT_DEFAULT,
            merge_enabled: MANIFEST_MERGE_ENABLED_DEFAULT,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            snapshot_id: None,
            new_data_file_sequence_number: None,
            target_branch: None,
            enable_delete_filter_manager: false,
            check_file_existence: false,
            validate_from_snapshot_id: None,
            _mode: PhantomData,
        }
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in data_files {
            match file.content_type() {
                DataContentType::Data => self.added_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(file)
                }
            }
        }

        self
    }

    /// Add remove files to the snapshot.
    pub fn delete_files(mut self, remove_data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in remove_data_files {
            match file.content_type() {
                DataContentType::Data => self.removed_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.removed_delete_files.push(file)
                }
            }
        }

        self
    }

    pub fn set_snapshot_properties(&mut self, properties: HashMap<String, String>) -> &mut Self {
        let target_size_bytes: u32 = properties
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = properties
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = properties
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);

        self.target_size_bytes = target_size_bytes;
        self.min_count_to_merge = min_count_to_merge;
        self.merge_enabled = merge_enabled;
        self.snapshot_properties = properties;

        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(&mut self, commit_uuid: Uuid) -> &mut Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot id
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Enable delete filter manager for this snapshot.
    /// By default, delete filter manager is disabled.
    pub fn set_enable_delete_filter_manager(mut self, enable_delete_filter_manager: bool) -> Self {
        self.enable_delete_filter_manager = enable_delete_filter_manager;
        self
    }

    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
    }

    // If the compaction should use the sequence number of the snapshot at compaction start time for
    // new data files, instead of using the sequence number of the newly produced snapshot.
    // This avoids commit conflicts with updates that add newer equality deletes at a higher sequence number.
    pub fn set_new_data_file_sequence_number(mut self, seq: i64) -> Self {
        self.new_data_file_sequence_number = Some(seq);
        self
    }

    pub fn set_check_file_existence(mut self, check: bool) -> Self {
        self.check_file_existence = check;
        self
    }

    /// Validate, at commit time, that no snapshot committed to the target branch after
    /// `snapshot_id` added a delete that applies to a data file this action removes.
    ///
    /// Without it a rewrite planned at `snapshot_id` silently resurrects rows: another writer
    /// commits a position delete against data file `D`, this action replaces `D` with a file
    /// computed without that delete, and the delete is left pointing at a removed file.
    /// `Transaction::commit` rebases onto the latest table and re-runs [`TransactionAction::commit`]
    /// on every attempt, and the commit itself requires the branch to still be at the snapshot
    /// the action was applied to, so the check covers every snapshot up to the one this commit
    /// lands on. Mirrors Java's `RewriteFiles.validateFromSnapshot`.
    ///
    /// Equality deletes conflict too, unless [`Self::set_new_data_file_sequence_number`] is set:
    /// the new files then carry the older sequence number, so later equality deletes still apply
    /// to them.
    ///
    /// `snapshot_id` need not be the planning snapshot. A caller that has already judged the
    /// snapshots up to some later `H` itself -- more precisely than this check can, for example by
    /// reading delete files that carry no `file_path` bounds -- may pass `H`, so that only what
    /// landed after its own check is judged here. Everything up to `H` is then the caller's
    /// responsibility.
    ///
    /// A conflict, or a `snapshot_id` that is not an ancestor of the branch head, fails the commit
    /// with [`ErrorKind::PreconditionFailed`], which is not retryable; tell the two apart with
    /// [`rewrite_validation_failure`].
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot_id = Some(snapshot_id);
        self
    }
}

/// Why [`ReplaceFilesAction::validate_from_snapshot`] refused a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteValidationFailure {
    /// A snapshot after the validation start added a delete that may apply to a removed data
    /// file. Re-planning, or re-judging that snapshot more precisely and validating from it, can
    /// succeed.
    ConcurrentDeletes,
    /// The validation start is not an ancestor of the branch head (expired, rolled back, or the
    /// branch is empty), so what happened since cannot be known. Retrying cannot help.
    UntraceableHistory,
}

/// Classify an error raised by [`ReplaceFilesAction::validate_from_snapshot`]; `None` for any
/// other error, including other `PreconditionFailed`s on the commit path.
pub fn rewrite_validation_failure(err: &Error) -> Option<RewriteValidationFailure> {
    if err.kind() != ErrorKind::PreconditionFailed {
        return None;
    }
    if err.message().starts_with(CONCURRENT_DELETES_PREFIX) {
        Some(RewriteValidationFailure::ConcurrentDeletes)
    } else if err.message().starts_with(UNTRACEABLE_HISTORY_PREFIX) {
        Some(RewriteValidationFailure::UntraceableHistory)
    } else {
        None
    }
}

const CONCURRENT_DELETES_PREFIX: &str =
    "Cannot commit the rewrite: a delete committed after snapshot";
const UNTRACEABLE_HISTORY_PREFIX: &str = "Cannot validate the rewrite from snapshot";

/// `path` without its scheme. Writers in one table have spelled the same object `s3://` and
/// `s3a://`, and a reader still applies such a delete, so matching must ignore the scheme.
fn without_scheme(path: &str) -> &str {
    path.split_once("://").map_or(path, |(_, rest)| rest)
}

/// Fail if a snapshot after `starting_snapshot_id` on `branch` added a delete that may apply to
/// one of `removed_data_files`.
///
/// Each snapshot is judged by the manifests it wrote itself (`added_snapshot_id`), read from its
/// own manifest list, so a later manifest rewrite cannot hide the entry. Stops at the first
/// [`MAX_CONFLICTS_IN_MESSAGE`] conflicts: one is enough to refuse.
async fn validate_no_new_deletes_for_data_files(
    table: &Table,
    branch: &str,
    starting_snapshot_id: i64,
    removed_data_files: &[DataFile],
    ignore_equality_deletes: bool,
) -> Result<()> {
    if removed_data_files.is_empty() {
        return Ok(());
    }
    let removed: HashSet<&str> = removed_data_files
        .iter()
        .map(|f| without_scheme(f.file_path()))
        .collect();
    let metadata = table.metadata();
    let untraceable = |why: String| {
        Error::new(
            ErrorKind::PreconditionFailed,
            format!("{UNTRACEABLE_HISTORY_PREFIX} {starting_snapshot_id}: {why}"),
        )
    };
    let Some(head) = metadata.snapshot_for_ref(branch) else {
        return Err(untraceable(format!("branch {branch} has no snapshot")));
    };

    let mut newer = Vec::new();
    let mut next = Some(head.clone());
    let mut reached_start = false;
    while let Some(snapshot) = next {
        if snapshot.snapshot_id() == starting_snapshot_id {
            reached_start = true;
            break;
        }
        next = snapshot
            .parent_snapshot_id()
            .and_then(|id| metadata.snapshot_by_id(id).cloned());
        newer.push(snapshot);
    }
    if !reached_start {
        return Err(untraceable(format!(
            "it is not an ancestor of {branch} head {}",
            head.snapshot_id()
        )));
    }

    let mut conflicts: Vec<String> = Vec::new();
    'snapshots: for snapshot in &newer {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await?;
        for manifest_file in manifest_list.entries() {
            // Not skipped on `added_files_count`: that is the writer's own claim, and a wrong 0
            // would hide a delete from the one check that sees this window. Entry status decides.
            if manifest_file.content != ManifestContentType::Deletes
                || manifest_file.added_snapshot_id != snapshot.snapshot_id()
            {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.status() != ManifestStatus::Added {
                    continue;
                }
                let delete_file = entry.data_file();
                for data_file in delete_targets(delete_file, &removed, ignore_equality_deletes) {
                    conflicts.push(format!(
                        "{data_file} (snapshot {} added {})",
                        snapshot.snapshot_id(),
                        delete_file.file_path()
                    ));
                    if conflicts.len() >= MAX_CONFLICTS_IN_MESSAGE {
                        break 'snapshots;
                    }
                }
            }
        }
    }

    if conflicts.is_empty() {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::PreconditionFailed,
        format!(
            "{CONCURRENT_DELETES_PREFIX} {starting_snapshot_id} applies to data files it removes: {}",
            conflicts.join(", ")
        ),
    ))
}

/// The removed data files `delete_file` may apply to, as scheme-less paths.
///
/// A position delete or deletion vector names its data file through `referenced_data_file`, or
/// through equal `file_path` bounds; otherwise the bounds are a range, and every removed file
/// inside it may be a target. A position delete with no bounds may target any removed file --
/// DuckDB writes those, so a caller that can read the delete file should judge such snapshots
/// itself and validate only from after them.
fn delete_targets<'a>(
    delete_file: &DataFile,
    removed: &HashSet<&'a str>,
    ignore_equality_deletes: bool,
) -> Vec<&'a str> {
    match delete_file.content_type() {
        DataContentType::Data => vec![],
        DataContentType::EqualityDeletes if ignore_equality_deletes => vec![],
        DataContentType::EqualityDeletes => removed.iter().copied().collect(),
        DataContentType::PositionDeletes => {
            if let Some(path) = delete_file.referenced_data_file() {
                return removed
                    .get(without_scheme(&path))
                    .copied()
                    .into_iter()
                    .collect();
            }
            let bound = |bounds: &HashMap<i32, crate::spec::Datum>| {
                bounds
                    .get(&FIELD_ID_POSITIONAL_DELETE_FILE_PATH)
                    .and_then(|d| d.to_bytes().ok())
                    .and_then(|b| String::from_utf8(b.to_vec()).ok())
            };
            match (
                bound(delete_file.lower_bounds()),
                bound(delete_file.upper_bounds()),
            ) {
                (Some(lower), Some(upper)) => {
                    let (lower, upper) = (without_scheme(&lower), without_scheme(&upper));
                    removed
                        .iter()
                        .copied()
                        .filter(|path| lower <= *path && *path <= upper)
                        .collect()
                }
                _ => removed.iter().copied().collect(),
            }
        }
    }
}

#[async_trait::async_trait]
impl<M: ReplaceFilesMode> TransactionAction for ReplaceFilesAction<M> {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_id,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.removed_data_files.clone(),
            self.removed_delete_files.clone(),
        );

        if let Some(seq) = self.new_data_file_sequence_number {
            snapshot_producer.set_new_data_file_sequence_number(seq);
        }

        if let Some(branch) = &self.target_branch {
            snapshot_producer.set_target_branch(branch.clone());
        }

        if self.enable_delete_filter_manager {
            snapshot_producer.enable_delete_filter_manager();
        }

        if let Some(starting_snapshot_id) = self.validate_from_snapshot_id {
            validate_no_new_deletes_for_data_files(
                table,
                snapshot_producer.target_branch(),
                starting_snapshot_id,
                &self.removed_data_files,
                self.new_data_file_sequence_number.is_some(),
            )
            .await?;
        }

        if self.check_file_existence {
            snapshot_producer.validate_data_file_changes().await?;
        }

        if self.merge_enabled {
            let process =
                MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
            snapshot_producer
                .commit(ReplaceFilesOperation::<M>::new(), process)
                .await
        } else {
            snapshot_producer
                .commit(ReplaceFilesOperation::<M>::new(), DefaultManifestProcess)
                .await
        }
    }
}

impl<M: ReplaceFilesMode> Default for ReplaceFilesAction<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use uuid::Uuid;

    use super::{
        CONCURRENT_DELETES_PREFIX, FIELD_ID_POSITIONAL_DELETE_FILE_PATH, Overwrite,
        ReplaceFilesMode, ReplaceFilesOperation, Rewrite, RewriteValidationFailure, delete_targets,
        rewrite_validation_failure, validate_no_new_deletes_for_data_files, without_scheme,
    };
    use crate::catalog::MockCatalog;
    use crate::error::Result;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, MAIN_BRANCH,
        ManifestContentType, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation,
        Snapshot, SnapshotReference, SnapshotRetention, Struct, Summary,
    };
    use crate::table::Table;
    use crate::transaction::snapshot::{SnapshotProduceOperation, SnapshotProducer};
    use crate::transaction::tests::{
        PARENT_SEQUENCE_NUMBER, PARENT_SNAPSHOT_ID, REMOVED_DELETE_FILE, RETAINED_DELETE_FILE,
        make_v2_table_with_delete_manifest, position_delete_file,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{Error, ErrorKind};

    #[test]
    fn test_modes_map_to_their_operations() {
        assert_eq!(Rewrite::OPERATION, Operation::Replace);
        assert_eq!(Overwrite::OPERATION, Operation::Overwrite);
        assert_eq!(
            ReplaceFilesOperation::<Rewrite>::new().operation(),
            Operation::Replace
        );
        assert_eq!(
            ReplaceFilesOperation::<Overwrite>::new().operation(),
            Operation::Overwrite
        );
    }

    /// Regression test: a rewrite/overwrite that removes one delete file must not
    /// mark *unrelated* delete files as deleted.
    ///
    /// `delete_entries` once guarded the delete-file branch with
    ///   `content == PositionDeletes || content == EqualityDeletes && removed.contains(path)`
    /// and because `&&` binds tighter than `||`, every `PositionDeletes` entry in
    /// the parent snapshot matched regardless of `removed_delete_file_paths`.
    async fn assert_only_removed_delete_files_marked<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let deleted_entries = ReplaceFilesOperation::<M>::new()
            .delete_entries(&producer)
            .await
            .unwrap();
        let deleted_paths: Vec<&str> = deleted_entries
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();

        assert_eq!(
            deleted_paths,
            vec![REMOVED_DELETE_FILE],
            "only the removed delete file should be marked deleted; \
             {RETAINED_DELETE_FILE} must stay live"
        );
    }

    /// Regression test: rewriting a partially-deleted *delete* manifest must
    /// preserve its `Deletes` content type, and must carry survivors forward as
    /// `Existing` rather than restamping them as `Added`.
    async fn assert_delete_manifest_carried_forward_intact<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let mut producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let existing = ReplaceFilesOperation::<M>::new()
            .existing_manifest(&mut producer)
            .await
            .unwrap();

        assert_eq!(existing.len(), 1, "the delete manifest should be rewritten");
        assert_eq!(
            existing[0].content,
            ManifestContentType::Deletes,
            "a rewritten delete manifest must stay a Deletes manifest"
        );

        let entries = existing[0].load_manifest(table.file_io()).await.unwrap();
        let paths: Vec<&str> = entries
            .entries()
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();
        assert_eq!(paths, vec![RETAINED_DELETE_FILE]);

        let retained = &entries.entries()[0];
        assert_eq!(retained.status(), ManifestStatus::Existing);
        assert_eq!(retained.snapshot_id(), Some(PARENT_SNAPSHOT_ID));
        assert_eq!(retained.sequence_number(), Some(PARENT_SEQUENCE_NUMBER));
        assert_eq!(
            retained.file_sequence_number(),
            Some(PARENT_SEQUENCE_NUMBER)
        );
    }

    #[tokio::test]
    async fn test_overwrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Rewrite>().await;
    }

    #[tokio::test]
    async fn test_overwrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Rewrite>().await;
    }

    const CHILD_SNAPSHOT_ID: i64 = 43;
    const REWRITTEN_DATA_FILE: &str = "s3://bucket/data/b-rewritten.parquet";
    const OTHER_DATA_FILE: &str = "s3://bucket/data/z-other.parquet";

    fn data_file(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    fn path_bounds(lower: &str, upper: &str) -> (HashMap<i32, Datum>, HashMap<i32, Datum>) {
        (
            HashMap::from([(FIELD_ID_POSITIONAL_DELETE_FILE_PATH, Datum::string(lower))]),
            HashMap::from([(FIELD_ID_POSITIONAL_DELETE_FILE_PATH, Datum::string(upper))]),
        )
    }

    fn position_delete(
        table: &Table,
        path: &str,
        referenced: Option<&str>,
        bounds: Option<(&str, &str)>,
    ) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(referenced.map(str::to_string));
        if let Some((lower, upper)) = bounds {
            let (lower, upper) = path_bounds(lower, upper);
            builder.lower_bounds(lower).upper_bounds(upper);
        }
        builder.build().unwrap()
    }

    fn equality_delete(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap()
    }

    /// [`make_v2_table_with_delete_manifest`] plus a child of [`PARENT_SNAPSHOT_ID`] on `main`
    /// whose own delete manifest adds `deletes` -- another writer committing after the rewrite
    /// planned at the parent.
    ///
    /// The parent's own delete files carry no bounds, so they would conflict with any removed
    /// file if the validation (wrongly) looked at snapshots at or before the starting one.
    async fn table_with_concurrent_deletes(deletes: Vec<DataFile>) -> Table {
        let base = make_v2_table_with_delete_manifest().await;
        let file_io = base.file_io().clone();
        let location = base.metadata().location().to_string();
        let manifest_path = format!("{location}/metadata/child-delete-manifest.avro");
        let list_path = format!("{location}/metadata/child-manifest-list.avro");

        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(&manifest_path).unwrap(),
            Some(CHILD_SNAPSHOT_ID),
            None,
            base.metadata().current_schema().clone(),
            base.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v2_deletes();
        for delete in deletes {
            writer.add_file(delete, PARENT_SEQUENCE_NUMBER + 1).unwrap();
        }
        let manifest = writer.write_manifest_file().await.unwrap();

        let mut list_writer = ManifestListWriter::v2(
            file_io.new_output(&list_path).unwrap(),
            CHILD_SNAPSHOT_ID,
            Some(PARENT_SNAPSHOT_ID),
            PARENT_SEQUENCE_NUMBER + 1,
        );
        list_writer
            .add_manifests(vec![manifest].into_iter())
            .unwrap();
        list_writer.close().await.unwrap();

        let child = Snapshot::builder()
            .with_snapshot_id(CHILD_SNAPSHOT_ID)
            .with_parent_snapshot_id(Some(PARENT_SNAPSHOT_ID))
            .with_timestamp_ms(base.metadata().last_updated_ms() + 2)
            .with_sequence_number(PARENT_SEQUENCE_NUMBER + 1)
            .with_schema_id(0)
            .with_manifest_list(list_path)
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .build();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some("s3://bucket/test/location/metadata/v2.json".into()))
            .add_snapshot(child)
            .unwrap()
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id: CHILD_SNAPSHOT_ID,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        base.with_metadata(Arc::new(metadata))
    }

    async fn validate(table: &Table, from: i64, ignore_equality_deletes: bool) -> Result<()> {
        validate_no_new_deletes_for_data_files(
            table,
            MAIN_BRANCH,
            from,
            &[data_file(table, REWRITTEN_DATA_FILE)],
            ignore_equality_deletes,
        )
        .await
    }

    fn assert_conflict(result: Result<()>) {
        let err = result.expect_err("the rewrite must be refused");
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed, "{err}");
        assert!(!err.retryable(), "a conflict must not be retried: {err}");
        assert_eq!(
            rewrite_validation_failure(&err),
            Some(RewriteValidationFailure::ConcurrentDeletes),
            "{err}"
        );
        assert!(
            err.message().contains(without_scheme(REWRITTEN_DATA_FILE)),
            "{err}"
        );
    }

    /// The action-level wiring: a rewrite planned at the parent, committed against a table that
    /// has since taken a position delete on one of its inputs, fails before producing a snapshot.
    #[tokio::test]
    async fn test_rewrite_refuses_a_position_delete_added_after_the_starting_snapshot() {
        let base = make_v2_table_with_delete_manifest().await;
        let table = table_with_concurrent_deletes(vec![position_delete(
            &base,
            "s3://bucket/deletes/d1.parquet",
            Some(REWRITTEN_DATA_FILE),
            None,
        )])
        .await;
        let action = Transaction::new(&table)
            .rewrite_files()
            .add_data_files([data_file(&table, "s3://bucket/data/compacted.parquet")])
            .delete_files([data_file(&table, REWRITTEN_DATA_FILE)])
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER)
            .validate_from_snapshot(PARENT_SNAPSHOT_ID);

        let result = Arc::new(action).commit(&table).await.map(|_| ());
        assert_conflict(result);
    }

    /// The race itself: the transaction is built on the planning table, and the delete lands
    /// before it commits. `Transaction::commit` reloads, rebases onto the newer table and re-runs
    /// the action, which must then refuse without ever calling `update_table`.
    #[tokio::test]
    async fn test_a_delete_landing_before_commit_is_caught_after_the_rebase() {
        let planned = make_v2_table_with_delete_manifest().await;
        let concurrent = table_with_concurrent_deletes(vec![position_delete(
            &planned,
            "s3://bucket/deletes/d1.parquet",
            Some(REWRITTEN_DATA_FILE),
            None,
        )])
        .await;

        let mut catalog = MockCatalog::new();
        catalog.expect_load_table().returning_st(move |_| {
            let table = concurrent.clone();
            Box::pin(async move { Ok(table) })
        });
        catalog.expect_update_table().times(0);

        let txn = Transaction::new(&planned);
        let txn = txn
            .rewrite_files()
            .add_data_files([data_file(&planned, "s3://bucket/data/compacted.parquet")])
            .delete_files([data_file(&planned, REWRITTEN_DATA_FILE)])
            .set_new_data_file_sequence_number(PARENT_SEQUENCE_NUMBER)
            .validate_from_snapshot(PARENT_SNAPSHOT_ID)
            .apply(txn)
            .unwrap();

        assert_conflict(txn.commit(&catalog).await.map(|_| ()));
    }

    #[tokio::test]
    async fn test_delete_on_another_data_file_does_not_conflict() {
        let base = make_v2_table_with_delete_manifest().await;
        let table = table_with_concurrent_deletes(vec![
            position_delete(
                &base,
                "s3://bucket/deletes/d1.parquet",
                Some(OTHER_DATA_FILE),
                None,
            ),
            position_delete(
                &base,
                "s3://bucket/deletes/d2.parquet",
                None,
                Some((OTHER_DATA_FILE, OTHER_DATA_FILE)),
            ),
        ])
        .await;
        validate(&table, PARENT_SNAPSHOT_ID, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_equal_path_bounds_name_the_data_file() {
        let base = make_v2_table_with_delete_manifest().await;
        let table = table_with_concurrent_deletes(vec![position_delete(
            &base,
            "s3://bucket/deletes/d1.parquet",
            None,
            Some((REWRITTEN_DATA_FILE, REWRITTEN_DATA_FILE)),
        )])
        .await;
        assert_conflict(validate(&table, PARENT_SNAPSHOT_ID, true).await);
    }

    #[tokio::test]
    async fn test_a_path_range_covering_the_data_file_conflicts() {
        let base = make_v2_table_with_delete_manifest().await;
        let table = table_with_concurrent_deletes(vec![position_delete(
            &base,
            "s3://bucket/deletes/d1.parquet",
            None,
            Some(("s3://bucket/data/a.parquet", "s3://bucket/data/c.parquet")),
        )])
        .await;
        assert_conflict(validate(&table, PARENT_SNAPSHOT_ID, true).await);
    }

    #[tokio::test]
    async fn test_a_path_range_outside_the_data_file_does_not_conflict() {
        let base = make_v2_table_with_delete_manifest().await;
        let table = table_with_concurrent_deletes(vec![position_delete(
            &base,
            "s3://bucket/deletes/d1.parquet",
            None,
            Some(("s3://bucket/data/c.parquet", "s3://bucket/data/y.parquet")),
        )])
        .await;
        validate(&table, PARENT_SNAPSHOT_ID, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_an_unplaceable_position_delete_conflicts() {
        let base = make_v2_table_with_delete_manifest().await;
        let table = table_with_concurrent_deletes(vec![position_delete(
            &base,
            "s3://bucket/deletes/d1.parquet",
            None,
            None,
        )])
        .await;
        assert_conflict(validate(&table, PARENT_SNAPSHOT_ID, true).await);
    }

    #[tokio::test]
    async fn test_equality_deletes_conflict_unless_the_starting_sequence_number_is_kept() {
        let base = make_v2_table_with_delete_manifest().await;
        let table = table_with_concurrent_deletes(vec![equality_delete(
            &base,
            "s3://bucket/deletes/e1.parquet",
        )])
        .await;
        assert_conflict(validate(&table, PARENT_SNAPSHOT_ID, false).await);
        validate(&table, PARENT_SNAPSHOT_ID, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_nothing_committed_since_the_starting_snapshot_is_valid() {
        let base = make_v2_table_with_delete_manifest().await;
        let table = table_with_concurrent_deletes(vec![position_delete(
            &base,
            "s3://bucket/deletes/d1.parquet",
            Some(REWRITTEN_DATA_FILE),
            None,
        )])
        .await;
        validate(&table, CHILD_SNAPSHOT_ID, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_a_starting_snapshot_outside_the_branch_history_is_refused() {
        let table = table_with_concurrent_deletes(vec![]).await;
        let err = validate(&table, 7, false)
            .await
            .expect_err("an untraceable history must refuse");
        assert_eq!(err.kind(), ErrorKind::PreconditionFailed, "{err}");
        assert!(!err.retryable());
        assert_eq!(
            rewrite_validation_failure(&err),
            Some(RewriteValidationFailure::UntraceableHistory),
            "{err}"
        );
    }

    /// The same object spelled `s3a://` in the delete and `s3://` in the manifest is still the
    /// same file: a reader applies the delete, so the rewrite must not strand it.
    #[tokio::test]
    async fn test_a_scheme_mismatch_still_names_the_data_file() {
        let base = make_v2_table_with_delete_manifest().await;
        let s3a = REWRITTEN_DATA_FILE.replacen("s3://", "s3a://", 1);
        let table = table_with_concurrent_deletes(vec![position_delete(
            &base,
            "s3://bucket/deletes/d1.parquet",
            Some(&s3a),
            None,
        )])
        .await;
        assert_conflict(validate(&table, PARENT_SNAPSHOT_ID, true).await);

        let table = table_with_concurrent_deletes(vec![position_delete(
            &base,
            "s3://bucket/deletes/d2.parquet",
            None,
            Some((&s3a, &s3a)),
        )])
        .await;
        assert_conflict(validate(&table, PARENT_SNAPSHOT_ID, true).await);
    }

    /// A caller that judged the concurrent snapshot itself validates from after it: a bounds-less
    /// delete it has already placed elsewhere no longer refuses the commit.
    #[tokio::test]
    async fn test_validating_from_a_later_snapshot_skips_what_the_caller_judged() {
        let base = make_v2_table_with_delete_manifest().await;
        let table = table_with_concurrent_deletes(vec![position_delete(
            &base,
            "s3://bucket/deletes/duckdb-style.parquet",
            None,
            None,
        )])
        .await;
        assert_conflict(validate(&table, PARENT_SNAPSHOT_ID, true).await);
        validate(&table, CHILD_SNAPSHOT_ID, true).await.unwrap();
    }

    #[test]
    fn test_other_errors_are_not_rewrite_validation_failures() {
        for err in [
            Error::new(ErrorKind::PreconditionFailed, "No added data files found"),
            Error::new(ErrorKind::DataInvalid, CONCURRENT_DELETES_PREFIX),
            Error::new(ErrorKind::CatalogCommitConflicts, "Commit conflict"),
        ] {
            assert_eq!(rewrite_validation_failure(&err), None, "{err}");
        }
    }

    #[test]
    fn test_a_data_file_entry_has_no_delete_targets() {
        let table = crate::transaction::tests::make_v2_minimal_table();
        let removed = HashSet::from([REWRITTEN_DATA_FILE]);
        assert!(
            delete_targets(&data_file(&table, REWRITTEN_DATA_FILE), &removed, false).is_empty()
        );
    }
}
