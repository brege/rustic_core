use std::path::{Path, PathBuf};

use itertools::Itertools;
use log::info;
use path_dedot::ParseDot;

use crate::{
    archiver::Archiver,
    backend::{ReadSource, dry_run::DryRunBackend},
    commands::backup::BackupOptions,
    error::{ErrorKind, RusticError, RusticResult},
    repofile::SnapshotFile,
    repository::{IndexedIds, Repository},
};

fn resolve_as_path(opts: &BackupOptions) -> RusticResult<Option<PathBuf>> {
    opts.as_path
        .as_ref()
        .map(|p| {
            p.parse_dot()
                .map_err(|err| {
                    RusticError::with_source(
                        ErrorKind::InvalidInput,
                        "Failed to parse dotted path `{path}`",
                        err,
                    )
                    .attach_context("path", p.display().to_string())
                })
                .map(|p| p.to_path_buf())
        })
        .transpose()
}

pub(crate) fn backup_source<S, R>(
    repo: &Repository<S>,
    opts: &BackupOptions,
    backup_root: &Path,
    src: &R,
    mut snap: SnapshotFile,
) -> RusticResult<SnapshotFile>
where
    S: IndexedIds,
    R: ReadSource + 'static,
    <R as ReadSource>::Open: Send,
    <R as ReadSource>::Iter: Send,
{
    let backup_root = backup_root.to_path_buf();
    let as_path = resolve_as_path(opts)?;
    let paths = match &as_path {
        Some(path) => std::slice::from_ref(path),
        None => std::slice::from_ref(&backup_root),
    };

    snap.paths.set_paths(paths).map_err(|err| {
        RusticError::with_source(
            ErrorKind::Internal,
            "Failed to set paths `{paths}` in snapshot.",
            err,
        )
        .attach_context("paths", backup_root.display().to_string())
    })?;

    let (parent_ids, parent) = opts.parent_opts.get_parent(repo, &snap, false);
    if parent_ids.is_empty() {
        info!("using no parent");
    } else {
        info!("using parents {}", parent_ids.iter().join(", "));
        snap.parent = Some(parent_ids[0]);
        snap.parents = parent_ids;
    }

    let be = DryRunBackend::new(repo.dbe().clone(), opts.dry_run);
    info!("starting to backup {} ...", backup_root.display());
    let archiver = Archiver::new(be, repo.index(), repo.config(), parent, snap)?;
    let progress = repo.progress_bytes("backing up...");

    archiver.archive(
        src,
        &backup_root,
        as_path.as_ref(),
        opts.parent_opts.skip_if_unchanged,
        opts.no_scan,
        &progress,
    )
}
