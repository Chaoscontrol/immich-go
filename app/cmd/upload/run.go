package upload

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/simulot/immich-go/adapters"
	gp "github.com/simulot/immich-go/adapters/googlePhotos"
	"github.com/simulot/immich-go/app"
	"github.com/simulot/immich-go/immich"
	"github.com/simulot/immich-go/internal/assets"
	"github.com/simulot/immich-go/internal/assets/cache"
	cliflags "github.com/simulot/immich-go/internal/cliFlags"
	"github.com/simulot/immich-go/internal/fileevent"
	"github.com/simulot/immich-go/internal/filters"
	"github.com/simulot/immich-go/internal/fshelper"
	"github.com/simulot/immich-go/internal/gen/syncset"
)

type UpCmd struct {
	Mode UpLoadMode
	*UploadOptions
	app *app.Application

	assetIndex        *immichIndex         // List of assets present on the server
	localAssets       *syncset.Set[string] // List of assets present on the local input by name+size
	immichAssetsReady chan struct{}        // Signal that the asset index is ready
	deleteServerList  []*immich.Asset      // List of server assets to remove

	adapter       adapters.Reader
	DebugCounters bool // Enable CSV action counters per file

	Paths          []string // Path to explore
	takeoutOptions *gp.ImportFlags

	albumsCache *cache.CollectionCache[assets.Album] // List of albums present on the server
	tagsCache   *cache.CollectionCache[assets.Tag]   // List of tags present on the server

	shouldResumeJobs map[string]bool // List of jobs to resume
	finished         bool            // the finish task has been run

	// Delayed metadata and tagging operations
	delayedAlbums   map[string][]assets.Album   // Map of asset IDs to albums to be added
	delayedTags     map[string][]assets.Tag     // Map of asset IDs to tags to be added
	delayedMetadata map[string]*assets.Metadata // Map of asset IDs to metadata to be applied
}

func newUpload(mode UpLoadMode, app *app.Application, options *UploadOptions) *UpCmd {
	upCmd := &UpCmd{
		UploadOptions:     options,
		app:               app,
		Mode:              mode,
		localAssets:       syncset.New[string](),
		immichAssetsReady: make(chan struct{}),
		delayedAlbums:     make(map[string][]assets.Album),
		delayedTags:       make(map[string][]assets.Tag),
		delayedMetadata:   make(map[string]*assets.Metadata),
	}

	return upCmd
}

func (upCmd *UpCmd) setTakeoutOptions(options *gp.ImportFlags) *UpCmd {
	upCmd.takeoutOptions = options
	return upCmd
}

func (upCmd *UpCmd) saveAlbum(ctx context.Context, album assets.Album, ids []string) (assets.Album, error) {
	if len(ids) == 0 {
		return album, nil
	}
	if album.ID == "" {
		r, err := upCmd.app.Client().Immich.CreateAlbum(ctx, album.Title, album.Description, ids)
		if err != nil {
			upCmd.app.Jnl().Log().Error("failed to create album", "err", err, "album", album.Title)
			return album, err
		}
		upCmd.app.Jnl().Log().Info("created album", "album", album.Title, "assets", len(ids))
		album.ID = r.ID
		return album, nil
	}
	_, err := upCmd.app.Client().Immich.AddAssetToAlbum(ctx, album.ID, ids)
	if err != nil {
		upCmd.app.Jnl().Log().Error("failed to add assets to album", "err", err, "album", album.Title, "assets", len(ids))
		return album, err
	}
	upCmd.app.Jnl().Log().Info("updated album", "album", album.Title, "assets", len(ids))
	return album, err
}

func (upCmd *UpCmd) saveTags(ctx context.Context, tag assets.Tag, ids []string) (assets.Tag, error) {
	if len(ids) == 0 {
		return tag, nil
	}
	if tag.ID == "" {
		r, err := upCmd.app.Client().Immich.UpsertTags(ctx, []string{tag.Value})
		if err != nil {
			upCmd.app.Jnl().Log().Error("failed to create tag", "err", err, "tag", tag.Name)
			return tag, err
		}
		upCmd.app.Jnl().Log().Info("created tag", "tag", tag.Value)
		tag.ID = r[0].ID
	}
	_, err := upCmd.app.Client().Immich.TagAssets(ctx, tag.ID, ids)
	if err != nil {
		upCmd.app.Jnl().Log().Error("failed to add assets to tag", "err", err, "tag", tag.Value, "assets", len(ids))
		return tag, err
	}
	upCmd.app.Jnl().Log().Info("updated tag", "tag", tag.Value, "assets", len(ids))
	return tag, err
}

func (UpCmd *UpCmd) pauseJobs(ctx context.Context, app *app.Application) error {
	UpCmd.shouldResumeJobs = make(map[string]bool)
	jobs, err := app.Client().AdminImmich.GetJobs(ctx)
	if err != nil {
		return err
	}
	for name, job := range jobs {
		// Skip the sidecar job - it needs to remain active
		if name == "sidecar" {
			UpCmd.app.Jnl().Log().Info("Skipping pause of sidecar job - keeping it active")
			continue
		}
		UpCmd.shouldResumeJobs[name] = !job.QueueStatus.IsPaused
		if UpCmd.shouldResumeJobs[name] {
			_, err = app.Client().AdminImmich.SendJobCommand(ctx, name, "pause", true)
			if err != nil {
				UpCmd.app.Jnl().Log().Error("Immich Job command sent", "pause", name, "err", err.Error())
				return err
			}
			UpCmd.app.Jnl().Log().Info("Immich Job command sent", "pause", name)
		}
	}
	return nil
}

func (UpCmd *UpCmd) resumeJobs(_ context.Context, app *app.Application) error {
	if UpCmd.shouldResumeJobs == nil {
		return nil
	}
	// Start with a context not yet cancelled
	ctx := context.Background() //nolint
	for name, shouldResume := range UpCmd.shouldResumeJobs {
		if shouldResume {
			_, err := app.Client().AdminImmich.SendJobCommand(ctx, name, "resume", true) //nolint:contextcheck
			if err != nil {
				UpCmd.app.Jnl().Log().Error("Immich Job command sent", "resume", name, "err", err.Error())
				return err
			}
			UpCmd.app.Jnl().Log().Info("Immich Job command sent", "resume", name)
		}
	}
	return nil
}

// isSidecarQueueClear checks if the sidecar job queue is clear (no active, waiting, delayed, or paused jobs)
func (UpCmd *UpCmd) isSidecarQueueClear(ctx context.Context, app *app.Application) (bool, error) {
	jobs, err := app.Client().AdminImmich.GetJobs(ctx)
	if err != nil {
		return false, err
	}

	sidecarJob, exists := jobs["sidecar"]
	if !exists {
		// If sidecar job doesn't exist, consider it clear
		return true, nil
	}

	jobCounts := sidecarJob.JobCounts

	// Queue is clear when all counts are 0
	return jobCounts.Active == 0 &&
		jobCounts.Waiting == 0 &&
		jobCounts.Delayed == 0 &&
		jobCounts.Paused == 0, nil
}

// isMetadataExtractionQueueClear checks if the metadataExtraction job queue is clear
func (UpCmd *UpCmd) isMetadataExtractionQueueClear(ctx context.Context, app *app.Application) (bool, error) {
	jobs, err := app.Client().AdminImmich.GetJobs(ctx)
	if err != nil {
		return false, err
	}

	metadataJob, exists := jobs["metadataExtraction"]
	if !exists {
		// If metadataExtraction job doesn't exist, consider it clear
		return true, nil
	}

	jobCounts := metadataJob.JobCounts

	// Queue is clear when all counts are 0
	return jobCounts.Active == 0 &&
		jobCounts.Waiting == 0 &&
		jobCounts.Delayed == 0 &&
		jobCounts.Paused == 0, nil
}

// waitForSidecarQueueToClear waits until the sidecar job queue is clear
func (UpCmd *UpCmd) waitForSidecarQueueToClear(ctx context.Context, app *app.Application) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Get job status for logging
			jobs, err := app.Client().AdminImmich.GetJobs(ctx)
			if err != nil {
				return err
			}

			sidecarJob, exists := jobs["sidecar"]
			if !exists {
				UpCmd.app.Jnl().Log().Info("Sidecar job queue is clear (job doesn't exist)")
				return nil
			}

			jobCounts := sidecarJob.JobCounts
			UpCmd.app.Jnl().Log().Info("Sidecar job queue status",
				"active", jobCounts.Active,
				"completed", jobCounts.Completed,
				"failed", jobCounts.Failed,
				"delayed", jobCounts.Delayed,
				"waiting", jobCounts.Waiting,
				"paused", jobCounts.Paused)

			// Check if queue is clear
			clear := jobCounts.Active == 0 &&
				jobCounts.Waiting == 0 &&
				jobCounts.Delayed == 0 &&
				jobCounts.Paused == 0

			if clear {
				return nil
			}

			// Wait a bit before checking again
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

// waitForMetadataExtractionQueueToClear waits until the metadataExtraction job queue is clear
func (UpCmd *UpCmd) waitForMetadataExtractionQueueToClear(ctx context.Context, app *app.Application) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Get job status for logging
			jobs, err := app.Client().AdminImmich.GetJobs(ctx)
			if err != nil {
				return err
			}

			metadataJob, exists := jobs["metadataExtraction"]
			if !exists {
				UpCmd.app.Jnl().Log().Info("MetadataExtraction job queue is clear (job doesn't exist)")
				return nil
			}

			jobCounts := metadataJob.JobCounts
			UpCmd.app.Jnl().Log().Info("MetadataExtraction job queue status",
				"active", jobCounts.Active,
				"completed", jobCounts.Completed,
				"failed", jobCounts.Failed,
				"delayed", jobCounts.Delayed,
				"waiting", jobCounts.Waiting,
				"paused", jobCounts.Paused)

			// Check if queue is clear
			clear := jobCounts.Active == 0 &&
				jobCounts.Waiting == 0 &&
				jobCounts.Delayed == 0 &&
				jobCounts.Paused == 0

			if clear {
				return nil
			}

			// Wait a bit before checking again
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

func (UpCmd *UpCmd) finishing(ctx context.Context, app *app.Application) error {
	if UpCmd.finished {
		return nil
	}
	defer func() { UpCmd.finished = true }()
	// do waiting operations

	if app.Client().PauseImmichBackgroundJobs {
		// Step 1: Wait for sidecar job to complete after uploads
		UpCmd.app.Jnl().Log().Info("Uploads complete, waiting for sidecar job queue to clear...")
		err := UpCmd.waitForSidecarQueueToClear(ctx, app)
		if err != nil {
			UpCmd.app.Jnl().Log().Error("Error waiting for sidecar job queue to clear", "err", err.Error())
			return err
		}
		UpCmd.app.Jnl().Log().Info("Sidecar job queue is clear")

		// Step 2: Resume metadataExtraction job only
		UpCmd.app.Jnl().Log().Info("Resuming metadataExtraction job only...")
		_, err = app.Client().AdminImmich.SendJobCommand(ctx, "metadataExtraction", "resume", true)
		if err != nil {
			UpCmd.app.Jnl().Log().Error("Error resuming metadataExtraction job", "err", err.Error())
			return err
		}
		UpCmd.app.Jnl().Log().Info("metadataExtraction job resumed")

		// Step 3: Wait for metadataExtraction job queue to clear
		UpCmd.app.Jnl().Log().Info("Waiting for metadataExtraction job queue to clear...")
		err = UpCmd.waitForMetadataExtractionQueueToClear(ctx, app)
		if err != nil {
			UpCmd.app.Jnl().Log().Error("Error waiting for metadataExtraction job queue to clear", "err", err.Error())
			return err
		}
		UpCmd.app.Jnl().Log().Info("MetadataExtraction job queue is clear")

		// Step 4: Pause metadataExtraction job again
		UpCmd.app.Jnl().Log().Info("Pausing metadataExtraction job again...")
		_, err = app.Client().AdminImmich.SendJobCommand(ctx, "metadataExtraction", "pause", true)
		if err != nil {
			UpCmd.app.Jnl().Log().Error("Error pausing metadataExtraction job", "err", err.Error())
			return err
		}
		UpCmd.app.Jnl().Log().Info("metadataExtraction job paused")

		// Step 5: Run the delayed album and tag operations (before metadata updates)
		UpCmd.app.Jnl().Log().Info("Running delayed album and tag operations...")
		err = UpCmd.executeDelayedAlbumAndTagOperations(ctx)
		if err != nil {
			UpCmd.app.Jnl().Log().Error("Error executing delayed album and tag operations", "err", err.Error())
			return err
		}
		UpCmd.app.Jnl().Log().Info("Delayed album and tag operations complete")

		// Clear the delayed albums and tags maps since they've been processed
		UpCmd.delayedAlbums = make(map[string][]assets.Album)
		UpCmd.delayedTags = make(map[string][]assets.Tag)

		// Step 6: Run the delayed metadata operations
		UpCmd.app.Jnl().Log().Info("Running delayed metadata operations...")
		err = UpCmd.executeDelayedMetadataOperations(ctx)
		if err != nil {
			UpCmd.app.Jnl().Log().Error("Error executing delayed metadata operations", "err", err.Error())
			return err
		}
		UpCmd.app.Jnl().Log().Info("Delayed metadata operations complete")

		// Step 7: Wait for sidecar job to clear after metadata updates
		UpCmd.app.Jnl().Log().Info("Waiting for sidecar job queue to clear after metadata updates...")
		err = UpCmd.waitForSidecarQueueToClear(ctx, app)
		if err != nil {
			UpCmd.app.Jnl().Log().Error("Error waiting for sidecar job queue to clear", "err", err.Error())
			return err
		}
		UpCmd.app.Jnl().Log().Info("Sidecar job queue is clear")

		// Step 7: Resume metadataExtraction job one final time
		UpCmd.app.Jnl().Log().Info("Resuming metadataExtraction job one final time...")
		_, err = app.Client().AdminImmich.SendJobCommand(ctx, "metadataExtraction", "resume", true)
		if err != nil {
			UpCmd.app.Jnl().Log().Error("Error resuming metadataExtraction job", "err", err.Error())
			return err
		}
		UpCmd.app.Jnl().Log().Info("metadataExtraction job resumed")

		// Step 8: Wait for metadataExtraction job queue to clear after metadata updates
		UpCmd.app.Jnl().Log().Info("Waiting for metadataExtraction job queue to clear after metadata updates...")
		err = UpCmd.waitForMetadataExtractionQueueToClear(ctx, app)
		if err != nil {
			UpCmd.app.Jnl().Log().Error("Error waiting for metadataExtraction job queue to clear", "err", err.Error())
			return err
		}
		UpCmd.app.Jnl().Log().Info("MetadataExtraction job queue is clear")

		// Step 9: Resume all remaining jobs
		UpCmd.app.Jnl().Log().Info("Resuming all remaining jobs...")
	}

	// Close the caches after we've finished using them
	UpCmd.albumsCache.Close()
	UpCmd.tagsCache.Close()

	// Resume immich background jobs if requested
	err := UpCmd.resumeJobs(ctx, app)
	if err != nil {
		return err
	}
	// Log the journal report
	report := app.Jnl().Report()

	if len(report) > 0 {
		lines := strings.Split(report, "\n")
		for _, s := range lines {
			app.Jnl().Log().Info(s)
		}
	}

	return nil
}

func (upCmd *UpCmd) run(ctx context.Context, adapter adapters.Reader, app *app.Application, fsys []fs.FS) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// Stop immich background jobs if requested
	// will be resumed with a call to finishing()
	if app.Client().PauseImmichBackgroundJobs {
		err := upCmd.pauseJobs(ctx, app)
		if err != nil {
			return fmt.Errorf("can't pause immich background jobs: pass an administrator key with the flag --admin-api-key or disable the jobs pausing with the flag --pause-immich-jobs=FALSE\n%w", err)
		}
	}
	defer func() { _ = upCmd.finishing(ctx, app) }()
	defer func() {
		fmt.Println(app.Jnl().Report())
	}()
	upCmd.albumsCache = cache.NewCollectionCache(50, func(album assets.Album, ids []string) (assets.Album, error) {
		return upCmd.saveAlbum(ctx, album, ids)
	})
	upCmd.tagsCache = cache.NewCollectionCache(50, func(tag assets.Tag, ids []string) (assets.Tag, error) {
		return upCmd.saveTags(ctx, tag, ids)
	})

	upCmd.adapter = adapter

	runner := upCmd.runUI
	upCmd.assetIndex = newAssetIndex()

	if upCmd.NoUI {
		runner = upCmd.runNoUI
	}
	_, err := tcell.NewScreen()
	if err != nil {
		upCmd.app.Log().Warn("can't initialize the screen for the UI mode. Falling back to no-gui mode", "err", err)
		fmt.Println("can't initialize the screen for the UI mode. Falling back to no-gui mode")
		runner = upCmd.runNoUI
	}
	err = runner(ctx, app)

	err = errors.Join(err, fshelper.CloseFSs(fsys))

	return err
}

func (upCmd *UpCmd) getImmichAlbums(ctx context.Context) error {
	// Get the album list from the server, but without assets.
	serverAlbums, err := upCmd.app.Client().Immich.GetAllAlbums(ctx)
	if err != nil {
		return fmt.Errorf("can't get the album list from the server: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-upCmd.immichAssetsReady:
		// Wait for the server's assets to be ready.
		for _, a := range serverAlbums {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Get the album info from the server, with assets.
				r, err := upCmd.app.Client().Immich.GetAlbumInfo(ctx, a.ID, false)
				if err != nil {
					upCmd.app.Log().Error("can't get the album info from the server", "album", a.AlbumName, "err", err)
					continue
				}
				ids := make([]string, 0, len(r.Assets))
				for _, aa := range r.Assets {
					ids = append(ids, aa.ID)
				}

				album := assets.NewAlbum(a.ID, a.AlbumName, a.Description)
				upCmd.albumsCache.NewCollection(a.AlbumName, album, ids)
				upCmd.app.Log().Info("got album from the server", "album", a.AlbumName, "assets", len(r.Assets))
				upCmd.app.Log().Debug("got album from the server", "album", a.AlbumName, "assets", ids)
				// assign the album to the assets
				for _, id := range ids {
					a := upCmd.assetIndex.getByID(id)
					if a == nil {
						upCmd.app.Log().Debug("processing the immich albums: asset not found in index", "id", id)
						continue
					}
					a.Albums = append(a.Albums, album)
				}
			}
		}
	}
	return nil
}

func (upCmd *UpCmd) getImmichAssets(ctx context.Context, updateFn progressUpdate) error {
	defer close(upCmd.immichAssetsReady)
	statistics, err := upCmd.app.Client().Immich.GetAssetStatistics(ctx)
	if err != nil {
		return err
	}
	totalOnImmich := statistics.Total
	received := 0

	err = upCmd.app.Client().Immich.GetAllAssetsWithFilter(ctx, nil, func(a *immich.Asset) error {
		if updateFn != nil {
			defer func() {
				updateFn(received, totalOnImmich)
			}()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			received++
			if a.OwnerID != upCmd.app.Client().User.ID {
				upCmd.app.Log().Debug("Skipping asset with different owner", "assetOwnerID", a.OwnerID, "clientUserID", upCmd.app.Client().User.ID, "ID", a.ID, "FileName", a.OriginalFileName, "Capture date", a.ExifInfo.DateTimeOriginal, "CheckSum", a.Checksum, "FileSize", a.ExifInfo.FileSizeInByte, "DeviceAssetID", a.DeviceAssetID, "OwnerID", a.OwnerID, "IsTrashed", a.IsTrashed, "IsArchived", a.IsArchived)
				return nil
			}
			if a.LibraryID != "" {
				upCmd.app.Log().Debug("Skipping asset with external library", "assetLibraryID", a.LibraryID, "ID", a.ID, "FileName", a.OriginalFileName, "Capture date", a.ExifInfo.DateTimeOriginal, "CheckSum", a.Checksum, "FileSize", a.ExifInfo.FileSizeInByte, "DeviceAssetID", a.DeviceAssetID, "OwnerID", a.OwnerID, "IsTrashed", a.IsTrashed, "IsArchived", a.IsArchived)
				return nil
			}
			upCmd.assetIndex.addImmichAsset(a)
			upCmd.app.Log().Debug("Immich asset:", "ID", a.ID, "FileName", a.OriginalFileName, "Capture date", a.ExifInfo.DateTimeOriginal, "CheckSum", a.Checksum, "FileSize", a.ExifInfo.FileSizeInByte, "DeviceAssetID", a.DeviceAssetID, "OwnerID", a.OwnerID, "IsTrashed", a.IsTrashed, "IsArchived", a.IsArchived)
			return nil
		}
	})
	if err != nil {
		return err
	}
	if updateFn != nil {
		updateFn(totalOnImmich, totalOnImmich)
	}
	upCmd.app.Log().Info(fmt.Sprintf("Assets on the server: %d", upCmd.assetIndex.len()))
	return nil
}

func (upCmd *UpCmd) uploadLoop(ctx context.Context, groupChan chan *assets.Group) error {
	var err error
	errorCount := 0
assetLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case g, ok := <-groupChan:
			if !ok {
				break assetLoop
			}
			err = upCmd.handleGroup(ctx, g)
			if err != nil {
				upCmd.app.Log().Error(err.Error())

				switch {
				case upCmd.app.Client().OnServerErrors == cliflags.OnServerErrorsNeverStop:
					// nop
				case upCmd.app.Client().OnServerErrors == cliflags.OnServerErrorsStop:
					return err
				default:
					errorCount++
					if errorCount >= int(upCmd.app.Client().OnServerErrors) {
						err := errors.New("too many errors, aborting")
						upCmd.app.Log().Error(err.Error())
						return err
					}
				}
			}
		}
	}

	if len(upCmd.deleteServerList) > 0 {
		ids := []string{}
		for _, da := range upCmd.deleteServerList {
			ids = append(ids, da.ID)
		}
		err := upCmd.DeleteServerAssets(ctx, ids)
		if err != nil {
			return fmt.Errorf("can't delete server's assets: %w", err)
		}
	}

	return err
}

func (upCmd *UpCmd) handleGroup(ctx context.Context, g *assets.Group) error {
	var errGroup error

	g = filters.ApplyFilters(g, upCmd.Filters...)

	// discard rejected assets
	for _, a := range g.Removed {
		a.Asset.Close()
		upCmd.app.Jnl().Record(ctx, fileevent.DiscoveredDiscarded, a.Asset.File, "reason", a.Reason)
	}

	// Upload assets from the group
	for _, a := range g.Assets {
		err := upCmd.handleAsset(ctx, a)
		errGroup = errors.Join(err)
	}

	// Manage groups
	// after the filtering and the upload, we can stack the assets

	if len(g.Assets) > 1 && g.Grouping != assets.GroupByNone {
		client := upCmd.app.Client().Immich.(immich.ImmichStackInterface)
		ids := []string{g.Assets[g.CoverIndex].ID}
		for i, a := range g.Assets {
			upCmd.app.Jnl().Record(ctx, fileevent.Stacked, g.Assets[i].File)
			if i != g.CoverIndex && a.ID != "" {
				ids = append(ids, a.ID)
			}
		}
		if len(ids) > 1 {
			_, err := client.CreateStack(ctx, ids)
			if err != nil {
				upCmd.app.Jnl().Log().Error("Can't create stack", "error", err)
			}
		}
	}

	if errGroup != nil {
		return errGroup
	}

	switch g.Grouping {
	case assets.GroupByNone:
	}

	return nil
}

func (upCmd *UpCmd) handleAsset(ctx context.Context, a *assets.Asset) error {
	defer func() {
		a.Close() // Close and clean resources linked to the local asset
	}()

	// var status stri g
	advice, err := upCmd.assetIndex.ShouldUpload(a, upCmd)
	if err != nil {
		return err
	}

	switch advice.Advice {
	case NotOnServer: // Upload and manage albums
		serverStatus, err := upCmd.uploadAsset(ctx, a)
		if err != nil {
			return err
		}

		upCmd.processUploadedAsset(ctx, a, serverStatus)
		return nil

	case SmallerOnServer: // Upload, manage albums and delete the server's asset

		// Remember existing asset's albums, if any
		a.Albums = append(a.Albums, advice.ServerAsset.Albums...)

		// Upload the superior asset
		serverStatus, err := upCmd.replaceAsset(ctx, advice.ServerAsset.ID, a, advice.ServerAsset)
		if err != nil {
			return err
		}

		upCmd.processUploadedAsset(ctx, a, serverStatus)
		return nil

	case AlreadyProcessed: // SHA1 already processed
		upCmd.app.Jnl().Record(ctx, fileevent.AnalysisLocalDuplicate, a.File, "reason", "the file is already present in the input", "original name", advice.ServerAsset.OriginalFileName)
		return nil

	case SameOnServer:
		a.ID = advice.ServerAsset.ID
		a.Albums = append(a.Albums, advice.ServerAsset.Albums...)
		upCmd.app.Jnl().Record(ctx, fileevent.UploadServerDuplicate, a.File, "reason", advice.Message)
		upCmd.manageAssetAlbums(ctx, a.File, a.ID, a.Albums)

	case BetterOnServer: // and manage albums
		a.ID = advice.ServerAsset.ID
		upCmd.app.Jnl().Record(ctx, fileevent.UploadServerBetter, a.File, "reason", advice.Message)
		// Instead of executing immediately, collect the operations for later execution
		if len(a.Albums) > 0 {
			upCmd.delayedAlbums[a.ID] = append(upCmd.delayedAlbums[a.ID], a.Albums...)
		}
		if len(a.Tags) > 0 {
			upCmd.delayedTags[a.ID] = append(upCmd.delayedTags[a.ID], a.Tags...)
		}

	case ForceUpload:
		var serverStatus string
		var err error

		if advice.ServerAsset != nil {
			// Remember existing asset's albums, if any
			a.Albums = append(a.Albums, advice.ServerAsset.Albums...)

			// Upload the superior asset
			serverStatus, err = upCmd.replaceAsset(ctx, advice.ServerAsset.ID, a, advice.ServerAsset)
		} else {
			serverStatus, err = upCmd.uploadAsset(ctx, a)
		}
		if err != nil {
			return err
		}

		upCmd.processUploadedAsset(ctx, a, serverStatus)
		return nil
	}

	return nil
}

// uploadAsset uploads the asset to the server.
// set the server's asset ID to the asset.
// return the duplicate condition and error.
func (upCmd *UpCmd) uploadAsset(ctx context.Context, a *assets.Asset) (string, error) {
	defer upCmd.app.Log().Debug("", "file", a)
	ar, err := upCmd.app.Client().Immich.AssetUpload(ctx, a)
	if err != nil {
		upCmd.app.Jnl().Record(ctx, fileevent.UploadServerError, a.File, "error", err.Error())
		return "", err // Must signal the error to the caller
	}
	if ar.Status == immich.UploadDuplicate {
		originalName := "unknown"
		original := upCmd.assetIndex.getByID(ar.ID)
		if original != nil {
			originalName = original.OriginalFileName
		}
		if a.ID == "" {
			upCmd.app.Jnl().Record(ctx, fileevent.AnalysisLocalDuplicate, a.File, "reason", "the file is already present in the input", "original name", originalName)
		} else {
			upCmd.app.Jnl().Record(ctx, fileevent.UploadServerDuplicate, a.File, "reason", "the server already has this file", "original name", originalName)
		}
	} else {
		upCmd.app.Jnl().Record(ctx, fileevent.Uploaded, a.File)
	}
	a.ID = ar.ID

	// // DEBGUG
	//  if theID, ok := upCmd.assetIndex.byI

	if a.FromApplication != nil && ar.Status != immich.StatusDuplicate {
		// Instead of applying metadata immediately, collect it for later application
		// This prevents racing conditions with the metadataExtraction job
		upCmd.delayedMetadata[a.ID] = a.FromApplication
	}
	upCmd.assetIndex.addLocalAsset(a)
	return ar.Status, nil
}

func (upCmd *UpCmd) replaceAsset(ctx context.Context, ID string, a, old *assets.Asset) (string, error) {
	defer upCmd.app.Log().Debug("replaced by", "ID", ID, "file", a)
	ar, err := upCmd.app.Client().Immich.ReplaceAsset(ctx, ID, a)
	if err != nil {
		upCmd.app.Jnl().Record(ctx, fileevent.UploadServerError, a.File, "error", err.Error())
		return "", err // Must signal the error to the caller
	}
	if ar.Status == immich.UploadDuplicate {
		originalName := "unknown"
		original := upCmd.assetIndex.getByID(ar.ID)
		if original != nil {
			originalName = original.OriginalFileName
		}
		if a.ID == "" {
			upCmd.app.Jnl().Record(ctx, fileevent.AnalysisLocalDuplicate, a.File, "reason", "the file is already present in the input", "original name", originalName)
		} else {
			upCmd.app.Jnl().Record(ctx, fileevent.UploadServerDuplicate, a.File, "reason", "the server already has this file", "original name", originalName)
		}
	} else {
		a.ID = ID
		upCmd.app.Jnl().Record(ctx, fileevent.UploadUpgraded, a.File)
		upCmd.assetIndex.replaceAsset(a, old)

		// Handle metadata for replaced assets the same way as uploaded assets
		if a.FromApplication != nil {
			// Instead of applying metadata immediately, collect it for later application
			// This prevents racing conditions with the metadataExtraction job
			upCmd.delayedMetadata[a.ID] = a.FromApplication
		}
	}
	return ar.Status, nil
}

// manageAssetAlbums add the assets to the albums listed.
// If an album does not exist, it is created.
// If the album already has the asset, it is not added.
// Errors are logged.
func (upCmd *UpCmd) manageAssetAlbums(ctx context.Context, f fshelper.FSAndName, ID string, albums []assets.Album) {
	if len(albums) == 0 {
		return
	}

	for _, album := range albums {
		al := assets.NewAlbum("", album.Title, album.Description)
		if upCmd.albumsCache.AddIDToCollection(al.Title, album, ID) {
			upCmd.app.Jnl().Record(ctx, fileevent.UploadAddToAlbum, f, "album", al.Title)
		}
	}
}

func (upCmd *UpCmd) manageAssetTags(ctx context.Context, a *assets.Asset) {
	if len(a.Tags) == 0 {
		return
	}

	tags := make([]string, len(a.Tags))
	for i := range a.Tags {
		tags[i] = a.Tags[i].Name
	}
	for _, t := range a.Tags {
		if upCmd.tagsCache.AddIDToCollection(t.Name, t, a.ID) {
			upCmd.app.Jnl().Record(ctx, fileevent.Tagged, a.File, "tag", t.Value)
		}
	}
}

func (upCmd *UpCmd) DeleteServerAssets(ctx context.Context, ids []string) error {
	upCmd.app.Log().Message("%d server assets to delete.", len(ids))
	return upCmd.app.Client().Immich.DeleteAssets(ctx, ids, false)
}

func (upCmd *UpCmd) processUploadedAsset(ctx context.Context, a *assets.Asset, serverStatus string) {
	if serverStatus != immich.StatusDuplicate {
		// TODO: current version of Immich doesn't allow to add same tag to an asset already tagged.
		//       there is no mean to go the list of tagged assets for a given tag.

		// Instead of executing immediately, collect the operations for later execution
		if len(a.Albums) > 0 {
			upCmd.delayedAlbums[a.ID] = append(upCmd.delayedAlbums[a.ID], a.Albums...)
		}
		if len(a.Tags) > 0 {
			upCmd.delayedTags[a.ID] = append(upCmd.delayedTags[a.ID], a.Tags...)
		}
	}
}

// executeDelayedAlbumAndTagOperations executes the delayed album and tag operations immediately
func (upCmd *UpCmd) executeDelayedAlbumAndTagOperations(ctx context.Context) error {
	// Group assets by album to avoid creating the same album multiple times
	albumToAssets := make(map[string][]string)    // Map of album title to asset IDs
	albumObjects := make(map[string]assets.Album) // Map of album title to album object

	// Collect all albums and their associated assets
	for assetID, albums := range upCmd.delayedAlbums {
		for _, album := range albums {
			albumToAssets[album.Title] = append(albumToAssets[album.Title], assetID)
			// Store the album object for later use
			if _, exists := albumObjects[album.Title]; !exists {
				albumObjects[album.Title] = album
			}
		}
	}

	// Execute delayed album operations
	for albumTitle, assetIDs := range albumToAssets {
		album := albumObjects[albumTitle]
		al := assets.NewAlbum("", album.Title, album.Description)
		// Create album only once for all assets with this album
		if al.ID == "" {
			// Create new album with all assets
			r, err := upCmd.app.Client().Immich.CreateAlbum(ctx, al.Title, al.Description, assetIDs)
			if err != nil {
				upCmd.app.Jnl().Log().Error("failed to create album", "err", err, "album", al.Title)
				return err
			}
			upCmd.app.Jnl().Log().Info("created album", "album", al.Title, "assets", len(assetIDs))
			al.ID = r.ID
		} else {
			// Add all assets to existing album
			_, err := upCmd.app.Client().Immich.AddAssetToAlbum(ctx, al.ID, assetIDs)
			if err != nil {
				upCmd.app.Jnl().Log().Error("failed to add assets to album", "err", err, "album", al.Title, "assets", len(assetIDs))
				return err
			}
			upCmd.app.Jnl().Log().Info("updated album", "album", al.Title, "assets", len(assetIDs))
		}

		// Record album events for each asset
		for _, assetID := range assetIDs {
			upCmd.app.Jnl().Record(ctx, fileevent.UploadAddToAlbum, fshelper.FSName(nil, assetID), "album", al.Title)
		}
	}

	// Group assets by tag to avoid creating the same tag multiple times
	tagToAssets := make(map[string][]string)  // Map of tag value to asset IDs
	tagObjects := make(map[string]assets.Tag) // Map of tag value to tag object

	// Collect all tags and their associated assets
	for assetID, tags := range upCmd.delayedTags {
		for _, tag := range tags {
			tagToAssets[tag.Value] = append(tagToAssets[tag.Value], assetID)
			// Store the tag object for later use
			if _, exists := tagObjects[tag.Value]; !exists {
				tagObjects[tag.Value] = tag
			}
		}
	}

	// Execute delayed tag operations
	for tagValue, assetIDs := range tagToAssets {
		tag := tagObjects[tagValue]
		// Create tag only once for all assets with this tag
		if tag.ID == "" {
			// Create new tag
			r, err := upCmd.app.Client().Immich.UpsertTags(ctx, []string{tag.Value})
			if err != nil {
				upCmd.app.Jnl().Log().Error("failed to create tag", "err", err, "tag", tag.Name)
				return err
			}
			upCmd.app.Jnl().Log().Info("created tag", "tag", tag.Value)
			tag.ID = r[0].ID
			// Update the tag object in our map
			tagObjects[tag.Value] = tag
		}

		// Add all assets to the tag
		_, err := upCmd.app.Client().Immich.TagAssets(ctx, tag.ID, assetIDs)
		if err != nil {
			upCmd.app.Jnl().Log().Error("failed to add assets to tag", "err", err, "tag", tag.Value, "assets", len(assetIDs))
			return err
		}
		upCmd.app.Jnl().Log().Info("updated tag", "tag", tag.Value, "assets", len(assetIDs))

		// Record tagging events for each asset
		for _, assetID := range assetIDs {
			upCmd.app.Jnl().Record(ctx, fileevent.Tagged, fshelper.FSName(nil, assetID), "tag", tag.Value)
		}
	}

	return nil
}

// executeDelayedMetadataOperations executes the delayed metadata operations
func (upCmd *UpCmd) executeDelayedMetadataOperations(ctx context.Context) error {
	// Execute delayed metadata operations
	for assetID, metadata := range upCmd.delayedMetadata {
		// Apply the metadata to the asset
		_, err := upCmd.app.Client().Immich.UpdateAsset(ctx, assetID, immich.UpdAssetField{
			Description:      metadata.Description,
			Latitude:         metadata.Latitude,
			Longitude:        metadata.Longitude,
			Rating:           int(metadata.Rating),
			DateTimeOriginal: metadata.DateTaken,
		})
		if err != nil {
			upCmd.app.Jnl().Record(ctx, fileevent.UploadServerError, fshelper.FSName(nil, assetID), "error", err.Error())
			return err
		}
		upCmd.app.Jnl().Record(ctx, fileevent.Metadata, fshelper.FSName(nil, assetID), "operation", "metadata update")
	}

	return nil
}

/*
func (app *UpCmd) DeleteLocalAssets() error {
	app.RootImmichFlags.Message(fmt.Sprintf("%d local assets to delete.", len(app.deleteLocalList)))

	for _, a := range app.deleteLocalList {
		if !app.DryRun {
			app.Log.Info(fmt.Sprintf("delete file %q", a.Title))
			err := a.Remove()
			if err != nil {
				return err
			}
		} else {
			app.Log.Info(fmt.Sprintf("file %q not deleted, dry run mode.", a.Title))
		}
	}
	return nil
}
*/
