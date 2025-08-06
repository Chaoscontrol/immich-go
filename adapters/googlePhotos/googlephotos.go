package gp

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/simulot/immich-go/internal/assets"
	"github.com/simulot/immich-go/internal/fileevent"
	"github.com/simulot/immich-go/internal/filenames"
	"github.com/simulot/immich-go/internal/filetypes"
	"github.com/simulot/immich-go/internal/filters"
	"github.com/simulot/immich-go/internal/fshelper"
	"github.com/simulot/immich-go/internal/gen"
	"github.com/simulot/immich-go/internal/groups"
	"github.com/simulot/immich-go/internal/groups/burst"
	"github.com/simulot/immich-go/internal/groups/epsonfastfoto"
	"github.com/simulot/immich-go/internal/groups/series"
)

type Takeout struct {
	fsyss        []fs.FS
	catalogs     map[string]directoryCatalog                // file catalogs by directory in the set of the all takeout parts
	albums       map[string]assets.Album                    // track album names by folder
	sharedAlbums map[string][]string                        // map of asset paths to album names for shared albums
	fileTracker  *gen.SyncMap[fileKeyTracker, trackingInfo] // map[fileKeyTracker]trackingInfo // key is base name + file size,  value is list of file paths
	// debugLinkedFiles []linkedFiles
	log      *fileevent.Recorder
	flags    *ImportFlags // command-line flags
	groupers []groups.Grouper
}

type fileKeyTracker struct {
	baseName string
	size     int64
}

type trackingInfo struct {
	paths    []string
	count    int
	metadata *assets.Metadata
	status   fileevent.Code
}

func trackerKeySortFunc(a, b fileKeyTracker) int {
	cmp := strings.Compare(a.baseName, b.baseName)
	if cmp != 0 {
		return cmp
	}
	return int(a.size) - int(b.size)
}

// directoryCatalog captures all files in a given directory
type directoryCatalog struct {
	jsons          map[string]*assets.Metadata // metadata in the catalog by base name
	unMatchedFiles map[string]*assetFile       // files to be matched map  by base name
	matchedFiles   map[string]*assets.Asset    // files matched by base name
}

// assetFile keep information collected during pass one
type assetFile struct {
	fsys   fs.FS            // Remember in which part of the archive the file is located
	base   string           // Remember the original file name
	length int              // file length in bytes
	date   time.Time        // file modification date
	md     *assets.Metadata // will point to the associated metadata
}

// sharedAlbumInfo holds information about shared albums extracted from photo descriptions
type sharedAlbumInfo struct {
	assetPath  string   // Path to the asset file
	albumNames []string // Names of albums the asset should be added to
}

// Implement slog.LogValuer for assetFile
func (af assetFile) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("base", af.base),
		slog.Int("length", af.length),
		slog.Time("date", af.date),
	)
}

func NewTakeout(ctx context.Context, l *fileevent.Recorder, flags *ImportFlags, fsyss ...fs.FS) (*Takeout, error) {
	to := Takeout{
		fsyss:        fsyss,
		catalogs:     map[string]directoryCatalog{},
		albums:       map[string]assets.Album{},
		sharedAlbums: map[string][]string{},
		fileTracker:  gen.NewSyncMap[fileKeyTracker, trackingInfo](), // map[fileKeyTracker]trackingInfo{},
		log:          l,
		flags:        flags,
	}
	if flags.InfoCollector == nil {
		flags.InfoCollector = filenames.NewInfoCollector(flags.TZ, flags.SupportedMedia)
	}
	// if flags.ExifToolFlags.UseExifTool {
	// 	err := exif.NewExifTool(&flags.ExifToolFlags)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }
	if flags.SessionTag {
		flags.session = fmt.Sprintf("{immich-go}/%s", time.Now().Format("2006-01-02 15:04:05"))
	}

	if flags.ManageEpsonFastFoto {
		g := epsonfastfoto.Group{}
		to.groupers = append(to.groupers, g.Group)
	}
	if flags.ManageBurst != filters.BurstNothing {
		to.groupers = append(to.groupers, burst.Group)
	}
	to.groupers = append(to.groupers, series.Group)

	return &to, nil
}

// Prepare scans all files in all walker to build the file catalog of the archive
// metadata files content is read and kept
// return a channel of asset groups after the puzzle is solved

func (to *Takeout) Browse(ctx context.Context) chan *assets.Group {
	ctx, cancel := context.WithCancelCause(ctx)
	gOut := make(chan *assets.Group)
	go func() {
		defer close(gOut)

		for _, w := range to.fsyss {
			err := to.passOneFsWalk(ctx, w)
			if err != nil {
				cancel(err)
				return
			}
		}
		err := to.solvePuzzle(ctx)
		if err != nil {
			cancel(err)
			return
		}
		err = to.passTwo(ctx, gOut)
		cancel(err)
	}()
	return gOut
}

func (to *Takeout) passOneFsWalk(ctx context.Context, w fs.FS) error {
	err := fs.WalkDir(w, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:

			if d.IsDir() {
				return nil
			}
			dir, base := path.Split(name)
			dir = strings.TrimSuffix(dir, "/")
			ext := strings.ToLower(path.Ext(base))

			// Exclude files to be ignored before processing
			if to.flags.BannedFiles.Match(name) {
				to.log.Record(ctx, fileevent.DiscoveredDiscarded, fshelper.FSName(w, name), "reason", "banned file")
				return nil
			}

			if to.flags.SupportedMedia.IsUseLess(name) {
				to.log.Record(ctx, fileevent.DiscoveredUseless, fshelper.FSName(w, name))
				return nil
			}

			if !to.flags.InclusionFlags.IncludedExtensions.Include(ext) {
				to.log.Record(ctx, fileevent.DiscoveredDiscarded, fshelper.FSName(w, name), "reason", "file extension not selected")
				return nil
			}
			if to.flags.InclusionFlags.ExcludedExtensions.Exclude(ext) {
				to.log.Record(ctx, fileevent.DiscoveredDiscarded, fshelper.FSName(w, name), "reason", "file extension not allowed")
				return nil
			}

			dirCatalog, ok := to.catalogs[dir]
			if !ok {
				dirCatalog.jsons = map[string]*assets.Metadata{}
				dirCatalog.unMatchedFiles = map[string]*assetFile{}
				dirCatalog.matchedFiles = map[string]*assets.Asset{}
			}
			finfo, err := d.Info()
			if err != nil {
				to.log.Record(ctx, fileevent.Error, fshelper.FSName(w, name), "error", err.Error())
				return err
			}
			switch ext {
			case ".json":
				var md *assets.Metadata
				b, err := fs.ReadFile(w, name)
				if err != nil {
					to.log.Record(ctx, fileevent.Error, fshelper.FSName(w, name), "error", err.Error())
					return nil
				}
				if bytes.Contains(b, []byte("immich-go version:")) {
					md, err = assets.UnMarshalMetadata(b)
					if err != nil {
						to.log.Record(ctx, fileevent.DiscoveredUnsupported, fshelper.FSName(w, name), "reason", "unknown JSONfile")
					}
					md.FileName = base
					to.log.Record(ctx, fileevent.DiscoveredSidecar, fshelper.FSName(w, name), "type", "immich-go metadata", "title", md.FileName)
					md.File = fshelper.FSName(w, name)
				} else {
					md, err := fshelper.UnmarshalJSON[GoogleMetaData](b)
					if err == nil {
						switch {
						case md.isAsset():
							// Extract album names from description before creating metadata object
							albumNames, cleanedDescription := []string{}, md.Description
							if to.flags.CreateSharedAlbums {
								albumNames, cleanedDescription = extractAlbumNamesFromDescription(md.Description)
							}

							// Create metadata object with potentially cleaned description
							assetMd := md.AsMetadata(fshelper.FSName(w, name), to.flags.PeopleTag, to.flags) // Keep metadata

							// Update the metadata description if we extracted album names
							if len(albumNames) > 0 {
								assetMd.Description = cleanedDescription
							}

							// Log the original metadata before any modifications
							to.log.Log().Debug("Original metadata", "fileName", assetMd.FileName, "description", assetMd.Description, "dateTaken", assetMd.DateTaken, "latitude", assetMd.Latitude, "longitude", assetMd.Longitude)

							// Check if this asset has album_name in its description for shared albums feature
							if to.flags.CreateSharedAlbums && len(albumNames) > 0 {
								// Log the modified metadata
								to.log.Log().Debug("Modified metadata", "fileName", assetMd.FileName, "description", assetMd.Description, "dateTaken", assetMd.DateTaken, "latitude", assetMd.Latitude, "longitude", assetMd.Longitude)

								// Store the shared album information using the actual asset file path
								// This matches how the asset file name is stored in the second pass
								assetPath := fshelper.FSName(w, path.Join(dir, assetMd.FileName)).Name()
								// Store without extension for matching
								assetPathWithoutExt := fshelper.FSName(w, path.Join(dir, strings.TrimSuffix(assetMd.FileName, path.Ext(assetMd.FileName)))).Name()

								// Store both with and without extension
								to.sharedAlbums[assetPath] = albumNames
								to.sharedAlbums[assetPathWithoutExt] = albumNames

								// Log what we stored
								to.log.Log().Debug("Stored shared album info", "assetPath", assetPath, "assetPathWithoutExt", assetPathWithoutExt, "albumNames", strings.Join(albumNames, ", "))

								// Log that we found a photo with shared album information
								// Check if this photo is in an album folder (unexpected but possible)
								isInAlbumFolder := false
								for albumDir := range to.albums {
									if strings.HasPrefix(dir, albumDir) && dir != albumDir {
										isInAlbumFolder = true
										break
									}
								}

								if isInAlbumFolder {
									to.log.Record(ctx, fileevent.DiscoveredSidecar, fshelper.FSName(w, name),
										"type", "shared album photo in album folder",
										"title", assetMd.FileName,
										"album_names", strings.Join(albumNames, ", "),
										"warning", "photo with album_name description found in album folder")
								} else {
									to.log.Record(ctx, fileevent.DiscoveredSidecar, fshelper.FSName(w, name),
										"type", "shared album photo",
										"title", assetMd.FileName,
										"album_names", strings.Join(albumNames, ", "))
								}
							}

							dirCatalog.jsons[base] = assetMd
							to.log.Log().Debug("Asset JSON", "metadata", assetMd)
							to.log.Record(ctx, fileevent.DiscoveredSidecar, fshelper.FSName(w, name), "type", "asset metadata", "title", assetMd.FileName, "date", assetMd.DateTaken)
						case md.isAlbum():
							to.log.Log().Debug("Album JSON", "metadata", md)
							if !to.flags.KeepUntitled && md.Title == "" {
								to.log.Record(ctx, fileevent.DiscoveredUnsupported, fshelper.FSName(w, name), "reason", "discard untitled album")
								return nil
							}
							a := to.albums[dir]
							a.Title = md.Title
							if a.Title == "" {
								a.Title = filepath.Base(dir)
							}
							if e := md.Enrichments; e != nil {
								a.Description = e.Text
								a.Latitude = e.Latitude
								a.Longitude = e.Longitude
							}
							to.albums[dir] = a
							to.log.Record(ctx, fileevent.DiscoveredSidecar, fshelper.FSName(w, name), "type", "album metadata", "title", md.Title)
						default:
							to.log.Record(ctx, fileevent.DiscoveredUnsupported, fshelper.FSName(w, name), "reason", "unknown JSONfile")
							return nil
						}
					} else {
						to.log.Record(ctx, fileevent.DiscoveredUnsupported, fshelper.FSName(w, name), "reason", "unknown JSONfile")
						return nil
					}
				}
			default:

				t := to.flags.SupportedMedia.TypeFromExt(ext)
				switch t {
				case filetypes.TypeUseless:
					to.log.Record(ctx, fileevent.DiscoveredUseless, fshelper.FSName(w, name), "reason", "useless file")
					return nil
				case filetypes.TypeUnknown:
					to.log.Record(ctx, fileevent.DiscoveredUnsupported, fshelper.FSName(w, name), "reason", "unsupported file type")
					return nil
				case filetypes.TypeVideo:
					to.log.Record(ctx, fileevent.DiscoveredVideo, fshelper.FSName(w, name))
					if strings.Contains(name, "Failed Videos") {
						to.log.Record(ctx, fileevent.DiscoveredDiscarded, fshelper.FSName(w, name), "reason", "can't upload failed videos")
						return nil
					}
				case filetypes.TypeImage:
					to.log.Record(ctx, fileevent.DiscoveredImage, fshelper.FSName(w, name))
				}

				key := fileKeyTracker{
					baseName: base,
					size:     finfo.Size(),
				}

				// TODO: remove debugging code
				tracking, _ := to.fileTracker.Load(key) // tracking := to.fileTracker[key]
				tracking.paths = append(tracking.paths, dir)
				tracking.count++
				to.fileTracker.Store(key, tracking) // to.fileTracker[key] = tracking

				if a, ok := dirCatalog.unMatchedFiles[base]; ok {
					to.logMessage(ctx, fileevent.AnalysisLocalDuplicate, a, "duplicated in the directory")
					return nil
				}

				dirCatalog.unMatchedFiles[base] = &assetFile{
					fsys:   w,
					base:   base,
					length: int(finfo.Size()),
					date:   finfo.ModTime(),
				}
			}
			to.catalogs[dir] = dirCatalog
			return nil
		}
	})
	return err
}

// solvePuzzle prepares metadata with information collected during pass one for each accepted files
//
// JSON files give important information about the relative photos / movies:
//   - The original name (useful when it as been truncated)
//   - The date of capture (useful when the files doesn't have this date)
//   - The GPS coordinates (will be useful in a future release)
//
// Each JSON is checked. JSON is duplicated in albums folder.
// --Associated files with the JSON can be found in the JSON's folder, or in the Year photos.--
// ++JSON and files are located in the same folder
///
// Once associated and sent to the main program, files are tagged for not been associated with an other one JSON.
// Association is done with the help of a set of matcher functions. Each one implement a rule
//
// 1 JSON can be associated with 1+ files that have a part of their name in common.
// -   the file is named after the JSON name
// -   the file name can be 1 UTF-16 char shorter (🤯) than the JSON name
// -   the file name is longer than 46 UTF-16 chars (🤯) is truncated. But the truncation can creates duplicates, then a number is added.
// -   if there are several files with same original name, the first instance kept as it is, the next has a sequence number.
//       File is renamed as IMG_1234(1).JPG and the JSON is renamed as IMG_1234.JPG(1).JSON
// -   of course those rules are likely to collide. They have to be applied from the most common to the least one.
// -   sometimes the file isn't in the same folder than the json... It can be found in Year's photos folder
//
// --The duplicates files (same name, same length in bytes) found in the local source are discarded before been presented to the immich server.
// ++ Duplicates are presented to the next layer to allow the album handling
//
// To solve the puzzle, each directory is checked with all matchers in the order of the most common to the least.

type matcherFn func(jsonName string, fileName string, sm filetypes.SupportedMedia) bool

// matchers is a list of matcherFn from the most likely to be used to the least one
var matchers = []struct {
	name string
	fn   matcherFn
}{
	{name: "matchFastTrack", fn: matchFastTrack},
	{name: "matchNormal", fn: matchNormal},
	{name: "matchForgottenDuplicates", fn: matchForgottenDuplicates},
	{name: "matchEditedName", fn: matchEditedName},
}

func (to *Takeout) solvePuzzle(ctx context.Context) error {
	dirs := gen.MapKeysSorted(to.catalogs)
	for _, dir := range dirs {
		cat := to.catalogs[dir]
		jsons := gen.MapKeysSorted(cat.jsons)
		for _, matcher := range matchers {
			for _, json := range jsons {
				md := cat.jsons[json]
				for f := range cat.unMatchedFiles {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						if matcher.fn(json, f, to.flags.SupportedMedia) {
							i := cat.unMatchedFiles[f]
							i.md = md
							a := to.makeAsset(ctx, dir, i, md)
							cat.matchedFiles[f] = a
							to.log.Record(ctx, fileevent.AnalysisAssociatedMetadata, fshelper.FSName(i.fsys, path.Join(dir, i.base)), "json", json, "matcher", matcher.name)
							delete(cat.unMatchedFiles, f)
						}
					}
				}
			}
		}
		to.catalogs[dir] = cat
		if len(cat.unMatchedFiles) > 0 {
			files := gen.MapKeys(cat.unMatchedFiles)
			sort.Strings(files)
			for _, f := range files {
				i := cat.unMatchedFiles[f]
				to.log.Record(ctx, fileevent.AnalysisMissingAssociatedMetadata, fshelper.FSName(i.fsys, path.Join(dir, i.base)))
				if to.flags.KeepJSONLess {
					a := to.makeAsset(ctx, dir, i, nil)
					cat.matchedFiles[f] = a
					delete(cat.unMatchedFiles, f)
				}
			}
		}
	}
	return nil
}

// Browse return a channel of assets
// Each asset is a group of files that are associated with each other

func (to *Takeout) passTwo(ctx context.Context, gOut chan *assets.Group) error {
	// Log the contents of sharedAlbums map for debugging
	to.log.Log().Debug("Shared albums map contents", "count", len(to.sharedAlbums))
	for assetPath, albumNames := range to.sharedAlbums {
		to.log.Log().Debug("Shared album entry", "assetPath", assetPath, "albumNames", strings.Join(albumNames, ", "))
	}

	dirs := gen.MapKeys(to.catalogs)
	sort.Strings(dirs)
	for _, dir := range dirs {
		if len(to.catalogs[dir].matchedFiles) > 0 {
			err := to.handleDir(ctx, dir, gOut)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// type linkedFiles struct {
// 	dir   string
// 	base  string
// 	video *assetFile
// 	image *assetFile
// }

func (to *Takeout) handleDir(ctx context.Context, dir string, gOut chan *assets.Group) error {
	catalog := to.catalogs[dir]

	dirEntries := make([]*assets.Asset, 0, len(catalog.matchedFiles))

	for name := range catalog.matchedFiles {
		a := catalog.matchedFiles[name]
		key := fileKeyTracker{baseName: name, size: int64(a.FileSize)}
		track, _ := to.fileTracker.Load(key) // track := to.fileTracker[key]
		if track.status == fileevent.Uploaded {
			a.Close()
			to.logMessage(ctx, fileevent.AnalysisLocalDuplicate, a.File, "local duplicate")
			continue
		}

		// Filter on metadata
		if code := to.filterOnMetadata(ctx, a); code != fileevent.Code(0) {
			a.Close()
			continue
		}
		dirEntries = append(dirEntries, a)
	}

	in := make(chan *assets.Asset)
	go func() {
		defer close(in)

		sort.Slice(dirEntries, func(i, j int) bool {
			// Sort by radical first
			radicalI := dirEntries[i].Radical
			radicalJ := dirEntries[j].Radical
			if radicalI != radicalJ {
				return radicalI < radicalJ
			}
			// If radicals are the same, sort by date
			return dirEntries[i].CaptureDate.Before(dirEntries[j].CaptureDate)
		})

		for _, a := range dirEntries {
			if to.flags.CreateAlbums {
				if to.flags.ImportIntoAlbum != "" {
					// Force this album
					a.Albums = []assets.Album{{Title: to.flags.ImportIntoAlbum}}
				} else {
					// check if its duplicates are in some albums, and push them all at once
					key := fileKeyTracker{baseName: filepath.Base(a.File.Name()), size: int64(a.FileSize)}
					track, _ := to.fileTracker.Load(key) // track := to.fileTracker[key]
					for _, p := range track.paths {
						if album, ok := to.albums[p]; ok {
							title := album.Title
							if title == "" {
								if !to.flags.KeepUntitled {
									continue
								}
								title = filepath.Base(p)
							}
							a.Albums = append(a.Albums, assets.Album{
								Title:       title,
								Description: album.Description,
								Latitude:    album.Latitude,
								Longitude:   album.Longitude,
							})
						}
					}
				}

				// Force this album for partners photos
				if to.flags.PartnerSharedAlbum != "" && a.FromPartner {
					a.Albums = append(a.Albums, assets.Album{Title: to.flags.PartnerSharedAlbum})
				}

				// Add shared albums for photos with album_name descriptions
				if to.flags.CreateSharedAlbums {
					// Directly match the asset file name with stored shared album information
					// We store the paths in passOneFsWalk with the same format as a.File.Name()

					to.log.Log().Debug("Looking for shared album info", "file", a.File.Name())

					// Try to find matching shared album information using the full asset file name
					var albumNames []string
					var found bool

					// First try with the full file name
					if names, exists := to.sharedAlbums[a.File.Name()]; exists {
						albumNames = names
						found = true
						to.log.Log().Debug("Found shared album info by full file name", "file", a.File.Name(), "albums", strings.Join(names, ", "))
					} else {
						to.log.Log().Debug("No shared album info found by full file name", "file", a.File.Name())
					}

					// If not found, try with the file name without extension
					if !found {
						fileNameWithoutExt := strings.TrimSuffix(a.File.Name(), path.Ext(a.File.Name()))
						if names, exists := to.sharedAlbums[fileNameWithoutExt]; exists {
							albumNames = names
							found = true
							to.log.Log().Debug("Found shared album info by file name without ext", "file", fileNameWithoutExt, "albums", strings.Join(names, ", "))
						} else {
							to.log.Log().Debug("No shared album info found by file name without ext", "file", fileNameWithoutExt)
						}
					}

					// Check if this asset has shared album information
					if found {
						// Add each album to the asset
						for _, albumName := range albumNames {
							// Check if this album is already in the asset's albums to avoid duplicates
							alreadyExists := false
							for _, existingAlbum := range a.Albums {
								if existingAlbum.Title == albumName {
									alreadyExists = true
									break
								}
							}

							if !alreadyExists {
								a.Albums = append(a.Albums, assets.Album{Title: albumName})
								to.log.Log().Debug("Added shared album to asset", "asset", a.File.Name(), "album", albumName)
							}
						}
					} else {
						to.log.Log().Debug("No shared album info found for asset", "asset", a.File.Name())
					}
				}

				if a.FromApplication != nil {
					a.FromApplication.Albums = a.Albums
				}
			}
			// If the asset has no GPS information, but the album has, use the album's location
			if a.Latitude == 0 && a.Longitude == 0 {
				for _, album := range a.Albums {
					if album.Latitude != 0 || album.Longitude != 0 {
						// when there isn't GPS information on the photo, but the album has a location,  use that location
						a.Latitude = album.Latitude
						a.Longitude = album.Longitude
						break
					}
				}
			}
			if to.flags.SessionTag {
				a.AddTag(to.flags.session)
			}
			if to.flags.Tags != nil {
				for _, tag := range to.flags.Tags {
					a.AddTag(tag)
				}
			}
			if to.flags.TakeoutTag {
				a.AddTag(to.flags.TakeoutName)
			}

			select {
			case in <- a:
			case <-ctx.Done():
				return
			}
		}
	}()

	gs := groups.NewGrouperPipeline(ctx, to.groupers...).PipeGrouper(ctx, in)
	for g := range gs {
		select {
		case gOut <- g:
			for _, a := range g.Assets {
				key := fileKeyTracker{
					baseName: path.Base(a.File.Name()),
					size:     int64(a.FileSize),
				}
				track, _ := to.fileTracker.Load(key) // track := to.fileTracker[key]
				track.status = fileevent.Uploaded
				to.fileTracker.Store(key, track) // to.fileTracker[key] = track
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// makeAsset makes a localAssetFile based on the google metadata
func (to *Takeout) makeAsset(_ context.Context, dir string, f *assetFile, md *assets.Metadata) *assets.Asset {
	file := path.Join(dir, f.base)
	a := &assets.Asset{
		File:             fshelper.FSName(f.fsys, file), // File as named in the archive
		FileSize:         f.length,
		OriginalFileName: f.base,
		FileDate:         f.date,
	}

	// get the original file name from metadata
	if md != nil && md.FileName != "" {
		// Log the metadata we're about to use
		to.log.Log().Debug("Using metadata in makeAsset", "fileName", md.FileName, "description", md.Description, "dateTaken", md.DateTaken, "latitude", md.Latitude, "longitude", md.Longitude)

		title := md.FileName

		// a.OriginalFileName = md.FileName
		// title := md.FileName

		// trim superfluous extensions
		titleExt := path.Ext(title)
		fileExt := path.Ext(file)

		if titleExt != fileExt {
			title = strings.TrimSuffix(title, titleExt)
			titleExt = path.Ext(title)
			if titleExt != fileExt {
				title = strings.TrimSuffix(title, titleExt) + fileExt
			}
		}
		a.FromApplication = a.UseMetadata(md)
		a.OriginalFileName = title

		// Log the asset after applying metadata
		to.log.Log().Debug("Asset after UseMetadata", "fileName", a.OriginalFileName, "description", a.Description, "dateTaken", a.CaptureDate, "latitude", a.Latitude, "longitude", a.Longitude)
	}
	a.SetNameInfo(to.flags.InfoCollector.GetInfo(a.OriginalFileName))

	// Tag unmatched assets when --include-unmatched (-u) is used.
	// Unmatched assets are created with md == nil in solvePuzzle when KeepJSONLess is true.
	if md == nil && to.flags.KeepJSONLess {
		a.AddTag("Unmatched")
	}

	return a
}

// extractAlbumNamesFromDescription extracts album names from a description with format "album_name: Album1, Album2"
// and returns the album names and the cleaned description without the album_name prefix
func extractAlbumNamesFromDescription(description string) ([]string, string) {
	// Check if the description has the album_name prefix
	if !strings.HasPrefix(description, "album_name: ") {
		return nil, description
	}

	// Extract the album names part after "album_name: "
	albumNamesStr := strings.TrimPrefix(description, "album_name: ")

	// Split by comma and trim whitespace from each album name
	albumNames := strings.Split(albumNamesStr, ",")
	var result []string
	for _, name := range albumNames {
		trimmedName := strings.TrimSpace(name)
		if trimmedName != "" {
			result = append(result, trimmedName)
		}
	}

	// Return the original description since we want to preserve it for verification
	return result, description
}

func (to *Takeout) filterOnMetadata(ctx context.Context, a *assets.Asset) fileevent.Code {
	if !to.flags.KeepArchived && a.Archived {
		to.logMessage(ctx, fileevent.DiscoveredDiscarded, a, "discarding archived file")
		a.Close()
		return fileevent.DiscoveredDiscarded
	}
	if !to.flags.KeepPartner && a.FromPartner {
		to.logMessage(ctx, fileevent.DiscoveredDiscarded, a, "discarding partner file")
		a.Close()
		return fileevent.DiscoveredDiscarded
	}
	if !to.flags.KeepSharedAlbum && a.FromSharedAlbum {
		to.logMessage(ctx, fileevent.DiscoveredDiscarded, a, "discarding shared album file")
		a.Close()
		return fileevent.DiscoveredDiscarded
	}
	if !to.flags.KeepTrashed && a.Trashed {
		to.logMessage(ctx, fileevent.DiscoveredDiscarded, a, "discarding trashed file")
		a.Close()
		return fileevent.DiscoveredDiscarded
	}

	if to.flags.InclusionFlags.DateRange.IsSet() && !to.flags.InclusionFlags.DateRange.InRange(a.CaptureDate) {
		to.logMessage(ctx, fileevent.DiscoveredDiscarded, a, "discarding files out of date range")
		a.Close()
		return fileevent.DiscoveredDiscarded
	}
	if to.flags.ImportFromAlbum != "" {
		keep := false
		dir := path.Dir(a.File.Name())
		if dir == "." {
			dir = ""
		}
		if album, ok := to.albums[dir]; ok {
			keep = keep || album.Title == to.flags.ImportFromAlbum
		}
		if !keep {
			to.logMessage(ctx, fileevent.DiscoveredDiscarded, a, "discarding files not in the specified album")
			a.Close()
			return fileevent.DiscoveredDiscarded
		}
	}
	return fileevent.Code(0)
}
