package cache

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/glog"

	"github.com/a-palchikov/poller/poll"
)

type (
	Config struct {
		Dir    string
		DryRun bool
	}

	CacheItem struct {
		Name      string    `json:"filename"`
		Timestamp time.Time `json:"time"`
	}

	// Cache reflects contents of a remote location
	Cache struct {
		config *Config
		Items  []*CacheItem
	}
)

var ErrNoCache = errors.New("directory cache not found")

func New(config *Config) (cache *Cache, err error) {
	cache = &Cache{config: config}
	if err = cache.ensureDirectories(); err != nil {
		return nil, err
	}
	r, err := os.Open(cache.remotesPath())
	if err != nil {
		if os.IsNotExist(err) {
			return cache, ErrNoCache
		}
		return nil, err
	}
	defer r.Close()
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, &cache.Items); err != nil {
		return nil, err
	}
	return cache, nil
}

func (c *Cache) Sync(update *poll.Update) error {
	file := filepath.Base(update.Path)
	newPath := filepath.Join(c.config.Dir, file)

	glog.V(2).Infof("file update: %s -> %s", update.Path, newPath)
	if !c.config.DryRun {
		if err := os.Rename(update.Path, newPath); err != nil {
			return err
		}
	}

	c.update(file, update.Timestamp)
	// FIXME: sync .cache on disk on every notification?
	if err := c.write(); err != nil {
		return err
	}
	return nil
}

func (c *Cache) ensureDirectories() error {
	dir := c.workingDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0777); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) write() error {
	path := c.remotesPath()
	tempPath := path + ".tmp"
	w, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	if data, err := json.Marshal(c.Items); err != nil {
		w.Close()
		return err
	} else {
		w.Write(data)
		w.Close()
		os.Rename(tempPath, path)
		return nil
	}
}

func (c *Cache) workingDir() string {
	return filepath.Join(c.config.Dir, ".poller")
}

func (c *Cache) remotesPath() string {
	return filepath.Join(c.workingDir(), "remote")
}

func (c *Cache) update(file string, timestamp time.Time) {
	updated := false
	for _, item := range c.Items {
		if item.Name == file {
			item.Timestamp = timestamp
			updated = true
			break
		}
	}
	if !updated {
		c.Items = append(c.Items, &CacheItem{Name: file, Timestamp: timestamp})
	}
}
