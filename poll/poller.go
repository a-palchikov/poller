package poll

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/net/context"

	"github.com/golang/glog"
	"github.com/pkg/sftp"
)

type (
	Config struct {
		// Directory for downloaded files
		TempDir string

		// True to only show commands w/o executing them
		DryRun bool

		// URL:port of the sftp server to poll
		HostPort string

		// ssh configuration
		Ssh *ssh.ClientConfig
	}

	Poller interface {
		io.Closer
		Updates() <-chan *Update
	}

	// Update represents either a poller downloaded artefact,
	// or describes a remote file entry
	Update struct {
		Path      string
		Timestamp time.Time
	}

	localUpdates  []*Update
	updatesByName struct {
		localUpdates
	}

	poller struct {
		// Remote path to poll/fetch from
		root string
		// Channel poller sends updates on
		updatesChan chan *Update
		config      *Config
		client      *sftp.Client
		conn        *ssh.Client
		// State poller maintains
		snapshot map[string]time.Time
		resumes  map[string]int64
	}
)

func New(snapshot map[string]time.Time, ctx context.Context, config *Config, remoteRoot string) (Poller, error) {
	sshConn, err := ssh.Dial("tcp", config.HostPort, config.Ssh)
	if err != nil {
		glog.Fatalf("cannot ssh.Dial: %s", err)
		return nil, err
	}

	client, err := sftp.NewClient(sshConn)
	if err != nil {
		sshConn.Close()
		glog.Fatalf("cannot create sftp client: %s", err)
		return nil, err
	}

	p := &poller{
		root:        remoteRoot,
		config:      config,
		conn:        sshConn,
		client:      client,
		updatesChan: make(chan *Update),
		snapshot:    snapshot,
		resumes:     make(map[string]int64),
	}

	go p.loop(ctx)
	glog.V(2).Infoln("poller: started")

	return p, nil
}

func (p *poller) Close() error {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
	if p.client != nil {
		p.client.Close()
		p.client = nil
	}
	return nil
}

func (p *poller) Updates() <-chan *Update {
	return p.updatesChan
}

func (p *poller) loop(ctx context.Context) {
	const POLL_TIMEOUT = time.Minute * 5
	var pollTimeout = POLL_TIMEOUT

	if err := p.fetch(); err != nil {
		glog.Errorf("poller error: %v", err)
		pollTimeout = 1
	}

L:
	for {
		select {
		case <-time.After(pollTimeout * time.Second):
			pollTimeout = POLL_TIMEOUT // reset
			for {
				if err := p.fetch(); err != nil {
					if isErrorEOF(err) {
						if err = p.reconnect(); err != nil {
							glog.Errorf("poller reconnect error: %v", err)
							break L
						} else {
							// TODO: use backoff for reconnects
							time.Sleep(1e9)
							continue
						}
					} else {
						glog.Errorf("poller error: %v", err)
					}
				}
				break
			}
		case <-ctx.Done():
			glog.V(2).Infoln("poller: cancelled")
			break L
		}
	}

	glog.V(2).Infoln("poller: exit loop")
	close(p.updatesChan)
}

func (p *poller) reconnect() error {
	glog.V(2).Infoln("poller: reconnect")

	p.conn.Close()
	p.conn = nil
	p.client.Close()
	p.client = nil

	conn, err := ssh.Dial("tcp", p.config.HostPort, p.config.Ssh)
	if err != nil {
		glog.Fatalf("cannot ssh.Dial: %s", err)
		return err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		return err
	}
	p.conn = conn
	p.client = client
	return nil
}

func (p *poller) fetch() error {
	entries, err := p.client.ReadDir(p.root)
	if err != nil {
		return err
	}

	var updates localUpdates
	for _, entry := range entries {
		if !requiredFileMask(entry.Name()) {
			continue
		}
		timestamp, ok := p.snapshot[entry.Name()]
		if !ok || timestamp.Before(entry.ModTime()) {
			glog.Infof(`poller: update for "%s"`, entry.Name())
			// p.snapshot[entry.Name()] = entry.ModTime()
			updates = append(updates, &Update{
				Path:      entry.Name(),
				Timestamp: entry.ModTime(),
			})
		}
	}

	if len(updates) > 0 {
		glog.V(2).Infof("poller: %d updates", len(updates))
		sort.Sort(updatesByName{updates})
		for _, update := range updates {
			updatePath := p.client.Join(p.root, update.Path)
			var notification *Update
			notification, err = p.download(updatePath, update.Timestamp)
			if err != nil {
				glog.Errorf(`poller: cannot download "%s" (giving up): %v`, updatePath, err)
				break
			}
			p.updatesChan <- notification
			p.snapshot[update.Path] = update.Timestamp
		}
	} else {
		glog.Infoln("poller: nothing to do")
	}

	return err
}

func (p *poller) download(path string, time time.Time) (*Update, error) {
	file, err := p.client.Open(path)
	if err != nil {
		return nil, err
	}

	fileName := filepath.Base(path)
	localPath := filepath.Join(p.config.TempDir, fileName)

	if !p.config.DryRun {
		var fileOut *os.File
		var err error

		// FIXME: make sure that if the file has been completely downloaded
		// its p.resumes entry gets removed
		if offset, ok := p.resumes[localPath]; ok {
			fileOut, err = os.OpenFile(localPath, os.O_WRONLY|os.O_APPEND, 0600)

			if err == nil {
				glog.Infof("restoring a resume point at %d for %s", offset, localPath)
				if n, err := file.Seek(offset, os.SEEK_SET); err != nil {
					glog.Errorf("cannot seek(%d) at %d: %v", offset, n, err)
				}
			}
		} else {
			fileOut, err = os.Create(localPath)
		}

		if err != nil {
			return nil, err
		}

		defer fileOut.Close()

		if n, err := io.Copy(fileOut, file); err != nil {
			// TODO: note the offset of incomplete Copy, and resume after reconnect
			glog.Infof("%s: saving a resume point for %d bytes", localPath, n)
			p.resumes[localPath] += n
			return nil, err
		} else {
			delete(p.resumes, localPath)
		}
	}

	return &Update{Path: localPath, Timestamp: time}, nil
}

func requiredFileMask(name string) bool {
	return strings.HasSuffix(name, ".xml")
}

func isErrorEOF(err error) bool {
	return err == io.EOF || err == io.ErrUnexpectedEOF
}

func (u updatesByName) Len() int { return len(u.localUpdates) }
func (u updatesByName) Swap(i, j int) {
	u.localUpdates[i], u.localUpdates[j] = u.localUpdates[j], u.localUpdates[i]
}
func (u updatesByName) Less(i, j int) bool { return u.localUpdates[i].Path < u.localUpdates[j].Path }

func (u localUpdates) String() string {
	result := make([]string, len(u))

	for i, update := range u {
		result[i] = update.Path
	}
	return strings.Join(result, ",")
}
