package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/a-palchikov/poller/cache"
	"github.com/a-palchikov/poller/poll"

	"github.com/golang/glog"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/net/context"
)

var (
	dir           = flag.String("dir", "", "path for synced files")
	tempDir       = flag.String("temp", "", "path for temporary storage (defaults to tmp in `dir`)")
	remoteDir     = flag.String("remote-dir", "", "path on server to sync")
	dryRun        = flag.Bool("dryrun", false, "display actions w/o execution")
	clientVersion = flag.String("client", "", "override SSH client identification")
	user          = flag.String("usr", "", "auth: user name")
	password      = flag.String("pwd", "", "auth: password")
	addr          = flag.String("addr", "", "server to connect to as host:port")
)

func main() {
	flag.Parse()

	if *dir == "" {
		flag.PrintDefaults()
		log.Fatalln("`dir` is mandatory")
	}

	if *tempDir == "" {
		*tempDir = filepath.Join(*dir, "tmp")
	}

	ensureDirectory(*dir)
	ensureDirectory(*tempDir)

	fcache, err := cache.New(&cache.Config{
		Dir:    *dir,
		DryRun: *dryRun,
	})
	if err != nil && err != cache.ErrNoCache {
		glog.Errorf(`cannot read cache from "%s": %s`, *dir, err)
	}

	var auths []ssh.AuthMethod
	if conn, err := net.Dial("", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		auths = append(auths, ssh.PublicKeysCallback(agent.NewClient(conn).Signers))
	}
	auths = append(auths, ssh.Password(*password))

	sshConfig := &ssh.ClientConfig{
		User: *user,
		Auth: auths,
		Config: ssh.Config{
			Ciphers: []string{"aes128-ctr", "aes192-ctr", "aes256-ctr",
				"arcfour256", "arcfour128", "aes128-cbc", "3des-cbc"},
		},
	}
	if *clientVersion != "" {
		sshConfig.ClientVersion = *clientVersion
	}

	snapshot := toSnapshot(fcache)
	ctx, cancel := context.WithCancel(context.Background())
	pollConfig := &poll.Config{
		TempDir:  *tempDir,
		DryRun:   *dryRun,
		HostPort: *addr,
		Ssh:      sshConfig,
	}
	poller, err := poll.New(snapshot, ctx, pollConfig, *remoteDir)
	if err != nil {
		glog.Fatalf("cannot start polling: %s", err)
	}
	defer poller.Close()

	var interrupts byte
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var interruptTimeout <-chan time.Time

L:
	for {
		select {
		case change, ok := <-poller.Updates():
			if !ok {
				glog.Warningln("poller unexpected exit")
				break L
			}
			fcache.Sync(change)
		case <-interrupt:
			interrupts += 1
			if interrupts > 1 {
				cancel()
				break L
			}
			fmt.Println("Dumping goroutine stacks. Press Ctrl-C again to quit.")
			pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			interruptTimeout = time.After(2 * time.Second)
		case <-interruptTimeout:
			interruptTimeout = nil
			interrupts = 0
		}
	}

	close(interrupt)
	glog.Infoln("bye-bye")
}

func toSnapshot(c *cache.Cache) map[string]time.Time {
	result := make(map[string]time.Time)

	if c != nil {
		for _, item := range c.Items {
			result[item.Name] = item.Timestamp
		}
	}
	return result
}

func ensureDirectory(path string) {
	if err := os.MkdirAll(path, 0777); err != nil {
		log.Fatalf(`Cannot create path "%s": %s\n`, path, err)
	}
}
