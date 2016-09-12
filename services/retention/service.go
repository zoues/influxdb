package retention // import "github.com/influxdata/influxdb/services/retention"

import (
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

// Service represents the retention policy enforcement service.
type Service struct {
	MetaClient interface {
		Databases() []meta.DatabaseInfo
		DeleteShardGroup(database, policy string, id uint64) error
	}
	TSDBStore interface {
		ShardIDs() []uint64
		DeleteShard(shardID uint64) error
	}

	enabled       bool
	checkInterval time.Duration

	// Channels only used when building with debug tag.
	forceDeleteShardGroups chan struct{}
	forceDeleteShards      chan struct{}

	wg   sync.WaitGroup
	done chan struct{}

	logger *log.Logger
}

// NewService returns a configured retention policy enforcement service.
func NewService(c Config) *Service {
	return &Service{
		checkInterval: time.Duration(c.CheckInterval),
		done:          make(chan struct{}),
		logger:        log.New(os.Stderr, "[retention] ", log.LstdFlags),

		forceDeleteShardGroups: make(chan struct{}),
		forceDeleteShards:      make(chan struct{}),
	}
}

// Open starts retention policy enforcement.
func (s *Service) Open() error {
	s.logger.Println("Starting retention policy enforcement service with check interval of", s.checkInterval)
	s.wg.Add(2)
	go s.serviceDeleteShardGroups()
	go s.serviceDeleteShards()
	return nil
}

// Close stops retention policy enforcement.
func (s *Service) Close() error {
	s.logger.Println("retention policy enforcement terminating")
	close(s.done)
	s.wg.Wait()
	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (s *Service) SetLogOutput(w io.Writer) {
	s.logger = log.New(w, "[retention] ", log.LstdFlags)
}

func (s *Service) serviceDeleteShardGroups() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.forceDeleteShardGroups:
			s.deleteShardGroups()
		case <-ticker.C:
			s.deleteShardGroups()
		case <-s.done:
			return
		}
	}
}

func (s *Service) deleteShardGroups() {
	dbs := s.MetaClient.Databases()
	for _, d := range dbs {
		for _, r := range d.RetentionPolicies {
			for _, g := range r.ExpiredShardGroups(time.Now().UTC()) {
				if err := s.MetaClient.DeleteShardGroup(d.Name, r.Name, g.ID); err != nil {
					s.logger.Printf("failed to delete shard group %d from database %s, retention policy %s: %s",
						g.ID, d.Name, r.Name, err.Error())
				} else {
					s.logger.Printf("deleted shard group %d from database %s, retention policy %s",
						g.ID, d.Name, r.Name)
				}
			}
		}
	}
}

func (s *Service) serviceDeleteShards() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.forceDeleteShards:
			s.deleteShards()
		case <-ticker.C:
			s.deleteShards()
		case <-s.done:
			return
		}
	}
}

func (s *Service) deleteShards() {
	s.logger.Println("retention policy shard deletion check commencing")

	type deletionInfo struct {
		db string
		rp string
	}
	deletedShardIDs := make(map[uint64]deletionInfo, 0)
	dbs := s.MetaClient.Databases()
	for _, d := range dbs {
		for _, r := range d.RetentionPolicies {
			for _, g := range r.DeletedShardGroups() {
				for _, sh := range g.Shards {
					deletedShardIDs[sh.ID] = deletionInfo{db: d.Name, rp: r.Name}
				}
			}
		}
	}

	for _, id := range s.TSDBStore.ShardIDs() {
		if di, ok := deletedShardIDs[id]; ok {
			if err := s.TSDBStore.DeleteShard(id); err != nil {
				s.logger.Printf("failed to delete shard ID %d from database %s, retention policy %s: %s",
					id, di.db, di.rp, err.Error())
				continue
			}
			s.logger.Printf("shard ID %d from database %s, retention policy %s, deleted",
				id, di.db, di.rp)
		}
	}
}
