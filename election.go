package goetcd

import (
	"context"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	log "github.com/thinkphoebe/golog"
)

func election(cli *clientv3.Client, id clientv3.LeaseID, timeout int, pfx string) (s *concurrency.Session, err error) {
	s, err = concurrency.NewSession(cli, concurrency.WithLease(id), concurrency.WithTTL(timeout))
	if err != nil {
		return nil, err
	}

	e := concurrency.NewElection(s, pfx)
	if err = e.Campaign(context.TODO(), ""); err != nil {
		log.Errorf("[election][SLA] campaign got error [%v]", err)
	} else {
		log.Infof("[election][SLA] campaign succeed")
	}

	return s, err
}

//每次Campaign成功后，将LeaseID写入文件，下次启动时先尝试使用记录值。在master退出又快速启动的情况下，可在启动后立即成为master提供服务。
//否则在旧session未timeout前，任何一方都无法成为master，此期间服务中断。
func DoElection(cli *clientv3.Client, sessionTimeout int, pfx, leaseIDFile string) *concurrency.Session {
	var leaseID clientv3.LeaseID = clientv3.NoLease

	buf, err := ioutil.ReadFile(leaseIDFile)
	if err == nil {
		id, err := strconv.ParseInt(string(buf), 10, 64)
		if err == nil {
			leaseID = clientv3.LeaseID(id)
			log.Infof("[election][SLA] read last LeaseID [%v]", leaseID)
		} else {
			log.Errorf("[election][SLA] parse last LeaseID file [%s] FAILED! [%v]", leaseIDFile, err)
		}
	} else {
		log.Warnf("[election][SLA] read last LeaseID file [%s] error [%v]", leaseIDFile, err)
	}

	for {
		s, err := election(cli, leaseID, sessionTimeout, pfx)
		if err != nil {
			if err.Error() == "etcdserver: requested lease not found" {
				log.Warnf("[election][SLA] server said LeaseID [%v] not found, retry with empty", leaseID)
				leaseID = clientv3.NoLease
				continue
			}
			time.Sleep(500 * time.Millisecond)
		} else {
			newID := strconv.FormatInt(int64(s.Lease()), 10)
			ioutil.WriteFile(leaseIDFile, []byte(newID), 0644)
			log.Infof("[election][SLA] DoElection succeed, new LeaseID [%s]", newID)
			return s
		}
	}
}
