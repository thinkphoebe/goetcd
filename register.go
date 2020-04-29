package goetcd

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	log "github.com/thinkphoebe/golog"
)

type RegisterInfo struct {
	Name      string
	Endpoints []string
	Timeout   time.Duration
	Key       string
	Value     string
	TTL       int64

	chExit  chan interface{}
	etcd    Etcd
	inited  bool
	leaseID clientv3.LeaseID
}

type Register struct {
	Targets []*RegisterInfo
}

func (self *Register) putWithLease(target *RegisterInfo) (clientv3.LeaseID, error) {
	opts := []clientv3.OpOption{}

	resp, err := target.etcd.Client.Grant(context.TODO(), target.TTL)
	if err != nil {
		log.Errorf("[%s] GEtcd.Client.Grant got err [%v]", target.Name, err)
		return 0, err
	}
	opts = append(opts, clientv3.WithLease(resp.ID))

	op := clientv3.OpPut(target.Key, target.Value, opts...)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*target.Timeout)
	_, err = target.etcd.Client.Do(ctx, op)
	if cancel != nil {
		cancel()
	}
	if err != nil {
		log.Errorf("[%s] GEtcd.Client.Do got err [%v]", target.Name, err)
		return 0, err
	}

	return resp.ID, nil
}

func (self *Register) Start() {
	update := func(target *RegisterInfo) {
		if !target.inited {
			log.Infof("[%s] not inited, try init", target.Name)
			err := target.etcd.Init(target.Endpoints, target.Timeout)
			if err != nil {
				log.Errorf("[%s] etcd.Init got err [%s], Endpoints [%#v], DialTimeout [%d]",
					target.Name, err.Error(), target.Endpoints, target.Timeout)
				return
			} else {
				log.Infof("[%s] inited SUCCEED", target.Name)
				target.inited = true
			}
		}
		if target.inited {
			if target.leaseID == 0 {
				log.Infof("[%s] no leaseId, put register info [%s][%s]",
					target.Name, target.Key, target.Value)
				target.leaseID, _ = self.putWithLease(target)
				if target.leaseID != 0 {
					log.Infof("[%s] leaseId:%d", target.Name, target.leaseID)
				}
			} else {
				resp, err := target.etcd.Client.KeepAliveOnce(context.TODO(), target.leaseID)
				if err != nil {
					log.Errorf("[%s] keepalive got error [%v]", target.Name, err)
					target.leaseID = 0
				} else {
					log.Infof("[%s] keepalive ok, TTL:%d", target.Name, resp.TTL)
				}
			}
		}
	}

	run := func(target *RegisterInfo) {
		ticker := time.NewTicker(time.Second * time.Duration(target.TTL) * 3 / 4)
		tickChan := ticker.C
		update(target)
		for {
			select {
			case <-target.chExit:
				log.Infof("[%s] receive exit signal", target.Name)
				if target.inited {
					log.Infof("[%s] inited, remove key and exit etcd", target.Name)
					target.etcd.Del(target.Key, false)
					target.etcd.Exit()
					target.inited = false
					ticker.Stop()
					return
				}
			case <-tickChan:
				update(target)
			}
		}
	}

	for _, target := range self.Targets {
		target.chExit = make(chan interface{}, 0)
		go run(target)
	}
}

func (self *Register) Stop() {
	log.Infof("stop BEGIN")
	for _, target := range self.Targets {
		log.Infof("stop [%s]...", target.Name)
		target.chExit <- struct{}{}
		log.Infof("stop [%s] complete", target.Name)
	}
	log.Infof("stop COMPLETE")
}
