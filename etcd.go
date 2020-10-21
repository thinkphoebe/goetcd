package goetcd

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/thinkphoebe/golog"
)

type Etcd struct {
	Client  *clientv3.Client
	timeout time.Duration
}

type EtcdVisitor interface {
	Visit(key string, val []byte) bool
}

func newContexTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.TODO(), nil
	}
	return context.WithTimeout(context.TODO(), timeout*time.Second)
}

func (this *Etcd) Init(endpoints []string, timeout time.Duration) error {
	var err error
	this.Client, err = clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout * time.Second,
	})
	this.timeout = timeout
	if err != nil {
		log.Criticalf("[etcd][SLA] clientv3.New got err [%v]", err)
	}
	return err
}

func (this *Etcd) Exit() {
	this.Client.Close()
}

func (this *Etcd) OpPut(key, val string, ttl int64) (*clientv3.Op, error) {
	opts := []clientv3.OpOption{}
	if ttl > 0 {
		resp, err := this.Client.Grant(context.TODO(), ttl)
		if err != nil {
			log.Errorf("[etcd][SLA] OpPut - cli.Grant got err [%v]", err)
			return nil, err
		}
		opts = append(opts, clientv3.WithLease(resp.ID))
	}
	op := clientv3.OpPut(key, val, opts...)
	return &op, nil
}

func (this *Etcd) Put(key, val string, ttl int64) error {
	op, err := this.OpPut(key, val, ttl)
	if err != nil {
		return err
	}
	ctx, cancel := newContexTimeout(this.timeout)
	_, err = this.Client.Do(ctx, *op)
	if cancel != nil {
		cancel()
	}
	log.Debugf("[etcd] Put - key [%s], val [%s], ttl [%d], err [%v]", key, val, ttl, err)
	return err
}

func (this *Etcd) OpGet(key string, prefix bool) *clientv3.Op {
	opts := []clientv3.OpOption{}
	if prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	op := clientv3.OpGet(key, opts...)
	return &op
}

func (this *Etcd) Get(key string, prefix bool) ([][]byte, error) {
	op := this.OpGet(key, prefix)
	ctx, cancel := newContexTimeout(this.timeout)
	resp, err := this.Client.Do(ctx, *op)
	if cancel != nil {
		cancel()
	}
	if err != nil {
		log.Errorf("[etcd][SLA] Get - key [%s], prefix [%t], err [%v]", key, prefix, err)
		return nil, err
	}

	log.Debugf("[etcd] Get - key [%s], prefix [%t], count [%d]", key, prefix, len(resp.Get().Kvs))
	vals := make([][]byte, len(resp.Get().Kvs))
	for i, kv := range resp.Get().Kvs {
		vals[i] = kv.Value
		log.Debugf("[etcd] Get - [%d] key [%s], val [%s]", i, string(kv.Key), string(kv.Value))
	}
	return vals, nil
}

func (this *Etcd) OpDel(key string, prefix bool) *clientv3.Op {
	opts := []clientv3.OpOption{}
	if prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	op := clientv3.OpDelete(key, opts...)
	return &op
}

func (this *Etcd) Del(key string, prefix bool) (int64, error) {
	op := this.OpDel(key, prefix)
	ctx, cancel := newContexTimeout(this.timeout)
	resp, err := this.Client.Do(ctx, *op)
	if cancel != nil {
		cancel()
	}
	if err == nil {
		log.Debugf("[etcd] Del OK - key [%s], count [%d]", key, resp.Del().Deleted)
		return resp.Del().Deleted, err
	} else {
		log.Errorf("[etcd] Del FAILED - key [%s], err [%v]", key, err)
		return 0, err
	}
}

func (this *Etcd) Count(prefix string) (int64, error) {
	opts := []clientv3.OpOption{}
	opts = append(opts, clientv3.WithPrefix())
	opts = append(opts, clientv3.WithCountOnly())
	ctx, cancel := newContexTimeout(this.timeout)
	out, err := this.Client.Get(ctx, prefix, opts...)
	cancel()
	if err != nil {
		log.Errorf("[etcd][SLA] Count - cli.Get [%s] got error [%v]", prefix, err)
		return 0, err
	}
	return out.Count, nil
}

func (this *Etcd) Version(key string) (int64, int64, int64, error) {
	ctx, cancel := newContexTimeout(this.timeout)
	out, err := this.Client.Get(ctx, key)
	cancel()
	if err != nil {
		log.Errorf("[etcd][SLA] Version - cli.Get [%s] got error [%v]", key, err)
		return 0, 0, 0, err
	}
	if len(out.Kvs) == 0 {
		log.Warnf("[etcd][SLA] Version - cli.Get [%s] got nothing", key, err)
		return 0, 0, 0, err
	}
	return out.Kvs[0].CreateRevision, out.Kvs[0].ModRevision, out.Kvs[0].Version, nil
}

func (this *Etcd) CmpKeyNotExist(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
}

func (this *Etcd) Txn(cmps []clientv3.Cmp, ifs []clientv3.Op, elses []clientv3.Op) (*clientv3.TxnResponse, error) {
	ctx, cancel := newContexTimeout(this.timeout)
	txn := this.Client.Txn(ctx).If(cmps...)
	txn = txn.Then(ifs...)
	if len(elses) > 0 {
		txn = txn.Else(elses...)
	}
	resp, err := txn.Commit()
	cancel()
	return resp, err
}

func (this *Etcd) WatchCallback(key, action string, prefix bool, callback func(key string, val []byte) bool, ctx context.Context) {
	log.Infof("[etcd][SLA] WatchCallback %s - key [%s], prefix [%t]", action, key, prefix)

	et := mvccpb.DELETE
	if action == "PUT" {
		et = mvccpb.PUT
	}

	opts := []clientv3.OpOption{}
	if prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	rch := this.Client.Watch(ctx, key, opts...)

	for wresp := range rch {
		for _, ev := range wresp.Events {
			if ev.Type == et {
				log.Debugf("[etcd][SLA] WatchCallback %s - key [%s], val [%s]",
					action, string(ev.Kv.Key), string(ev.Kv.Value))
				go callback(string(ev.Kv.Key), ev.Kv.Value)
			}
		}
	}
	log.Infof("[etcd][SLA] WatchCallback complete. key:%s, action:%s, prefix:%v", key, action, prefix)
}

func (this *Etcd) WatchVisitor(key, action string, prefix bool, visitor EtcdVisitor, ctx context.Context) {
	this.WatchCallback(key, action, prefix, visitor.Visit, ctx)
}

// limit
// 		walk的最大数量
// pageSize
// 		分页读取，避免匹配prefix的key数量过于庞大时有问题，但不要和sort同时使用
// 		etcd的sort是对匹配key range的key做排序，而分页时会改变key range，无法返回正确的结果
// 调用方可通过在callback返回false终止walk
func (this *Etcd) WalkCallback(prefix string, callback func(key string, val []byte) bool,
		limit, pageSize int64, opts []clientv3.OpOption) error {
	allOpts := []clientv3.OpOption{}
	allOpts = append(allOpts, clientv3.WithPrefix())
	allOpts = append(allOpts, clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)))
	if opts != nil {
		allOpts = append(allOpts, opts...)
	}

	keyStart := prefix
	for {
		thisOpts := allOpts[:]
		if limit > 0 && (limit < pageSize || pageSize == 0) {
			thisOpts = append(thisOpts, clientv3.WithLimit(limit))
		} else if pageSize > 0 {
			thisOpts = append(thisOpts, clientv3.WithLimit(pageSize))
		}

		ctx, cancel := newContexTimeout(this.timeout)
		out, err := this.Client.Get(ctx, keyStart, thisOpts...)
		cancel()
		if err != nil {
			log.Errorf("[etcd][SLA] WalkVisitor - cli.Get [%s] got error [%v]", keyStart, err)
			return err
		}
		if len(out.Kvs) == 0 {
			break
		}

		for _, kv := range out.Kvs {
			log.Debugf("[etcd] WalkVisitor - prefix [%s] keyStart [%s] got k[%s], v[%s]", prefix, keyStart, kv.Key, kv.Value)
			if limit > 0 {
				limit--
			}
			if !callback(string(kv.Key), kv.Value) {
				log.Infof("[etcd][SLA] WalkVisitor - canceled by visitor")
				return nil
			}
		}

		if pageSize <= 0 || limit == 0 {
			break
		}
		keyStart = string(out.Kvs[len(out.Kvs)-1].Key) + "\x00"
	}
	return nil
}

func (this *Etcd) WalkVisitor(prefix string, visitor EtcdVisitor, limit, pageSize int64, opts []clientv3.OpOption) error {
	return this.WalkCallback(prefix, visitor.Visit, limit, pageSize, opts)
}

// 通过写入key并watch写入值的方式监控和etcd的连接是否正常
// 之前有遇到过watch收不到etcd回调的情况但也没返回错误的情况，不确定这样做是否能检测出该情况
func (this *Etcd) StartKeepalive(key string, checkInterval time.Duration, maxFailCount int, callback func()) {
	// write check
	go func() {
		failCount := 0
		for {
			if err := this.Put(key, time.Now().String(), int64(checkInterval)+600); err != nil {
				log.Errorf("write_keepalive_fail, fail_count:%d, max_fail:%d, err:%v", failCount, maxFailCount, err)
				failCount += 1
			} else {
				log.Infof("write_keepalive_succeed, fail_count:%d", failCount)
				failCount = 0
			}
			if failCount >= maxFailCount {
				log.Criticalf("write keepalive failed too many, fail_count:%d, max_fail:%d", failCount, maxFailCount)
				callback()
			}
			time.Sleep(time.Second * checkInterval)
		}
	}()

	// watch check
	go func() {
		ch := make(chan string, 100)
		onKeepAlive := func(key string, val []byte) bool {
			msg := string(val)
			log.Infof("read_keepalive_succeed, now:%d", time.Now().Unix())
			ch <- msg
			return true
		}
		go this.WatchCallback(key, "PUT", true, onKeepAlive, nil)

		tickChan := time.NewTicker(time.Second * checkInterval).C
		lastUpdate := time.Now().Unix()
		for {
			select {
			case <-ch:
				lastUpdate = time.Now().Unix()
			case <-tickChan:
				if time.Now().Unix()-lastUpdate > int64(checkInterval)*int64(maxFailCount) {
					log.Criticalf("read keepalive failed long time, now:%d, last_update:%d, max_fail:%d",
						time.Now().Unix(), lastUpdate, maxFailCount)
					callback()
				} else {
					log.Infof("check_keepalive_succeed, now:%d, last_update:%d, max_fail:%d",
						time.Now().Unix(), lastUpdate, maxFailCount)
				}
			}
		}
	}()
}
