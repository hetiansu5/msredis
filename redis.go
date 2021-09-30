package msredis

import (
	"context"
	"github.com/gogf/gf/container/gvar"
	"github.com/gogf/gf/database/gredis"
	"github.com/gogf/gf/frame/gins"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var (
	readOps       map[string]bool
	redisGroupMap map[string]*RedisGroup
	mutex         sync.RWMutex
)

func init() {
	ops := []string{
		"get", "getbit", "exists", "mget", "hget", "hlen", "hkeys", "hvals", "hgetall", "hexists",
		"hmget", "lindex", "lget", "llen", "lsize", "lrange", "lgetrange", "scard", "ssize", "sdiff", "sinter",
		"sismember", "scontains", "smembers", "sgetmembers", "srandmember", "sunion", "zcard", "zsize",
		"zcount", "zrange", "zrangebyscore", "zrevrangebyscore", "zrangebylex", "zrank", "zrevrank", "zrevrange",
		"zscore", "zunion",
	}
	readOps = make(map[string]bool)
	for _, op := range ops {
		readOps[op] = true
	}
	redisGroupMap = make(map[string]*RedisGroup)
}

type RedisGroup struct {
	ctx        context.Context
	masterName string
	slaveNames []string
	slaveCount int
}

func Group(name ...string) *RedisGroup {
	var key string
	if len(name) > 0 {
		key = strings.Join(name, ":")
	} else {
		key = gredis.DefaultGroupName
	}
	mutex.RLock()
	obj, ok := redisGroupMap[key]
	mutex.RUnlock()
	if !ok {
		obj = New(name...)
		mutex.Lock()
		defer mutex.Unlock()
		redisGroupMap[key] = obj
	}
	return obj
}

func New(name ...string) *RedisGroup {
	masterName := gredis.DefaultGroupName
	var slaveNames []string
	if len(name) > 0 {
		if name[0] != "" {
			masterName = name[0]
		}
		if len(name) > 1 {
			slaveNames = append(slaveNames, name[1:]...)
		}
	}
	rGroup := &RedisGroup{
		masterName: masterName,
		slaveNames: slaveNames,
		slaveCount: len(slaveNames),
	}
	return rGroup
}

func (r *RedisGroup) Master() *gredis.Redis {
	return r.redis(r.masterName)
}

func (r *RedisGroup) Slave() *gredis.Redis {
	if r.slaveCount <= 0 {
		return r.Master()
	} else if r.slaveCount == 1 {
		return r.redis(r.slaveNames[0])
	}
	name := r.slaveNames[rand.Intn(r.slaveCount-1)]
	return r.redis(name)
}

func (r *RedisGroup) redis(name string) *gredis.Redis {
	return gins.Redis(name)
}

func (r *RedisGroup) isReadOp(commandName string) bool {
	cm := strings.ToLower(commandName)
	return readOps[cm] == true
}

func (r *RedisGroup) autoSelect(commandName string) *gredis.Redis {
	var rs *gredis.Redis
	if r.isReadOp(commandName) {
		rs = r.Slave()
	} else {
		rs = r.Master()
	}
	if r.ctx != nil {
		rs.Ctx(r.ctx)
	}
	return rs
}

// Clone clones and returns a new Redis object, which is a shallow copy of current one.
func (r *RedisGroup) Clone() *RedisGroup {
	newRedisGroup := &RedisGroup{}
	*newRedisGroup = *r
	return newRedisGroup
}

// Ctx is a channing function which sets the context for next operation.
func (r *RedisGroup) Ctx(ctx context.Context) *RedisGroup {
	newRedis := r.Clone()
	newRedis.ctx = ctx
	return newRedis
}

// Do sends a command to the server and returns the received reply.
// Do automatically get a connection from pool, and close it when the reply received.
// It does not really "close" the connection, but drops it back to the connection pool.
func (r *RedisGroup) Do(commandName string, args ...interface{}) (interface{}, error) {
	return r.autoSelect(commandName).Do(commandName, args...)
}

// DoWithTimeout sends a command to the server and returns the received reply.
// The timeout overrides the read timeout set when dialing the connection.
func (r *RedisGroup) DoWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (interface{}, error) {
	return r.autoSelect(commandName).DoWithTimeout(timeout, commandName, args...)
}

// DoVar returns value from Do as gvar.Var.
func (r *RedisGroup) DoVar(commandName string, args ...interface{}) (*gvar.Var, error) {
	return r.autoSelect(commandName).DoVar(commandName, args...)
}

// DoVarWithTimeout returns value from Do as gvar.Var.
// The timeout overrides the read timeout set when dialing the connection.
func (r *RedisGroup) DoVarWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (*gvar.Var, error) {
	return r.autoSelect(commandName).DoVarWithTimeout(timeout, commandName, args...)
}
