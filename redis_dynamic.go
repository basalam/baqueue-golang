package baqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/redis/go-redis/v9"
)

var _ DynamicQueueDriver = (*RedisDriver)(nil)

func (d *RedisDriver) dynamicKey(name string) string {
	return d.key("dynamic_queue", name)
}

func (d *RedisDriver) dynamicKeys(name string) []string {
	return []string{d.dynamicKey(name), d.index(name, ""), d.index(name, Pending),
		d.index(name, Processing), d.key("queue", name)}
}

func (d *RedisDriver) readDynamicQueue(ctx context.Context, client redis.Cmdable, name string) (*DynamicQueueInfo, error) {
	raw, err := client.HGet(ctx, d.dynamicKey(name), "data").Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var info DynamicQueueInfo
	if err = json.Unmarshal([]byte(raw), &info); err != nil {
		return nil, fmt.Errorf("baqueue: invalid dynamic queue %q: %w", name, err)
	}
	if info.Name != name || validateDynamicQueue(info.Name, info.IdleTimeoutSeconds) != nil ||
		math.IsNaN(info.CreatedAt) || math.IsInf(info.CreatedAt, 0) ||
		(info.IdleSince != nil && (math.IsNaN(*info.IdleSince) || math.IsInf(*info.IdleSince, 0))) {
		return nil, fmt.Errorf("baqueue: invalid dynamic queue policy %q", name)
	}
	return &info, nil
}

func (d *RedisDriver) storeDynamicQueue(plan *redisPlan, info *DynamicQueueInfo) error {
	raw, err := json.Marshal(info)
	if err != nil {
		return err
	}
	plan.add("HSET", d.dynamicKey(info.Name), "data", string(raw))
	plan.add("SADD", d.key("dynamic_queues"), info.Name)
	command := "SREM"
	if info.Active {
		command = "SADD"
	}
	plan.add(command, d.key("queues"), info.Name)
	return nil
}

func (d *RedisDriver) saveQueueMembership(ctx context.Context, tx *redis.Tx, plan *redisPlan, name string, busy bool) error {
	if plan.types[d.dynamicKey(name)] == "hash" {
		return nil
	}
	if err := tx.Watch(ctx, d.dynamicKey(name)).Err(); err != nil {
		return err
	}
	info, err := d.readDynamicQueue(ctx, tx, name)
	if err != nil {
		return err
	}
	if info != nil && busy {
		info.Active, info.IdleSince = true, nil
		return d.storeDynamicQueue(plan, info)
	}
	command := "SADD"
	if info != nil && !info.Active {
		command = "SREM"
	}
	plan.add(command, d.key("queues"), name)
	return nil
}

// The caller WATCHes dynamicKeys before reading these indexed counts.
func (d *RedisDriver) dynamicState(ctx context.Context, tx *redis.Tx, name string) (*DynamicQueueInfo, int64, bool, error) {
	info, err := d.readDynamicQueue(ctx, tx, name)
	if err != nil {
		return nil, 0, false, err
	}
	var retained, pending, processing *redis.IntCmd
	_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		retained = pipe.ZCard(ctx, d.index(name, ""))
		pending = pipe.ZCard(ctx, d.index(name, Pending))
		processing = pipe.ZCard(ctx, d.index(name, Processing))
		return nil
	})
	if err != nil {
		return nil, 0, false, err
	}
	return info, retained.Val(), pending.Val() != 0 || processing.Val() != 0, nil
}

func (d *RedisDriver) registeredDynamicQueue(ctx context.Context, tx *redis.Tx, name string, timeout float64) (*DynamicQueueInfo, error) {
	info, retained, busy, err := d.dynamicState(ctx, tx, name)
	if err != nil {
		return nil, err
	}
	now := Now()
	if info == nil {
		if retained != 0 || busy {
			return nil, fmt.Errorf("baqueue: queue %q already contains ordinary jobs", name)
		}
		created := newDynamicQueue(name, timeout, now)
		info = &created
	} else if info.IdleTimeoutSeconds != timeout {
		return nil, fmt.Errorf("baqueue: queue %q already has a different idle timeout", name)
	}
	info.Active, info.IdleSince = true, &now
	if busy {
		info.IdleSince = nil
	}
	return info, nil
}

func (d *RedisDriver) RegisterDynamicQueue(ctx context.Context, name string, timeout float64) (DynamicQueueInfo, error) {
	if err := validateDynamicQueue(name, timeout); err != nil {
		return DynamicQueueInfo{}, err
	}
	var result DynamicQueueInfo
	err := d.watch(ctx, d.dynamicKeys(name), func(tx *redis.Tx) error {
		info, err := d.registeredDynamicQueue(ctx, tx, name, timeout)
		if err != nil {
			return err
		}
		plan := &redisPlan{}
		if err = d.storeDynamicQueue(plan, info); err != nil {
			return err
		}
		if err = plan.commit(ctx, tx); err == nil {
			result = *info
		}
		return err
	})
	if err != nil {
		return DynamicQueueInfo{}, err
	}
	return result, nil
}

func (d *RedisDriver) GetDynamicQueues(ctx context.Context) ([]DynamicQueueInfo, error) {
	names, err := d.client().SMembers(ctx, d.key("dynamic_queues")).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	infos := make([]DynamicQueueInfo, 0, len(names))
	for _, name := range names {
		info, err := d.readDynamicQueue(ctx, d.client(), name)
		if err != nil {
			return nil, err
		}
		if info != nil && info.Active {
			infos = append(infos, *info)
		}
	}
	return infos, nil
}

func (d *RedisDriver) ExpireDynamicQueues(ctx context.Context) ([]string, error) {
	names, err := d.client().SMembers(ctx, d.key("dynamic_queues")).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	expired := make([]string, 0)
	for _, name := range names {
		newlyExpired := false
		err := d.watch(ctx, d.dynamicKeys(name), func(tx *redis.Tx) error {
			newlyExpired = false
			info, retained, busy, err := d.dynamicState(ctx, tx, name)
			if err != nil || info == nil {
				return err
			}
			changed := false
			now := Now()
			if busy {
				changed = !info.Active || info.IdleSince != nil
				info.Active, info.IdleSince = true, nil
			} else if info.Active {
				if info.IdleSince == nil {
					info.IdleSince, changed = &now, true
				} else if now >= *info.IdleSince+info.IdleTimeoutSeconds {
					info.Active, changed, newlyExpired = false, true, true
				}
			}
			plan := &redisPlan{}
			if !info.Active {
				n, err := tx.Exists(ctx, d.key("queue", name)).Result()
				if err != nil {
					return err
				}
				if n != 0 {
					plan.add("DEL", d.key("queue", name))
				}
			}
			if !info.Active && retained == 0 {
				plan.add("DEL", d.dynamicKey(name))
				plan.add("SREM", d.key("dynamic_queues"), name)
				plan.add("SREM", d.key("queues"), name)
			} else if changed {
				if err = d.storeDynamicQueue(plan, info); err != nil {
					return err
				}
			}
			return plan.commit(ctx, tx)
		})
		if err != nil {
			return expired, err
		}
		if newlyExpired {
			expired = append(expired, name)
		}
	}
	return expired, nil
}

func (d *RedisDriver) PushDynamic(ctx context.Context, payloads []*JobPayload, name string, timeout float64) ([]string, error) {
	if err := validateDynamicQueue(name, timeout); err != nil {
		return nil, err
	}
	for _, payload := range payloads {
		if payload == nil || payload.Queue != name {
			return nil, fmt.Errorf("baqueue: dynamic payloads must use queue %q", name)
		}
	}
	return d.pushMany(ctx, payloads, &DynamicQueueInfo{Name: name, IdleTimeoutSeconds: timeout})
}

func (d *RedisDriver) allQueueNames(ctx context.Context) ([]string, error) {
	names, err := d.client().SUnion(ctx, d.key("queues"), d.key("dynamic_queues")).Result()
	sort.Strings(names)
	return names, err
}
