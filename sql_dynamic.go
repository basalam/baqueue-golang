package baqueue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

var _ DynamicQueueDriver = (*SQLDriver)(nil)

const sqlDynamicColumns = "name,idle_timeout_seconds,created_at,idle_since,active"

func sqlScanDynamicQueue(row interface{ Scan(...any) error }) (DynamicQueueInfo, error) {
	var info DynamicQueueInfo
	var idle sql.NullFloat64
	err := row.Scan(&info.Name, &info.IdleTimeoutSeconds, &info.CreatedAt, &idle, &info.Active)
	if idle.Valid {
		info.IdleSince = &idle.Float64
	}
	return info, err
}

func sqlReadNames(rows *sql.Rows) ([]string, error) {
	defer rows.Close()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

func (d *SQLDriver) sqlLockDynamicQueues(ctx context.Context, tx *sql.Tx, names []string) error {
	if !d.postgres {
		return nil // SQLite's BEGIN IMMEDIATE already fences all writers.
	}
	// Lock order: registry table, sorted queue names, policy rows, job rows.
	// The table lock also serializes full flush against newly created names.
	if _, err := tx.ExecContext(ctx, "LOCK TABLE baqueue_go_dynamic_queues IN ROW EXCLUSIVE MODE"); err != nil {
		return err
	}
	names = append([]string(nil), names...)
	sort.Strings(names)
	for i, name := range names {
		if i > 0 && name == names[i-1] {
			continue
		}
		hash := sha256.Sum256([]byte("baqueue_go_dynamic_queues\x00" + name))
		key := int64(binary.BigEndian.Uint64(hash[:8]))
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", key); err != nil {
			return err
		}
		var locked string
		err := tx.QueryRowContext(ctx, "SELECT name FROM baqueue_go_dynamic_queues WHERE name=$1 FOR UPDATE", name).Scan(&locked)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	return nil
}

func sqlTouchDynamicQueue(ctx context.Context, tx *sql.Tx, name string) error {
	_, err := tx.ExecContext(ctx, "UPDATE baqueue_go_dynamic_queues SET active=TRUE,idle_since=NULL WHERE name=$1", name)
	return err
}

func (d *SQLDriver) sqlRegisterDynamicQueue(ctx context.Context, tx *sql.Tx, name string, timeout float64) (DynamicQueueInfo, error) {
	info, err := sqlScanDynamicQueue(tx.QueryRowContext(ctx,
		"SELECT "+sqlDynamicColumns+" FROM baqueue_go_dynamic_queues WHERE name=$1"+d.sqlLock(false), name))
	if errors.Is(err, sql.ErrNoRows) {
		var retained bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM baqueue_go_jobs WHERE queue=$1)", name).Scan(&retained); err != nil {
			return DynamicQueueInfo{}, err
		}
		if retained {
			return DynamicQueueInfo{}, fmt.Errorf("baqueue: queue %q already contains ordinary jobs", name)
		}
		info = newDynamicQueue(name, timeout, Now())
		_, err = tx.ExecContext(ctx, "INSERT INTO baqueue_go_dynamic_queues ("+sqlDynamicColumns+") VALUES($1,$2,$3,$4,TRUE)",
			name, timeout, info.CreatedAt, info.IdleSince)
		return info, err
	}
	if err != nil {
		return DynamicQueueInfo{}, err
	}
	if info.IdleTimeoutSeconds != timeout {
		return DynamicQueueInfo{}, fmt.Errorf("baqueue: queue %q already has a different idle timeout", name)
	}
	var busy bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM baqueue_go_jobs WHERE queue=$1 AND status IN ('pending','processing'))", name).Scan(&busy); err != nil {
		return DynamicQueueInfo{}, err
	}
	now := Now()
	info.Active, info.IdleSince = true, &now
	if busy {
		info.IdleSince = nil
	}
	_, err = tx.ExecContext(ctx, "UPDATE baqueue_go_dynamic_queues SET active=TRUE,idle_since=$2 WHERE name=$1", name, info.IdleSince)
	return info, err
}

func (d *SQLDriver) RegisterDynamicQueue(ctx context.Context, name string, timeout float64) (DynamicQueueInfo, error) {
	if err := validateDynamicQueue(name, timeout); err != nil {
		return DynamicQueueInfo{}, err
	}
	var info DynamicQueueInfo
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		if err := d.sqlLockDynamicQueues(ctx, tx, []string{name}); err != nil {
			return err
		}
		var err error
		info, err = d.sqlRegisterDynamicQueue(ctx, tx, name, timeout)
		return err
	})
	if err != nil {
		return DynamicQueueInfo{}, err
	}
	return info, nil
}

func (d *SQLDriver) GetDynamicQueues(ctx context.Context) ([]DynamicQueueInfo, error) {
	db, err := d.sqlDB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, "SELECT "+sqlDynamicColumns+" FROM baqueue_go_dynamic_queues WHERE active=TRUE ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	infos := make([]DynamicQueueInfo, 0)
	for rows.Next() {
		info, err := sqlScanDynamicQueue(rows)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, rows.Err()
}

func (d *SQLDriver) ExpireDynamicQueues(ctx context.Context) ([]string, error) {
	expired := make([]string, 0)
	err := d.sqlWrite(ctx, func(tx *sql.Tx) error {
		expired = expired[:0]
		if err := d.sqlLockDynamicQueues(ctx, tx, nil); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, "SELECT name FROM baqueue_go_dynamic_queues ORDER BY name")
		if err != nil {
			return err
		}
		names, err := sqlReadNames(rows)
		if err != nil {
			return err
		}
		sort.Strings(names)
		if err = d.sqlLockDynamicQueues(ctx, tx, names); err != nil {
			return err
		}
		now := Now()
		for _, name := range names {
			info, err := sqlScanDynamicQueue(tx.QueryRowContext(ctx,
				"SELECT "+sqlDynamicColumns+" FROM baqueue_go_dynamic_queues WHERE name=$1"+d.sqlLock(false), name))
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			var busy bool
			if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM baqueue_go_jobs WHERE queue=$1 AND status IN ('pending','processing'))", name).Scan(&busy); err != nil {
				return err
			}
			if busy {
				if !info.Active || info.IdleSince != nil {
					if err = sqlTouchDynamicQueue(ctx, tx, name); err != nil {
						return err
					}
				}
				continue
			}
			if info.Active {
				if info.IdleSince == nil {
					if _, err = tx.ExecContext(ctx, "UPDATE baqueue_go_dynamic_queues SET idle_since=$2 WHERE name=$1", name, now); err != nil {
						return err
					}
					continue
				}
				if now < *info.IdleSince+info.IdleTimeoutSeconds {
					continue
				}
				if _, err = tx.ExecContext(ctx, "UPDATE baqueue_go_dynamic_queues SET active=FALSE WHERE name=$1", name); err != nil {
					return err
				}
				expired = append(expired, name)
			}
			if _, err = tx.ExecContext(ctx, `DELETE FROM baqueue_go_dynamic_queues WHERE name=$1
 AND NOT EXISTS(SELECT 1 FROM baqueue_go_jobs WHERE queue=$1)`, name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return expired, nil
}

func (d *SQLDriver) PushDynamic(ctx context.Context, payloads []*JobPayload, name string, timeout float64) ([]string, error) {
	if err := validateDynamicQueue(name, timeout); err != nil {
		return nil, err
	}
	for _, p := range payloads {
		if p == nil || p.Queue != name {
			return nil, errors.New("baqueue: all payload queues must match the dynamic queue name")
		}
	}
	return d.sqlPushMany(ctx, payloads, &name, timeout)
}

// Read the stored queue before taking the job lock; never trust the caller's
// queue and never acquire a queue lock while already holding a job lock.
func (d *SQLDriver) sqlReadLockedJob(ctx context.Context, tx *sql.Tx, id string) (*JobPayload, error) {
	if err := d.sqlLockDynamicQueues(ctx, tx, nil); err != nil {
		return nil, err
	}
	var name string
	err := tx.QueryRowContext(ctx, "SELECT queue FROM baqueue_go_jobs WHERE id=$1", id).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = d.sqlLockDynamicQueues(ctx, tx, []string{name}); err != nil {
		return nil, err
	}
	return sqlScanJob(tx.QueryRowContext(ctx, "SELECT payload FROM baqueue_go_jobs WHERE id=$1 AND queue=$2"+d.sqlLock(false), id, name))
}

func (d *SQLDriver) sqlLockMatchingQueues(ctx context.Context, tx *sql.Tx, where string, args []any) (string, []any, error) {
	if !d.postgres {
		return where, args, nil
	}
	if err := d.sqlLockDynamicQueues(ctx, tx, nil); err != nil {
		return "", nil, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT DISTINCT queue FROM baqueue_go_jobs WHERE "+where, args...)
	if err != nil {
		return "", nil, err
	}
	names, err := sqlReadNames(rows)
	if err != nil {
		return "", nil, err
	}
	if err = d.sqlLockDynamicQueues(ctx, tx, names); err != nil {
		return "", nil, err
	}
	// Restrict the write to the snapshot of locked queues; newly inserted work
	// in other queues can be handled by the next promotion/recovery pass.
	args = append(args, names)
	return where + " AND queue=ANY($" + strconv.Itoa(len(args)) + "::text[])", args, nil
}
