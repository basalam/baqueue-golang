package baqueue

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func optionNumber(name string, value any) (float64, error) {
	n, err := strconv.ParseFloat(fmt.Sprint(value), 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
		return 0, fmt.Errorf("baqueue: invalid numeric driver option %s", name)
	}
	return n, nil
}

func configureDriverOptions(driver Driver, options map[string]any) error {
	for key, value := range options {
		switch d := driver.(type) {
		case *RedisDriver:
			switch key {
			case "username", "password":
				s, ok := value.(string)
				if !ok {
					return fmt.Errorf("baqueue: %s must be a string", key)
				}
				if key == "username" {
					d.options.Username = s
				} else {
					d.options.Password = s
				}
			case "db", "max_connections", "pool_size":
				n, err := optionNumber(key, value)
				if err != nil {
					return err
				}
				if n != math.Trunc(n) || n > 1e6 {
					return fmt.Errorf("baqueue: invalid integer option %s", key)
				}
				if key == "db" {
					d.options.DB = int(n)
				} else {
					if n == 0 {
						return fmt.Errorf("baqueue: pool size must be positive")
					}
					d.options.PoolSize = int(n)
				}
			case "socket_timeout", "socket_connect_timeout", "read_timeout", "write_timeout", "pool_timeout":
				n, err := optionNumber(key, value)
				if err != nil || !validSeconds(n, false) {
					return fmt.Errorf("baqueue: invalid timeout option %s", key)
				}
				duration := time.Duration(n * float64(time.Second))
				switch key {
				case "socket_timeout":
					d.options.ReadTimeout = duration
					d.options.WriteTimeout = duration
				case "socket_connect_timeout":
					d.options.DialTimeout = duration
				case "read_timeout":
					d.options.ReadTimeout = duration
				case "write_timeout":
					d.options.WriteTimeout = duration
				case "pool_timeout":
					d.options.PoolTimeout = duration
				}
			default:
				return fmt.Errorf("baqueue: unsupported Redis option %q", key)
			}
		case *SQLDriver:
			if !d.postgres {
				return fmt.Errorf("baqueue: unsupported SQLite option %q; use URI pragma parameters", key)
			}
			switch key {
			case "max_size", "max_open_conns", "min_size", "max_idle_conns":
				n, err := optionNumber(key, value)
				if err != nil {
					return err
				}
				if n < 1 || n != math.Trunc(n) || n > 1e6 {
					return fmt.Errorf("baqueue: invalid pool option %s", key)
				}
				if key == "max_size" || key == "max_open_conns" {
					d.MaxOpenConns = int(n)
				} else {
					d.MaxIdleConns = int(n)
				}
			case "command_timeout":
				n, err := optionNumber(key, value)
				if err != nil || !validSeconds(n, false) || n < .001 {
					return fmt.Errorf("baqueue: invalid command_timeout")
				}
				u, err := url.Parse(d.dsn)
				if err != nil {
					return err
				}
				params := u.Query()
				params.Set("statement_timeout", strconv.FormatInt(int64(n*1000), 10))
				u.RawQuery = params.Encode()
				d.dsn = u.String()
			default:
				return fmt.Errorf("baqueue: unsupported PostgreSQL option %q", key)
			}
		default:
			return fmt.Errorf("baqueue: driver does not accept option %q", key)
		}
	}
	if d, ok := driver.(*RedisDriver); ok && len(options) > 0 {
		_ = d.redis.Close()
		// NewClient normalizes MaxRetries in place; retain -1 for later reconnects.
		clientOptions := *d.options
		d.redis = redis.NewClient(&clientOptions)
	}
	return nil
}
