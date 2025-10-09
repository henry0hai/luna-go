package luna

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"time"
)

func init() {
	sql.Register("luna", Driver{})
}

type Driver struct{}

// Implements the driver.Driver interface.
func (d Driver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}

	return c.Connect(context.Background())
}

// Implements the driver.DriverContext interface.
func (Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return NewConnector(dsn, func(execerContext driver.ExecerContext) error {
		return nil
	})
}

type Connector struct {
	u *url.URL
	// Callback to perform additional initialization steps.
	connInitFn func(execer driver.ExecerContext) error
	// True, if the connector has been closed, else false.
	closed bool
}

// Implements the driver.Connector interface.
func (*Connector) Driver() driver.Driver { return Driver{} }

// Implements the driver.Connector interface.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	slog.Info("host:", "v", c.u.Host)
	var nc net.Conn
	var err error
	deadline, ok := ctx.Deadline()
	if ok {
		nc, err = net.DialTimeout("tcp", c.u.Host, time.Until(deadline))
		if err != nil {
			return nil, err
		}
	} else {
		nc, err = net.Dial("tcp", c.u.Host)
		if err != nil {
			return nil, err
		}
	}

	conn := &Conn{conn: nc}

	if c.connInitFn != nil {
		if err := c.connInitFn(conn); err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func (c *Connector) Close() error {
	if c.closed {
		return nil
	}

	c.closed = true
	return nil
}

// The user must close the Connector, if it is not passed to the sql.OpenDB function.
// Otherwise, sql.DB closes the Connector when calling sql.DB.Close().
func NewConnector(dsn string, connInitFn func(execer driver.ExecerContext) error) (*Connector, error) {
	fdsn := dsn
	if !strings.Contains(fdsn, "://") {
		fdsn = "http://" + fdsn
	}

	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	return &Connector{
		u:          parsedDSN,
		connInitFn: connInitFn,
	}, nil
}
