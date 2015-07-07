/*
* Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
*
* This file is covered by the GNU General Public License,
* which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"fmt"
	"os"
	"sync"
)

// Logfile represents an active logging object.
type Logfile struct {
	sync.RWMutex

	fd     *os.File
	opened bool

	Name string
}

// OpenLogfile opens new logfile
func OpenLogfile(name string) (*Logfile, error) {
	logfile := &Logfile{
		Name: name,
	}

	if err := logfile.Reopen(); err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	return logfile, nil
}

func (f *Logfile) closeLog() error {
	if !f.opened {
		return fmt.Errorf("already closed")
	}

	if err := f.fd.Close(); err != nil {
		return err
	}

	f.opened = false
	return nil
}

func (f *Logfile) openLog() error {
	if f.opened {
		return fmt.Errorf("already opened")
	}

	var err error

	f.fd, err = os.OpenFile(f.Name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}

	f.opened = true
	return nil
}

// Close closes logfile
func (f *Logfile) Close() error {
	f.Lock()
	defer f.Unlock()

	return f.closeLog()
}

// Reopen closes the log file and opens it again.
func (f *Logfile) Reopen() error {
	f.Lock()
	defer f.Unlock()

	if f.opened {
		if err := f.closeLog(); err != nil {
			return err
		}
	}

	if err := f.openLog(); err != nil {
		return err
	}

	return nil
}

// Write writes len(p) bytes from p to the underlying data stream.
func (f *Logfile) Write(p []byte) (int, error) {
	f.Lock()
	defer f.Unlock()

	if !f.opened {
		return 0, fmt.Errorf("logfile closed")
	}

	return f.fd.Write(p)
}
