/*
 * Copyright (C) 2015 Alexey Gladkov <gladkov.alexey@gmail.com>
 *
 * This file is covered by the GNU General Public License,
 * which should be included with kafka-http-proxy as the file COPYING.
 */

package main

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

type Pidfile struct {
	*os.File
	opened bool
}

func OpenPidfile(filepath string) (*Pidfile, error) {
	fd, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		fd.Close()
		return nil, fmt.Errorf("can't lock '%s', lock is held", fd.Name())
	}

	return &Pidfile{File: fd, opened: true}, nil
}

func (p *Pidfile) Close() error {
	if !p.opened {
		return nil
	}

	if err := syscall.Flock(int(p.File.Fd()), syscall.LOCK_UN); err != nil {
		return err
	}

	if err := p.File.Close(); err != nil {
		return err
	}

	p.opened = false
	return nil
}

func (p *Pidfile) Read() (int, error) {
	if _, err := p.File.Seek(0, 0); err != nil {
		return 0, err
	}

	var pid int

	n, err := fmt.Fscanf(p.File, "%d", &pid)

	if err == io.EOF {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	if n != 1 {
		return 0, fmt.Errorf("error parsing pid from %s: %s", p.Name, err)
	}

	return pid, nil
}

func (p *Pidfile) Check() error {
	pid, err := p.Read()
	if err != nil {
		return err
	}

	if pid == 0 || pid == os.Getpid() {
		return nil
	}

	if err := syscall.Kill(pid, 0); err != nil {
		if err == syscall.ESRCH {
			return nil
		}
		return err
	}

	return fmt.Errorf("process is already running")
}

func (p *Pidfile) Write() error {
	if _, err := p.File.Seek(0, 0); err != nil {
		return err
	}

	if err := p.File.Truncate(0); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(p.File, "%d", os.Getpid()); err != nil {
		return err
	}

	if err := p.File.Sync(); err != nil {
		return err
	}

	return nil
}

func (p *Pidfile) Remove() error {
	if err := os.Remove(p.File.Name()); err != nil {
		return err
	}
	return nil
}
